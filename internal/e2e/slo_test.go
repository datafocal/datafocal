package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/api"
	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/compact"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/slo"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// sloDoc is an Occurrences SLO over a day of total's events, good adding good to it.
func sloDoc(name, annotations, total, good, policies string) string {
	return fmt.Sprintf(`apiVersion: openslo/v1
kind: SLO
metadata: {name: %s, annotations: {%s}}
spec:
  service: %s
  budgetingMethod: Occurrences
  timeWindow: [{duration: 1d, isRolling: true}]
  objectives: [{target: 0.99}]
  indicator:
    spec:
      ratioMetric:
        total: {metricSource: {type: DataFocal, spec: {query: '%s'}}}
        good: {metricSource: {type: DataFocal, spec: {query: '%s %s'}}}
  alertPolicies: [%s]
`, name, annotations, name, total, total, good, policies)
}

// burn is an alert policy of one condition: a burn rate of threshold over lookback and short.
func burn(threshold float64, lookback, short string) string {
	return fmt.Sprintf(`{kind: AlertPolicy, metadata: {name: burn}, spec: {conditions: [{kind: AlertCondition,
    metadata: {name: fast, annotations: {datafocal/short-window: %s}}, spec: {severity: page, condition: {kind: burnrate, op: gte, threshold: %v, lookbackWindow: %s}}}]}}`,
		short, threshold, lookback)
}

// definitionsDir is a directory holding each document in a file of its own.
func definitionsDir(t *testing.T, docs ...string) string {
	t.Helper()
	dir := t.TempDir()
	for i, doc := range docs {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d.yaml", i)), []byte(doc), 0o644))
	}
	return dir
}

// The main corpus's SLOs the fixtures read: the gateway's 5xx by endpoint, one bound to
// Prometheus, and, for the views alone, a time-slice SLO of minutes without a 5xx.
var (
	gatewaySLO = sloDoc("gateway", "datafocal/group-by: endpoint", `{service="gateway"}`, "| status < 500", burn(14.4, "1h", "5m"))
	promSLO    = strings.NewReplacer("type: DataFocal", "type: Prometheus", "name: gateway", "name: gateway-prometheus").Replace(gatewaySLO)
	cleanSLO   = `apiVersion: openslo/v1
kind: SLO
metadata: {name: gateway-clean}
spec:
  service: gateway
  budgetingMethod: Timeslices
  timeWindow: [{duration: 1d, isRolling: true}]
  objectives: [{target: 0.9, op: lt, value: 1, timeSliceWindow: 1m}]
  indicator:
    spec:
      thresholdMetric: {metricSource: {type: DataFocal, spec: {query: 'count_over_time({service="gateway"} | status >= 500 [1m])'}}}
`
	// gatewayView shares the gateway SLO's name, so only kind/name exports one of them.
	gatewayView = `apiVersion: datafocal/v1
kind: View
metadata: {name: gateway, displayName: Gateway}
spec:
  time: {from: now-1h, to: now}
  variables: [{name: endpoint, column: endpoint, allowAll: true}]
  panels:
  - {kind: timeseries, query: 'sum by (status) (count_over_time({service="gateway", endpoint="$endpoint"} [1m]))', grid: {x: 0, y: 0, w: 8, h: 4}}
  - {kind: slo, slo: gateway, grid: {x: 8, y: 0, w: 4, h: 4}}
`
)

func init() {
	fixtures["list_slos"] = toolFixture{name: "list_slos", assert: assertListSLOs}
	fixtures["slo_status"] = toolFixture{name: "slo_status", assert: assertSLOStatus}
	fixtures["import_definitions"] = toolFixture{name: "import_definitions", assert: assertImportDefinitions}
	fixtures["export_definitions"] = toolFixture{name: "export_definitions", assert: assertExportDefinitions}
	fixtures["list_views"] = toolFixture{name: "list_views", assert: assertListViews}
	fixtures["get_view"] = toolFixture{name: "get_view", assert: assertGetView}
}

// sloHarness is newHarness with the fixtures' SLOs defined, answering into h's bodies and bill.
func sloHarness(t *testing.T, h *toolHarness, tool string, req any) json.RawMessage {
	t.Helper()
	opts := querier.Options{Catalog: catalog.New(storage.NewFS(corpusDir), tenant), Store: storage.NewFS(corpusDir),
		Definitions: definitionsDir(t, gatewaySLO, promSLO, gatewayView)}
	if h.useSummary {
		opts.Summary = &summary.Config{}
	}
	app := querier.Open(opts)
	require.NoError(t, app.Refresh(context.Background()))
	ts := httptest.NewServer(app.Handler())
	t.Cleanup(ts.Close)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "e2e"}, nil).Connect(context.Background(),
		&mcp.StreamableClientTransport{Endpoint: ts.URL + "/mcp", DisableStandaloneSSE: true}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	sh := &toolHarness{server: api.NewServer(app.Tools(), api.Limits{}), baseURL: ts.URL, mcp: session, useSummary: h.useSummary, bodies: map[string][]byte{}}
	env := postTool[json.RawMessage](t, sh, tool, req)
	if _, ok := h.bodies[tool]; !ok {
		h.bodies[tool] = sh.bodies[tool]
	}
	h.billedGETs += sh.billedGETs
	return env.Data
}

func assertListSLOs(t *testing.T, h *toolHarness) any {
	var resp tools.ListSLOsResponse
	require.NoError(t, json.Unmarshal(sloHarness(t, h, "list_slos", tools.ListSLOsRequest{}), &resp))
	var states []string
	for _, st := range resp.SLOs {
		states = append(states, st.Name+" "+st.State)
	}
	assert.Equal(t, []string{"gateway ok", "gateway-prometheus unbound"}, states)
	assert.Len(t, resp.SLOs[0].Groups, 2, "the gateway's endpoints")
	assert.Nil(t, resp.SLOs[0].BudgetTrend, "the trend only when asked for")

	var page tools.ListSLOsResponse
	require.NoError(t, json.Unmarshal(sloHarness(t, h, "list_slos", tools.ListSLOsRequest{Query: "GATEWAY", Limit: 1, Trend: true}), &page))
	require.Len(t, page.SLOs, 1)
	assert.Equal(t, "gateway", page.SLOs[0].Name, "the first of both the query matches")
	assert.Equal(t, 2, page.Total)
	assert.Len(t, page.SLOs[0].BudgetTrend, 48)
	assert.Equal(t, page.SLOs[0].BudgetRemaining, page.SLOs[0].BudgetTrend[47], "the trend ends at the budget")
	return resp
}

func assertSLOStatus(t *testing.T, h *toolHarness) any {
	var resp tools.SLOStatusResponse
	require.NoError(t, json.Unmarshal(sloHarness(t, h, "slo_status", tools.SLOStatusRequest{Name: "gateway", TimeRange: twoMinutes()}), &resp))
	assert.Len(t, resp.SLO.Points, 2, "a point a minute")
	assert.Equal(t, []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "gateway"}, {Field: "status", Op: selector.OpGe, Value: "500"}}, resp.SLO.BadEvents)
	// Each point's own counts are its minute's events: their bad ones are the bad events then.
	var bad, counted float64
	for _, p := range resp.SLO.Points {
		bad += float64(p.Valid - p.Good)
	}
	env := postTool[tools.AggregateResponse](t, h, "aggregate", tools.AggregateRequest{
		Selector: selector.Selector{Time: twoMinutes(), Where: resp.SLO.BadEvents}, Measure: "count", Bins: "all"})
	for _, s := range env.Data.Series {
		for _, v := range s.Values {
			if v != nil {
				counted += *v
			}
		}
	}
	assert.Positive(t, bad)
	assert.Equal(t, counted, bad, "the points' bad events are the bad events counted")
	return resp
}

// twoMinutes is the incident's first two minutes.
func twoMinutes() selector.TimeSpec {
	from, _ := seedcorpus.IncidentWindow()
	return selector.TimeSpec{From: from.Format(time.RFC3339), To: from.Add(2 * time.Minute).Format(time.RFC3339)}
}

func assertImportDefinitions(t *testing.T, h *toolHarness) any {
	var resp tools.ImportDefinitionsResponse
	require.NoError(t, json.Unmarshal(sloHarness(t, h, "import_definitions", tools.ImportDefinitionsRequest{Content: gatewaySLO + "---\n" + strings.ReplaceAll(promSLO, "0.99", "0.9")}), &resp))
	var classes []string
	for _, im := range resp.Documents {
		classes = append(classes, im.Name+" "+im.Class)
	}
	assert.Equal(t, []string{"gateway unchanged", "gateway-prometheus unbound"}, classes)
	assert.False(t, resp.Saved, "a dry run by default")
	return resp
}

func assertExportDefinitions(t *testing.T, h *toolHarness) any {
	var resp tools.ExportDefinitionsResponse
	require.NoError(t, json.Unmarshal(sloHarness(t, h, "export_definitions", tools.ExportDefinitionsRequest{Names: []string{"gateway-prometheus", "View/gateway"}}), &resp))
	assert.Contains(t, resp.Content, "type: Prometheus")
	assert.Contains(t, resp.Content, "kind: View")
	assert.Equal(t, 1, strings.Count(resp.Content, "\n---\n"), "the view, not the SLO it shares a name with")
	return resp
}

func assertListViews(t *testing.T, h *toolHarness) any {
	var resp tools.ListViewsResponse
	require.NoError(t, json.Unmarshal(sloHarness(t, h, "list_views", tools.ListViewsRequest{Query: "GATE"}), &resp))
	assert.Equal(t, []tools.ViewSummary{{Name: "gateway", Title: "Gateway", Panels: 2}}, resp.Views)
	return resp
}

func assertGetView(t *testing.T, h *toolHarness) any {
	var resp tools.GetViewResponse
	require.NoError(t, json.Unmarshal(sloHarness(t, h, "get_view", tools.GetViewRequest{Name: "gateway"}), &resp))
	require.Len(t, resp.View.Panels, 2)
	// A panel's query and the view's variables are the call that draws it.
	var query tools.QueryResponse
	require.NoError(t, json.Unmarshal(sloHarness(t, h, "query", tools.QueryRequest{Q: resp.View.Panels[0].Query, Time: twoMinutes(),
		Vars: map[string]string{"endpoint": "*"}}), &query))
	assert.Equal(t, `sum by (status) (count_over_time({service="gateway"} [1m]))`, query.Canonical)
	assert.Equal(t, "gateway", resp.View.Panels[1].Title, "an slo panel is titled by its SLO")
	return resp
}

// The factory corpus's SLOs: its tasks, which fail often enough to fire over its last day;
// the querier's health probes and the orchestrator's dispatches, which fire over the error
// burst in IncidentHour; kindnet's heartbeat, by slices; and every line, by service.
var (
	tasksSLO = sloDoc("tasks", "", `{service="orchestrator"} |~ "task (finished|failed)"`, `|= "task finished"`, burn(1, "1d", "1d"))
	// probesSLO's short window is a bin, so it holds from the burst's first bin to its last.
	probesSLO   = sloDoc("probes", "", `{service=~"querier|orchestrator"}`, `| level="info"`, burn(14.4, "5m", "1m"))
	servicesSLO = sloDoc("services", "datafocal/group-by: service", `{namespace=~".+"}`, `| level!="error"`, "")
	// heartbeatSLO is kindnet's heartbeat by 5-minute slices, an absence SLO.
	heartbeatSLO = func() string {
		b, err := os.ReadFile(filepath.Join("..", "definitions", "testdata", "absence.yaml"))
		if err != nil {
			panic(err)
		}
		return string(b)
	}()
)

func listSLOs(t *testing.T, app querier.App, state string) []slo.Status {
	t.Helper()
	env, err := app.Tools().ListSLOs(context.Background(), tools.ListSLOsRequest{State: state})
	require.NoError(t, err)
	return env.Data.SLOs
}

// TestSLOsOverTheFactory: list_slos names the firing SLOs first, and the probes' SLO fires
// from the first bin of the error burst, within a bin of its burn rate passing 14.4, until
// the bin after the burst ends, when its short window clears.
func TestSLOsOverTheFactory(t *testing.T) {
	dir, _ := factoryCorpus(t)
	app := querier.Open(querier.Options{Catalog: catalog.New(storage.NewFS(dir), tenant), Store: storage.NewFS(dir),
		Definitions: definitionsDir(t, tasksSLO, probesSLO, servicesSLO, heartbeatSLO)})
	require.NoError(t, app.Refresh(context.Background()))

	firing := listSLOs(t, app, slo.StateFiring)
	require.Len(t, firing, 1)
	assert.Equal(t, "tasks", firing[0].Name)
	assert.NotNil(t, firing[0].Since)
	var names []string
	for _, st := range listSLOs(t, app, "") {
		names = append(names, st.Name+" "+st.State)
		assert.Equal(t, 1.0, st.Coverage, st.Name)
	}
	assert.Equal(t, "tasks firing", names[0], "firing first")
	assert.ElementsMatch(t, []string{"tasks firing", "probes ok", "services ok", "heartbeat-present ok"}, names)

	from, _ := seedcorpus.IncidentHour()
	burst := from.Add(30 * time.Minute)
	var fired []time.Time
	var since *time.Time
	for at := burst.Add(-5 * time.Minute); at.Before(burst.Add(8 * time.Minute)); at = at.Add(time.Minute) {
		env, err := app.Tools().SLOStatus(context.Background(), tools.SLOStatusRequest{Name: "probes",
			TimeRange: selector.TimeSpec{From: at.Add(-time.Minute).Format(time.RFC3339), To: at.Format(time.RFC3339)}})
		require.NoError(t, err)
		if env.Data.SLO.State == slo.StateFiring {
			fired, since = append(fired, at), env.Data.SLO.Since
		}
	}
	assert.Equal(t, []time.Time{burst.Add(time.Minute), burst.Add(2 * time.Minute)}, fired, "firing from the burst's first bin to the bin after its last")
	assert.Equal(t, burst.Add(time.Minute), *since)
}

// prefix is a catalog of the first *n of descs at version n, or cat itself once *n passes them.
func prefix(cat catalog.Catalog, descs []segment.Descriptor, n *int) catalog.Catalog {
	return catalog.Fake{
		VersionFn: func() (catalog.Version, error) {
			if *n > len(descs) {
				return cat.Version(context.Background())
			}
			return catalog.Version(fmt.Sprint(*n)), nil
		},
		PruneFn: func(v catalog.Version, sel selector.Resolved) ([]segment.Descriptor, error) {
			if *n > len(descs) {
				return cat.Prune(context.Background(), v, sel)
			}
			return descs[:*n], nil
		},
	}
}

// TestSLOsIncrementalEqualsRebuilt: a querier refreshed a segment at a time answers the
// status a querier started after the last segment does, and both again after the hours'
// segments are compacted into hour segments.
func TestSLOsIncrementalEqualsRebuilt(t *testing.T) {
	ctx := context.Background()
	// A copy of the shared factory corpus, since the compaction rewrites it.
	shared, _ := factoryCorpus(t)
	dir := t.TempDir()
	require.NoError(t, os.CopyFS(dir, os.DirFS(shared)))
	store := storage.NewFS(dir)
	cat := catalog.New(store, tenant)
	v, err := cat.Version(ctx)
	require.NoError(t, err)
	descs, err := cat.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	definitions := definitionsDir(t, tasksSLO, probesSLO, servicesSLO, heartbeatSLO)
	statuses := func(app querier.App) string {
		b, err := json.Marshal(listSLOs(t, app, ""))
		require.NoError(t, err)
		return string(b)
	}
	rebuilt := func() string {
		app := querier.Open(querier.Options{Catalog: catalog.New(store, tenant), Store: store, Definitions: definitions})
		require.NoError(t, app.Refresh(ctx))
		return statuses(app)
	}

	n := 0
	incremental := querier.Open(querier.Options{Catalog: prefix(cat, descs, &n), Store: store, Definitions: definitions})
	for n = 1; n <= len(descs); n++ {
		require.NoError(t, incremental.Refresh(ctx))
	}
	n--
	want := rebuilt()
	assert.Equal(t, want, statuses(incremental))

	// No catalog snapshot, whose encoding the process's shared zstd encoders would keep
	// buffers for, as later memory budgets in this package measure them.
	require.NoError(t, compact.New(cat, catalog.Fake{}, segment.NewReader(store), store, tenant).Run(ctx, time.Now()))
	n = len(descs) + 1
	require.NoError(t, incremental.Refresh(ctx))
	assert.Equal(t, want, statuses(incremental), "across the compaction")
	assert.Equal(t, want, rebuilt(), "after the compaction")
}

// rangeReads is a store counting the ranged reads that fetch rows, beside the suffix reads
// that fetch footers.
type rangeReads struct {
	storage.Store
	n *atomic.Int64
}

func (s rangeReads) GetRange(ctx context.Context, key string, off, n int64) ([]byte, error) {
	s.n.Add(1)
	return s.Store.GetRange(ctx, key, off, n)
}

// TestSLOFooterAnsweredReadsNoRows: refreshing an SLI whose good query adds one equality on
// a counted column reads the new segments' footers and none of their rows.
func TestSLOFooterAnsweredReadsNoRows(t *testing.T) {
	ctx := context.Background()
	dir, _ := factoryCorpus(t)
	cat := catalog.New(storage.NewFS(dir), tenant)
	v, err := cat.Version(ctx)
	require.NoError(t, err)
	descs, err := cat.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	// The incident hour's last 10 minutes arrive last, each segment inside one minute so its
	// footer counts it whole.
	_, to := seedcorpus.IncidentHour()
	var earlier, late []segment.Descriptor
	for _, d := range descs {
		switch {
		case !d.TimeRange.Max.Before(to):
		case d.TimeRange.Min.Before(to.Add(-10 * time.Minute)):
			earlier = append(earlier, d)
		case d.TimeRange.Min.Truncate(time.Minute).Equal(d.TimeRange.Max.Truncate(time.Minute)):
			late = append(late, d)
		}
	}
	all := slices.Concat(earlier, late)
	n := len(earlier)
	var rows atomic.Int64
	// A window of the 10 minutes, so its SLI is of the late segments alone.
	doc := strings.Replace(sloDoc("kindnet", "", `{service="kindnet"}`, `| level="info"`, ""), "duration: 1d", "duration: 10m", 1)
	app := querier.Open(querier.Options{Catalog: prefix(cat, all, &n), Store: rangeReads{Store: storage.NewFS(dir), n: &rows},
		Definitions: definitionsDir(t, doc)})
	require.NoError(t, app.Refresh(ctx))
	rows.Store(0)
	n = len(all)
	require.NoError(t, app.Refresh(ctx))
	assert.Zero(t, rows.Load(), "ranged reads refreshing %d segments", len(late))
	assert.Equal(t, ptr(1.0), listSLOs(t, app, "")[0].SLI, "the late segments' heartbeats, counted off their footers")
}

func ptr[T any](v T) *T { return &v }
