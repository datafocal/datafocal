package e2e

import (
	"bytes"
	"encoding/json"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/api"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/selector"
)

// toolFixture wires one registered tool to the assertions that exercise it
// against the seeded corpus.
//
// Every fixture runs both passes with no opt-out: the summary is a cache, and a
// querier pod's cache is cold on its first request and after every restart, so a
// tool that answers only when it is warm is unreachable in production
// (cmd/querier wires nil).
//
// equalityCheck, when non-nil, replaces the default byte-for-byte JSON comparison
// of Data across the two paths — for tools whose sketched numbers (correlate's rho)
// legitimately drift within the sketch's error while their reasoning-critical
// fields must still agree.
type toolFixture struct {
	name          string
	assert        func(t *testing.T, h *toolHarness) any
	equalityCheck func(t *testing.T, noSummary, withSummary any)
}

// fixtures maps every registered tool name to the fixture that drives it.
// Adding a tool to internal/api's Registry without adding an entry here fails
// TestEveryRegisteredToolHasFixture — that structural gap is the point of the
// map. Adding an entry that names an absent tool fails
// TestNoFixtureEntryIsOrphan.
var fixtures = map[string]toolFixture{
	"aggregate":        {name: "aggregate", assert: assertAggregate},
	"attribute_writes": {name: "attribute_writes", assert: assertAttributeWrites},
	"batch":            {name: "batch", assert: assertBatch},
	"co_occurrence":    {name: "co_occurrence", assert: assertCoOccurrence},
	"correlate":        {name: "correlate", assert: assertCorrelate, equalityCheck: correlateEqualityCheck},
	"explain_spike":    {name: "explain_spike", assert: assertExplainSpike},
	"find_anomalies":   {name: "find_anomalies", assert: assertFindAnomalies},
	"focus":            {name: "focus", assert: assertFocus},
	"overview":         {name: "overview", assert: assertOverview},
	"funnel":           {name: "funnel", assert: assertFunnel},
	"search_logs":      {name: "search_logs", assert: assertSearchLogs},
	"rank_fields":      {name: "rank_fields", assert: assertRankFields},
	"describe_schema":  {name: "describe_schema", assert: assertDescribeSchema},
	"get_trace":        {name: "get_trace", assert: assertGetTrace},
	"trace_shape":      {name: "trace_shape", assert: assertTraceShape},
	"list_patterns":    {name: "list_patterns", assert: assertListPatterns},
	"query":            {name: "query", assert: assertQuery},
	"service_graph":    {name: "service_graph", assert: assertServiceGraph},
	"rca_walk":         {name: "rca_walk", assert: assertRCAWalk},
	"resource_usage":   {name: "resource_usage", assert: assertResourceUsage},
}

// TestEveryRegisteredToolHasFixture is one half of the structural coverage
// gate the acceptance criteria calls out: a registered tool without a fixture
// entry fails the suite by name.
func TestEveryRegisteredToolHasFixture(t *testing.T) {
	h := newHarness(t, false)
	var missing []string
	for _, td := range h.server.Registry().Tools() {
		if _, ok := fixtures[td.Name]; !ok {
			missing = append(missing, td.Name)
		}
	}
	sort.Strings(missing)
	require.Emptyf(t, missing, "registered tools with no fixture entry: %v", missing)
}

// TestNoFixtureEntryIsOrphan is the other half: a fixture entry that names an
// unregistered tool fails the suite by name.
func TestNoFixtureEntryIsOrphan(t *testing.T) {
	h := newHarness(t, false)
	registered := make(map[string]struct{})
	for _, td := range h.server.Registry().Tools() {
		registered[td.Name] = struct{}{}
	}
	var orphan []string
	for name := range fixtures {
		if _, ok := registered[name]; !ok {
			orphan = append(orphan, name)
		}
	}
	sort.Strings(orphan)
	require.Emptyf(t, orphan, "fixture entries naming an unregistered tool: %v", orphan)
}

// TestEveryToolRefusesInTheSameShape: a caller parses one error shape whatever
// the tool and whatever the status. Every tool validates the selector before it
// does anything else, so a request with an unparseable time is the one input all of
// them refuse — and the refusal must arrive as a query.APIError carrying a code, not
// the bare {"error": ...} object three of these paths used to return.
func TestEveryToolRefusesInTheSameShape(t *testing.T) {
	h := newHarness(t, false)
	for _, td := range h.server.Registry().Tools() {
		t.Run(td.Name, func(t *testing.T) {
			status, body := postToolRaw(t, h, td.Name, map[string]any{"selector": map[string]any{"time": map[string]any{"from": "bogus"}}})
			require.Equalf(t, http.StatusBadRequest, status, "/v1/%s body: %s", td.Name, body)

			var apiErr query.APIError
			require.NoErrorf(t, json.Unmarshal(body, &apiErr), "/v1/%s body: %s", td.Name, body)
			assert.Equal(t, query.ErrCodeInvalidRequest, apiErr.Code)
			assert.NotEmpty(t, apiErr.Message, "a refusal must say what was wrong")
		})
	}
}

// TestServiceGraphSkipsTheTopologyFold: a wired summary answers service_graph without
// re-folding the topology per request, so its warm cost is strictly below its cold one.
// The matrix asserts warm <= cold for every tool; this asserts the strict inequality,
// which is the observable proof the fold was skipped rather than merely cheap.
func TestServiceGraphSkipsTheTopologyFold(t *testing.T) {
	hWith := newHarness(t, true)
	hNo := newHarness(t, false)

	req := tools.ServiceGraphRequest{
		Selector: incidentSelector(),
		Limit:    500,
	}
	warm := postTool[tools.ServiceGraphResponse](t, hWith, "service_graph", req)
	cold := postTool[tools.ServiceGraphResponse](t, hNo, "service_graph", req)

	// The placement scan is billed on both paths and is what warm keeps.
	assert.Lessf(t, warm.Meta.Cost.GETs, cold.Meta.Cost.GETs,
		"warm GETs %d must be strictly below cold GETs %d",
		warm.Meta.Cost.GETs, cold.Meta.Cost.GETs)
}

var updateGoldens = flag.Bool("update", false, "rewrite the tool goldens under internal/query/tools/testdata")

// TestToolGoldens pins each tool's first answer in its fixture as the golden make
// web-fixtures hands the frontend, on a fresh harness so its cost is always a cold read.
func TestToolGoldens(t *testing.T) {
	for name, f := range fixtures {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, false)
			f.assert(t, h)
			first := h.bodies[name]
			clear(h.bodies)
			f.assert(t, h)
			var once, again struct{ Data json.RawMessage }
			require.NoError(t, json.Unmarshal(first, &once))
			require.NoError(t, json.Unmarshal(h.bodies[name], &again))
			assert.Equal(t, string(once.Data), string(again.Data), "the call made twice answers alike")
			h.bodies[name] = first
			var got bytes.Buffer
			require.NoError(t, json.Indent(&got, h.bodies[name], "", "  "))

			path := filepath.Join("..", "query", "tools", "testdata", name+".golden.json")
			if *updateGoldens {
				require.NoError(t, os.WriteFile(path, got.Bytes(), 0o644))
				return
			}
			want, err := os.ReadFile(path)
			require.NoError(t, err, "run with -update to create")
			assert.JSONEq(t, string(want), got.String(), "golden mismatch (rerun with -update if intentional)")
		})
	}
}

// TestToolsOverSeededCorpus drives every fixture through the tool surface
// twice — once with a nil summary store (footer-merge / scan path) and once with
// a summary populated from every sealed segment (hot path) — and requires the
// two Envelope.Data payloads to marshal identically. Cost and Snapshot
// legitimately differ between the paths and are not compared.
func TestToolsOverSeededCorpus(t *testing.T) {
	names := make([]string, 0, len(fixtures))
	for name := range fixtures {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		f := fixtures[name]
		t.Run(f.name, func(t *testing.T) {
			var noSummaryData, withSummaryData any
			// Fresh per fixture: a querier caches the chunks a call reads whole, and the two paths
			// read different ones, so an earlier fixture's reads would discount this one's.
			hNoSummary, hWithSummary := newHarness(t, false), newHarness(t, true)
			t.Run("with_summary", func(t *testing.T) {
				withSummaryData = f.assert(t, hWithSummary)
			})
			t.Run("no_summary", func(t *testing.T) {
				noSummaryData = f.assert(t, hNoSummary)
			})
			// The hot path merges summaries and never rows, so wiring one
			// can only remove reads. Asserted per fixture rather than for the two tools
			// someone thought to check, because a tool that starts scanning past its
			// summary is a cost regression no Data comparison notices.
			t.Run("summary_never_costs_more", func(t *testing.T) {
				warm, cold := hWithSummary.billedGETs, hNoSummary.billedGETs
				assert.LessOrEqualf(t, warm, cold,
					"/v1/%s billed %d GETs with a summary and %d without", f.name, warm, cold)
			})
			t.Run("data_equal_across_paths", func(t *testing.T) {
				if f.equalityCheck != nil {
					f.equalityCheck(t, noSummaryData, withSummaryData)
					return
				}
				a, err := json.Marshal(noSummaryData)
				require.NoError(t, err)
				b, err := json.Marshal(withSummaryData)
				require.NoError(t, err)
				if !bytes.Equal(a, b) {
					assert.Failf(t, "Data diverged",
						"%s: nil-summary vs summary-populated Envelope.Data must be identical\n--- nil summary ---\n%s\n--- with summary ---\n%s",
						f.name, a, b)
				}
			})
		})
	}
}

// assertQuery runs a LogQL metric query and requires the data the aggregate it names
// returns for the same typed request.
func assertQuery(t *testing.T, h *toolHarness) any {
	t.Helper()
	env := postTool[tools.QueryResponse](t, h, "query", tools.QueryRequest{
		Q:    `quantile_over_time(0.95, {service="db"} [15s]) by (instance)`,
		Time: incidentTimeSpec(),
	})
	direct := postTool[tools.AggregateResponse](t, h, "aggregate", tools.AggregateRequest{
		Selector: withPredicates(incidentSelector(), selector.Predicate{Field: "service", Op: selector.OpEq, Value: "db"}),
		Measure:  "p95",
		GroupBy:  "instance",
		Bins:     "15s",
	})
	want, err := json.Marshal(direct.Data)
	require.NoError(t, err)
	assert.Equal(t, "aggregate", env.Data.Call.Tool)
	assert.JSONEq(t, string(want), string(env.Data.Result))
	return env.Data
}

// assertOverview requires each service's lines and error lines over the incident to be
// those aggregate counts, whichever namespaces they fall in.
func assertOverview(t *testing.T, h *toolHarness) any {
	t.Helper()
	env := postTool[tools.OverviewResponse](t, h, "overview", tools.OverviewRequest{Selector: incidentSelector()})
	require.NotEmpty(t, env.Data.Services)
	lines, errs := map[string]float64{}, map[string]float64{}
	for _, row := range env.Data.Services {
		lines[row.Service] += float64(row.Lines)
		errs[row.Service] += float64(row.ErrorLines)
	}
	for want, where := range map[*map[string]float64][]selector.Predicate{
		&lines: nil,
		&errs:  {{Field: "level", Op: selector.OpRegex, Value: "^(error|fatal)$"}},
	} {
		agg := postTool[tools.AggregateResponse](t, h, "aggregate", tools.AggregateRequest{
			Selector: withPredicates(incidentSelector(), where...), Measure: "count", GroupBy: "service", Bins: "all",
		})
		got := map[string]float64{}
		for _, s := range agg.Data.Series {
			got[s.Key] = *s.Values[0]
		}
		for service, n := range *want {
			assert.Equalf(t, got[service], n, "%s lines matching %v", service, where)
		}
	}
	return env.Data
}

// assertFocus focuses db's lines on its slow-query pattern, which every section answers.
// assertBatch asks for two tools' answers and a tool there is none of in one call: each
// answers as its own route would, and the third fails alone.
func assertBatch(t *testing.T, h *toolHarness) any {
	t.Helper()
	agg, _ := json.Marshal(tools.AggregateRequest{Selector: incidentSelector(), Measure: "count", GroupBy: "service", Bins: "all"})
	search, _ := json.Marshal(tools.SearchRequest{Selector: incidentSelector(), Limit: 2})
	status, body := postToolRaw(t, h, "batch", api.BatchRequest{Calls: []tools.ToolCall{
		{Tool: "aggregate", Args: agg}, {Tool: "search_logs", Args: search}, {Tool: "no_such_tool"},
	}})
	require.Equal(t, http.StatusOK, status, string(body))
	if _, ok := h.bodies["batch"]; !ok {
		h.bodies["batch"] = body
	}
	var resp struct {
		Results []struct {
			Envelope query.Envelope[json.RawMessage]
			Error    *query.APIError
		}
	}
	require.NoError(t, json.Unmarshal(body, &resp))
	require.Len(t, resp.Results, 3)
	for _, r := range resp.Results[:2] {
		assertEnvelope(t, h, "batch", r.Envelope.Meta)
	}
	assert.Equal(t, query.ErrCodeInvalidRequest, resp.Results[2].Error.Code)
	return []json.RawMessage{resp.Results[0].Envelope.Data, resp.Results[1].Envelope.Data}
}

func assertFocus(t *testing.T, h *toolHarness) any {
	t.Helper()
	slow := patternIDForPrefix("slow query")
	env := postTool[tools.FocusResponse](t, h, "focus", tools.FocusRequest{
		Selector: withPredicates(incidentSelector(), selector.Predicate{Field: "service", Op: selector.OpEq, Value: "db"}),
		Slice:    []selector.Predicate{{Field: "pattern_id", Op: selector.OpEq, Value: slow}},
	})
	require.Empty(t, env.Data.Errors)
	assert.NotEmpty(t, env.Data.Trend.Series)
	// Every slow query is db-3's, so the pattern fixes instance and nothing else stands out.
	assert.Empty(t, env.Data.Lift.Shifts)
	assert.Positive(t, env.Data.Lift.Rest)
	require.NotEmpty(t, env.Data.Related.Patterns)
	assert.Equal(t, slow, env.Data.Related.Patterns[0].PatternID)
	require.NotEmpty(t, env.Data.Examples.Lines)
	for _, l := range env.Data.Examples.Lines {
		assert.Equal(t, slow, l.PatternID)
	}
	return env.Data
}
