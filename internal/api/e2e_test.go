package api_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/api"
	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/internal/value"
)

// TestAggregateEndToEnd is the walking-skeleton proof: seed a real corpus through
// the collector into an fs object store + fs catalog, serve internal/api over it
// with httptest, POST an aggregate request, and assert the response carries the
// exact per-service row counts. This exercises the whole vertical slice —
// HTTP decode → resolve time → catalog prune → merge descriptor summaries →
// envelope encode — against real on-disk data.
func TestAggregateEndToEnd(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)

	// Distinct counts per service so an accidental cross-wiring is caught.
	want := map[string]int{
		"analytics-export": 5,
		"db":               7,
		"host":             3,
		"jobs":             11,
		"gateway":          9,
		"checkout":         8,
		"recommendations":  13,
	}
	for service, n := range want {
		seedService(t, store, cat, service, n)
	}

	_, baseURL := serve(t, dir, store)

	// healthz first — the liveness leg of the slice.
	resp, err := http.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want 200", resp.StatusCode)
	}

	env := postTool[tools.AggregateResponse](t, baseURL, "/v1/aggregate", tools.AggregateRequest{
		Selector: selector.Selector{Time: wideWindow()},
		Measure:  "count",
		GroupBy:  "service",
		Bins:     "auto",
	})

	if len(env.Data.Series) != len(want) {
		t.Fatalf("got %d series, want %d: %+v", len(env.Data.Series), len(want), env.Data.Series)
	}
	got := map[string]int{}
	for _, s := range env.Data.Series {
		// bins="auto" makes multi-bin series; the binned-count invariant is
		// that values sums to the total count for the group.
		got[s.Key] = int(total(s))
	}
	for service, n := range want {
		if got[service] != n {
			t.Errorf("service %q count = %d, want %d", service, got[service], n)
		}
	}

	// Envelope meta must be filled: exact answer, epoch snapshot, no data GETs.
	if env.Meta.Approx {
		t.Errorf("meta.approx = true, want false (count is exact)")
	}
	if env.Meta.Coverage != 1.0 {
		t.Errorf("meta.coverage = %v, want 1.0", env.Meta.Coverage)
	}
	wantSnap := fmt.Sprintf("e%d.s0", len(want)) // one segment (epoch entry) per service
	if env.Meta.Snapshot != wantSnap {
		t.Errorf("meta.snapshot = %q, want %q", env.Meta.Snapshot, wantSnap)
	}
	if env.Meta.Cost.GETs != 0 {
		t.Errorf("meta.cost.gets = %d, want 0 (count answered from descriptors)", env.Meta.Cost.GETs)
	}
	if env.Meta.Tokens <= 0 {
		t.Errorf("meta.token_estimate = %d, want > 0", env.Meta.Tokens)
	}
}

// TestAggregateOverS3 has a collector write into the DATAFOCAL_S3_* bucket and a querier
// over it count the rows, under a fresh tenant so reruns see only their own segments.
func TestAggregateOverS3(t *testing.T) {
	if os.Getenv("DATAFOCAL_S3_ENDPOINT") == "" {
		t.Skip("DATAFOCAL_S3_ENDPOINT is unset")
	}
	var cfg storage.S3Config
	require.NoError(t, envconfig.Process("DATAFOCAL_S3", &cfg))
	store, err := storage.NewS3(cfg)
	require.NoError(t, err)
	tenant := "s3-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	seedTenantEvents(t, store, catalog.New(store, tenant), tenant, "api", requestEvents("api", 6, nil))

	ts := httptest.NewServer(querier.Open(querier.Options{Catalog: catalog.New(store, tenant), Store: store}).Handler())
	t.Cleanup(ts.Close)
	env := postTool[tools.AggregateResponse](t, ts.URL, "/v1/aggregate", tools.AggregateRequest{
		Selector: selector.Selector{Time: wideWindow()},
		Measure:  "count",
		GroupBy:  "service",
		Bins:     "auto",
	})

	require.Len(t, env.Data.Series, 1)
	assert.Equal(t, "api", env.Data.Series[0].Key)
	assert.Equal(t, 6.0, total(env.Data.Series[0]))
}

// TestAggregateRejectsUnsupported confirms the aggregate tool fails loudly on
// measures it does not implement, rather than returning a wrong answer.
func TestAggregateRejectsUnsupported(t *testing.T) {
	t.Parallel()

	dir, store, _ := newCorpus(t)
	_, baseURL := serve(t, dir, store)

	body, _ := json.Marshal(tools.AggregateRequest{
		Selector: selector.Selector{},
		Measure:  "nosuchmeasure",
		GroupBy:  "service",
	})
	resp, err := http.Post(baseURL+"/v1/aggregate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for unsupported measure", resp.StatusCode)
	}
}

// TestWronglyTypedFieldIsInvalidRequest: a body that does not fit the tool's
// request type is the caller's to fix, on HTTP and MCP alike.
func TestWronglyTypedFieldIsInvalidRequest(t *testing.T) {
	t.Parallel()

	dir, store, _ := newCorpus(t)
	srv, baseURL := serve(t, dir, store)

	ctx := context.Background()
	st, ct := mcp.NewInMemoryTransports()
	ss, err := api.NewMCPServer(srv.Registry(), 0).Connect(ctx, st, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil).Connect(ctx, ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	// service_graph is the one request without a snapshot field.
	for _, tc := range []struct{ tool, field, message string }{
		{"aggregate", "snapshot", "Go struct field AggregateRequest.snapshot of type string"},
		{"attribute_writes", "snapshot", "Go struct field AttributeWritesRequest.snapshot of type string"},
		{"correlate", "snapshot", "Go struct field CorrelateRequest.snapshot of type string"},
		{"explain_spike", "snapshot", "Go struct field ExplainSpikeRequest.snapshot of type string"},
		{"find_anomalies", "snapshot", "Go struct field FindAnomaliesRequest.snapshot of type string"},
		{"search_logs", "snapshot", "Go struct field SearchRequest.snapshot of type string"},
		{"rank_fields", "snapshot", "Go struct field FieldsRequest.snapshot of type string"},
		{"list_patterns", "snapshot", "Go struct field PatternsRequest.snapshot of type string"},
		{"get_trace", "snapshot", "Go struct field TraceRequest.snapshot of type string"},
		{"service_graph", "limit", "Go struct field ServiceGraphRequest.limit of type int"},
		{"rca_walk", "snapshot", "Go struct field RCAWalkRequest.snapshot of type string"},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			want := query.NewAPIError(query.ErrCodeInvalidRequest,
				"decode request: json: cannot unmarshal bool into "+tc.message)

			resp, err := http.Post(baseURL+"/v1/"+tc.tool, "application/json", strings.NewReader(`{"`+tc.field+`":true}`))
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			var httpErr *query.APIError
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&httpErr))
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, want, httpErr)

			res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: tc.tool, Arguments: map[string]any{tc.field: true}})
			require.NoError(t, err)
			require.Len(t, res.Content, 1)
			text, ok := res.Content[0].(*mcp.TextContent)
			require.True(t, ok, "content type = %T", res.Content[0])
			var mcpErr *query.APIError
			require.NoError(t, json.Unmarshal([]byte(text.Text), &mcpErr))
			assert.True(t, res.IsError)
			assert.Equal(t, want, mcpErr)
		})
	}
}

// TestSearchLogsNeedleEndToEnd: seed a corpus with trace_ids, then search for
// an exact trace_id via /v1/search_logs and verify the returned LogLines are exact.
func TestSearchLogsNeedleEndToEnd(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	seedServiceWithAttrs(t, store, cat, "api", 5, map[string]string{"trace_id": "trace-001"})
	seedServiceWithAttrs(t, store, cat, "api", 3, map[string]string{"trace_id": "trace-002"})

	_, baseURL := serve(t, dir, store)

	env := postTool[tools.SearchResponse](t, baseURL, "/v1/search_logs", tools.SearchRequest{
		Selector: selector.Selector{
			Time:  wideWindow(),
			Where: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: "trace-001"}},
		},
		Limit: 100,
	})

	if env.Data.Mode != "needle" {
		t.Errorf("mode = %q, want needle", env.Data.Mode)
	}
	if len(env.Data.Lines) != 5 {
		t.Errorf("got %d lines, want 5 for trace-001", len(env.Data.Lines))
	}
	for _, line := range env.Data.Lines {
		if line.Attributes["trace_id"] != "trace-001" {
			t.Errorf("line has trace_id=%q, want trace-001", line.Attributes["trace_id"])
		}
	}
	if env.Meta.Coverage != 1.0 {
		t.Errorf("coverage = %v, want 1.0", env.Meta.Coverage)
	}
	if env.Meta.Approx {
		t.Errorf("approx = true, want false (needle is exact)")
	}
}

// TestServerDescribeIncludesServiceGraph: /v1/describe must list service_graph
// so the tool is discoverable via the standard describe surface.
func TestServerDescribeIncludesServiceGraph(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	seedService(t, store, cat, "svc", 1)
	_, baseURL := serve(t, dir, store)

	resp, err := http.Get(baseURL + "/v1/describe")
	if err != nil {
		t.Fatalf("GET /v1/describe: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var desc struct {
		Tools []struct{ Name string } `json:"tools"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&desc); err != nil {
		t.Fatalf("decode describe: %v", err)
	}
	found := false
	for _, tool := range desc.Tools {
		if tool.Name == "service_graph" {
			found = true
			break
		}
	}
	if !found {
		names := make([]string, len(desc.Tools))
		for i, tool := range desc.Tools {
			names[i] = tool.Name
		}
		t.Errorf("service_graph not found in /v1/describe; got tools: %v", names)
	}
}

// TestServiceGraphWithoutSummaryStore: service_graph answers over a server with no
// summary wired, which is the only wiring production has (cmd/querier passes nil).
// The summary is a cache, so its absence costs reads and never an answer — the tool
// folds the window's footers itself.
func TestServiceGraphWithoutSummaryStore(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	seedEvents(t, store, cat, "scheduler", []segment.Record{segment.LogRecord(seedStart, "info", "scheduled pod db-3-0 host host-7", map[string]string{"pod": "db-3-0", "host": "host-7"})})
	_, baseURL := serve(t, dir, store) // serve passes nil summary store

	env := postTool[tools.ServiceGraphResponse](t, baseURL, "/v1/service_graph", tools.ServiceGraphRequest{
		Selector: selector.Selector{Time: wideWindow()},
	})

	want := []tools.GraphEdge{{From: "db-3-0", To: "host-7", Method: "topology", Weight: 1, Confidence: 1}}
	if !reflect.DeepEqual(want, env.Data.Edges) {
		t.Fatalf("edges = %+v, want %+v", env.Data.Edges, want)
	}
}

// TestServiceGraphPlacementEndToEnd is the issue's acceptance criterion over a real
// corpus: the scheduler records db-3-0's placement in a log line, not in a stream
// label, so the pod→host edge exists only if the whole slice works — HTTP decode →
// placement plan → column reads → graph merge → envelope.
func TestServiceGraphPlacementEndToEnd(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	seedEvents(t, store, cat, "scheduler", []segment.Record{
		segment.LogRecord(seedStart, "info", "scheduled pod db-3-0 host host-7", map[string]string{"pod": "db-3-0", "host": "host-7"}),
		segment.LogRecord(seedStart.Add(time.Second), "info", "scheduled pod gateway-0 host host-4", map[string]string{"pod": "gateway-0", "host": "host-4"}),
	})
	seedService(t, store, cat, "db", 5)

	_, baseURL := serveWithSummary(t, dir, store, warm)

	env := postTool[tools.ServiceGraphResponse](t, baseURL, "/v1/service_graph", tools.ServiceGraphRequest{
		Selector: selector.Selector{Time: wideWindow()},
	})

	want := []tools.GraphEdge{
		{From: "db-3-0", To: "host-7", Method: "topology", Weight: 1, Confidence: 1},
		{From: "gateway-0", To: "host-4", Method: "topology", Weight: 1, Confidence: 1},
	}
	if !reflect.DeepEqual(want, env.Data.Edges) {
		t.Fatalf("edges = %+v, want %+v", env.Data.Edges, want)
	}
	// The placement scan is real work and must be billed.
	if env.Meta.Cost.GETs == 0 {
		t.Error("meta.cost.gets = 0, want > 0 (the placement scan reads columns)")
	}
}

// TestAggregateRatioDecodesItsNumerator covers the one thing the HTTP slice adds to
// the ratio measure: that `numerator` survives JSON as typed predicates, the
// arithmetic itself being the package oracle's (tools.TestAggregateRatioVsCount).
// A numerator lost on the wire makes the ratio count/count = 1.0, so the assertion
// below fails by an order of magnitude rather than by a rounding error.
func TestAggregateRatioDecodesItsNumerator(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	events := make([]segment.Record, 20)
	for i := range events {
		status := "200"
		if i < 2 {
			status = "503"
		}
		events[i] = segment.LogRecord(seedStart.Add(time.Duration(i)*time.Second), "info", "handled request", map[string]string{"status": status})
	}
	seedEvents(t, store, cat, "gateway", events)
	_, baseURL := serve(t, dir, store)

	env := postTool[tools.AggregateResponse](t, baseURL, "/v1/aggregate", tools.AggregateRequest{
		Selector: selector.Selector{Time: selector.TimeSpec{
			From: seedStart.Format(time.RFC3339),
			To:   seedStart.Add(time.Minute).Format(time.RFC3339),
		}},
		Measure:   "ratio",
		GroupBy:   "service",
		Numerator: []selector.Predicate{{Field: "status", Op: selector.OpGe, Value: "500"}},
		Bins:      "1m",
	})

	if len(env.Data.Series) != 1 || len(env.Data.Series[0].Values) != 1 {
		t.Fatalf("want one series of one bin, got %+v", env.Data.Series)
	}
	if got := *env.Data.Series[0].Values[0]; got != 0.1 {
		t.Errorf("error rate = %v, want 0.1 (1.0 means the numerator was dropped on the wire)", got)
	}
}

// TestAggregateFilteredQuantileSurfacesApprox covers the wire half of the scanned
// quantile: that a row-level predicate reaches the tool, and that Meta.Approx comes
// back false because a scanned quantile is exact where a sketch merge is not. The two
// routes have disjoint latencies, so a predicate lost on the wire answers 600 not 30.
func TestAggregateFilteredQuantileSurfacesApprox(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	var events []segment.Record
	for i, e := range []struct {
		route string
		ms    int
	}{{"/checkout", 10}, {"/checkout", 20}, {"/checkout", 30}, {"/cart", 500}, {"/cart", 600}} {
		events = append(events, segment.LogRecord(seedStart.Add(time.Duration(i)*time.Second), "info", fmt.Sprintf("req took %d ms", e.ms), map[string]string{"endpoint": e.route}))
	}
	seedEvents(t, store, cat, "gateway", events)
	_, baseURL := serve(t, dir, store)

	env := postTool[tools.AggregateResponse](t, baseURL, "/v1/aggregate", tools.AggregateRequest{
		Selector: selector.Selector{
			Time: selector.TimeSpec{
				From: seedStart.Format(time.RFC3339),
				To:   seedStart.Add(time.Minute).Format(time.RFC3339),
			},
			Where: []selector.Predicate{{Field: "endpoint", Op: selector.OpEq, Value: "/checkout"}},
		},
		Measure: "p99",
		GroupBy: "service",
		Bins:    "1m",
	})

	if len(env.Data.Series) != 1 || len(env.Data.Series[0].Values) != 1 {
		t.Fatalf("want one series of one bin, got %+v", env.Data.Series)
	}
	// nearest-rank p99 of [10,20,30]: ceil(0.99*3)=3 -> the max.
	if got := *env.Data.Series[0].Values[0]; got != 30.0 {
		t.Errorf("p99(/checkout) = %v, want 30 (600 means the route predicate was dropped)", got)
	}
	if env.Meta.Approx {
		t.Error("meta.approx = true, want false (a scanned quantile is exact)")
	}
}

// TestAggregateScanTooLargeIsReplannable covers the refusal's wire shape: a scan
// bound the caller set is honoured, and exceeding it returns HTTP 200 with an
// APIError body rather than a 4xx, because scan_too_large is re-plannable: it
// names the cheaper next call.
func TestAggregateScanTooLargeIsReplannable(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	for i := range 6 {
		seedEvents(t, store, cat, fmt.Sprintf("svc-%d", i), []segment.Record{segment.LogRecord(seedStart.Add(time.Duration(i)*time.Minute), "info", "handled request", map[string]string{"status": "503"})})
	}
	_, baseURL := serve(t, dir, store)

	body, _ := json.Marshal(tools.AggregateRequest{
		Selector:        selector.Selector{Time: wideWindow()},
		Measure:         "ratio",
		GroupBy:         "service",
		Numerator:       []selector.Predicate{{Field: "status", Op: selector.OpGe, Value: "500"}},
		Bins:            "1m",
		MaxScanSegments: 2,
	})
	resp, err := http.Post(baseURL+"/v1/aggregate", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (re-plannable error)", resp.StatusCode)
	}
	var apiErr query.APIError
	if err := json.NewDecoder(resp.Body).Decode(&apiErr); err != nil {
		t.Fatalf("decode APIError: %v", err)
	}
	if apiErr.Code != query.ErrCodeScanTooLarge {
		t.Errorf("error code = %q, want %q", apiErr.Code, query.ErrCodeScanTooLarge)
	}
	if apiErr.Hint == "" {
		t.Error("a re-plannable refusal must carry the re-plan")
	}
}

// TestPageLoadStaysInItsMemBudget runs the Logs page load's rank_fields and three aggregates
// at once over an hour of unmerged segments, under a heap ceiling of the cap times the budget
// over a fixed baseline: the heap stays under it, and a request past its budget fails
// too_large rather than growing the heap.
func TestPageLoadStaysInItsMemBudget(t *testing.T) {
	const (
		maxInFlight = 4
		budget      = 4 << 20
		baseline    = 24 << 20
	)
	_, store, cat := newCorpus(t)
	for s := range 150 {
		service := fmt.Sprintf("svc-%03d", s)
		events := requestEvents(service, 300, nil)
		for i, e := range events {
			e.Columns = append(e.Columns, segment.Column{Name: "user_id", Type: value.TypeString, Value: value.Value{String: fmt.Sprintf("user-%05d", (i*7+s)%60)}})
			for a := range 30 {
				e.Columns = append(e.Columns, segment.Column{Name: fmt.Sprintf("attr_%02d", a), Type: value.TypeString, Value: value.Value{String: fmt.Sprintf("value-%d-%d-%s", a, (i+s)%50, strings.Repeat("x", 20))}})
			}
			events[i] = e
		}
		seedEvents(t, store, cat, service, events)
	}
	app := querier.Open(querier.Options{Catalog: cat, Store: store, MaxInFlight: maxInFlight, RequestMemBudget: budget})
	ts := httptest.NewServer(app.Handler())
	t.Cleanup(ts.Close)

	runtime.GC()
	var start runtime.MemStats
	runtime.ReadMemStats(&start)
	ceiling := start.HeapAlloc + maxInFlight*budget + baseline
	defer debug.SetMemoryLimit(debug.SetMemoryLimit(int64(ceiling)))
	var peak uint64
	stop, sampled := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			peak = max(peak, m.HeapAlloc)
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	sel := `{"time":{"from":"2026-07-18T14:00:00Z","to":"2026-07-18T14:10:00Z"},"where":[{"field":"user_id","op":"eq","value":"user-00042"}]}`
	calls := []struct{ tool, body string }{
		{"rank_fields", `{"selector":` + sel + `}`},
		{"aggregate", `{"bins":"all","group_by":"level","limit":5,"measure":"count","selector":` + sel + `}`},
		{"aggregate", `{"bins":"all","group_by":"service","limit":5,"measure":"count","selector":` + sel + `}`},
		{"aggregate", `{"bins":"auto","group_by":"level","measure":"count","selector":` + sel + `}`},
	}
	codes := make([]string, len(calls))
	var callers sync.WaitGroup
	for i, c := range calls {
		callers.Go(func() {
			resp, err := http.Post(ts.URL+"/v1/"+c.tool, "application/json", strings.NewReader(c.body))
			if !assert.NoError(t, err) {
				return
			}
			defer func() { _ = resp.Body.Close() }()
			var body struct{ Code string }
			assert.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
			codes[i] = body.Code
		})
	}
	callers.Wait()
	close(stop)
	<-sampled
	assert.Less(t, peak, ceiling, "peak %d MiB over the start, answered %q", (peak-start.HeapAlloc)>>20, codes)
	for i, code := range codes {
		assert.Contains(t, []string{"", query.ErrCodeTooLarge}, code, calls[i].tool)
	}
	assert.Contains(t, codes, query.ErrCodeTooLarge, "the corpus must overrun the budget to test it")
}

// TestSearchLogsScanEndToEnd: a regex where returns only the lines it matches.
func TestSearchLogsScanEndToEnd(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	seedService(t, store, cat, "api", 10)
	seedService(t, store, cat, "worker", 5)

	_, baseURL := serve(t, dir, store)

	req := tools.SearchRequest{Selector: selector.Selector{Time: wideWindow(),
		Where: []selector.Predicate{{Field: "service", Op: selector.OpRegex, Value: "^w"}}}}
	env := postTool[tools.SearchResponse](t, baseURL, "/v1/search_logs", req)
	assert.Equal(t, "scan", env.Data.Mode)
	require.Len(t, env.Data.Lines, 5)
}

// TestSearchLogsFollowOutlivesTheRequestTimeout: a follow stream still sends the lines
// that arrive after the 30s other requests are cut at.
func TestSearchLogsFollowOutlivesTheRequestTimeout(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	seedService(t, store, cat, "worker", 5)
	_, baseURL := serve(t, dir, store)

	body, err := json.Marshal(tools.SearchRequest{Selector: selector.Selector{Time: wideWindow(),
		Where: []selector.Predicate{{Field: "service", Op: selector.OpRegex, Value: "^w"}}}})
	require.NoError(t, err)
	resp, err := http.Post(baseURL+"/v1/search_logs/follow", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	events := bufio.NewScanner(resp.Body)
	// Each event is the tail's, one cut short sending an error instead; one with no lines
	// says the follow is attached or keeps it alive.
	next := func() []tools.LogLine {
		for events.Scan() && events.Text() == "" {
		}
		var followed query.Envelope[tools.SearchResponse]
		require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(events.Text(), "data: ")), &followed))
		require.Equal(t, "tail", followed.Data.Mode, events.Text())
		return followed.Data.Lines
	}
	require.Empty(t, next())
	// Each segment seeded comes back, until the stream has outlived 30s.
	for n, start := 0, time.Now(); time.Since(start) < 31*time.Second; n++ {
		seedService(t, store, cat, fmt.Sprint("worker-", n), 5)
		for len(next()) == 0 {
		}
	}
}

// TestOneNodeSegmentAnswersPerService covers one collector sealing three services into
// one segment: each tool reads each service's own share of it, and the summary agrees
// with a scan of the rows.
func TestOneNodeSegmentAnswersPerService(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	var events []segment.Record
	for _, svc := range []struct {
		name, message, traces string
		rows                  int
	}{{"nginx", "served request", "shared", 300}, {"checkout", "charged card", "shared", 300}, {"db", "ran query", "db", 100}} {
		for i := range svc.rows {
			r := segment.LogRecord(seedStart.Add(time.Duration(i)*100*time.Millisecond), "info", svc.message,
				map[string]string{"service": svc.name, "trace_id": fmt.Sprint(svc.traces, i)})
			r.Columns = append(r.Columns, segment.Column{Name: "duration_ms", Type: value.TypeInt, Value: value.Value{Int: int64(i % 50)}})
			events = append(events, r)
		}
	}
	seedEvents(t, store, cat, "node-1", events)
	_, scanURL := serve(t, dir, store)
	_, hotURL := serveWithSummary(t, dir, store, &summary.Config{SketchColumns: []string{"duration_ms", "trace_id"}})

	for _, measure := range []string{"count", "p95:duration_ms"} {
		req := tools.AggregateRequest{Selector: selector.Selector{Time: wideWindow()}, Measure: measure, GroupBy: "service", Bins: "1h"}
		scan := postTool[tools.AggregateResponse](t, scanURL, "/v1/aggregate", req)
		hot := postTool[tools.AggregateResponse](t, hotURL, "/v1/aggregate", req)
		assert.Zero(t, hot.Meta.Cost.GETs, "%s is answered from the summary", measure)
		assert.Equal(t, seriesKeys(scan.Data.Series), seriesKeys(hot.Data.Series), measure)
		for i, s := range scan.Data.Series {
			assert.InEpsilon(t, total(s), total(hot.Data.Series[i]), 0.02, "%s of %s", measure, s.Key)
		}
	}

	patterns := postTool[tools.PatternsResponse](t, hotURL, "/v1/list_patterns", tools.PatternsRequest{Selector: selector.Selector{Time: wideWindow()}})
	require.Len(t, patterns.Data.Patterns, 3)
	rows := map[string]int64{"nginx": 300, "checkout": 300, "db": 100}
	for _, p := range patterns.Data.Patterns {
		assert.Equal(t, rows[p.Service], p.Count, p.Service)
	}

	graph := postTool[tools.ServiceGraphResponse](t, hotURL, "/v1/service_graph", tools.ServiceGraphRequest{
		Selector: selector.Selector{Time: wideWindow(), Where: []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "nginx"}}},
	})
	assert.True(t, slices.ContainsFunc(graph.Data.Edges, func(e tools.GraphEdge) bool {
		return e.From == "checkout" && e.To == "nginx" || e.From == "nginx" && e.To == "checkout"
	}), "nginx and checkout share traces: %+v", graph.Data.Edges)
}

// total sums a series over its bins, the one bin these tests seed being the only nonzero.
func total(s tools.Series) float64 {
	var sum float64
	for _, v := range s.Values {
		if v != nil {
			sum += *v
		}
	}
	return sum
}

func seriesKeys(series []tools.Series) []string {
	keys := make([]string, len(series))
	for i, s := range series {
		keys[i] = s.Key
	}
	return keys
}

// TestMCPAnyReplica: two queriers over one store behind a proxy that alternates between
// them serve one MCP client's every call, as replicas behind a Service with no affinity.
func TestMCPAnyReplica(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	seedService(t, store, cat, "api", 5)
	var replicas []http.Handler
	for range 2 {
		replicas = append(replicas, querier.Open(querier.Options{Catalog: catalog.New(storage.NewFS(dir), testTenant), Store: store}).Handler())
	}
	var calls atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		replicas[calls.Add(1)%2].ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)

	ctx := context.Background()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{Endpoint: proxy.URL + "/mcp"}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	for i := range 10 {
		res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "aggregate", Arguments: map[string]any{
			"selector": map[string]any{"time": wideWindow()}, "measure": "count", "group_by": "service", "bins": "all",
		}})
		require.NoError(t, err, "call %d", i)
		require.False(t, res.IsError, "call %d: %v", i, res.Content)
	}
	assert.Greater(t, calls.Load(), int64(10), "the calls reached both replicas")
}

// TestMCPResultBudget: an MCP search_logs result over the token budget keeps the head of
// its lines with each long value cut, and says so in meta.truncated, while /v1 answers the
// same call whole.
func TestMCPResultBudget(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	seedServiceWithAttrs(t, store, cat, "api", 200, map[string]string{"payload": strings.Repeat("x", 2000)})
	srv, baseURL := serve(t, dir, store)
	req := tools.SearchRequest{Selector: selector.Selector{Time: wideWindow()}, Limit: 200}

	whole := postTool[tools.SearchResponse](t, baseURL, "/v1/search_logs", req)
	require.Len(t, whole.Data.Lines, 200)
	require.Greater(t, whole.Meta.Tokens, api.DefaultMCPResultTokens, "the fixture must overflow the budget")
	assert.Equal(t, strings.Repeat("x", 2000), whole.Data.Lines[0].Attributes["payload"], "/v1 is unchanged")

	ctx := context.Background()
	st, ct := mcp.NewInMemoryTransports()
	ss, err := api.NewMCPServer(srv.Registry(), 0).Connect(ctx, st, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil).Connect(ctx, ct, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	args, err := json.Marshal(req)
	require.NoError(t, err)
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "search_logs", Arguments: json.RawMessage(args)})
	require.NoError(t, err)
	text := res.Content[0].(*mcp.TextContent).Text

	var got struct {
		Data tools.SearchResponse `json:"data"`
		Meta struct {
			Tokens    int `json:"token_estimate"`
			Truncated struct {
				Kept, Total int
				Hint        string
			} `json:"truncated"`
		} `json:"meta"`
	}
	require.NoError(t, json.Unmarshal([]byte(text), &got))
	assert.LessOrEqual(t, (len(text)+3)/4, api.DefaultMCPResultTokens, "the result fits the budget")
	assert.Equal(t, (len(text)+3)/4, got.Meta.Tokens, "token_estimate is the result's")
	assert.Equal(t, 200, got.Meta.Truncated.Total)
	assert.Less(t, got.Meta.Truncated.Kept, got.Meta.Truncated.Total)
	assert.Len(t, got.Data.Lines, got.Meta.Truncated.Kept)
	assert.NotEmpty(t, got.Meta.Truncated.Hint)
	for _, line := range got.Data.Lines {
		assert.Equal(t, strings.Repeat("x", 1024)+"…(+976 bytes)", line.Attributes["payload"])
	}
}
