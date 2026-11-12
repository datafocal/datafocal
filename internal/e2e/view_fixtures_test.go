package e2e

import (
	"bytes"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/lang/logql"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// golden writes v to path under -update, else requires the file to hold it.
func golden(t *testing.T, path string, v any) {
	t.Helper()
	var got bytes.Buffer
	enc := json.NewEncoder(&got)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	require.NoError(t, enc.Encode(v))
	if *updateGoldens {
		require.NoError(t, os.WriteFile(path, got.Bytes(), 0o644))
		return
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err, "rerun with -update to create")
	assert.JSONEq(t, string(want), got.String(), "%s mismatch (rerun with -update if intentional)", path)
}

// writeLines writes entries to path one to a line.
func writeLines(t *testing.T, path string, entries []viewRequest) {
	t.Helper()
	var got bytes.Buffer
	enc := json.NewEncoder(&got)
	enc.SetEscapeHTML(false)
	got.WriteString("[\n")
	for i, e := range entries {
		require.NoError(t, enc.Encode(e))
		if i < len(entries)-1 {
			got.Truncate(got.Len() - 1)
			got.WriteString(",\n")
		}
	}
	got.WriteString("]\n")
	require.NoError(t, os.WriteFile(path, got.Bytes(), 0o644))
}

func readJSON(t *testing.T, path string, v any) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, v), path)
}

// requestRule is a malformed request and the refusal the engine gives it, which the
// frontend's fake client gives too (web/packages/api/src/fake.ts).
type requestRule struct {
	Tool    string          `json:"tool"`
	Req     json.RawMessage `json:"req"`
	Code    string          `json:"code"`
	Message string          `json:"message"`
}

// TestRequestRules pins the refusal of each request in request_rules.json, so a changed
// validation message fails here until the file, and the fake client, follow it.
func TestRequestRules(t *testing.T) {
	path := filepath.Join("..", "query", "tools", "testdata", "request_rules.json")
	var rules []requestRule
	readJSON(t, path, &rules)
	h := newHarness(t, false)
	for i, r := range rules {
		status, body := postToolRaw(t, h, r.Tool, r.Req)
		require.Equalf(t, http.StatusBadRequest, status, "/v1/%s %s: %s", r.Tool, r.Req, body)
		var refusal query.APIError
		require.NoError(t, json.Unmarshal(body, &refusal))
		rules[i].Code, rules[i].Message = refusal.Code, refusal.Message
	}
	golden(t, path, rules)
}

// viewRequest is a request a view sends, and the engine's answer to it.
type viewRequest struct {
	Tool   string          `json:"tool"`
	Req    json.RawMessage `json:"req"`
	Answer json.RawMessage `json:"answer,omitempty"`
}

// TestViewFixtures answers each view's requests on its corpus, by a querier each, requiring no
// server error and, answered twice, the same bytes; -update (make view-fixtures) answers once
// and writes them as fixtures/engine.json, which its tests and make preview read. Logs and graph
// read TestMain's incident corpus since the factory's has no service graph or id lines.
// The AI view is written twice more: as fixtures/sources.json on every agent source in
// testdata/ai ingested together, the shapes it must read without any one field in common.
// SLOs reads the incident corpus with the gateway's SLOs, in a directory of each request's own,
// and Views with the sample view and the SLO it draws.
func TestViewFixtures(t *testing.T) {
	factory, _ := factoryCorpus(t)
	derived, err := plan.NewDerived(querier.DefaultDerived())
	require.NoError(t, err)
	views := []struct{ name, corpus, set string }{
		{"view-ai", factory, "engine"}, {"view-ai", ingestAIFixtures(t), "sources"},
		{"view-trace", factory, "engine"}, {"view-trace", corpusDir, "search"},
		{"view-logs", corpusDir, "engine"}, {"view-graph", corpusDir, "engine"},
		{"view-resources", ingestResourceFixtures(t), "engine"}, {"view-slos", corpusDir, "engine"},
		{"view-views", factory, "engine"},
	}
	sample := func(file string) string {
		doc, err := os.ReadFile(filepath.Join("..", "..", "deploy", "definitions", file))
		require.NoError(t, err)
		return string(doc)
	}
	samples := []string{sample("view/querier.yaml"), sample("slo/querier-availability.yaml")}
	for _, view := range views {
		store := storage.NewFS(view.corpus)
		t.Run(view.name+"/"+view.set, func(t *testing.T) {
			t.Parallel() // each request reads the corpus through a querier of its own
			root := filepath.Join("..", "..", "web", "packages", view.name)
			requestsFile := map[string]string{"engine": "requests.json", "sources": "requests.sources.json", "search": "requests.search.json"}[view.set]
			var requests []viewRequest
			readJSON(t, filepath.Join(root, requestsFile), &requests)
			answer := func(r viewRequest) (querier.App, []byte) {
				opts := querier.Options{Catalog: catalog.New(store, tenant), Store: store, Derived: derived}
				switch view.name {
				case "view-slos":
					opts.Definitions = definitionsDir(t, gatewaySLO, promSLO, cleanSLO)
				case "view-views":
					opts.Definitions = definitionsDir(t, samples...)
				}
				app := querier.Open(opts)
				require.NoError(t, app.Refresh(t.Context()))
				rec := httptest.NewRecorder()
				app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+r.Tool, bytes.NewReader(r.Req)))
				assert.Less(t, rec.Code, http.StatusInternalServerError, "/v1/%s %s: %s", r.Tool, r.Req, rec.Body)
				return app, rec.Body.Bytes()
			}
			for i, r := range requests {
				app, body := answer(r)
				if !*updateGoldens {
					_, again := answer(r)
					assert.Equal(t, string(body), string(again), "/v1/%s %s answered twice", r.Tool, r.Req)
				}
				requests[i].Answer = body
				if r.Tool == "get_trace" {
					assertWholeTrace(t, app, r.Req, body)
				}
			}
			if *updateGoldens {
				require.NoError(t, os.MkdirAll(filepath.Join(root, "fixtures"), 0o755))
				writeLines(t, filepath.Join(root, "fixtures", view.set+".json"), requests)
			}
		})
	}
}

// TestMatchAllAnswersAsNoPredicate: on the factory, the empty search's {service=~".+"}
// answers as no predicate does, and so rank_fields reads no object.
func TestMatchAllAnswersAsNoPredicate(t *testing.T) {
	dir, _ := factoryCorpus(t)
	store := storage.NewFS(dir)
	app := querier.Open(querier.Options{Catalog: catalog.New(store, tenant), Store: store, Summary: &summary.Config{}})
	require.NoError(t, app.Refresh(t.Context()))
	parsed, err := logql.New().Parse(`{service=~".+"}`)
	require.NoError(t, err)
	day := selector.TimeSpec{From: "2026-07-27T00:00:00Z", To: "2026-07-28T00:00:00Z"}
	for tool, req := range map[string]func(selector.Selector) any{
		"rank_fields": func(s selector.Selector) any { return tools.FieldsRequest{Selector: s} },
		"aggregate": func(s selector.Selector) any {
			return tools.AggregateRequest{Selector: s, Measure: "count", GroupBy: "level", Bins: "all"}
		},
		"search_logs": func(s selector.Selector) any { return tools.SearchRequest{Selector: s} },
	} {
		call := func(where []selector.Predicate) query.Envelope[json.RawMessage] {
			body, err := json.Marshal(req(selector.Selector{Time: day, Where: where}))
			require.NoError(t, err)
			rec := httptest.NewRecorder()
			app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+tool, bytes.NewReader(body)))
			var env query.Envelope[json.RawMessage]
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
			require.NotEmpty(t, env.Meta.Snapshot, "%s: %s", tool, rec.Body)
			return env
		}
		matchAll := call(parsed.Where)
		assert.JSONEq(t, string(call(nil).Data), string(matchAll.Data), tool)
		if tool == "rank_fields" {
			assert.Zero(t, matchAll.Meta.Cost.GETs)
		}
	}
}

// TestSearchLogsOnTheFactory reads the factory's lines by what the views search them by:
// a service, a task, a session, and the lines around a sandbox's.
func TestSearchLogsOnTheFactory(t *testing.T) {
	dir, _ := factoryCorpus(t)
	derived, err := plan.NewDerived(querier.DefaultDerived())
	require.NoError(t, err)
	store := storage.NewFS(dir)
	app := querier.Open(querier.Options{Catalog: catalog.New(store, tenant), Store: store, Derived: derived})
	search := func(req tools.SearchRequest) []tools.LogLine {
		if req.Selector.Time.From == "" {
			req.Selector.Time = selector.TimeSpec{From: "2026-07-27T00:00:00Z", To: "2026-07-28T00:00:00Z"}
		}
		body, err := json.Marshal(req)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/search_logs", bytes.NewReader(body)))
		var env query.Envelope[tools.SearchResponse]
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
		require.NotEmpty(t, env.Data.Lines, "%s: %s", body, rec.Body)
		return env.Data.Lines
	}
	where := func(field, value string) tools.SearchRequest {
		return tools.SearchRequest{Selector: selector.Selector{Where: []selector.Predicate{{Field: field, Op: selector.OpEq, Value: value}}}}
	}
	task := search(where("service", "orchestrator"))[0].Attributes["task"]
	require.NotEmpty(t, task)
	for _, l := range search(where("task", task)) {
		assert.Equal(t, task, l.Attributes["task"])
	}
	// The orchestrator names the session it ran, and a column of its transcript's lines,
	// written in the hour before, names it too.
	const session = "842e33af-9917-f20e-e46e-5bec950ddc23"
	ran, err := time.Parse(time.RFC3339Nano, search(where("session", session))[0].Timestamp)
	require.NoError(t, err)
	hour := where("service", "workspace")
	hour.Selector.Time = selector.TimeSpec{From: ran.Add(-time.Hour).Format(time.RFC3339), To: ran.Format(time.RFC3339Nano)}
	var column string
	for _, l := range search(hour) {
		for k, v := range l.Attributes {
			if v == session {
				column = k
			}
		}
	}
	require.NotEmpty(t, column)
	lines := search(where(column, session))
	for _, l := range lines {
		assert.Equal(t, session, l.Attributes[column])
	}
	assert.True(t, slices.ContainsFunc(lines, func(l tools.LogLine) bool { return l.Attributes["cost_usd"] != "" }), "a turn's cost_usd")

	anchor := search(where("namespace", "factory-sandboxes"))[0]
	for _, l := range search(tools.SearchRequest{Around: &tools.LineRef{Segment: anchor.Segment, Row: anchor.Row}}) {
		assert.Equal(t, anchor.Attributes["pod"], l.Attributes["pod"])
	}
}

// TestPatternsAndFieldsOnTheFactory: list_patterns counts each pattern's lines a service or
// a level matches as aggregate does, and rank_fields over one service sees only its lines.
func TestPatternsAndFieldsOnTheFactory(t *testing.T) {
	dir, _ := factoryCorpus(t)
	store := storage.NewFS(dir)
	app := querier.Open(querier.Options{Catalog: catalog.New(store, tenant), Store: store})
	call := func(tool string, req, resp any) {
		body, err := json.Marshal(req)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+tool, bytes.NewReader(body)))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), resp))
	}
	day := selector.TimeSpec{From: "2026-07-27T00:00:00Z", To: "2026-07-28T00:00:00Z"}
	orchestrator := selector.Predicate{Field: "service", Op: selector.OpEq, Value: "orchestrator"}
	for _, p := range []selector.Predicate{orchestrator, {Field: "level", Op: selector.OpEq, Value: "error"}} {
		sel := selector.Selector{Time: day, Where: []selector.Predicate{p}}
		var listed query.Envelope[tools.PatternsResponse]
		call("list_patterns", tools.PatternsRequest{Selector: sel, Limit: 1000}, &listed)
		var counted query.Envelope[tools.AggregateResponse]
		call("aggregate", tools.AggregateRequest{Selector: sel, Measure: "count", GroupBy: "pattern_id", Bins: "all", Limit: 1000}, &counted)
		got, want := map[string]float64{}, map[string]float64{}
		for _, pat := range listed.Data.Patterns {
			got[pat.PatternID] = float64(pat.Count)
		}
		for _, s := range counted.Data.Series {
			want[s.Key] = *s.Values[0]
		}
		require.NotEmpty(t, got, p.Value)
		assert.Equal(t, want, got, p.Value)
	}

	// A service's lines split into those carrying the session column and those, "" to
	// group_by, without it; only the transcripts' service has any of the first.
	byService := func(where ...selector.Predicate) map[string]float64 {
		var counted query.Envelope[tools.AggregateResponse]
		call("aggregate", tools.AggregateRequest{Selector: selector.Selector{Time: day, Where: where}, Measure: "count", GroupBy: "service", Bins: "all"}, &counted)
		counts := map[string]float64{}
		for _, s := range counted.Data.Series {
			counts[s.Key] = *s.Values[0]
		}
		return counts
	}
	session := selector.Predicate{Field: seedcorpus.SessionColumn, Op: selector.OpExists}
	carrying, all := byService(session), byService()
	session.Op = selector.OpEq
	without := byService(session)
	assert.Equal(t, []string{"workspace"}, slices.Sorted(maps.Keys(carrying)))
	for service, n := range all {
		assert.Equal(t, n, carrying[service]+without[service], service)
	}

	var fields query.Envelope[tools.FieldsResponse]
	call("rank_fields", tools.FieldsRequest{Selector: selector.Selector{Time: day, Where: []selector.Predicate{orchestrator}}}, &fields)
	i := slices.IndexFunc(fields.Data.Fields, func(f tools.FieldStat) bool { return f.Name == "service" })
	require.GreaterOrEqual(t, i, 0)
	assert.Equal(t, int64(1), fields.Data.Fields[i].Cardinality)
}

// TestFocusOnTheFactory ranks the error burst of the factory's incident hour first and its
// health probe, routine in every bin, last, then focuses the hour on the burst's pattern: the
// pattern and its lines come back with their text, and its trend counts the anomaly's bin.
func TestFocusOnTheFactory(t *testing.T) {
	dir, _ := factoryCorpus(t)
	store := storage.NewFS(dir)
	app := querier.Open(querier.Options{Catalog: catalog.New(store, tenant), Store: store})
	call := func(tool string, req, resp any) {
		body, err := json.Marshal(req)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+tool, bytes.NewReader(body)))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), resp))
	}
	from, to := seedcorpus.IncidentHour()
	hour := selector.Selector{Time: selector.TimeSpec{From: from.Format(time.RFC3339), To: to.Format(time.RFC3339)}}
	var anomalies query.Envelope[tools.FindAnomaliesResponse]
	call("find_anomalies", tools.FindAnomaliesRequest{Selector: hour}, &anomalies)
	found := anomalies.Data.Anomalies
	require.NotEmpty(t, found)
	top, last := found[0], found[len(found)-1]
	assert.Equal(t, []string{"orchestrator", "dispatch failed", "error"}, []string{top.Service, top.Pattern, top.Level})
	assert.Equal(t, []any{"querier", "request", true}, []any{last.Service, last.Pattern, last.Routine})

	var focus query.Envelope[tools.FocusResponse]
	call("focus", tools.FocusRequest{Selector: hour,
		Slice: []selector.Predicate{{Field: "pattern_id", Op: selector.OpEq, Value: top.PatternID}}}, &focus)
	require.Empty(t, focus.Data.Errors)
	assert.True(t, slices.ContainsFunc(focus.Data.Related.Patterns, func(p tools.Pattern) bool {
		return p.PatternID == top.PatternID && p.Pattern == top.Pattern && p.Pattern != ""
	}), "related holds %s %q", top.PatternID, top.Pattern)
	require.NotEmpty(t, focus.Data.Examples.Lines)
	for _, l := range focus.Data.Examples.Lines {
		assert.Equal(t, top.PatternID, l.PatternID)
		assert.NotEmpty(t, l.Message)
	}
	require.Len(t, focus.Data.Trend.Series, 1)
	trend := focus.Data.Trend.Series[0]
	bin := (top.At.UnixMilli() - trend.Start) / trend.Step
	require.Less(t, bin, int64(len(trend.Values)))
	require.NotNil(t, trend.Values[bin])
	assert.Positive(t, *trend.Values[bin])
}

// TestPatternStatusOnTheFactory: the minute from twenty seconds into the incident hour's error
// burst holds three times the burst's lines of the minute before, and the heartbeat is routine.
func TestPatternStatusOnTheFactory(t *testing.T) {
	dir, _ := factoryCorpus(t)
	store := storage.NewFS(dir)
	app := querier.Open(querier.Options{Catalog: catalog.New(store, tenant), Store: store})
	from, _ := seedcorpus.IncidentHour()
	burst := from.Add(30*time.Minute + 20*time.Second)
	body, err := json.Marshal(tools.PatternsRequest{Selector: selector.Selector{
		Time: selector.TimeSpec{From: burst.Format(time.RFC3339), To: burst.Add(time.Minute).Format(time.RFC3339)}}})
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/list_patterns", bytes.NewReader(body)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var patterns query.Envelope[tools.PatternsResponse]
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &patterns))
	status := map[string]string{}
	for _, p := range patterns.Data.Patterns {
		status[p.Service+" "+p.Pattern] = p.Status
	}
	assert.Equal(t, "rising", status["orchestrator dispatch failed"])
	assert.Equal(t, "routine", status["kindnet Handling node with IPs: <*>"], status)
}

// assertWholeTrace requires the answer to a get_trace request to hold every span of its trace,
// each named, the last ending at the trace's last line in any of the fields it joined.
func assertWholeTrace(t *testing.T, app querier.App, request, answer []byte) {
	t.Helper()
	call := func(tool string, req, resp any) {
		body, err := json.Marshal(req)
		require.NoError(t, err)
		rec := httptest.NewRecorder()
		app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+tool, bytes.NewReader(body)))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), resp))
	}
	var trace query.Envelope[tools.TraceResponse]
	require.NoError(t, json.Unmarshal(answer, &trace))
	if trace.Data.Field == "" { // a refusal, carried with a 200
		return
	}
	match := selector.Predicate{Any: []selector.Predicate{{Field: trace.Data.Field, Op: selector.OpEq, Value: trace.Data.Value}}}
	for _, f := range trace.Data.Fields {
		match.Any = append(match.Any, selector.Predicate{Field: f, Op: selector.OpEq, Value: trace.Data.Value})
	}
	var req tools.TraceRequest
	require.NoError(t, json.Unmarshal(request, &req))
	sel := selector.Selector{Time: req.Time, Where: []selector.Predicate{match}}
	var last query.Envelope[tools.AggregateResponse]
	call("aggregate", tools.AggregateRequest{Selector: sel, Measure: "max:" + segment.ColTimestamp, Bins: "all"}, &last)
	var end int64
	for _, s := range trace.Data.Spans {
		assert.NotEmpty(t, s.Name, "span %s is unnamed", s.ID)
		end = max(end, s.StartMs+s.DurMs)
	}
	assert.Len(t, trace.Data.Spans, trace.Data.Total)
	assert.Equal(t, int64(*last.Data.Series[0].Values[0]), end, trace.Data.Value)
}

// TestAIViewAnswersAreTheFactorys: the engine's answers to the AI view's requests over the
// factory are the generator's spend, tokens and peak context, overall and per run, and its
// characters per kind of block and per tool result.
func TestAIViewAnswersAreTheFactorys(t *testing.T) {
	dir, built := factoryCorpus(t)
	var requests []viewRequest
	readJSON(t, filepath.Join("..", "..", "web", "packages", "view-ai", "requests.json"), &requests)
	// sums is, by group, the sum over bins of the measures named with prefix in the first
	// unfiltered request grouped by groupBy asking for one, and each group's last label.
	sums := func(groupBy, prefix string) (map[string]float64, map[string]string) {
		for _, r := range requests {
			var req tools.AggregateRequest
			require.NoError(t, json.Unmarshal(r.Req, &req))
			if r.Tool != "aggregate" || req.GroupBy != groupBy || len(req.Selector.Where) != 0 ||
				!slices.ContainsFunc(req.Measures, func(m selector.MeasureSpec) bool { return strings.HasPrefix(m.Measure, prefix) }) {
				continue
			}
			got, labels := map[string]float64{}, map[string]string{}
			for _, s := range aggregate(t, dir, req) {
				for i, m := range req.Measures {
					for _, v := range s.MeasureValues[i] {
						if v != nil && strings.HasPrefix(m.Measure, prefix) {
							got[s.Key] += *v
						}
					}
				}
				if n := len(s.MeasureLabels); n > 0 {
					labels[s.Key] = s.MeasureLabels[n-1][0]
				}
			}
			return got, labels
		}
		t.Fatalf("the view asks for no %s by %q", prefix, groupBy)
		return nil, nil
	}
	var want seedcorpus.SessionTruth
	blocks, results, peaks := map[string]float64{}, map[string]float64{}, map[string]float64{}
	spend, _ := sums("run_id", "sum:cost_usd")
	for session, s := range built.Truth.Sessions {
		want.CostUSD, want.Input, want.Output = want.CostUSD+s.CostUSD, want.Input+s.Input, want.Output+s.Output
		want.CacheRead, want.CacheWrite = want.CacheRead+s.CacheRead, want.CacheWrite+s.CacheWrite
		for k, n := range s.BlockChars {
			blocks[k] += float64(n)
		}
		for k, n := range s.ResultChars {
			results[k] += float64(n)
		}
		peaks[session] = float64(s.PeakContext)
		assert.InDelta(t, s.CostUSD, spend[session], 0.005, "session %s's spend", session)
	}
	total := func(groupBy, measure string) (n float64) {
		got, _ := sums(groupBy, measure)
		for _, v := range got {
			n += v
		}
		return n
	}
	assert.InDelta(t, want.CostUSD, total("model", "sum:cost_usd"), 0.005)
	assert.Equal(t, []float64{float64(want.Input), float64(want.Output), float64(want.CacheRead), float64(want.CacheWrite)},
		[]float64{total("model", "sum:input_tokens"), total("model", "sum:output_tokens"), total("model", "sum:cache_read_tokens"), total("model", "sum:cache_write_tokens")})
	calls, runs := sums("message.id", "max:")
	gotPeaks := map[string]float64{}
	for call, peak := range calls {
		gotPeaks[runs[call]] = max(gotPeaks[runs[call]], peak)
	}
	assert.Equal(t, peaks, gotPeaks, "peak context per run")
	gotResults, _ := sums("tool_name", "sum:context_tool_chars")
	delete(gotResults, "")
	assert.Equal(t, results, gotResults, "result characters per tool")
	assert.Equal(t, []float64{blocks["text"], blocks["thinking"], blocks["tool_use"] + blocks["tool_result"]},
		[]float64{total("content_role", "sum:context_text_chars"), total("content_role", "sum:context_thinking_chars"), total("content_role", "sum:context_tool_chars")},
		"characters per kind of block")
}
