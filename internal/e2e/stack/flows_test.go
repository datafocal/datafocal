//go:build stack

package stack

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/e2e"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

var benchCorpusDir = flag.String("stack.corpus", "", "a captured corpus's directory (scripts/capture-corpus.sh) the benchmark replays in place of the seeded one")

// flow is what a view, or an agent, asks the querier for to show one thing: its calls in
// order, measured as one.
type flow struct {
	name  string
	calls []flowCall
}

type flowCall struct {
	tool string
	req  any
	// pages is how many lines a search reads a page at a time, following each answer's next
	// as the trace view does, 0 for one read.
	pages int
}

func archetypeFlow(a e2e.Archetype) flow {
	return flow{name: a.Name, calls: []flowCall{{tool: a.Tool, req: a.Request}}}
}

// timeFlow runs f on the querier at base once, and then -stack.reps times warm, or records
// the refusal of its first call when every call is refused.
func timeFlow(t *testing.T, base string, f flow) queryReport {
	t.Helper()
	cold, refusal := runFlow(t, base, f)
	if cold.Refused == len(f.calls) {
		return queryReport{Name: f.name, Refused: refusal}
	}
	r := queryReport{Name: f.name, Cold: cold}
	var warm []float64
	// Warm runs stop at -stack.reps, or at minWarm once warmBudget has passed, so a flow of
	// minutes is timed a few times rather than holding the run for an hour.
	for start := time.Now(); len(warm) < *benchReps && (len(warm) < minWarm || time.Since(start) < warmBudget); {
		r.Warm, _ = runFlow(t, base, f)
		warm = append(warm, r.Warm.MS)
	}
	slices.Sort(warm)
	r.WarmP50MS = warm[len(warm)/2]
	r.WarmP95MS = warm[min(len(warm)-1, len(warm)*95/100)]
	r.WarmMaxMS = warm[len(warm)-1]
	return r
}

// A flow's warm runs are at least minWarm, and more only within warmBudget.
const (
	minWarm    = 3
	warmBudget = 5 * time.Second
)

// runFlow is f's calls' costs summed, its coverage the least of theirs, and the first
// refusal's body; what the querier allocated is read from its memory stats around them.
func runFlow(t *testing.T, base string, f flow) (callStats, string) {
	t.Helper()
	before := querierVars(t, base)
	stats, refusal := callStats{Coverage: 1}, ""
	for _, c := range f.calls {
		if c.pages > 0 {
			paged := pagedSearch(t, base, c)
			stats.Calls += paged.Calls
			stats.MS += paged.MS
			stats.ResponseBytes += paged.ResponseBytes
			stats.GETs += paged.GETs
			stats.BytesRead += paged.BytesRead
			stats.Tokens += paged.Tokens
			stats.Segments += paged.Segments
			continue
		}
		env, elapsed, raw := askFlow(t, base, c)
		stats.Calls++
		stats.MS += ms(elapsed)
		stats.ResponseBytes += int64(len(raw))
		if env.Meta.Snapshot == "" {
			stats.Refused++
			refusal = cmp.Or(refusal, string(raw))
			continue
		}
		one := statsOf(env.Meta, elapsed)
		stats.GETs += one.GETs
		stats.BytesRead += one.BytesRead
		stats.Tokens += one.Tokens
		stats.Segments += one.Segments
		stats.Coverage = min(stats.Coverage, one.Coverage)
	}
	if after, err := tryVars(base); err == nil { // a querier that died answers nothing
		stats.AllocBytes = after.Memstats.TotalAlloc - before.Memstats.TotalAlloc
	}
	return stats, refusal
}

// askFlow is tryCall answering a request the querier refuses or fails, which a view's request
// on another corpus can be, as refused rather than failing the run.
func askFlow(t *testing.T, base string, c flowCall) (query.Envelope[json.RawMessage], time.Duration, []byte) {
	t.Helper()
	body, err := json.Marshal(c.req)
	require.NoError(t, err)
	start := time.Now()
	resp, err := http.Post(base+"/v1/"+c.tool, "application/json", bytes.NewReader(body))
	if err != nil { // the querier died, which measureFlow reports
		return query.Envelope[json.RawMessage]{}, time.Since(start), []byte(err.Error())
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	elapsed := time.Since(start)
	require.NoError(t, err)
	// A failure, a timeout's among them, is what the view would show, so it is counted too.
	var env query.Envelope[json.RawMessage]
	if resp.StatusCode == http.StatusOK {
		require.NoErrorf(t, json.Unmarshal(raw, &env), "decode /v1/%s: %s", c.tool, raw)
	}
	return env, elapsed, raw
}

// pagedSearch reads c's search a page of c.pages lines at a time until its limit or its lines
// run out, summing the pages' costs.
func pagedSearch(t *testing.T, base string, c flowCall) callStats {
	t.Helper()
	req := c.req.(tools.SearchRequest)
	var stats callStats
	for read := 0; read < req.Limit; {
		page := req
		page.Limit = min(c.pages, req.Limit-read)
		env, elapsed, raw := tryCall[tools.SearchResponse](t, base, "search_logs", page)
		require.NotEmptyf(t, env.Meta.Snapshot, "a page refused: %s", raw)
		one := statsOf(env.Meta, elapsed)
		stats.Calls++
		stats.MS += one.MS
		stats.ResponseBytes += int64(len(raw))
		stats.GETs, stats.BytesRead, stats.Tokens, stats.Segments = stats.GETs+one.GETs, stats.BytesRead+one.BytesRead, stats.Tokens+one.Tokens, stats.Segments+one.Segments
		read += len(env.Data.Lines)
		if req.After = env.Data.Next; req.After == nil {
			break
		}
	}
	return stats
}

// tracePage is the lines the trace view reads a page at a time (web/packages/view-trace).
const tracePage = 1000

// vars is what the querier's /debug/vars says of its memory.
type vars struct {
	HeapLive uint64 `json:"heap_live_bytes"`
	Memstats struct {
		TotalAlloc uint64
		HeapAlloc  uint64
		HeapSys    uint64
		Sys        uint64
		NumGC      uint32
	} `json:"memstats"`
}

func querierVars(t *testing.T, base string) vars {
	t.Helper()
	v, err := tryVars(base)
	require.NoError(t, err)
	return v
}

func tryVars(base string) (vars, error) {
	var v vars
	resp, err := http.Get(base + "/debug/vars")
	if err != nil {
		return v, err
	}
	defer resp.Body.Close()
	return v, json.NewDecoder(resp.Body).Decode(&v)
}

// querierReport is a querier's start over the benchmark's bucket: until its summary serves,
// and at rest after it, once a collection has run since.
type querierReport struct {
	StartupMS      float64 `json:"startup_ms"`
	StartupPeakMB  float64 `json:"startup_peak_rss_mb"`
	IdleRSSMB      float64 `json:"idle_rss_mb"`
	IdleHeapLiveMB float64 `json:"idle_heap_live_mb"`
	IdleHeapSysMB  float64 `json:"idle_heap_sys_mb"`
	IdleSysMB      float64 `json:"idle_sys_mb"`
	// PeakRSSMB is the most any querier the flows ran on held, each started cold.
	PeakRSSMB float64 `json:"peak_rss_mb"`
}

// measureQuerier starts a querier, waits for its summary to serve and has it collect, so the
// live heap holds no garbage, then runs then on it.
func (s *stack) measureQuerier(t *testing.T, then func(base string)) querierReport {
	t.Helper()
	start := time.Now()
	p, base := s.startQuerier(t)
	defer p.stop(t)
	s.waitSummary(t, base)
	r := querierReport{StartupMS: ms(time.Since(start)), StartupPeakMB: procStatusMB(p.cmd.Process.Pid, "VmHWM:")}
	resp, err := http.Get(base + "/debug/pprof/heap?gc=1")
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, errors.Join(err, resp.Body.Close()))
	v := querierVars(t, base)
	r.IdleRSSMB = procStatusMB(p.cmd.Process.Pid, "VmRSS:")
	r.IdleHeapLiveMB, r.IdleHeapSysMB, r.IdleSysMB = mb(v.HeapLive), mb(v.Memstats.HeapSys), mb(v.Memstats.Sys)
	then(base)
	return r
}

func mb(n uint64) float64 { return float64(n) / (1 << 20) }

// procStatusMB is the line of /proc/<pid>/status starting with key, in MiB.
func procStatusMB(pid int, key string) float64 {
	data, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	for line := range strings.SplitSeq(string(data), "\n") {
		if kb, ok := strings.CutPrefix(line, key); ok {
			n, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimSpace(kb), " kB"))
			return float64(n) / 1024
		}
	}
	return 0
}

// measureFlow runs f on its own querier, started cold, noting the most it held in r; a
// querier that dies running it, out of memory say, is the flow's refusal.
func (s *stack) measureFlow(t *testing.T, f flow, r *querierReport, timed func(string, func())) queryReport {
	t.Helper()
	p, base := s.startQuerier(t)
	timed("summary build", func() { s.waitSummary(t, base) })
	q := timeFlow(t, base, f)
	r.PeakRSSMB = max(r.PeakRSSMB, procStatusMB(p.cmd.Process.Pid, "VmHWM:"))
	select {
	case <-p.done:
		return queryReport{Name: f.name, Refused: fmt.Sprintf("querier exited: %s\n%s", p.cmd.ProcessState, p.tail(5))}
	default:
		p.stop(t)
		return q
	}
}

// sessionField is the field the trace view gathers an agent's session by.
const sessionField = "sessionId"

// discover is the flows a corpus of kind is read by over [start, end), their inputs found on
// the querier at base: the AI view, session as the trace view and Run tab open it and as read
// whole, and a line's id; or the graph and logs views and a trace. Both count by service.
func discover(t *testing.T, base, kind, session string, start, end time.Time) []flow {
	t.Helper()
	all := selector.TimeSpec{From: rfc3339(start), To: rfc3339(end)}
	// Auto bins, since a capture's days at 1m bins pass the bins a series may hold.
	flows := []flow{{name: "aggregate: count by service, auto bins", calls: []flowCall{{tool: selector.StageAggregate, req: tools.AggregateRequest{Selector: selector.Selector{Time: all}, Measure: "count", GroupBy: segment.ColService, Bins: "auto"}}}}}
	if kind == "ai" {
		require.NotEmpty(t, session, "an ai corpus holds a transcript")
		where := []selector.Predicate{{Field: sessionField, Op: selector.OpEq, Value: session}}
		flows = append(flows, viewFlow(t, "ai view: open, last 24h", "view-ai", end.Add(-24*time.Hour), end, session, ""),
			traceFlow(t, base, "trace view: open the longest session", all, sessionField, session),
			flow{name: "run tab: open the longest session", calls: []flowCall{
				{tool: "search_logs", req: tools.SearchRequest{Limit: 10_000, Selector: selector.Selector{Time: all, Where: append(slices.Clone(where),
					selector.Predicate{Any: []selector.Predicate{{Field: "cost_usd", Op: selector.OpExists}, {Field: "input_tokens", Op: selector.OpExists}}})}}},
				{tool: "search_logs", req: tools.SearchRequest{Limit: 10_000, Columns: []string{sessionField}, Selector: selector.Selector{Time: all, Where: where}}}}},
			flow{name: "search: the longest session's lines whole", calls: []flowCall{
				{tool: "search_logs", req: tools.SearchRequest{Limit: 10_000, Selector: selector.Selector{Time: all, Where: where}}}}},
			lookupFlow(t, base, "point lookup: a line's uuid over all data", all, "uuid", where))
		return flows
	}
	// A trace is looked for in the newest hour, since a search of every copy passes its bound.
	hour := selector.TimeSpec{From: rfc3339(end.Add(-time.Hour)), To: rfc3339(end)}
	flows = append(flows, viewFlow(t, "graph view: open, last hour", "view-graph", end.Add(-time.Hour), end, "", ""),
		viewFlow(t, "logs view: open, last hour", "view-logs", end.Add(-time.Hour), end, "", firstValue(t, base, hour, segment.ColPatternID, nil)))
	for _, field := range []string{"trace_id", "traceId"} {
		if id := firstValue(t, base, hour, field, nil); id != "" {
			return append(flows, traceFlow(t, base, "trace view: open a trace", all, field, id),
				lookupFlow(t, base, "point lookup: a trace id over all data", hour, field, nil))
		}
	}
	return flows
}

// firstValue is field's value on the first line where finds holding it, "" for none.
func firstValue(t *testing.T, base string, time selector.TimeSpec, field string, where []selector.Predicate) string {
	t.Helper()
	env, _ := call[tools.SearchResponse](t, base, "search_logs", tools.SearchRequest{Limit: 1, Columns: []string{field},
		Selector: selector.Selector{Time: time, Where: append(slices.Clone(where), selector.Predicate{Field: field, Op: selector.OpExists})}})
	if len(env.Data.Lines) == 0 {
		return ""
	}
	return env.Data.Lines[0].Attributes[field]
}

// lookupFlow is a search for the value of field on a line where finds, over all data.
func lookupFlow(t *testing.T, base, name string, all selector.TimeSpec, field string, where []selector.Predicate) flow {
	t.Helper()
	value := firstValue(t, base, all, field, where)
	require.NotEmptyf(t, value, "no line holds %s", field)
	return flow{name: name, calls: []flowCall{{tool: "search_logs", req: tools.SearchRequest{
		Selector: selector.Selector{Where: []selector.Predicate{{Field: field, Op: selector.OpEq, Value: value}}}}}}}
}

// traceFlow is the trace view opening value under field (web/packages/view-trace): when its
// lines began and ended, the trace over that time, then every line its spans hold.
func traceFlow(t *testing.T, base, name string, all selector.TimeSpec, field, value string) flow {
	t.Helper()
	where := []selector.Predicate{{Field: field, Op: selector.OpEq, Value: value}}
	extent := tools.AggregateRequest{Selector: selector.Selector{Time: all, Where: where}, GroupBy: field, Bins: "all",
		Measures: []selector.MeasureSpec{{Measure: "min:ts"}, {Measure: "max:ts"}}}
	env, _ := call[tools.AggregateResponse](t, base, selector.StageAggregate, extent)
	require.NotEmpty(t, env.Data.Series, "%s %s has no lines", field, value)
	values := env.Data.Series[0].MeasureValues
	lasted := selector.TimeSpec{From: rfc3339(time.UnixMilli(int64(*values[0][0]))), To: rfc3339(time.UnixMilli(int64(*values[1][0]) + 1000))}
	trace := tools.TraceRequest{Field: field, Value: value, Time: lasted, Limit: 10_000}
	got, _ := call[tools.TraceResponse](t, base, "get_trace", trace)
	lines, from, to := 0, int64(1<<62), int64(0)
	for _, s := range got.Data.Spans {
		lines, from, to = lines+s.Lines, min(from, s.StartMs), max(to, s.StartMs+s.DurMs)
	}
	require.Positive(t, lines, "%s %s has no spans", field, value)
	spanned := selector.TimeSpec{From: rfc3339(time.UnixMilli(from)), To: rfc3339(time.UnixMilli(to + 1))}
	return flow{name: name, calls: []flowCall{{tool: selector.StageAggregate, req: extent}, {tool: "get_trace", req: trace},
		{tool: "search_logs", req: tools.SearchRequest{Limit: lines, Selector: selector.Selector{Time: spanned, Where: where}}, pages: tracePage}}}
}

// viewFlow is a view's recorded requests (web/packages/<view>/requests.json) re-timed onto
// [from, to) and fitted to the corpus, a slice's pattern made pattern unless it is "", but a
// trace's or a query's, which name what only the recorded corpus holds.
func viewFlow(t *testing.T, name, view string, from, to time.Time, session, pattern string) flow {
	t.Helper()
	var recorded []struct {
		Tool string         `json:"tool"`
		Req  map[string]any `json:"req"`
	}
	data, err := os.ReadFile(filepath.Join(moduleRoot(), "web", "packages", view, "requests.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &recorded))
	var first, last time.Time
	for _, r := range recorded {
		eachTime(r.Req, func(m map[string]any) {
			f, _ := time.Parse(time.RFC3339Nano, fmt.Sprint(m["from"]))
			l, _ := time.Parse(time.RFC3339Nano, fmt.Sprint(m["to"]))
			if first.IsZero() || f.Before(first) {
				first = f
			}
			last = maxTime(last, l)
		})
	}
	scale := float64(to.Sub(from)) / float64(last.Sub(first))
	at := func(v any) string {
		t, _ := time.Parse(time.RFC3339Nano, fmt.Sprint(v))
		return rfc3339(from.Add(time.Duration(float64(t.Sub(first)) * scale)))
	}
	f := flow{name: name}
	for _, r := range recorded {
		if _, named := r.Req["value"]; named || r.Req["q"] != nil {
			continue
		}
		// The snapshot the recording pinned is the fixture's, which no other catalog serves.
		delete(r.Req, "snapshot")
		fit(r.Req, session)
		slice, _ := r.Req["slice"].([]any)
		for _, p := range slice {
			if p, _ := p.(map[string]any); pattern != "" && p["field"] == segment.ColPatternID {
				p["value"] = pattern
			}
		}
		eachTime(r.Req, func(m map[string]any) { m["from"], m["to"] = at(m["from"]), at(m["to"]) })
		f.calls = append(f.calls, flowCall{tool: r.Tool, req: r.Req})
	}
	require.NotEmptyf(t, f.calls, "%s recorded no request another corpus answers", view)
	return f
}

// eachTime calls fn with every time range req holds.
func eachTime(v any, fn func(map[string]any)) {
	switch v := v.(type) {
	case map[string]any:
		if tm, ok := v["time"].(map[string]any); ok && tm["from"] != nil && tm["to"] != nil {
			fn(tm)
		}
		for _, k := range slices.Sorted(maps.Keys(v)) {
			if k != "time" {
				eachTime(v[k], fn)
			}
		}
	case []any:
		for _, e := range v {
			eachTime(e, fn)
		}
	}
}

// fit fits v's predicates to another corpus: one on a session's field names session, and one
// on any other value, which another corpus does not hold, is left out, as the view opened
// without it would ask.
func fit(v any, session string) {
	switch v := v.(type) {
	case map[string]any:
		if where, ok := v["where"].([]any); ok {
			v["where"] = fitWhere(where, session)
		}
		for k, e := range v {
			if k != "where" {
				fit(e, session)
			}
		}
	case []any:
		for _, e := range v {
			fit(e, session)
		}
	}
}

// fitWhere is where with fit's predicates: each kept naming session, each group of
// alternatives fitted and left out once none is left.
func fitWhere(where []any, session string) []any {
	kept := []any{}
	for _, e := range where {
		p, _ := e.(map[string]any)
		if alts, ok := p["any"].([]any); ok {
			if p["any"] = fitWhere(alts, session); len(p["any"].([]any)) > 0 {
				kept = append(kept, p)
			}
			continue
		}
		if _, valued := p["value"]; valued {
			if session == "" || !slices.Contains([]string{"run_id", "session_id", sessionField}, fmt.Sprint(p["field"])) {
				continue
			}
			p["field"], p["value"] = sessionField, session
		}
		kept = append(kept, e)
	}
	return kept
}

func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
