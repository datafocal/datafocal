package tools_test

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

var updateGolden = flag.Bool("update", false, "update golden files")

var patternsBase = time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)

// seedCorpus writes a deterministic corpus whose per-service pattern counts the tests assert.
func seedCorpus(t *testing.T) string {
	t.Helper()
	dir, store, cat := newCorpus(t)

	// Service "api" yields exactly two pattern_ids because the varying token becomes a wildcard.
	writeSegment(t, store, cat, "api", messageEvents(patternsBase,
		"request 1 ok",
		"request 2 ok",
		"request 3 ok",
		"connection timeout from host1",
		"connection timeout from host2",
	))

	writeSegment(t, store, cat, "db", messageEvents(patternsBase.Add(time.Minute),
		"query executed in 10 ms",
		"query executed in 20 ms",
		"query executed in 30 ms",
		"query executed in 40 ms",
		"query executed in 50 ms",
		"query executed in 60 ms",
		"query executed in 70 ms",
	))

	writeSegment(t, store, cat, "web", messageEvents(patternsBase.Add(2*time.Minute),
		"GET /page 200",
		"GET /page 201",
		"POST /form 200",
		"POST /form 201",
	))

	return dir
}

// patternsRequest is a request over the hour either side of patternsBase.
func patternsRequest(sort string, limit int) tools.PatternsRequest {
	return tools.PatternsRequest{
		Selector: selector.Selector{
			Time: window(patternsBase.Add(-time.Hour), patternsBase.Add(time.Hour)),
		},
		Sort:  sort,
		Limit: limit,
	}
}

func callPatterns(t *testing.T, dir string, req tools.PatternsRequest) query.Envelope[tools.PatternsResponse] {
	t.Helper()
	cat, rd := readers(dir)
	env, err := toolsOver(cat, rd, nil).Patterns(context.Background(), req)
	require.NoError(t, err)
	return env
}

// TestPatternsGolden pins the whole envelope, and that the answer reads one footer per pruned descriptor.
func TestPatternsGolden(t *testing.T) {
	t.Parallel()
	dir := seedCorpus(t)
	env := callPatterns(t, dir, patternsRequest("count", 50))

	cat := catalog.New(storage.NewFS(dir), testTenant)
	version, err := cat.Version(context.Background())
	require.NoError(t, err)
	descs, err := cat.Prune(context.Background(), version, selector.Resolved{
		Window: selector.Window{From: patternsBase.Add(-time.Hour), To: patternsBase.Add(time.Hour)},
	})
	require.NoError(t, err)
	assert.Equal(t, len(descs), env.Meta.Cost.GETs, "one footer per pruned descriptor")

	goldenPath := filepath.Join("testdata", "patterns.golden.json")
	gotJSON, err := json.MarshalIndent(env, "", "  ")
	require.NoError(t, err)
	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(goldenPath), 0o755))
		require.NoError(t, os.WriteFile(goldenPath, append(gotJSON, '\n'), 0o644))
		t.Log("golden file updated; rerun without -update to validate")
		return
	}
	raw, err := os.ReadFile(goldenPath)
	require.NoError(t, err, "run with -update to create")
	assert.JSONEq(t, string(raw), string(gotJSON), "golden mismatch (rerun with -update if intentional)")
}

// TestPatternsSortAndPaging asserts each sort mode is non-increasing and that paging two at a time reassembles the full answer on one snapshot.
func TestPatternsSortAndPaging(t *testing.T) {
	t.Parallel()
	dir := seedCorpus(t)

	tests := []struct {
		sort    string
		extract func(tools.Pattern) float64
	}{
		{"count", func(p tools.Pattern) float64 { return float64(p.Count) }},
		{"rate", func(p tools.Pattern) float64 { return p.Rate }},
	}
	for _, tc := range tests {
		t.Run(tc.sort, func(t *testing.T) {
			full := callPatterns(t, dir, patternsRequest(tc.sort, 50))
			all := full.Data.Patterns
			require.GreaterOrEqual(t, len(all), 3, "pagination needs at least 3 patterns")
			for i := 1; i < len(all); i++ {
				assert.LessOrEqual(t, tc.extract(all[i]), tc.extract(all[i-1]), "patterns[%d] is above patterns[%d]", i, i-1)
			}

			var collected []tools.Pattern
			req := patternsRequest(tc.sort, 2)
			req.Snapshot = full.Meta.Snapshot
			for pages := 1; ; pages++ {
				require.LessOrEqual(t, pages, 20, "pagination did not terminate")
				env := callPatterns(t, dir, req)
				collected = append(collected, env.Data.Patterns...)
				assert.Equal(t, full.Meta.Snapshot, env.Meta.Snapshot, "page %d snapshot", pages)
				if req.Cursor = env.Meta.Cursor; req.Cursor == "" {
					break
				}
			}
			assert.Equal(t, all, collected, "paginated result != full result")
		})
	}
}

// TestPatternsInvalidRequest asserts each malformed request, including a cursor replayed under another sort or window, is an InvalidRequestError.
func TestPatternsInvalidRequest(t *testing.T) {
	t.Parallel()
	dir := seedCorpus(t)
	cat, rd := readers(dir)
	ctx := context.Background()

	first, err := toolsOver(cat, rd, nil).Patterns(ctx, patternsRequest("count", 2))
	require.NoError(t, err)
	require.NotEmpty(t, first.Meta.Cursor, "expected cursor from first page")
	cursor := first.Meta.Cursor

	tests := []struct {
		name string
		mod  func(*tools.PatternsRequest)
	}{
		{"unknown_sort", func(r *tools.PatternsRequest) { r.Sort = "bogus" }},
		{"malformed_cursor", func(r *tools.PatternsRequest) { r.Cursor = "not-valid!!!" }},
		{"cursor_sort_mismatch", func(r *tools.PatternsRequest) { r.Sort, r.Limit, r.Cursor = "rate", 2, cursor }},
		// Offset-based paging over a widened window is a different result set, so it must be rejected rather than silently served.
		{"cursor_selector_mismatch", func(r *tools.PatternsRequest) {
			r.Selector.Time = window(patternsBase.Add(-2*time.Hour), patternsBase.Add(time.Hour))
			r.Limit, r.Cursor = 2, cursor
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := patternsRequest("count", 50)
			tc.mod(&req)
			_, err := toolsOver(cat, rd, nil).Patterns(ctx, req)
			var inv *query.InvalidRequestError
			require.ErrorAs(t, err, &inv)
		})
	}
}

// signalCorpus writes a corpus that separates novelty from volume: "common" was
// already being emitted a day before the query window and again inside it, while
// "novel" appears only inside it. That history is the only thing that makes the
// distinction checkable — it lives in the manifest, not in anyone's cache.
func signalCorpus(t *testing.T) string {
	t.Helper()
	dir, store, cat := newCorpus(t)

	writeSegment(t, store, cat, "svc-common", messageEvents(patternsBase.Add(-2*time.Hour),
		"common message here",
	))
	writeSegment(t, store, cat, "svc-common", messageEvents(patternsBase,
		"common message here",
	))
	writeSegment(t, store, cat, "svc-novel", messageEvents(patternsBase.Add(2*time.Minute),
		"novel unseen event",
	))
	return dir
}

// TestPatternsSortSignal: patterns come back ordered by signal descending, and the
// pattern with no history before the window carries Status="new" while the one
// the manifest records from a day earlier does not — the invariants list_patterns
// advertises for the "new" status word.
//
// The summary is a cache, so the answer must also be identical without one: every
// term of the score is measured off durable artifacts.
func TestPatternsSortSignal(t *testing.T) {
	t.Parallel()
	dir := signalCorpus(t)
	cr, rd := readers(dir)
	// The window opens after the earlier "common" segment, which is the history novelty and rate-of-change measure against.
	req := patternsRequest("signal", 50)

	rs := wireSummary(t, cr, rd, summary.Config{})
	cold := callPatterns(t, dir, req)
	warm, err := toolsOver(cr, rd, rs).Patterns(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, cold.Data, warm.Data, "sort=signal answer must not depend on summary warmth")

	patterns := warm.Data.Patterns
	require.GreaterOrEqual(t, len(patterns), 2)
	for i := 1; i < len(patterns); i++ {
		assert.LessOrEqual(t, patterns[i].Signal, patterns[i-1].Signal, "patterns[%d] is above patterns[%d]", i, i-1)
	}
	head := patterns[0]
	assert.Equal(t, "new", head.Status, "novel pattern ranks first")
	assert.Equal(t, "svc-novel", head.Service)
	var commonSeen bool
	for _, p := range patterns[1:] {
		if p.Service == "svc-common" {
			commonSeen = true
			assert.Empty(t, p.Status, "the manifest records the common pattern a day earlier")
			assert.Less(t, p.Signal, head.Signal)
		}
	}
	assert.True(t, commonSeen, "expected a svc-common pattern below the novel head, got %d results", len(patterns))
}

// TestPatternsEmptyResult asserts an empty corpus answers a non-nil empty list rather than an error.
func TestPatternsEmptyResult(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_ = catalog.New(storage.NewFS(dir), testTenant)

	env := callPatterns(t, dir, patternsRequest("count", 50))
	assert.Equal(t, tools.PatternsResponse{Patterns: []tools.Pattern{}}, env.Data)
}

// mixedCorpus writes one segment holding services a and b, its first row a second
// before patternsBase.
func mixedCorpus(t *testing.T) string {
	t.Helper()
	dir, store, cat := newCorpus(t)
	rows := []struct{ service, level, message, attr string }{
		{"a", "info", "request 1 ok", "route"},
		{"a", "error", "connection timeout from host1", "route"},
		{"a", "error", "connection timeout from host2", "route"},
		{"b", "info", "query executed in 10 ms", "disk"},
		{"b", "error", "disk full on sda1", "disk"},
	}
	events := make([]segment.Record, len(rows))
	for i, r := range rows {
		attrs := map[string]string{"service": r.service, r.attr: "x"}
		events[i] = segment.LogRecord(patternsBase.Add(time.Duration(i-1)*time.Second), r.level, r.message, attrs)
	}
	writeSegment(t, store, cat, "mixed", events)
	return dir
}

// TestPatternsCountMatchedRows: list_patterns counts the rows the selector matches, the
// same with and without a summary, and names a pattern by the service it equates.
func TestPatternsCountMatchedRows(t *testing.T) {
	t.Parallel()
	dir := mixedCorpus(t)
	cr, rd := readers(dir)
	rs := wireSummary(t, cr, rd, summary.Config{})
	hour := window(patternsBase.Add(-time.Hour), patternsBase.Add(time.Hour))
	counts := func(tl tools.Tools, sel selector.Selector) map[string]int64 {
		env, err := tl.Patterns(context.Background(), tools.PatternsRequest{Selector: sel})
		require.NoError(t, err)
		got := map[string]int64{}
		for _, p := range env.Data.Patterns {
			got[p.Service+": "+p.Pattern] = p.Count
		}
		return got
	}
	for _, tc := range []struct {
		name string
		sel  selector.Selector
		want map[string]int64
	}{
		{"service", selector.Selector{Time: hour, Where: []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "a"}}},
			map[string]int64{"a: request <*> ok": 1, "a: connection timeout from <*>": 2}},
		{"level", selector.Selector{Time: hour, Where: []selector.Predicate{{Field: "level", Op: selector.OpEq, Value: "error"}}},
			map[string]int64{"a: connection timeout from <*>": 2, "b: disk full on <*>": 1}},
		{"line", selector.Selector{Time: hour, Where: []selector.Predicate{{Field: "message", Op: selector.OpMatch, Value: "timeout"}}},
			map[string]int64{"a: connection timeout from <*>": 2}},
		{"window edge", selector.Selector{Time: window(patternsBase, patternsBase.Add(time.Hour))},
			map[string]int64{"a: connection timeout from <*>": 2, "b: query executed in <*> ms": 1, "b: disk full on <*>": 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, counts(toolsOver(cr, rd, nil), tc.sel))
			assert.Equal(t, tc.want, counts(toolsOver(cr, rd, rs), tc.sel), "with a summary")
		})
	}

	_, err := toolsOver(cr, rd, nil).Patterns(context.Background(), tools.PatternsRequest{})
	var inv *query.InvalidRequestError
	require.ErrorAs(t, err, &inv)
}

// TestPatternsRising: a pattern reads rising at twice its lines per bin before the window,
// counting the baseline from the corpus's first line, and never from a zero baseline.
func TestPatternsRising(t *testing.T) {
	t.Parallel()
	dir, store, cat := newCorpus(t)
	// The corpus begins an hour into the two-hour baseline, besides one earlier line.
	writeSegment(t, store, cat, "svc", messageEvents(patternsBase.Add(-3*time.Hour), "cache warmed"))
	for slot := patternsBase.Add(-time.Hour); slot.Before(patternsBase.Add(2 * time.Hour)); slot = slot.Add(10 * time.Minute) {
		lines := []string{"steady heartbeat from the scheduler", "queue backlog grew"}
		if !slot.Before(patternsBase) {
			lines = append(lines, "queue backlog grew", "cache warmed")
		}
		writeSegment(t, store, cat, "svc", messageEvents(slot, lines...))
	}
	cr, rd := readers(dir)
	req := tools.PatternsRequest{Selector: selector.Selector{Time: window(patternsBase, patternsBase.Add(2*time.Hour))}}
	for name, rs := range map[string]summary.Store{"cold": nil, "warm": wireSummary(t, cr, rd, summary.Config{})} {
		env, err := toolsOver(cr, rd, rs).Patterns(context.Background(), req)
		require.NoError(t, err)
		status := map[string]string{}
		for _, p := range env.Data.Patterns {
			status[p.Pattern] = p.Status
		}
		assert.Equal(t, map[string]string{
			"queue backlog grew":                  "rising",
			"steady heartbeat from the scheduler": "",
			"cache warmed":                        "",
		}, status, "%s: doubled per bin; doubled in total but flat per bin; known but absent from the baseline", name)
	}
}

// TestPatternsByAColumnPastTheSummarysValues: broken down by a column with more values than the
// summary keeps series for, as a fleet's host column is, list_patterns answers from the segments
// as it does without a summary, rather than from what is left after the series cap evicts.
func TestPatternsByAColumnPastTheSummarysValues(t *testing.T) {
	t.Parallel()
	dir, store, cat := newCorpus(t)
	for i, host := range []string{"host-1", "host-2", "host-3"} {
		at := patternsBase.Add(time.Duration(i) * time.Minute)
		writeSegment(t, store, cat, "svc", []segment.Record{
			segment.LogRecord(at, "error", "disk full on sda", map[string]string{"service": "svc", "instance": host}),
			segment.LogRecord(at.Add(time.Second), "error", "disk full on sda", map[string]string{"service": "svc", "instance": host}),
		})
	}
	cr, rd := readers(dir)
	req := tools.PatternsRequest{Selector: selector.Selector{Time: window(patternsBase.Add(-time.Hour), patternsBase.Add(time.Hour))}, By: "instance"}
	byHost := func(rs summary.Store) map[string]int64 {
		env, err := toolsOver(cr, rd, rs).Patterns(context.Background(), req)
		require.NoError(t, err)
		out := map[string]int64{}
		for _, p := range env.Data.Patterns {
			out[p.Value] += p.Count
		}
		return out
	}
	want := map[string]int64{"host-1": 2, "host-2": 2, "host-3": 2}
	assert.Equal(t, want, byHost(nil))
	assert.Equal(t, want, byHost(wireSummary(t, cr, rd, summary.Config{MaxValueSeries: 2, MaxColumnValues: 2})))
}
