package tools_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

var searchBase = time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)

// searchWindow is searchBase bracketed by an hour on each side.
var searchWindow = window(searchBase.Add(-time.Hour), searchBase.Add(time.Hour))

// searchRows is rows one-second-apart events, each with its own copy of attrs because the codec keeps the map it is handed.
func searchRows(start time.Time, rows int, attrs map[string]string) []segment.Record {
	msgs := make([]string, rows)
	for i := range msgs {
		msgs[i] = fmt.Sprintf("row %d", i)
	}
	events := messageEvents(start, msgs...)
	for i := range events {
		events[i] = segment.LogRecord(events[i].Timestamp, "info", msgs[i], maps.Clone(attrs))
	}
	return events
}

// searchLogs seeds segs into a fresh corpus and runs search_logs over it.
func searchLogs(t *testing.T, segs []traceSeg, req tools.SearchRequest) (query.Envelope[tools.SearchResponse], error) {
	t.Helper()
	cat, rd := readers(seedTrace(t, segs))
	return toolsOver(cat, rd, nil).SearchLogs(context.Background(), req)
}

// TestSearchLogsNeedle: an exact equality on an id returns the matching lines, only those, and at most Limit of them.
func TestSearchLogsNeedle(t *testing.T) {
	tests := []struct {
		name      string
		segs      []traceSeg
		field     string
		value     string
		limit     int
		wantLines int
	}{
		{
			name:  "trace_id",
			segs:  []traceSeg{{"api", searchRows(searchBase, 5, map[string]string{"trace_id": "trace-001"})}, {"api", searchRows(searchBase.Add(time.Minute), 5, map[string]string{"trace_id": "trace-002"})}},
			field: "trace_id", value: "trace-001", limit: 100, wantLines: 5,
		},
		{
			name:  "request_id",
			segs:  []traceSeg{{"api", searchRows(searchBase, 5, map[string]string{"request_id": "req-001"})}, {"api", searchRows(searchBase.Add(time.Minute), 5, map[string]string{"request_id": "req-002"})}},
			field: "request_id", value: "req-001", limit: 100, wantLines: 5,
		},
		{
			name:  "user_id",
			segs:  []traceSeg{{"api", searchRows(searchBase, 5, map[string]string{"user_id": "user-001"})}, {"api", searchRows(searchBase.Add(time.Minute), 5, map[string]string{"user_id": "user-002"})}},
			field: "user_id", value: "user-001", limit: 100, wantLines: 5,
		},
		{
			name:  "any id",
			segs:  []traceSeg{{"api", searchRows(searchBase, 5, map[string]string{"order_id": "order-001"})}, {"api", searchRows(searchBase.Add(time.Minute), 5, map[string]string{"order_id": "order-002"})}},
			field: "order_id", value: "order-001", limit: 100, wantLines: 5,
		},
		{
			name:  "a segment the filter admits contributes no false positive",
			segs:  []traceSeg{{"api", searchRows(searchBase, 10, map[string]string{"trace_id": "other-trace"})}, {"api", searchRows(searchBase.Add(time.Minute), 5, map[string]string{"trace_id": "target-trace"})}},
			field: "trace_id", value: "target-trace", limit: 100, wantLines: 5,
		},
		{
			name:  "limit caps the lines",
			segs:  []traceSeg{{"api", searchRows(searchBase, 150, map[string]string{"trace_id": "trace-001"})}},
			field: "trace_id", value: "trace-001", limit: 50, wantLines: 50,
		},
		{
			name:  "limit defaults to 100",
			segs:  []traceSeg{{"api", searchRows(searchBase, 150, map[string]string{"trace_id": "trace-001"})}},
			field: "trace_id", value: "trace-001", limit: 0, wantLines: 100,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, err := searchLogs(t, tc.segs, tools.SearchRequest{
				Selector: selector.Selector{
					Time:  searchWindow,
					Where: []selector.Predicate{{Field: tc.field, Op: selector.OpEq, Value: tc.value}},
				},
				Limit: tc.limit,
			})
			require.NoError(t, err)
			assert.Equal(t, "needle", env.Data.Mode)
			assert.Len(t, env.Data.Lines, tc.wantLines)
			for _, line := range env.Data.Lines {
				// The message's pattern names its number param_1.
				want := map[string]string{tc.field: tc.value, "param_1": strings.TrimPrefix(line.Message, "row ")}
				assert.Equal(t, want, line.Attributes, "line from another %s", tc.field)
			}
		})
	}
}

// TestSearchLogsColumns: a search given columns returns only those attributes, and none for none.
func TestSearchLogsColumns(t *testing.T) {
	segs := []traceSeg{{"api", searchRows(searchBase, 3, map[string]string{"trace_id": "trace-001", "user_id": "user-001"})}}
	for _, tc := range []struct {
		columns []string
		want    map[string]string
	}{
		{[]string{"user_id", "absent"}, map[string]string{"user_id": "user-001"}},
		{[]string{}, map[string]string{}},
	} {
		env, err := searchLogs(t, segs, tools.SearchRequest{Columns: tc.columns, Selector: selector.Selector{Time: searchWindow,
			Where: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: "trace-001"}}}})
		require.NoError(t, err)
		require.Len(t, env.Data.Lines, 3)
		for _, line := range env.Data.Lines {
			assert.Equal(t, tc.want, maps.Collect(maps.All(line.Attributes)), "columns %v", tc.columns)
		}
	}
}

// TestSearchLogsWideAllocBudget reads 2,000 lines of a segment with 1,500 sparse columns, as
// an agent's flattened transcript is, each line holding two of them. A line kept costs what it
// holds rather than a cell for every column, and asking for one column reads only it.
func TestSearchLogsWideAllocBudget(t *testing.T) {
	const lines, sparse = 2000, 1500
	var events []segment.Record
	for i := range lines {
		events = append(events, segment.LogRecord(searchBase.Add(time.Duration(i)*time.Millisecond), "info", "step",
			map[string]string{"session_id": "sess-1", "x" + strconv.Itoa(i%sparse): "v", "y" + strconv.Itoa((i+7)%sparse): "w"}))
	}
	cat, rd := readers(seedTrace(t, []traceSeg{{"claude", events}}))
	ts := toolsOver(cat, rd, nil)
	for _, tc := range []struct {
		columns []string
		budget  uint64
	}{{nil, 48 << 20}, {[]string{"session_id"}, 10 << 20}} { // twice what each measured, from 94 and 6.7 MB
		req := tools.SearchRequest{Limit: lines, Columns: tc.columns, Selector: selector.Selector{Time: searchWindow,
			Where: []selector.Predicate{{Field: "session_id", Op: selector.OpEq, Value: "sess-1"}}}}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		env, err := ts.SearchLogs(context.Background(), req)
		runtime.ReadMemStats(&after)
		require.NoError(t, err)
		require.Len(t, env.Data.Lines, lines)
		assert.LessOrEqual(t, after.TotalAlloc-before.TotalAlloc, tc.budget, "columns %v", tc.columns)
	}
}

// markedRows is 100 lines of attrs one-second-apart, the line at mark carrying trace_id "mark" so a needle search
// returns its reference, each followed half a second later by a line of noise in the same segment.
func markedRows(start time.Time, mark int, attrs, noise map[string]string) []segment.Record {
	var events []segment.Record
	for i := range 100 {
		a, at := maps.Clone(attrs), start.Add(time.Duration(i)*time.Second)
		if i == mark {
			a = map[string]string{"trace_id": "mark"}
			maps.Copy(a, attrs)
		}
		events = append(events, segment.LogRecord(at, "info", fmt.Sprintf("row %d", i), a),
			segment.LogRecord(at.Add(time.Second/2), "info", "noise", maps.Clone(noise)))
	}
	return events
}

// TestSearchLogsAround: the lines around a returned line are its producer's either side of it, in order, crossing
// into adjacent segments and never into another producer's lines.
func TestSearchLogsAround(t *testing.T) {
	later := searchBase.Add(5 * time.Minute)
	worker, podA, podB := map[string]string{"service": "worker"}, map[string]string{"pod": "a"}, map[string]string{"pod": "b"}
	rowsFrom := func(from, to int) []string {
		var msgs []string
		for i := from; i <= to; i++ {
			msgs = append(msgs, fmt.Sprintf("row %d", i))
		}
		return msgs
	}
	tests := []struct {
		name string
		segs []traceSeg
		want []string
	}{
		{
			name: "a line mid-segment",
			segs: []traceSeg{{"api", markedRows(searchBase, 50, nil, worker)}},
			want: rowsFrom(30, 70),
		},
		{
			name: "a line near a segment's end continues into the next",
			segs: []traceSeg{{"api", markedRows(searchBase, 97, nil, worker)}, {"worker", searchRows(later, 100, nil)}, {"api", markedRows(later, -1, nil, worker)}},
			want: slices.Concat(rowsFrom(77, 99), rowsFrom(0, 17)),
		},
		{
			name: "a line near a segment's start continues into the previous",
			segs: []traceSeg{{"api", markedRows(searchBase, -1, nil, worker)}, {"worker", searchRows(later, 100, nil)}, {"api", markedRows(later, 3, nil, worker)}},
			want: slices.Concat(rowsFrom(83, 99), rowsFrom(0, 23)),
		},
		{
			name: "another pod of the service is not the line's producer",
			segs: []traceSeg{{"api", markedRows(searchBase, 50, podA, podB)}},
			want: rowsFrom(30, 70),
		},
		{
			name: "a producer with fewer lines before the line returns those",
			segs: []traceSeg{{"api", markedRows(searchBase, 3, nil, worker)}},
			want: rowsFrom(0, 23),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cat, rd := readers(seedTrace(t, tc.segs))
			tl := toolsOver(cat, rd, nil)
			needle, err := tl.SearchLogs(context.Background(), tools.SearchRequest{
				Selector: selector.Selector{Where: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: "mark"}}},
			})
			require.NoError(t, err)
			require.Len(t, needle.Data.Lines, 1)
			mark := needle.Data.Lines[0]

			req := tools.SearchRequest{Around: &tools.LineRef{Segment: mark.Segment, Row: mark.Row}}
			env, err := tl.SearchLogs(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, "around", env.Data.Mode)
			var got []string
			for _, line := range env.Data.Lines {
				got = append(got, line.Message)
				assert.Equal(t, "api", line.Service)
			}
			assert.Equal(t, tc.want, got)
			assert.Contains(t, env.Data.Lines, mark)

			again, err := tl.SearchLogs(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, env.Data, again.Data)
		})
	}
}

// TestSearchLogsScan: any other selector over a bounded window returns exactly its matching lines, newest first.
func TestSearchLogsScan(t *testing.T) {
	segs := []traceSeg{
		{"api", messageEvents(searchBase, "request served", "upstream timeout", "request served")},
		{"worker", messageEvents(searchBase.Add(time.Minute), "job done", "job timeout")},
	}
	cat, rd := readers(seedTrace(t, segs))
	tl := toolsOver(cat, rd, nil)
	search := func(limit int, where ...selector.Predicate) []string {
		env, err := tl.SearchLogs(context.Background(), tools.SearchRequest{Selector: selector.Selector{Time: searchWindow, Where: where}, Limit: limit})
		require.NoError(t, err)
		assert.Equal(t, "scan", env.Data.Mode)
		assert.Equal(t, query.EstimateTokens(env.Data), env.Meta.Tokens, "the lines' tokens counted apart are the response's")
		var got []string
		for _, line := range env.Data.Lines {
			got = append(got, line.Service+": "+line.Message)
		}
		return got
	}
	all := []string{"worker: job timeout", "worker: job done", "api: request served", "api: upstream timeout", "api: request served"}
	assert.Equal(t, all[2:], search(0, selector.Predicate{Field: "service", Op: selector.OpEq, Value: "api"}))
	assert.Equal(t, all, search(0, selector.Predicate{Field: "service", Op: selector.OpRegex, Value: ".+"}))
	assert.Equal(t, all[:3], search(3))
	timeouts := []string{all[0], all[3]}
	assert.Equal(t, timeouts, search(0, selector.Predicate{Field: "message", Op: selector.OpMatch, Value: "timeout"}))

	env, err := tl.SearchLogs(context.Background(), tools.SearchRequest{Selector: selector.Selector{Time: searchWindow,
		Where: []selector.Predicate{{Field: "message", Op: selector.OpEq, Value: "job done"}}}})
	require.NoError(t, err)
	assert.Equal(t, all[1:2], search(0, selector.Predicate{Field: "pattern_id", Op: selector.OpEq, Value: env.Data.Lines[0].PatternID}))
}

// TestSearchLogsPages: a search read page by page, each after the last's next, returns the
// lines one read of them all does, lines logged at one instant split across pages included,
// and a page the lines run out on names no next.
func TestSearchLogsPages(t *testing.T) {
	var events []segment.Record
	for i := range 7 {
		at := searchBase.Add(time.Duration(i/3) * time.Second) // three lines an instant
		events = append(events, segment.LogRecord(at, "info", fmt.Sprint("line ", i), map[string]string{"service": "api"}))
	}
	cat, rd := readers(seedTrace(t, []traceSeg{{"api", events[:4]}, {"api", events[4:]}}))
	tl := toolsOver(cat, rd, nil)
	search := func(limit int, after *tools.LineCursor) tools.SearchResponse {
		env, err := tl.SearchLogs(context.Background(), tools.SearchRequest{Limit: limit, After: after, Selector: selector.Selector{Time: searchWindow}})
		require.NoError(t, err)
		return env.Data
	}
	all := search(100, nil)
	require.Len(t, all.Lines, 7)
	for _, size := range []int{1, 2, 3, 7} {
		var paged []tools.LogLine
		page := search(size, nil)
		for paged = page.Lines; page.Next != nil; paged = append(paged, page.Lines...) {
			page = search(size, page.Next)
		}
		assert.Equal(t, all.Lines, paged, "pages of %d", size)
	}
	assert.Nil(t, search(8, nil).Next, "a page the lines ran out on")
}

// TestSearchLogsBound: a search refuses past max_scan_segments only when it needs another segment to fill its limit.
func TestSearchLogsBound(t *testing.T) {
	segs := []traceSeg{{"api", searchRows(searchBase, 5, nil)}, {"api", searchRows(searchBase.Add(time.Minute), 5, nil)}}
	cat, rd := readers(seedTrace(t, segs))
	tl := toolsOver(cat, rd, nil)
	search := func(limit int) (query.Envelope[tools.SearchResponse], error) {
		return tl.SearchLogs(context.Background(), tools.SearchRequest{Selector: selector.Selector{Time: searchWindow}, Limit: limit, MaxScanSegments: 1})
	}
	_, err := search(6)
	var apiErr *query.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, query.ErrCodeScanTooLarge, apiErr.Code)
	assert.Equal(t, "search_logs: this search would scan more than 1 segments (2 in scope)", apiErr.Message)

	env, err := search(5)
	require.NoError(t, err)
	assert.Len(t, env.Data.Lines, 5)

	_, err = tl.SearchLogs(context.Background(), tools.SearchRequest{Selector: selector.Selector{
		Where: []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "api"}}}})
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, query.ErrCodeUnboundedScan, apiErr.Code)
	assert.Contains(t, apiErr.Message, "time range or an id equality")
}

// TestSearchLogsReadsSegmentsAhead: a needle's store reads of one segment wait on
// another's starting, which only reading ahead lets happen; it returns the lines of
// reading them one at a time, its early stop past the newest included, billing every read.
func TestSearchLogsReadsSegmentsAhead(t *testing.T) {
	var segs []traceSeg
	for i := range 3 {
		segs = append(segs, traceSeg{"api", searchRows(searchBase.Add(time.Duration(i)*time.Minute), 5, map[string]string{"trace_id": "t-1"})})
	}
	dir := seedTrace(t, segs)
	cat, rd := uncachedReaders(dir)
	ctx, fs := context.Background(), storage.NewFS(dir)
	var mu sync.Mutex
	started, overlapped := map[string]bool{}, make(chan struct{})
	blocking := segment.NewReader(storage.Fake{
		GetSuffixFn: func(key string, n int64) ([]byte, error) { return fs.GetSuffix(ctx, key, n) },
		GetRangeFn: func(key string, off, n int64) ([]byte, error) {
			mu.Lock()
			if !started[key] {
				if started[key] = true; len(started) == 2 {
					close(overlapped)
				}
			}
			mu.Unlock()
			select {
			case <-overlapped:
				return fs.GetRange(ctx, key, off, n)
			case <-time.After(5 * time.Second):
				return nil, errors.New("no other segment's read started")
			}
		},
	})
	search := func(rd segment.Reader, from time.Time, limit int) query.Envelope[tools.SearchResponse] {
		env, err := toolsOver(cat, rd, nil).SearchLogs(ctx, tools.SearchRequest{Limit: limit, Selector: selector.Selector{
			Time: window(from, searchBase.Add(time.Hour)), Where: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: "t-1"}}}})
		require.NoError(t, err)
		return env
	}
	all, early := search(blocking, searchBase, 15), search(blocking, searchBase, 5)
	assert.Equal(t, search(rd, searchBase, 15), all)
	assert.Equal(t, search(rd, searchBase.Add(2*time.Minute), 5).Data, early.Data)
	assert.Equal(t, all.Meta.Cost, early.Meta.Cost)
}

// TestSearchLogsRejects covers the requests search_logs refuses before reading anything.
func TestSearchLogsRejects(t *testing.T) {
	tests := []struct {
		name        string
		attrs       map[string]string
		sel         selector.Selector
		around      *tools.LineRef
		wantInvalid string // a substring of the InvalidRequestError, or "" when an APIError is expected
		wantCode    string
	}{
		{
			// Value is omitempty, and a row group without the column reads "" for every row.
			name:        "eq with an empty value",
			attrs:       map[string]string{"trace_id": "trace-001"},
			sel:         selector.Selector{Time: searchWindow, Where: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq}}},
			wantInvalid: "trace_id",
		},
		{
			name:        "around an unknown segment",
			around:      &tools.LineRef{Segment: "missing"},
			wantInvalid: "missing",
		},
		{
			name:     "match without a time bound",
			attrs:    map[string]string{},
			sel:      selector.Selector{Where: []selector.Predicate{{Field: "message", Op: selector.OpMatch, Value: "error"}}},
			wantCode: query.ErrCodeUnboundedScan,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := searchLogs(t, []traceSeg{{"api", searchRows(searchBase, 5, tc.attrs)}}, tools.SearchRequest{Selector: tc.sel, Around: tc.around})
			if tc.wantInvalid != "" {
				var invalid *query.InvalidRequestError
				require.ErrorAs(t, err, &invalid)
				assert.Contains(t, err.Error(), tc.wantInvalid)
				return
			}
			var apiErr *query.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tc.wantCode, apiErr.Code)
		})
	}
}

// TestCostNeedleBoundedByXorFilter: needle GETs and bytes track the xor-admitted candidates, not the xor-rejected decoys.
func TestCostNeedleBoundedByXorFilter(t *testing.T) {
	run := func(decoys int) query.Cost {
		segs := make([]traceSeg, 0, decoys+1)
		for i := range decoys {
			segs = append(segs, traceSeg{"api", searchRows(searchBase.Add(time.Duration(i)*time.Minute), 10, map[string]string{"trace_id": fmt.Sprintf("decoy-%d", i)})})
		}
		segs = append(segs, traceSeg{"api", searchRows(searchBase.Add(time.Duration(decoys)*time.Minute), 10, map[string]string{"trace_id": "target"})})
		env, err := searchLogs(t, segs, tools.SearchRequest{
			Selector: selector.Selector{
				Time:  searchWindow,
				Where: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: "target"}},
			},
			Limit: 1000,
		})
		require.NoError(t, err)
		return env.Meta.Cost
	}

	small, large := run(2), run(8)
	require.NotZero(t, small.GETs, "expected footers to be read")
	if large.GETs > small.GETs*2 || large.BytesRead > small.BytesRead*2 {
		t.Errorf("needle-lookup cost scaled with decoy count: 2-decoys=%+v vs 8-decoys=%+v", small, large)
	}
}

// TestSearchLogsNeedleByValues: an equality on req, an id by its values whatever its name,
// needs no time range, and the segments' filters prune it to the one holding the value.
func TestSearchLogsNeedleByValues(t *testing.T) {
	var segs []traceSeg
	for s := range 3 {
		rows := make([]segment.Record, 30)
		for i := range rows {
			// Each segment's values interleave the others', so only the filters tell them apart.
			req := map[string]string{"req": fmt.Sprintf("r-%d", i/3*3+s)}
			rows[i] = segment.LogRecord(searchBase.Add(time.Duration(s)*time.Minute+time.Duration(i)*time.Second), "info", "served", req)
		}
		segs = append(segs, traceSeg{"api", rows})
	}
	env, err := searchLogs(t, segs, tools.SearchRequest{Selector: selector.Selector{
		Where: []selector.Predicate{{Field: "req", Op: selector.OpEq, Value: "r-13"}}}})
	require.NoError(t, err)
	assert.Equal(t, "needle", env.Data.Mode)
	assert.Len(t, env.Data.Lines, 3)
	assert.Equal(t, 1, env.Meta.Cost.Segments)
}

// TestGetTraceAndGraphByValues: over a corpus whose only shared key is req, get_trace
// follows req across services with no time range, and service_graph links them by it.
func TestGetTraceAndGraphByValues(t *testing.T) {
	var api, db []segment.Record
	for i := range 30 {
		req := fmt.Sprintf("r-%d", i/3)
		api = append(api, logEvent(at(i*100), "info", "req", req, "GET /orders"))
		db = append(db, logEvent(at(i*100+10), "info", "req", req, "SELECT orders"))
	}
	cat, rd := readers(seedTrace(t, []traceSeg{{"api", api}, {"db", db}}))
	tl := toolsOver(cat, rd, nil)
	trace, err := tl.GetTrace(context.Background(), tools.TraceRequest{Field: "req", Value: "r-4"})
	require.NoError(t, err)
	var services []string
	for _, s := range trace.Data.Spans {
		services = append(services, s.Service)
	}
	assert.Equal(t, []string{"api", "db"}, slices.Compact(slices.Sorted(slices.Values(services))))

	graph, err := tl.ServiceGraph(context.Background(), tools.ServiceGraphRequest{Selector: selector.Selector{Time: traceWindow}})
	require.NoError(t, err)
	assert.True(t, slices.ContainsFunc(graph.Data.Edges, func(e tools.GraphEdge) bool {
		return e.From == "api" && e.To == "db" && e.Method == "co-occurrence"
	}), "%+v", graph.Data.Edges)
}
