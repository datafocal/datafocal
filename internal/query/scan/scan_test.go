package scan

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func ts(d time.Duration) []byte { return []byte(t0.Add(d).Format(time.RFC3339Nano)) }

// chunkReader serves chunks by row group and logs each Columns call as "group:cols", or
// "group:cols@rows" for one decoding only rows;
// a row group missing from chunks fails its read with err. Row groups are read
// concurrently, so the log is guarded and its order across row groups is not fixed.
func chunkReader(chunks map[int]map[string][][]byte, reads *[]string, err error) segment.Fake {
	var mu sync.Mutex
	return segment.Fake{ColumnsFn: func(_ segment.ObjectLocation, _ segment.Footer, rg int, rows []int, cols []string) (map[string]segment.ColumnChunk, segment.Cost, error) {
		read := fmt.Sprintf("%d:%s", rg, strings.Join(cols, ","))
		if rows != nil {
			read += fmt.Sprint("@", rows)
		}
		mu.Lock()
		*reads = append(*reads, read)
		mu.Unlock()
		group, ok := chunks[rg]
		if !ok {
			return nil, segment.Cost{GETs: 1}, err
		}
		out := map[string]segment.ColumnChunk{}
		for _, col := range cols {
			if values, carried := group[col]; carried {
				out[col] = segment.TextChunk(values)
			}
		}
		return out, segment.Cost{GETs: len(cols), Bytes: 10}, nil
	}}
}

func levelIs(v string) []selector.ParsedPredicate {
	return selector.ParsePredicates([]selector.Predicate{{Field: segment.ColLevel, Op: selector.OpEq, Value: v}})
}

// matched is a row reduced to what a case asserts: its segment, its time and the
// values of the query's columns.
type matched struct {
	segment string
	at      time.Time
	values  string
}

// TestScan reads row groups through a fake segment reader: each case is the chunks, the
// plan and the query in, and the reads made, the rows passed back and their cost out.
func TestScan(t *testing.T) {
	down := errors.New("store down")
	settled := segment.Descriptor{ID: "s1", Stream: segment.Stream{Labels: map[string]string{"region": "eu"}}}
	carrying := segment.Footer{Offsets: segment.OffsetTable{}}
	carrying.Offsets.Set(0, "host", segment.ByteRange{})

	tests := []struct {
		name   string
		chunks map[int]map[string][][]byte
		items  []plan.WorkItem
		q      Query
		// take stops reading after this many rows; zero reads them all.
		take int

		wantReads []string
		wantRows  []matched
		wantCost  query.Cost
		wantErr   error
	}{
		{
			name: "predicate columns and ts first, the rest only where a row in the window matched, never a settled column",
			chunks: map[int]map[string][][]byte{
				0: {"ts": {ts(0), ts(time.Second)}, "level": {[]byte("error"), []byte("info")}, "message": {[]byte("boom"), []byte("ok")}},
				1: {"ts": {ts(time.Hour)}, "level": {[]byte("error")}, "message": {[]byte("late")}},
			},
			items:     []plan.WorkItem{{Segment: settled, RowGroup: 0}, {Segment: settled, RowGroup: 1}},
			q:         Query{Window: selector.Window{From: t0, To: t0.Add(time.Minute)}, Where: levelIs("error"), Columns: []string{segment.ColMessage, "region"}},
			wantReads: []string{"0:level,ts", "0:message@[0]", "1:level,ts"},
			wantRows:  []matched{{"s1", t0, "boom|eu"}},
			wantCost:  query.Cost{GETs: 5, BytesRead: 30},
		},
		{
			name:      "an untimed query reads no ts, and with nothing to filter on reads everything at once",
			chunks:    map[int]map[string][][]byte{0: {"pod": {[]byte("p1"), []byte("p2")}, "host": {[]byte("h1"), []byte("h2")}}},
			items:     []plan.WorkItem{{Segment: segment.Descriptor{ID: "s1"}}},
			q:         Query{Columns: []string{"pod", "host"}},
			wantReads: []string{"0:pod,host"},
			wantRows:  []matched{{"s1", time.Time{}, "p1|h1"}, {"s1", time.Time{}, "p2|h2"}},
			wantCost:  query.Cost{GETs: 2, BytesRead: 10},
		},
		{
			name:   "a row group without a required column is skipped before any read",
			chunks: map[int]map[string][][]byte{0: {"host": {[]byte("h1")}}},
			items: []plan.WorkItem{
				{Segment: segment.Descriptor{ID: "without"}, Footer: segment.Footer{Offsets: segment.OffsetTable{}}},
				{Segment: segment.Descriptor{ID: "with"}, Footer: carrying},
			},
			q:         Query{Columns: []string{"host"}, Require: []string{"host"}},
			wantReads: []string{"0:host"},
			wantRows:  []matched{{"with", time.Time{}, "h1"}},
			wantCost:  query.Cost{GETs: 1, BytesRead: 10},
		},
		{
			name:      "a caller that stops is billed for the row groups read ahead, and no more are read",
			chunks:    hostGroups(readAhead + 2),
			items:     groupItems(readAhead + 2),
			q:         Query{Columns: []string{"host"}},
			take:      1,
			wantReads: hostReads(readAhead),
			wantRows:  []matched{{"s1", time.Time{}, "h0"}},
			wantCost:  query.Cost{GETs: readAhead, BytesRead: 10 * readAhead},
		},
		{
			name:      "rows come back in row-group order though the groups are read concurrently",
			chunks:    hostGroups(readAhead + 2),
			items:     groupItems(readAhead + 2),
			q:         Query{Columns: []string{"host"}},
			wantReads: hostReads(readAhead + 2),
			wantRows:  hostRows(readAhead + 2),
			wantCost:  query.Cost{GETs: readAhead + 2, BytesRead: 10 * (readAhead + 2)},
		},
		{
			name:      "exists passes only the rows carrying the column, not a null row of its chunk",
			chunks:    map[int]map[string][][]byte{0: {"session": {[]byte("s-1"), nil}, "host": {[]byte("h1"), []byte("h2")}}},
			items:     []plan.WorkItem{{Segment: segment.Descriptor{ID: "s1"}}},
			q:         Query{Where: selector.ParsePredicates([]selector.Predicate{{Field: "session", Op: selector.OpExists}}), Columns: []string{"host"}},
			wantReads: []string{"0:session", "0:host@[0]"},
			wantRows:  []matched{{"s1", time.Time{}, "h1"}},
			wantCost:  query.Cost{GETs: 2, BytesRead: 20},
		},
		{
			name:      `eq "" passes a null row, as the "" key group_by gives it`,
			chunks:    map[int]map[string][][]byte{0: {"session": {[]byte("s-1"), nil}, "host": {[]byte("h1"), []byte("h2")}}},
			items:     []plan.WorkItem{{Segment: segment.Descriptor{ID: "s1"}}},
			q:         Query{Where: selector.ParsePredicates([]selector.Predicate{{Field: "session", Op: selector.OpEq}}), Columns: []string{"host"}},
			wantReads: []string{"0:session", "0:host@[1]"},
			wantRows:  []matched{{"s1", time.Time{}, "h2"}},
			wantCost:  query.Cost{GETs: 2, BytesRead: 20},
		},
		{
			name: "an equality on service compares only the run its binary search finds, not a row out of service order",
			chunks: map[int]map[string][][]byte{0: {
				"service": {[]byte("db"), []byte("api"), []byte("db"), []byte("db"), []byte("web")},
				"host":    {[]byte("h0"), []byte("h1"), []byte("h2"), []byte("h3"), []byte("h4")},
			}},
			items:     []plan.WorkItem{{Segment: segment.Descriptor{ID: "s1"}}},
			q:         Query{Where: selector.ParsePredicates([]selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "db"}}), Columns: []string{"host"}},
			wantReads: []string{"0:service", "0:host@[2 3]"},
			wantRows:  []matched{{"s1", time.Time{}, "h2"}, {"s1", time.Time{}, "h3"}},
			wantCost:  query.Cost{GETs: 2, BytesRead: 20},
		},
		{
			name:      "a failed read stops the rows and names its row group",
			items:     []plan.WorkItem{{Segment: segment.Descriptor{ID: "s1"}, RowGroup: 2}},
			q:         Query{Columns: []string{"host"}},
			wantReads: []string{"2:host"},
			wantCost:  query.Cost{GETs: 1},
			wantErr:   down,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reads []string
			rows := New(chunkReader(tt.chunks, &reads, down), plan.Derived{}).Scan(context.Background(), plan.Plan{Items: tt.items}, tt.q)

			var got []matched
			for (tt.take == 0 || len(got) < tt.take) && rows.Next() {
				r := rows.Row()
				var values []string
				for _, col := range tt.q.Columns {
					v, _ := r.Text(col)
					values = append(values, v)
				}
				got = append(got, matched{r.Segment.ID, r.Time, strings.Join(values, "|")})
			}

			// Cost waits for the reads still in flight, so it comes before the log is read.
			assert.Equal(t, tt.wantCost, rows.Cost())
			assert.ElementsMatch(t, tt.wantReads, reads)
			assert.Equal(t, tt.wantRows, got)
			if tt.wantErr == nil {
				require.NoError(t, rows.Err())
				return
			}
			require.ErrorIs(t, rows.Err(), tt.wantErr)
			assert.ErrorContains(t, rows.Err(), "s1 row group 2")
		})
	}
}

// hostGroups is n row groups of one row each, row group i's host hi.
func hostGroups(n int) map[int]map[string][][]byte {
	chunks := map[int]map[string][][]byte{}
	for i := range n {
		chunks[i] = map[string][][]byte{"host": {[]byte(fmt.Sprint("h", i))}}
	}
	return chunks
}

func groupItems(n int) []plan.WorkItem {
	items := make([]plan.WorkItem, n)
	for i := range items {
		items[i] = plan.WorkItem{Segment: segment.Descriptor{ID: "s1"}, RowGroup: i}
	}
	return items
}

func hostReads(n int) []string {
	reads := make([]string, n)
	for i := range reads {
		reads[i] = fmt.Sprintf("%d:host", i)
	}
	return reads
}

func hostRows(n int) []matched {
	rows := make([]matched, n)
	for i := range rows {
		rows[i] = matched{"s1", time.Time{}, fmt.Sprint("h", i)}
	}
	return rows
}

// TestRowIndex: a row's index counts the rows of the segment's earlier row groups,
// including one the plan dropped.
func TestRowIndex(t *testing.T) {
	chunks := map[int]map[string][][]byte{1: {"level": {[]byte("info"), []byte("error")}}}
	footer := segment.Footer{RowGroupZoneMaps: []segment.ZoneMaps{{segment.ColTimestamp: {PresentRows: 3}}, {segment.ColTimestamp: {PresentRows: 2}}}}
	items := []plan.WorkItem{{Segment: segment.Descriptor{ID: "s1"}, Footer: footer, RowGroup: 1}}
	rows := New(chunkReader(chunks, new([]string), nil), plan.Derived{}).Scan(context.Background(), plan.Plan{Items: items}, Query{Where: levelIs("error")})

	require.True(t, rows.Next())
	assert.Equal(t, 4, rows.Row().Index())
}

// TestRowDetach: a detached row reads as it did, with its own copy of each value.
func TestRowDetach(t *testing.T) {
	chunks := map[int]map[string][][]byte{1: {"level": {[]byte("info"), []byte("error")}, "host": {[]byte("h1"), []byte("h2")}}}
	footer := segment.Footer{RowGroupZoneMaps: []segment.ZoneMaps{{segment.ColTimestamp: {PresentRows: 3}}, {segment.ColTimestamp: {PresentRows: 2}}}}
	items := []plan.WorkItem{{Segment: segment.Descriptor{ID: "s1"}, Footer: footer, RowGroup: 1}}
	rows := New(chunkReader(chunks, new([]string), nil), plan.Derived{}).Scan(context.Background(), plan.Plan{Items: items}, Query{Where: levelIs("error"), Columns: []string{"host"}})
	require.True(t, rows.Next())

	row := rows.Row().Detach()
	chunks[1]["host"][1][1] = 'X'
	host, _ := row.Text("host")
	assert.Equal(t, "h2", host)
	assert.Equal(t, 4, row.Index())
}

// TestRowMatchesZeroAlloc gates the row filter at zero heap allocations, a group of equalities
// among its predicates: Scan calls Matches once per row, so a copied value costs one allocation
// per row per predicate.
func TestRowMatchesZeroAlloc(t *testing.T) {
	row := Row{group: &rowGroup{
		// Over 32 bytes, so a reverted copy cannot land in the compiler's stack buffer.
		cells: map[string]segment.ColumnChunk{
			"level":       segment.TextChunk([][]byte{[]byte("info-with-a-deliberately-long-value")}),
			"duration_ms": segment.TextChunk([][]byte{[]byte("42")}),
		},
		settled: map[string][]byte{"service": []byte("checkout-with-a-deliberately-long-name")},
	}}
	where := selector.ParsePredicates([]selector.Predicate{
		{Field: "level", Op: selector.OpEq, Value: "info-with-a-deliberately-long-value"},
		{Field: "service", Op: selector.OpEq, Value: "checkout-with-a-deliberately-long-name"},
		{Field: "duration_ms", Op: selector.OpGt, Value: "10"},
		// A batch of ids, matched as each field's values: by the settled service, the trace absent.
		{Any: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: "t-1"}, {Field: "trace_id", Op: selector.OpEq, Value: "t-2"},
			{Field: "service", Op: selector.OpEq, Value: "checkout-with-a-deliberately-long-name"}}},
	})
	if !row.Matches(where) {
		t.Fatal("the row matches every predicate")
	}
	if got := testing.AllocsPerRun(200, func() { _ = row.Matches(where) }); got != 0 {
		t.Errorf("Matches: %.0f allocs/op, want 0", got)
	}
}

// TestScanMemBudget reads row groups of 100 rows through the segment reader under a memory
// budget: one row group's columns over it fail too_large, and row groups each under it but
// summing far over, and over it even across those read ahead at once, stream through
// whether their rows match or not.
func TestScanMemBudget(t *testing.T) {
	const rows, groups = 100, 300
	b := segment.NewBuilder(segment.BuilderConfig{Tenant: "acme", Stream: segment.Stream{Service: "api"}, CreatedAt: t0})
	for i := range rows {
		require.NoError(t, b.Append(segment.LogRecord(t0.Add(time.Duration(i)), "info", "m", map[string]string{"trace_id": fmt.Sprintf("trace-%08d", i)}), segment.ParsedPattern{}))
	}
	obj, err := b.Seal().Encode()
	require.NoError(t, err)
	rd := segment.NewReader(storage.Fake{
		GetSuffixFn: func(_ string, n int64) ([]byte, error) { return obj[max(0, int64(len(obj))-n):], nil },
		GetRangeFn:  func(_ string, off, n int64) ([]byte, error) { return obj[off : off+n], nil },
	})
	footer, _, err := rd.Footer(context.Background(), segment.ObjectLocation{})
	require.NoError(t, err)
	items := make([]plan.WorkItem, groups)
	for i := range items {
		items[i] = plan.WorkItem{Segment: segment.Descriptor{ID: "s1"}, Footer: footer}
	}
	traceIs := func(v string) []selector.ParsedPredicate {
		return selector.ParsePredicates([]selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: v}})
	}
	// A trace_id row holds under 64 bytes and over 16, so this fits any one row group but
	// not those read ahead together.
	tight := int64(2) * rows * 64

	tests := []struct {
		name     string
		budget   int64
		q        Query
		wantRows int
		wantErr  bool
	}{
		{name: "one row group's columns over the budget", budget: 64, q: Query{Columns: []string{"trace_id"}}, wantErr: true},
		{name: "row groups matching nothing", budget: tight, q: Query{Where: traceIs("none"), Columns: []string{"trace_id"}}},
		{name: "row groups whose every row is passed", budget: tight, q: Query{Columns: []string{"trace_id"}}, wantRows: rows * groups},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := query.WithMemBudget(context.Background(), tt.budget)
			scan := New(rd, plan.Derived{}).Scan(ctx, plan.Plan{Items: items}, tt.q)
			n := 0
			for scan.Next() {
				n++
			}
			if !tt.wantErr {
				require.NoError(t, scan.Err())
				assert.Equal(t, tt.wantRows, n)
				return
			}
			var apiErr *query.APIError
			require.ErrorAs(t, scan.Err(), &apiErr)
			assert.Equal(t, query.ErrCodeTooLarge, apiErr.Code)
		})
	}
}
