package tools_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/lang"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/internal/value"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cost-model regression gate. It pins the two-tier design's defining cost
// properties so a change that makes the query read more than the design
// allows fails the build rather than a reviewer's intuition:
//
//  1. The hot path merges summaries — a count over segments fully inside the
//     window reads ZERO data (0 GETs, 0 bytes), independent of corpus size.
//  2. The scan path is bounded by the SELECTION, not the corpus — a narrow
//     window that straddles one segment costs the same however many fully
//     contained segments sit beside it.
//
// When the pruning pipeline (footer/row-group selectivity) lands, this extends
// to assert the full G/D closed form; today it gates the invariants that exist.

var costBase = time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)

// costWindow contains every segment the cost tests write.
var costWindow = window(costBase.Add(-time.Hour), costBase.Add(time.Hour))

// writeRows seeds one segment of `rows` one-second-apart events spanning
// [start, start+(rows-1)s]. The message carries the row index so each row is
// distinguishable, but all rows share one pattern.
func writeRows(t *testing.T, store storage.Store, cat catalog.Catalog, service string, start time.Time, rows int) {
	t.Helper()
	msgs := make([]string, rows)
	for i := range msgs {
		msgs[i] = fmt.Sprintf("req %d ms", i)
	}
	writeSegment(t, store, cat, service, messageEvents(start, msgs...))
}

// TestCostHotPathReadsNoData: over segments fully inside the window a count is
// answered from catalog summaries (0 GETs, 0 bytes) and a sketch measure reads one
// footer per segment and no data, whatever the corpus size.
func TestCostHotPathReadsNoData(t *testing.T) {
	tests := []struct {
		name     string
		measure  string
		segments int
		want     query.Cost
	}{
		{name: "count/3 segments", measure: "count", segments: 3, want: query.Cost{}},
		{name: "count/12 segments", measure: "count", segments: 12, want: query.Cost{}},
		{name: "p95/2 segments", measure: "p95", segments: 2, want: query.Cost{GETs: 2, FootersDecoded: 2, Segments: 2}},
		{name: "p95/8 segments", measure: "p95", segments: 8, want: query.Cost{GETs: 8, FootersDecoded: 8, Segments: 8}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir, store, cat := newCorpus(t)
			for i := range tc.segments {
				writeRows(t, store, cat, "svc", costBase.Add(time.Duration(i)*time.Minute), 20)
			}
			cr, rd := readers(dir)
			env := aggregateCost(t, toolsOver(cr, rd, nil), tools.AggregateRequest{
				Selector: selector.Selector{Time: costWindow},
				Measure:  tc.measure,
				GroupBy:  "service",
			})
			assert.Equal(t, tc.want, env.Meta.Cost)
		})
	}
}

// TestCostGroupByScanProportional pins only that a larger corpus costs more, not an exact multiplier.
func TestCostGroupByScanProportional(t *testing.T) {
	run := func(n int) query.Cost {
		dir, store, cat := newCorpus(t)
		for i := range n {
			start := costBase.Add(time.Duration(i) * time.Minute)
			records := make([]segment.Record, 5)
			for j := range records {
				pattern := "tmpl-a"
				if j%2 == 1 {
					pattern = "tmpl-b"
				}
				records[j] = segment.Record{
					Timestamp: start.Add(time.Duration(j) * time.Second),
					Columns: []segment.Column{
						{Name: segment.ColLevel, Type: value.TypeString, Value: value.Value{String: "info"}},
						{Name: segment.ColMessage, Type: value.TypeString, Value: value.Value{String: fmt.Sprintf("req %d ms", j)}},
						{Name: segment.ColService, Type: value.TypeString, Value: value.Value{String: "svc"}},
						{Name: segment.ColPatternID, Type: value.TypeString, Value: value.Value{String: pattern}},
					},
				}
			}
			writeRecords(t, store, cat, "svc", records)
		}
		cr, rd := readers(dir)
		return aggregateCost(t, toolsOver(cr, rd, nil), tools.AggregateRequest{
			Selector: selector.Selector{Time: costWindow},
			Measure:  "count",
			GroupBy:  "pattern_id",
		}).Meta.Cost
	}
	small := run(2)
	large := run(8)
	if small.GETs == 0 {
		t.Fatalf("expected group_by=pattern_id to force scans, got 0 GETs")
	}
	if large.GETs <= small.GETs {
		t.Errorf("group_by scan cost did not grow with corpus: 2-seg=%+v vs 8-seg=%+v", small, large)
	}
}

// wireSummary builds a summary over the whole catalog at cfg through the production
// builder, binning wherever its own width puts a segment rather than as the query asks.
func wireSummary(t *testing.T, cat catalog.Catalog, rd segment.Reader, cfg summary.Config) summary.Store {
	t.Helper()
	bldr := summary.NewBuilder(cat, rd, cfg)
	require.NoError(t, bldr.Build(context.Background()))
	return bldr.Store()
}

// catalogVersion is the corpus's catalog version, which a hand-seeded store must
// report for the serve-time gate to let it answer.
func catalogVersion(t *testing.T, cat catalog.Catalog) catalog.Version {
	t.Helper()
	version, err := cat.Version(context.Background())
	require.NoError(t, err)
	return version
}

// seedLatencyLogs writes three segments of timing logs a second apart, so both
// sketch families are populated: a trace_id HLL from the attribute, and a
// duration_ms DDSketch from the number in the message text.
//
// The duration has to be in the message. buildColumnIndex sketches quantiles
// only for a column of kind param (segment/codec.go), so a duration_ms the
// producer attached as an attribute — what spanEvent emits — gets an HLL and no
// DDSketch, and no quantile over this corpus would be answerable at all.
// TestCostQuantileWithoutASketchErrors covers that shape deliberately.
func seedLatencyLogs(t *testing.T, store storage.Store, cat catalog.Catalog) {
	t.Helper()
	for seg := range 3 {
		events := make([]segment.Record, 20)
		for i := range events {
			events[i] = logEvent(
				costBase.Add(time.Duration(seg)*time.Minute+time.Duration(i)*time.Second),
				"info", "trace_id", fmt.Sprintf("s%d-trace-%d", seg, i),
				fmt.Sprintf("query ok %d ms", i+1))
		}
		writeSegment(t, store, cat, "svc", events)
	}
}

// summaryColumns are the columns the cost tests' stores sketch. summary.Config
// names them rather than defaulting to every column a footer carries, because
// an HLL costs 16KB per point whatever its cardinality.
var summaryColumns = []string{segment.ColTraceID, segment.ColDurationMs}

// aggregateCost runs one aggregate and returns the whole envelope, so a caller
// can compare both what it cost and what it answered.
func aggregateCost(t *testing.T, tl tools.Tools, req tools.AggregateRequest) query.Envelope[tools.AggregateResponse] {
	t.Helper()
	env, err := tl.Aggregate(context.Background(), req)
	if err != nil {
		t.Fatalf("aggregate %s bins=%q: %v", req.Measure, req.Bins, err)
	}
	return env
}

// TestCostSummaryServedZeroGETs: summary-served aggregates read no data (0 GETs, 0
// bytes) — the machine-checked gate for the summary hot path — and answers the
// same series the scan path does.
//
// The scan comparison is the load-bearing half. Asserting only the cost let the
// summary collapse every point into the first bin of the window for any bins spec
// it had not been sealed at, and report it as the fast path working. A cheap
// wrong answer is worse than an expensive right one.
func TestCostSummaryServedZeroGETs(t *testing.T) {
	dir, store, cat := newCorpus(t)
	seedLatencyLogs(t, store, cat)

	cr, rd := readers(dir)
	// A 15s summary against 1m and 5m queries: the summary's own granularity is finer
	// than anything asked of it, so every case re-bins rather than reading points
	// out at the width they were stored in.
	rs := wireSummary(t, cr, rd, summary.Config{BinWidth: 15 * time.Second, SketchColumns: summaryColumns})

	measures := []string{"count", "rate", "distinct:trace_id", "p95:duration_ms"}
	for _, measure := range measures {
		for _, bins := range []string{"", "1m", "5m"} {
			t.Run(fmt.Sprintf("%s/bins=%q", measure, bins), func(t *testing.T) {
				req := tools.AggregateRequest{
					Selector: selector.Selector{Time: costWindow},
					Measure:  measure,
					GroupBy:  "service",
					Bins:     bins,
				}
				want := aggregateCost(t, toolsOver(cr, rd, nil), req)
				got := aggregateCost(t, toolsOver(cr, rd, rs), req)

				assert.Equal(t, query.Cost{}, got.Meta.Cost, "summary-served read data")
				assertSeriesClose(t, want.Data.Series, got.Data.Series)
			})
		}
	}
}

// TestCostQuantileWithoutASketchErrors: a sketch measure no segment in the
// window can answer is a structured error naming the columns that are there,
// not an empty series.
//
// The shape is real, not contrived: spanEvent attaches duration_ms as an attribute
// and buildColumnIndex sketches quantiles only for param columns, so every footer
// here has a duration_ms HLL and no DDSketch. Returning `series: []` for that reads
// as "the column exists and nothing happened", a guess an agent must never be left
// to make, and the error has to carry the re-plan, which suggested_fields does.
//
// The same measure over a corpus that parses the duration out of message text
// answers normally — the split is the column's kind, not the field name.
func TestCostQuantileWithoutASketchErrors(t *testing.T) {
	tests := []struct {
		name     string
		seed     func(*testing.T, storage.Store, catalog.Catalog)
		wantCode string // empty when the quantile is answerable
	}{
		{
			name: "duration_ms as a span attribute",
			seed: func(t *testing.T, store storage.Store, cat catalog.Catalog) {
				events := make([]segment.Record, 10)
				for i := range events {
					events[i] = spanEvent(costBase.Add(time.Duration(i)*time.Second),
						"info", fmt.Sprintf("trace-%d", i), "root", "", "GET /", int64(i+1))
				}
				writeSegment(t, store, cat, "svc", events)
			},
			wantCode: query.ErrCodeUnknownField,
		},
		{name: "duration_ms parsed from the message", seed: seedLatencyLogs},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir, store, cat := newCorpus(t)
			tc.seed(t, store, cat)
			cr, rd := readers(dir)
			env, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), tools.AggregateRequest{
				Selector: selector.Selector{Time: costWindow},
				Measure:  "p95:duration_ms",
				GroupBy:  "service",
			})
			if tc.wantCode == "" {
				require.NoError(t, err)
				require.NotEmpty(t, env.Data.Series, "want a quantile series when duration_ms arrives as a param column")
				return
			}
			var apiErr *query.APIError
			require.ErrorAs(t, err, &apiErr, "want an APIError for a quantile no segment can answer")
			assert.Equal(t, tc.wantCode, apiErr.Code)
			assert.Contains(t, apiErr.Message, "duration_ms", "message does not name the column that was missing")
			// The corpus has no numeric param column, so this segment records no distribution and there is nothing to suggest.
			assert.Empty(t, apiErr.SuggestedFields, "want no suggested_fields for a corpus with no DDSketch column")
		})
	}
}

// assertSeriesClose compares two series for equal shape and near-equal values.
// The tolerance is for the sketch-backed measures: a summary quantile merges
// DDSketches that the scan path reduces directly, and both carry the sketch's
// relative error.
func assertSeriesClose(t *testing.T, want, got []tools.Series) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("series count: want %d, got %d\n want=%+v\n got=%+v", len(want), len(got), want, got)
	}
	for i := range want {
		w, g := want[i], got[i]
		if w.Key != g.Key || w.Start != g.Start || w.Step != g.Step || len(w.Values) != len(g.Values) {
			t.Fatalf("series %d shape: want %+v, got %+v", i, w, g)
		}
		for j, wv := range w.Values {
			gv := g.Values[j]
			if (wv == nil) != (gv == nil) {
				t.Errorf("series %q bin %d: want %v, got %v", w.Key, j, wv, gv)
				continue
			}
			if wv == nil {
				continue
			}
			if tolerance := 0.05 * max(1, *wv); *gv < *wv-tolerance || *gv > *wv+tolerance {
				t.Errorf("series %q bin %d: want %v, got %v", w.Key, j, *wv, *gv)
			}
		}
	}
}

// TestCostScanBoundedBySelection: a narrow window straddling exactly one segment
// costs the same regardless of how many fully contained segments sit beside it —
// scan cost tracks the selection, not the corpus.
func TestCostScanBoundedBySelection(t *testing.T) {
	from := costBase.Add(5 * time.Minute)       // window start
	to := from.Add(30 * time.Second)            // narrow: 30s
	boundaryStart := from.Add(-5 * time.Second) // straddles `from`
	containedStart := from.Add(2 * time.Second) // wholly inside [from,to]

	run := func(contained int) query.Cost {
		dir, store, cat := newCorpus(t)
		writeRows(t, store, cat, "svc", boundaryStart, 10) // one boundary segment
		for range contained {
			writeRows(t, store, cat, "svc", containedStart, 5)
		}
		cr, rd := readers(dir)
		return aggregateCost(t, toolsOver(cr, rd, nil), tools.AggregateRequest{
			Selector: selector.Selector{Time: window(from, to)},
			Measure:  "count",
			GroupBy:  "service",
		}).Meta.Cost
	}

	small := run(2)
	large := run(8)

	if small.GETs == 0 {
		t.Fatalf("expected the boundary segment to be scanned, got 0 GETs")
	}
	if small != large {
		t.Errorf("scan cost tracks corpus size, not selection: 2-contained=%+v vs 8-contained=%+v", small, large)
	}
}

// TestCostGetTraceBoundedByXorFilter: get_trace is a needle lookup, so its cost
// (GETs, BytesRead) is invariant to the number of decoy segments carrying other
// trace_ids — the xor filter rejects them before any span column is read, so
// cost tracks the admitted candidates, not the corpus.
func TestCostGetTraceBoundedByXorFilter(t *testing.T) {
	run := func(decoys int) query.Cost {
		dir, store, cat := newCorpus(t)

		// Decoy segments carry a different trace_id and are xor-rejected.
		for i := range decoys {
			trace := fmt.Sprintf("decoy-%d", i)
			writeSegment(t, store, cat, "api", []segment.Record{
				spanEvent(costBase.Add(time.Duration(i)*time.Minute), "info", trace, "d-root", "", "noise", 10),
			})
		}
		// One segment holds the needle trace.
		writeSegment(t, store, cat, "api", []segment.Record{
			spanEvent(costBase.Add(time.Duration(decoys)*time.Minute), "info", "target", "s-root", "", "GET /", 20),
		})

		reader, rd := readers(dir)
		env, err := toolsOver(reader, rd, nil).GetTrace(context.Background(), tools.TraceRequest{
			Value: "target",
		})
		if err != nil {
			t.Fatalf("decoys=%d: get_trace: %v", decoys, err)
		}
		return env.Meta.Cost
	}

	small := run(2)
	large := run(8)

	if small.GETs == 0 {
		t.Fatalf("expected footers to be read, got 0 GETs")
	}
	if small.GETs == large.GETs && small.BytesRead == large.BytesRead {
		return // ideal: cost is identical regardless of decoy count
	}
	// Allow modest variance for an occasional xor false positive, but cost must
	// not scale with decoy count.
	if large.GETs > small.GETs*2 || large.BytesRead > small.BytesRead*2 {
		t.Errorf("get_trace cost scaled with decoy count: 2-decoys=%+v vs 8-decoys=%+v", small, large)
	}
}

// TestCostTraceReadsOnlyTheMatchingRowGroup: a trace held by one of four row groups
// costs a GET each for its footer, its trace_id filter and that row group's trace_id and ts;
// the other span columns hold one value across the segment, so its descriptor answers them.
func TestCostTraceReadsOnlyTheMatchingRowGroup(t *testing.T) {
	dir, store, cat := newCorpus(t)
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic test fixture
	payload := make([]byte, 32<<10)
	events := make([]segment.Record, 800)
	for i := range events {
		rng.Read(payload)
		events[i] = spanEvent(costBase.Add(time.Duration(i)*time.Second), "info", fmt.Sprintf("trace-%04d", i), "root", "", "GET /", 1)
		// Incompressible, so the segment spans several byte-sized row groups.
		events[i].Columns = append(events[i].Columns, segment.Column{Name: "payload", Type: value.TypeString, Value: value.Value{String: hex.EncodeToString(payload)}})
	}
	writeSegment(t, store, cat, "api", events)

	cr, rd := uncachedReaders(dir)
	env, err := toolsOver(cr, rd, nil).GetTrace(context.Background(), tools.TraceRequest{Value: "trace-0400"})
	require.NoError(t, err)
	assert.Equal(t, 3, env.Meta.Cost.GETs)
}

// TestCostTraceOnAnyIDReadsOneSegment: get_trace on an id no list names, over segments
// whose zone maps all admit the value, reads each segment's footer and order_id filter
// section and the span columns of only the one segment holding it.
func TestCostTraceOnAnyIDReadsOneSegment(t *testing.T) {
	run := func(segments int) query.Cost {
		dir, store, cat := newCorpus(t)
		for s := range segments {
			// More ids than a value count holds, so each segment's order_id gets a filter.
			events := make([]segment.Record, 1100)
			for i := range events {
				events[i] = segment.LogRecord(costBase.Add(time.Duration(s)*time.Hour+time.Duration(i)*time.Millisecond), "info", "placed",
					map[string]string{"order_id": fmt.Sprintf("o-%04d-%d", i, s)})
			}
			writeRecords(t, store, cat, "api", events)
		}
		cr, rd := uncachedReaders(dir)
		env, err := toolsOver(cr, rd, nil).GetTrace(context.Background(), tools.TraceRequest{Field: "order_id", Value: "o-0500-0"})
		require.NoError(t, err)
		require.Len(t, env.Data.Spans, 1)
		return env.Meta.Cost
	}

	one, hundred := run(1), run(100)
	assert.Equal(t, one.GETs+99*2, hundred.GETs, "each other segment costs its footer and filter, no rows")
}

// TestCostSeededCorpusEncoding gates the bytes the format stores for the seeded
// corpus: each column kind's compression ratio, and the row groups of the corpus,
// twice over, compacted into one segment inside the 4–32 MB band or cut short by the cell cap.
func TestCostSeededCorpusEncoding(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	built, err := seedcorpus.Build(ctx, dir, seedcorpus.ScenarioAll)
	require.NoError(t, err)
	store := storage.NewFS(dir)
	rd := segment.NewReader(store)

	decoded, encoded := map[string]int64{}, map[string]int64{}
	var records []segment.Record
	for _, d := range built.Descriptors {
		footer, _, err := rd.Footer(ctx, d.Object)
		require.NoError(t, err)
		for rg := range footer.RowGroups {
			cols, _, err := rd.Columns(ctx, d.Object, footer, rg, nil, slices.Sorted(maps.Keys(footer.ZoneMaps))...)
			require.NoError(t, err)
			for col, rows := range cols {
				br, _ := footer.Offsets.Get(rg, col)
				encoded[col] += br.Length
				for _, row := range rows.Rows() {
					decoded[col] += int64(len(row))
				}
			}
			rows := make(map[string][][]byte, len(cols))
			for col, chunk := range cols {
				rows[col] = chunk.Rows()
			}
			records = append(records, corpusRecords(t, footer, rows)...)
		}
	}
	// One column per kind: delta-coded timestamps and ints, dictionary text, raw text
	// and raw ids, each floored at its ratio when this gate was set. pattern_id was
	// lowered from 45 when masking split each categorical word into its own pattern.
	floors := map[string]float64{
		segment.ColTimestamp:  178,
		segment.ColPatternID:  35,
		segment.ColDurationMs: 2,
		segment.ColMessage:    6,
		segment.ColTraceID:    2,
	}
	for col, floor := range floors {
		assert.GreaterOrEqual(t, float64(decoded[col])/float64(encoded[col]), floor, col)
	}

	// Twice over, since the corpus alone encodes to a last row group under the band.
	object, err := segment.Sealed{Records: slices.Concat(records, records)}.Encode()
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, "compacted.seg", object))
	footer, _, err := rd.Footer(ctx, segment.ObjectLocation{Key: "compacted.seg"})
	require.NoError(t, err)
	groupBytes, groupCols := make([]int64, footer.RowGroups), make([]int, footer.RowGroups)
	for key, br := range footer.Offsets {
		groupBytes[key.RowGroup] += br.Length
		groupCols[key.RowGroup]++
	}
	for rg, size := range groupBytes {
		assert.LessOrEqual(t, size, int64(32<<20), rg)
		// Under the floor only where the corpus ran out or the cell cap cut the group first,
		// as it does for rows that encode to about a byte a cell.
		cells := footer.RowGroupZoneMaps[rg][segment.ColTimestamp].PresentRows * groupCols[rg]
		if rg < footer.RowGroups-1 && size < 4<<20 {
			assert.Greater(t, cells, segment.MaxGroupCells-groupCols[rg], rg)
		}
	}
}

// corpusRecords rebuilds one row group's records from its decoded columns, typing
// each column by its zone map.
func corpusRecords(t *testing.T, footer segment.Footer, cols map[string][][]byte) []segment.Record {
	t.Helper()
	records := make([]segment.Record, len(cols[segment.ColTimestamp]))
	for col, rows := range cols {
		typ := footer.ZoneMaps[col].Type
		for i, row := range rows {
			if row == nil {
				continue
			}
			v, err := value.Parse(typ, string(row))
			require.NoError(t, err)
			if col == segment.ColTimestamp {
				records[i].Timestamp = v.Time
				continue
			}
			records[i].Columns = append(records[i].Columns, segment.Column{Name: col, Type: typ, Value: v})
		}
	}
	return records
}

// TestCostCorrelateSummaryServedReadsNoData: a covering summary answers correlate and
// rca_walk from CountSlice alone, whether a bin contains a segment or splits it, and
// rca_walk's cold path bills the scan.
func TestCostCorrelateSummaryServedReadsNoData(t *testing.T) {
	dir, store, cat := newCorpus(t)
	seedLatencyLogs(t, store, cat)

	tests := []struct {
		name     string
		call     func(*testing.T, tools.Tools, string) query.Cost
		binWidth time.Duration // zero wires no summary
		wantScan bool
	}{
		{name: "correlate/contained counts", call: correlateCost, binWidth: time.Minute},
		{name: "correlate/straddling counts", call: correlateCost, binWidth: 15 * time.Second},
		{name: "rca_walk/summary", call: rcaWalkCost, binWidth: time.Minute},
		{name: "rca_walk/no summary", call: rcaWalkCost, wantScan: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cr, rd := readers(dir)
			var rs summary.Store
			if tc.binWidth != 0 {
				rs = wireSummary(t, cr, rd, summary.Config{BinWidth: tc.binWidth, SketchColumns: summaryColumns})
			}
			got := tc.call(t, toolsOver(cr, rd, rs), latencyPatternID(t, cr, rd, costWindow))
			if tc.wantScan {
				assert.NotZero(t, got.GETs, "want the scan billed, got %+v", got)
				return
			}
			assert.Equal(t, query.Cost{}, got, "summary-served call read data")
		})
	}
}

// correlateCost correlates the pattern with itself over costWindow and returns what it cost.
func correlateCost(t *testing.T, tl tools.Tools, pattern string) query.Cost {
	t.Helper()
	env, err := tl.Correlate(context.Background(), tools.CorrelateRequest{
		Selector: selector.Selector{Time: costWindow},
		SeriesA:  tools.SeriesRef{PatternID: pattern},
		SeriesB:  tools.SeriesRef{PatternID: pattern},
	})
	require.NoError(t, err, "correlate")
	return env.Meta.Cost
}

// rcaWalkCost walks from the pattern as symptom to itself as the only candidate over costWindow and returns what it cost.
func rcaWalkCost(t *testing.T, tl tools.Tools, pattern string) query.Cost {
	t.Helper()
	env, err := tl.RCAWalk(context.Background(), tools.RCAWalkRequest{
		Selector:   selector.Selector{Time: costWindow},
		Symptom:    tools.SeriesRef{PatternID: pattern},
		Candidates: []tools.SeriesRef{{PatternID: pattern}},
	})
	require.NoError(t, err, "rca_walk")
	return env.Meta.Cost
}

// TestCostAttributeWritesBoundedByPlacementStream: the only data attribute_writes
// reads is the placement stream. Its cost must therefore track that stream's size
// and not the corpus' — adding activity segments beside it changes what the
// breakdown says, never what it costs to find out who is on the entity.
func TestCostAttributeWritesBoundedByPlacementStream(t *testing.T) {
	run := func(decoys int) (query.Cost, int) {
		dir, store, cat := newCorpus(t)
		writeSegment(t, store, cat, "scheduler", []segment.Record{segment.LogRecord(costBase, "info", "bound pod to host", map[string]string{"pod": "svc-0", "host": "host-7"})})
		for i := range decoys {
			writeRows(t, store, cat, fmt.Sprintf("noise-%d", i), costBase.Add(time.Duration(i)*time.Minute), 20)
		}
		writeRows(t, store, cat, "svc", costBase, 30)

		reader, rd := readers(dir)
		env, err := toolsOver(reader, rd, nil).AttributeWrites(context.Background(), tools.AttributeWritesRequest{
			Selector: selector.Selector{Time: costWindow},
			Entity:   tools.EntityRef{Value: "host-7"},
		})
		if err != nil {
			t.Fatalf("decoys=%d: attribute_writes: %v", decoys, err)
		}
		return env.Meta.Cost, len(env.Data.Breakdown)
	}

	small, smallGroups := run(2)
	large, largeGroups := run(8)

	if small.GETs == 0 {
		t.Fatalf("expected the placement stream to be read, got 0 GETs")
	}
	if small != large {
		t.Errorf("attribute_writes cost tracks corpus size, not the placement stream: 2-decoys=%+v vs 8-decoys=%+v", small, large)
	}
	if smallGroups != 1 || largeGroups != 1 {
		t.Errorf("breakdown should hold only the group placed on host-7, got %d and %d", smallGroups, largeGroups)
	}
}

// TestCostRCAWalkBoundedByCandidateSet: adding noise services to the corpus does
// not increase rca_walk's GETs when a summary is wired. The hot path serves both
// the topology view and the per-candidate correlate step from the summary (0 GETs
// each), so total cost is bounded by the candidate set × window segments, not
// the whole corpus.
//
// This gates the explicit-candidate path only — the caller named the set, so corpus
// growth beside it must not add reads. The other half of the O(n²) bound, that an
// auto-picked set is cut at max_candidates rather than spanning every pattern in
// the window, is not observable in Cost while the summary serves the correlate step;
// TestRCAWalkAutoCandidatesRespectMax gates it on the correlated count instead.
func TestCostRCAWalkBoundedByCandidateSet(t *testing.T) {
	run := func(decoys int) query.Cost {
		dir, store, cat := newCorpus(t)
		// Symptom and candidate data — summary covers these.
		seedLatencyLogs(t, store, cat)
		// Noise services inflate corpus size but are not in the candidate set.
		for i := range decoys {
			writeRows(t, store, cat, fmt.Sprintf("noise-%d", i), costBase.Add(time.Duration(i)*time.Minute), 20)
		}

		cr, rd := readers(dir)
		rs := wireSummary(t, cr, rd, summary.Config{BinWidth: time.Minute, SketchColumns: summaryColumns})
		return rcaWalkCost(t, toolsOver(cr, rd, rs), latencyPatternID(t, cr, rd, costWindow))
	}

	small := run(2)
	large := run(8)

	if small != large {
		t.Errorf("rca_walk cost scaled with decoy count: 2-decoys=%+v vs 8-decoys=%+v", small, large)
	}
}

// latencyPatternID resolves a real pattern because the scan narrows to segments whose manifest
// lists it, so a made-up id would read and measure nothing.
func latencyPatternID(t *testing.T, cr catalog.Catalog, rd segment.Reader, w selector.TimeSpec) string {
	t.Helper()
	env, err := toolsOver(cr, rd, nil).Patterns(context.Background(), tools.PatternsRequest{
		Selector: selector.Selector{Time: w}, Limit: 100,
	})
	if err != nil {
		t.Fatalf("resolve latency pattern: %v", err)
	}
	for _, p := range env.Data.Patterns {
		if p.Service == "svc" {
			return p.PatternID
		}
	}
	t.Fatal("seedLatencyLogs must produce a pattern under service svc")
	return ""
}

// TestCostFooterCacheIsNotBilled: Cost.GETs is what the caller is charged, so repeating
// a scan over a warm footer cache must report less than the cold run did. Counting the
// call rather than the request made every warm query overstate its own spend — the
// number every response puts in front of an agent's owner.
//
// The uncached reader is the control: with no cache the second run bills exactly what
// the first did, so the drop below is the cache and not the query changing.
func TestCostFooterCacheIsNotBilled(t *testing.T) {
	dir, store, cat := newCorpus(t)
	seedLatencyLogs(t, store, cat)

	// A bare quantile the summary cannot serve, so both runs take the footer path.
	req := func() tools.AggregateRequest {
		return tools.AggregateRequest{
			Selector: selector.Selector{
				Time: window(costBase.Truncate(time.Minute), costBase.Add(10*time.Minute))},
			Measure: "p95", GroupBy: "service", Bins: "1m",
		}
	}
	gets := func(cr catalog.Catalog, rd segment.Reader) int {
		env, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), req())
		require.NoError(t, err)
		return env.Meta.Cost.GETs
	}

	cr, rd := readers(dir)
	cold, warm := gets(cr, rd), gets(cr, rd)
	require.Positive(t, cold, "the first run reaches the objects")
	assert.Zero(t, warm, "every footer is cached by the second run, so it is billed nothing")

	ucr, urd := uncachedReaders(dir)
	first, second := gets(ucr, urd), gets(ucr, urd)
	assert.Equal(t, first, second, "with no cache both runs reach the objects and bill alike")
	assert.Equal(t, cold, first, "the cold cached run bills what the uncached run does")
}

// TestCostValueCountsReadNoData: a count grouped by a small string column, and
// explain_spike over one, for one service and across all, read each footer once and
// no data, and answer what the rows say.
func TestCostValueCountsReadNoData(t *testing.T) {
	dir, store, cat := newCorpus(t)
	want := map[string]map[string]float64{"": {}, "api": {}, "web": {}}
	for s := range 2 {
		var records []segment.Record
		for i := range 16 {
			service, region, errPattern := []string{"api", "web"}[i%2], "eu", []string{"timeout", "refused", "reset"}[i%3]
			if (i%2 == 0) == (i%8 < 2) {
				region = "us"
			}
			records = append(records, segment.LogRecord(costBase.Add(time.Duration(s*60+i)*time.Second), "error", "failed",
				map[string]string{segment.ColService: service, "region": region, "error_pattern": errPattern}))
			want[""][errPattern]++
			want[service][errPattern]++
		}
		writeSegment(t, store, cat, "mixed", records)
	}
	api := []selector.Predicate{{Field: segment.ColService, Op: selector.OpEq, Value: "api"}}

	for scope, where := range map[string][]selector.Predicate{"": nil, "api": api} {
		cr, rd := readers(dir)
		env := aggregateCost(t, toolsOver(cr, rd, nil), tools.AggregateRequest{
			Selector: selector.Selector{Time: costWindow, Where: where}, Measure: "count", GroupBy: "error_pattern", Bins: "1h",
		})
		assert.Equal(t, query.Cost{GETs: 2, FootersDecoded: 2, Segments: 2}, env.Meta.Cost, scope)
		got := map[string]float64{}
		for _, s := range env.Data.Series {
			for _, v := range present(s.Values) {
				got[s.Key] += v
			}
		}
		assert.Equal(t, want[scope], got, scope)
	}

	cr, rd := readers(dir)
	env, err := toolsOver(cr, rd, nil).ExplainSpike(context.Background(), tools.ExplainSpikeRequest{
		Selector: selector.Selector{Time: costWindow}, Slice: api,
	})
	require.NoError(t, err)
	assert.Equal(t, query.Cost{GETs: 2, FootersDecoded: 2, Segments: 4}, env.Meta.Cost, "every field off the footers in one read")
	assert.Equal(t, []tools.Shift{{Field: "region", Value: "eu", SliceShare: 0.75, BaselineShare: 0.25, SliceCount: 12, BaselineCount: 4}}, env.Data.Shifts)

	// A slice on a row column the footers cannot count by reads each segment's rows once
	// for every field, rather than once per field.
	cr, rd = readers(dir)
	env, err = toolsOver(cr, rd, nil).ExplainSpike(context.Background(), tools.ExplainSpikeRequest{
		Selector: selector.Selector{Time: costWindow}, Slice: []selector.Predicate{{Field: "region", Op: selector.OpEq, Value: "us"}},
	})
	require.NoError(t, err)
	assert.Equal(t, query.Cost{GETs: 6, BytesRead: 344, FootersDecoded: 2, Segments: 4}, env.Meta.Cost)

	// A slice no footer holds a line of, a pattern another corpus logged, counts only the scope.
	cr, rd = readers(dir)
	env, err = toolsOver(cr, rd, nil).ExplainSpike(context.Background(), tools.ExplainSpikeRequest{
		Selector: selector.Selector{Time: costWindow}, Slice: []selector.Predicate{{Field: segment.ColPatternID, Op: selector.OpEq, Value: "absent"}},
	})
	require.NoError(t, err)
	assert.Equal(t, query.Cost{GETs: 2, FootersDecoded: 2, Segments: 4}, env.Meta.Cost)
	assert.Equal(t, tools.ExplainSpikeResponse{Shifts: []tools.Shift{}, Rest: 32}, env.Data)
}

// TestCostFindAnomaliesSkipsAnIDColumn: a column too wide for the summary that holds text is
// an id, neither a code nor a measure, so find_anomalies costs and answers as it would without it.
func TestCostFindAnomaliesSkipsAnIDColumn(t *testing.T) {
	answer := func(withID bool) query.Envelope[tools.FindAnomaliesResponse] {
		dir, store, cat := newCorpus(t)
		var records []segment.Record
		for i := range 60 {
			fields := map[string]string{segment.ColService: "api", "status": []string{"200", "500"}[i%2]}
			if withID {
				fields["request_id"] = fmt.Sprintf("req-%d", i/2)
			}
			records = append(records, segment.LogRecord(costBase.Add(time.Duration(i)*time.Minute), "info", "served", fields))
		}
		writeSegment(t, store, cat, "api", records)
		cr, rd := readers(dir)
		rs := wireSummary(t, cr, rd, summary.Config{MaxValueSeries: 1, MaxColumnValues: 4})
		env, err := toolsOver(cr, rd, rs).FindAnomalies(context.Background(), tools.FindAnomaliesRequest{
			Selector: selector.Selector{Time: window(costBase, costBase.Add(time.Hour))},
		})
		require.NoError(t, err)
		return env
	}
	want, got := answer(false), answer(true)
	assert.NotZero(t, want.Meta.Cost.BytesRead, "the evicted status is counted from rows")
	assert.Equal(t, want.Meta.Cost, got.Meta.Cost)
	assert.Equal(t, want.Data, got.Data)
}

// TestQueryCallsTheToolItsStageNames parses through a fake language, so what is pinned
// is the call: the same data and cost as the named tool answering the typed request.
func TestQueryCallsTheToolItsStageNames(t *testing.T) {
	dir, store, cat := newCorpus(t)
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	var events []segment.Record
	for i, level := range []string{"info", "error", "info", "error", "warn"} {
		events = append(events, segment.LogRecord(start.Add(time.Duration(i)*time.Second), level,
			"request served", map[string]string{"trace_id": "t1", "route": []string{"/b", "/a"}[i%2]}))
	}
	writeSegment(t, store, cat, "checkout", events)
	cat, rd := uncachedReaders(dir)
	var parsed selector.Query
	fake := lang.Fake{
		ParseFn:  func(string) (selector.Query, error) { return parsed, nil },
		FormatFn: func(selector.Query) (string, error) { return "canonical", nil },
	}
	planner, scanner := plan.New(plan.Deps{Catalog: cat, Reader: rd}), scan.New(rd, plan.Derived{})
	ts := tools.New(tools.Deps{Catalog: cat, Series: series.New(planner, scanner), Planner: planner, Scanner: scanner,
		Languages: lang.Registry{"fake": fake}})

	ctx, when := context.Background(), window(start, start.Add(time.Minute))
	errs := []selector.Predicate{{Field: "level", Op: selector.OpEq, Value: "error"}}
	trace := []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: "t1"}}
	sel := selector.Selector{Time: when, Where: errs}
	byRoute := tools.AggregateRequest{Selector: selector.Selector{Time: when}, Measure: "count", GroupBy: "route", Bins: "1m"}
	topRoute := byRoute
	topRoute.Limit = 1 // /b's three rows outrank /a's two
	// /b's rows are info, info and warn.
	byLevel := []selector.Predicate{{Field: "route", Op: selector.OpEq, Value: "/b"}, {Field: "level", Op: selector.OpEq, Value: "$level"}}
	info := []selector.Predicate{byLevel[0], {Field: "level", Op: selector.OpEq, Value: "info"}}
	injected := []selector.Predicate{byLevel[0], {Field: "level", Op: selector.OpEq, Value: `info"} or {level=~".*`}}
	for _, tc := range []struct {
		tool  string
		query selector.Query
		vars  map[string]string
		run   func() (any, query.Meta, error)
		err   string
	}{
		{tool: "search_logs", query: selector.Query{Where: errs}, run: envelope(ts.SearchLogs, tools.SearchRequest{Selector: sel})},
		{tool: "a variable substituted", query: selector.Query{Where: byLevel}, vars: map[string]string{"level": "info"},
			run: envelope(ts.SearchLogs, tools.SearchRequest{Selector: selector.Selector{Time: when, Where: info}})},
		{tool: "a variable set to All", query: selector.Query{Where: byLevel}, vars: map[string]string{"level": "*"},
			run: envelope(ts.SearchLogs, tools.SearchRequest{Selector: selector.Selector{Time: when, Where: byLevel[:1]}})},
		{tool: "a variable set to All among alternatives", query: selector.Query{Where: []selector.Predicate{{Any: byLevel}}}, vars: map[string]string{"level": "*"},
			run: envelope(ts.SearchLogs, tools.SearchRequest{Selector: selector.Selector{Time: when}})},
		{tool: "a variable holding LogQL, a literal", query: selector.Query{Where: byLevel}, vars: map[string]string{"level": injected[1].Value},
			run: envelope(ts.SearchLogs, tools.SearchRequest{Selector: selector.Selector{Time: when, Where: injected}})},
		{tool: "a variable without a value", query: selector.Query{Where: byLevel}, err: "query: $level has no value"},
		{tool: "list_patterns", query: selector.Query{Where: errs, Stage: &selector.Stage{Kind: selector.StagePatterns}},
			run: envelope(ts.Patterns, tools.PatternsRequest{Selector: sel})},
		{tool: "aggregate", query: selector.Query{Stage: &selector.Stage{Kind: selector.StageAggregate, Measure: "count", GroupBy: "route", Bins: "1m"}},
			run: envelope(ts.Aggregate, byRoute)},
		{tool: "top", query: selector.Query{Stage: &selector.Stage{Kind: selector.StageTop, Measure: "count", GroupBy: "route", Bins: "1m", K: 1}},
			run: envelope(ts.Aggregate, topRoute)},
		{tool: "explain_spike", query: selector.Query{Where: errs, Stage: &selector.Stage{Kind: selector.StageExplain}},
			run: envelope(ts.ExplainSpike, tools.ExplainSpikeRequest{Selector: selector.Selector{Time: when}, Slice: errs})},
		{tool: "get_trace", query: selector.Query{Where: trace, Stage: &selector.Stage{Kind: selector.StageTrace}},
			run: envelope(ts.GetTrace, tools.TraceRequest{Field: "trace_id", Value: "t1", Time: when})},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			parsed = tc.query
			got, err := ts.Query(ctx, tools.QueryRequest{Q: "q", Lang: "fake", Time: when, Vars: tc.vars})
			if tc.err != "" {
				require.EqualError(t, err, tc.err)
				return
			}
			require.NoError(t, err)

			data, meta, err := tc.run()
			require.NoError(t, err)
			want, err := json.Marshal(data)
			require.NoError(t, err)
			assert.Equal(t, "canonical", got.Data.Canonical)
			assert.Equal(t, string(want), string(got.Data.Result))
			assert.Equal(t, meta.Cost, got.Meta.Cost)
		})
	}
}

func envelope[Req, Resp any](run func(context.Context, Req) (query.Envelope[Resp], error), req Req) func() (any, query.Meta, error) {
	return func() (any, query.Meta, error) {
		env, err := run(context.Background(), req)
		return env.Data, env.Meta, err
	}
}
