package summary_test

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/summary"
)

// testSketchColumns are the columns these series are configured to summarize:
// one per family, matching what buildFooter seeds. A series is not told to
// summarize every column a footer carries — see summary.Config.SketchColumns.
var testSketchColumns = []string{segment.ColTraceID, segment.ColDurationMs}

// footerSpec is a compact description of the sketches to seed a hand-built
// footer with — used by the property and unit tests to control series inputs.
type footerSpec struct {
	// service owns every row; empty means "svc".
	service  string
	rows     int
	patterns []string
	// tmplCounts is each pattern's rows; nil splits rows evenly over patterns.
	tmplCounts map[string]int
	// hllValues seeds the distinct-traces sketch. Nil skips that family.
	hllValues [][]byte
	// ddValues seeds the latency-distribution sketch. Nil skips that family.
	ddValues []float64
}

func buildFooter(spec footerSpec) segment.Footer {
	service := spec.service
	if service == "" {
		service = "svc"
	}
	whole := segment.Footer{Rows: spec.rows, Patterns: segment.NewPresentSet(), ServicePatternCounts: map[segment.ServicePattern]int{}}
	for _, t := range spec.patterns {
		whole.Patterns.Add(t)
		whole.ServicePatternCounts[segment.ServicePattern{Service: service, Pattern: t}] = spec.rows / len(spec.patterns)
	}
	for t, n := range spec.tmplCounts {
		whole.ServicePatternCounts[segment.ServicePattern{Service: service, Pattern: t}] = n
	}
	f := segment.Footer{Rows: spec.rows, DistinctCounts: map[string]*sketch.HLL{}, Distributions: map[string]*sketch.DDSketch{}}
	if spec.hllValues != nil {
		h := sketch.NewHLL()
		for _, v := range spec.hllValues {
			h.Add(v)
		}
		f.DistinctCounts[segment.ColTraceID] = h
	}
	if spec.ddValues != nil {
		dd := sketch.NewDDSketch(sketch.QuantileAccuracy)
		for _, v := range spec.ddValues {
			dd.Add(v)
		}
		f.Distributions[segment.ColDurationMs] = dd
	}
	return segment.FooterWithServices(whole, map[string]segment.Footer{service: f})
}

func descriptor(id, service string, rows int, tmin, tmax time.Time) segment.Descriptor {
	return segment.Descriptor{
		ID:        id,
		Stream:    segment.Stream{Service: service},
		TimeRange: segment.TimeRange{Min: tmin, Max: tmax},
		Rows:      rows,
	}
}

// hourSeries is a series with an hour-wide bin, for tests that want one point to
// swallow a whole segment. Retention is long enough that nothing evicts.
func hourSeries() *summary.BinTable {
	return summary.NewBinTable(summary.Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour, SketchColumns: testSketchColumns})
}

// TestSeriesMergeFooterSingle merges one footer covering a single bin and
// verifies the produced points carry the expected count and sketches.
func TestSeriesMergeFooterSingle(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	f := buildFooter(footerSpec{
		service:    "svc-a",
		rows:       42,
		patterns:   []string{"tmpl_a"},
		tmplCounts: map[string]int{"tmpl_a": 42},
		hllValues:  [][]byte{[]byte("trace1"), []byte("trace2")},
		ddValues:   []float64{1.0, 2.0, 3.0},
	})
	desc := descriptor("seg1", "svc-a", 42, start.Add(time.Minute), start.Add(2*time.Minute))

	c := hourSeries()
	perPattern, err := c.MergeFooter(desc, f, nil)
	require.NoError(t, err)
	assert.Equal(t, []summary.PatternLines{{PatternID: "tmpl_a", Lines: 42}}, perPattern,
		"MergeFooter must report the counts it stored, for the signal index to score")

	assert.Equal(t, []summary.CountPoint{{Key: summary.CountKey{PatternID: "tmpl_a", Service: "svc-a", Bin: start.UnixMilli()}, Lines: 42}},
		c.CountSlice("", 0, 1<<62), "count should come from the pattern-frequency sketch")

	sketches, err := c.SketchSlice("", start.UnixMilli(), start.Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	require.Len(t, sketches, 1)
	assert.Equal(t, summary.SketchKey{Service: "svc-a", Bin: start.UnixMilli()}, sketches[0].Key)
	require.NotNil(t, sketches[0].Distinct[segment.ColTraceID])
	require.NotNil(t, sketches[0].Quantiles[segment.ColDurationMs])
	assert.InDelta(t, 2.0, sketches[0].Distinct[segment.ColTraceID].Estimate(), 0.5)
}

// TestSeriesBinsAlignToEpoch: points land on absolute bin boundaries, so the same
// segment produces the same point no matter what window a query later asks
// about. Aligning to a caller's window instead is what let a series built for one
// query be read back as a different series by the next.
func TestSeriesBinsAlignToEpoch(t *testing.T) {
	c := summary.NewBinTable(summary.Config{BinWidth: time.Minute, SketchColumns: testSketchColumns})
	// 12:03:47.123 → 12:03:00, whichever query is asking.
	ts := time.Date(2026, 1, 1, 12, 3, 47, 123e6, time.UTC)
	f := buildFooter(footerSpec{rows: 5, patterns: []string{"tmpl_x"}, tmplCounts: map[string]int{"tmpl_x": 5}})
	_, err := c.MergeFooter(descriptor("s", "svc", 5, ts, ts.Add(10*time.Second)), f, nil)
	require.NoError(t, err)
	want := []summary.CountPoint{{Key: summary.CountKey{PatternID: "tmpl_x", Service: "svc", Bin: time.Date(2026, 1, 1, 12, 3, 0, 0, time.UTC).UnixMilli()}, Lines: 5}}
	assert.Equal(t, want, c.CountSlice("", 0, 1<<62))
}

// TestSeriesMergeFooterNilSketches verifies a footer with no sketches still
// produces count points reflecting desc.Rows divided across patterns, and no
// sketch points at all.
func TestSeriesMergeFooterNilSketches(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	f := buildFooter(footerSpec{rows: 40, patterns: []string{"a", "b"}})
	desc := descriptor("s", "svc", 40, start.Add(time.Minute), start.Add(2*time.Minute))

	c := hourSeries()
	_, err := c.MergeFooter(desc, f, nil)
	require.NoError(t, err)

	assert.Equal(t, []summary.CountPoint{
		{Key: summary.CountKey{PatternID: "a", Service: "svc", Bin: start.UnixMilli()}, Lines: 20},
		{Key: summary.CountKey{PatternID: "b", Service: "svc", Bin: start.UnixMilli()}, Lines: 20},
	}, c.CountSlice("", 0, 1<<62), "count should split desc.Rows across patterns when no frequency sketch is present")
	got, err := c.SketchSlice("", start.UnixMilli(), start.Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	assert.Empty(t, got, "a footer with no sketches should leave no sketch point behind")
}

// TestSeriesMergeFooterNoPatterns verifies that a footer with an empty
// PresentSet creates no points.
func TestSeriesMergeFooterNoPatterns(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	f := buildFooter(footerSpec{rows: 10}) // no patterns
	desc := descriptor("s", "svc", 10, start.Add(time.Minute), start.Add(2*time.Minute))

	c := hourSeries()
	perPattern, err := c.MergeFooter(desc, f, nil)
	require.NoError(t, err)
	assert.Empty(t, perPattern)
	assert.Empty(t, c.CountSlice("", 0, start.Add(time.Hour).UnixMilli()))
}

// TestSeriesMergeFooterOverlappingBins: a multi-bin segment's counts land in their own bins.
func TestSeriesMergeFooterOverlappingBins(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	f := buildFooter(footerSpec{rows: 30, patterns: []string{"tmpl_x"}, tmplCounts: map[string]int{"tmpl_x": 30}})
	desc := descriptor("s", "svc", 30, start.Add(10*time.Second), start.Add(2*time.Minute+30*time.Second))
	want := []summary.CountPoint{{Key: summary.CountKey{PatternID: "tmpl_x", Service: "svc", Bin: start.Add(time.Minute).UnixMilli()}, Lines: 30}}

	c := summary.NewBinTable(summary.Config{BinWidth: time.Minute, SketchColumns: testSketchColumns})
	_, err := c.MergeFooter(desc, f, summary.BinCounts{want[0].Key: 30})
	require.NoError(t, err)
	assert.Equal(t, want, c.CountSlice("", start.UnixMilli(), start.Add(time.Hour).UnixMilli()))
}

// TestSeriesSketchesMergedOncePerBin: a segment's sketches describe the segment,
// so they must reach a bin's point exactly once however many patterns the
// segment holds. Merging once per pattern inflates the latency distribution by
// that factor — invisibly, since the quantile still looks like a latency.
func TestSeriesSketchesMergedOncePerBin(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	values := []float64{10, 20, 30, 40}
	spec := footerSpec{
		rows:     4,
		patterns: []string{"tmpl_a", "tmpl_b", "tmpl_c"},
		ddValues: values,
	}
	desc := descriptor("s", "svc", 4, start.Add(time.Minute), start.Add(2*time.Minute))

	c := hourSeries()
	_, err := c.MergeFooter(desc, buildFooter(spec), nil)
	require.NoError(t, err)

	oracle := sketch.NewDDSketch(sketch.QuantileAccuracy)
	for _, v := range values {
		oracle.Add(v)
	}

	points, err := c.SketchSlice("", start.UnixMilli(), start.Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	require.Len(t, points, 1, "three patterns in one segment are still one sketch point")
	require.NotNil(t, points[0].Quantiles[segment.ColDurationMs])
	for _, q := range []float64{0.5, 0.95, 0.99} {
		assert.InDelta(t, oracle.Quantile(q), points[0].Quantiles[segment.ColDurationMs].Quantile(q), 1e-9,
			"quantile %v must match the footer's own sketch, not a multiple of it", q)
	}
}

// TestSeriesSketchSliceCopies: the returned sketches must not alias the series',
// or a later merge silently rewrites a slice a caller is still reading.
func TestSeriesSketchSliceCopies(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	desc := descriptor("s1", "svc", 2, start.Add(time.Minute), start.Add(2*time.Minute))

	c := hourSeries()
	_, err := c.MergeFooter(desc, buildFooter(footerSpec{
		rows: 2, patterns: []string{"t"}, hllValues: [][]byte{[]byte("a"), []byte("b")},
	}), nil)
	require.NoError(t, err)

	points, err := c.SketchSlice("", start.UnixMilli(), start.Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	require.Len(t, points, 1)
	before := points[0].Distinct[segment.ColTraceID].Estimate()

	// A second segment in the same bin, with entirely new traces.
	_, err = c.MergeFooter(
		descriptor("s2", "svc", 2, start.Add(3*time.Minute), start.Add(4*time.Minute)),
		buildFooter(footerSpec{
			rows: 2, patterns: []string{"t"}, hllValues: [][]byte{[]byte("c"), []byte("d")},
		}), nil)
	require.NoError(t, err)

	assert.Equal(t, before, points[0].Distinct[segment.ColTraceID].Estimate(),
		"the already-returned sketch must not change when the series merges again")
}

// TestSeriesMergeFooterSplitsByService: one node's segment holds several services, and
// each one's rows count under its own name rather than the stream's.
func TestSeriesMergeFooterSplitsByService(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	views := map[string]segment.Footer{}
	counts := map[segment.ServicePattern]int{}
	for service, rows := range map[string]int{"nginx": 3, "checkout": 2} {
		views[service] = segment.Footer{Rows: rows}
		counts[segment.ServicePattern{Service: service, Pattern: "t"}] = rows
	}
	c := hourSeries()
	perPattern, err := c.MergeFooter(descriptor("s", "node-1", 5, start.Add(time.Minute), start.Add(2*time.Minute)),
		segment.FooterWithServices(segment.Footer{ServicePatternCounts: counts}, views), nil)
	require.NoError(t, err)
	assert.Equal(t, []summary.PatternLines{{PatternID: "t", Lines: 5}}, perPattern)
	got := map[string]int64{}
	for _, p := range c.CountSlice("", start.UnixMilli(), start.Add(time.Hour).UnixMilli()) {
		got[p.Key.Service] += p.Lines
	}
	assert.Equal(t, map[string]int64{"nginx": 3, "checkout": 2}, got)
}

// TestSeriesMergeFooterFailsWholeOrNotAtAll: a merge that errors leaves the
// series exactly as it found it.
//
// The store only records a descriptor as sealed when MergeFooter returns nil, so
// an errored merge is one the caller is expected to retry. Anything a failed
// attempt had already written — the counts, or the bins whose sketches merged
// before the failing column — would be added a second time by that retry, and a
// count series that double-counts a segment is wrong in a way no later merge
// corrects.
func TestSeriesMergeFooterFailsWholeOrNotAtAll(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	c := hourSeries()

	good := buildFooter(footerSpec{
		rows: 10, patterns: []string{"t"}, tmplCounts: map[string]int{"t": 10},
		hllValues: [][]byte{[]byte("a")}, ddValues: []float64{5},
	})
	_, err := c.MergeFooter(descriptor("s1", "svc", 10, start.Add(time.Minute), start.Add(2*time.Minute)), good, nil)
	require.NoError(t, err)

	wantCounts := c.CountSlice("", start.UnixMilli(), start.Add(time.Hour).UnixMilli())
	wantPoints, err := c.SketchSlice("", start.UnixMilli(), start.Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	wantTraces := wantPoints[0].Distinct[segment.ColTraceID].Estimate()
	wantP50 := wantPoints[0].Quantiles[segment.ColDurationMs].Quantile(0.5)

	// A footer whose latency sketch was built at another relative accuracy: the
	// index mappings do not match, so DDSketch.Merge refuses it. The trace_id HLL
	// merges fine and comes first, which is what makes this a partial write.
	bad := buildFooter(footerSpec{
		rows: 99, patterns: []string{"t"}, tmplCounts: map[string]int{"t": 99},
		hllValues: [][]byte{[]byte("z")},
	})
	foreign := sketch.NewDDSketch(sketch.QuantileAccuracy * 2)
	foreign.Add(1000)
	bad.ForService("svc").Distributions[segment.ColDurationMs] = foreign

	badDesc := descriptor("s2", "svc", 99, start.Add(3*time.Minute), start.Add(4*time.Minute))
	_, err = c.MergeFooter(badDesc, bad, nil)
	require.Error(t, err, "a sketch built at a foreign accuracy must not merge")

	gotCounts := c.CountSlice("", start.UnixMilli(), start.Add(time.Hour).UnixMilli())
	assert.Equal(t, wantCounts, gotCounts, "a failed merge must leave the counts untouched")

	gotPoints, err := c.SketchSlice("", start.UnixMilli(), start.Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	require.Len(t, gotPoints, 1)
	assert.Equal(t, wantTraces, gotPoints[0].Distinct[segment.ColTraceID].Estimate(),
		"the distinct sketch merged before the failing column must be rolled back too")
	assert.Equal(t, wantP50, gotPoints[0].Quantiles[segment.ColDurationMs].Quantile(0.5))
}

// TestSeriesSketchColumnsAreTheAllowlist: a point carries a sketch for a column
// exactly when Config.SketchColumns names it and the footer has one.
//
// Both halves matter. The series used to hardcode trace_id and duration_ms, which
// meant it could never answer distinct or a quantile over any other column
// however plainly the footer carried the sketch — and it kept calling a
// duration_ms accessor no footer this write path produces ever fills. Naming
// the columns is also the only bound on the series' memory: an HLL is 16KB
// whatever its cardinality, so "every column a footer has" is not a default.
func TestSeriesSketchColumnsAreTheAllowlist(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	desc := descriptor("s1", "svc", 3, start.Add(time.Minute), start.Add(2*time.Minute))

	// A footer whose sketches sit on columns nothing hardcoded: a distinct count
	// on user_id and a latency distribution on the param the collector parsed a
	// duration out of.
	f := buildFooter(footerSpec{rows: 3, patterns: []string{"t"}})
	view := f.ForService("svc")
	view.DistinctCounts["user_id"] = sketch.NewHLL()
	for _, u := range []string{"u1", "u2", "u3"} {
		view.DistinctCounts["user_id"].Add([]byte(u))
	}
	view.DistinctCounts["session_id"] = sketch.NewHLL()
	view.DistinctCounts["session_id"].Add([]byte("s"))
	view.Distributions["param_1"] = sketch.NewDDSketch(sketch.QuantileAccuracy)
	for _, v := range []float64{10, 20, 30} {
		view.Distributions["param_1"].Add(v)
	}

	// session_id is in the footer but not configured; duration_ms is configured
	// but not in the footer.
	c := summary.NewBinTable(summary.Config{
		BinWidth:      time.Hour,
		SketchColumns: []string{"user_id", "param_1", segment.ColDurationMs},
	})
	_, err := c.MergeFooter(desc, f, nil)
	require.NoError(t, err)

	points, err := c.SketchSlice("", start.UnixMilli(), start.Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	require.Len(t, points, 1)

	assert.Equal(t, []string{"user_id"}, slices.Sorted(maps.Keys(points[0].Distinct)))
	assert.Equal(t, []string{"param_1"}, slices.Sorted(maps.Keys(points[0].Quantiles)))
	assert.InDelta(t, 3.0, points[0].Distinct["user_id"].Estimate(), 0.5)
	assert.InDelta(t, 20.0, points[0].Quantiles["param_1"].Quantile(0.5), 20.0*sketch.QuantileAccuracy)
}

// TestSeriesNoSketchColumnsKeepsNoCells: the zero Config summarizes nothing, and
// an empty point is worse than no point — a read finds it and answers zero.
func TestSeriesNoSketchColumnsKeepsNoCells(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	c := summary.NewBinTable(summary.Config{BinWidth: time.Hour})
	_, err := c.MergeFooter(
		descriptor("s1", "svc", 2, start.Add(time.Minute), start.Add(2*time.Minute)),
		buildFooter(footerSpec{rows: 2, patterns: []string{"t"}, hllValues: [][]byte{[]byte("a")}}), nil)
	require.NoError(t, err)

	points, err := c.SketchSlice("", 0, 1<<62)
	require.NoError(t, err)
	assert.Empty(t, points)
	assert.Len(t, c.CountSlice("", 0, 1<<62), 1, "counts do not depend on SketchColumns")
}

// TestSeriesSegmentBinCap: a corrupt timestamp claiming centuries is an error, not
// a few million allocated points.
func TestSeriesSegmentBinCap(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	desc := descriptor("bad", "svc", 1, start, start.Add(365*24*time.Hour))
	f := buildFooter(footerSpec{rows: 1, patterns: []string{"t"}})

	c := summary.NewBinTable(summary.Config{BinWidth: time.Second, SketchColumns: testSketchColumns})
	_, err := c.MergeFooter(desc, f, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bad")
	assert.Empty(t, c.CountSlice("", 0, 1<<62), "a rejected segment must leave no points")
}

// TestSeriesRetentionEvicts: points older than the retention horizon go away, and
// the horizon is measured from the newest bin the series has seen rather than the
// wall clock — so the same sequence of merges evicts the same points on every
// run, and a corpus of old data is not empty on arrival.
func TestSeriesRetentionEvicts(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	c := summary.NewBinTable(summary.Config{BinWidth: time.Minute, Retention: 10 * time.Minute, SketchColumns: testSketchColumns})

	seal := func(id string, at time.Time) {
		_, err := c.MergeFooter(
			descriptor(id, "svc", 5, at, at.Add(time.Second)),
			buildFooter(footerSpec{rows: 5, patterns: []string{"t"}}), nil)
		require.NoError(t, err)
	}

	seal("old", start)
	seal("recent", start.Add(30*time.Minute))

	assert.Equal(t, start.Add(20*time.Minute).UnixMilli(), c.RetentionStart())
	got := c.CountSlice("", 0, 1<<62)
	require.Len(t, got, 1, "the bin 30 minutes behind the newest should have been evicted")
	assert.Equal(t, start.Add(30*time.Minute).UnixMilli(), got[0].Key.Bin)
}

// TestSeriesSlice covers the service filter, the bin-range filter, and
// deterministic ordering in one table.
func TestSeriesSlice(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	c := summary.NewBinTable(summary.Config{BinWidth: time.Minute, SketchColumns: testSketchColumns})

	seed := func(service, tmpl string, binIdx int) {
		t0 := start.Add(time.Duration(binIdx) * time.Minute)
		_, err := c.MergeFooter(
			descriptor(fmt.Sprintf("s-%s-%s-%d", service, tmpl, binIdx), service, 5,
				t0.Add(time.Second), t0.Add(30*time.Second)),
			buildFooter(footerSpec{
				service: service, rows: 5, patterns: []string{tmpl}, tmplCounts: map[string]int{tmpl: 5},
			}), nil)
		require.NoError(t, err)
	}

	seed("svc-a", "tmpl_1", 0)
	seed("svc-a", "tmpl_2", 1)
	seed("svc-b", "tmpl_1", 0)
	seed("svc-b", "tmpl_2", 2)

	bin0 := start.UnixMilli()
	bin1 := start.Add(time.Minute).UnixMilli()
	bin2 := start.Add(2 * time.Minute).UnixMilli()
	bin3 := start.Add(3 * time.Minute).UnixMilli()

	key := func(tmpl, svc string, bin int64) summary.CountKey {
		return summary.CountKey{PatternID: tmpl, Service: svc, Bin: bin}
	}

	tests := []struct {
		name     string
		service  string
		from, to int64
		want     []summary.CountKey
	}{
		{
			// Sorted by (Service, PatternID, Bin) — the determinism contract.
			name: "all services in order", from: bin0, to: bin3,
			want: []summary.CountKey{
				key("tmpl_1", "svc-a", bin0),
				key("tmpl_2", "svc-a", bin1),
				key("tmpl_1", "svc-b", bin0),
				key("tmpl_2", "svc-b", bin2),
			},
		},
		{
			name: "service filter", service: "svc-a", from: bin0, to: bin3,
			want: []summary.CountKey{key("tmpl_1", "svc-a", bin0), key("tmpl_2", "svc-a", bin1)},
		},
		{
			// [from, to): a bin at exactly to holds data outside the window.
			name: "range excludes the upper bound", from: bin0, to: bin1,
			want: []summary.CountKey{key("tmpl_1", "svc-a", bin0), key("tmpl_1", "svc-b", bin0)},
		},
		{
			name: "range excludes below", from: bin1, to: bin3,
			want: []summary.CountKey{key("tmpl_2", "svc-a", bin1), key("tmpl_2", "svc-b", bin2)},
		},
		{
			name: "empty range", from: bin0, to: bin0, want: []summary.CountKey{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			points := c.CountSlice(tc.service, tc.from, tc.to)
			got := make([]summary.CountKey, len(points))
			for i, point := range points {
				got[i] = point.Key
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestSeriesPropertyMergeEquivalence is the design's key oracle: merging N
// footers into the series one at a time produces the same merged count and
// sketch values as merging the same N footers directly.
func TestSeriesPropertyMergeEquivalence(t *testing.T) {
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	tmpl, svc := "tmpl_a", "svc"

	perFooter := []footerSpec{
		{
			rows:       10,
			patterns:   []string{tmpl},
			tmplCounts: map[string]int{tmpl: 5},
			hllValues:  [][]byte{[]byte("a"), []byte("b")},
			ddValues:   []float64{1, 2},
		},
		{
			rows:       15,
			patterns:   []string{tmpl},
			tmplCounts: map[string]int{tmpl: 7},
			hllValues:  [][]byte{[]byte("b"), []byte("c")},
			ddValues:   []float64{3, 4, 5},
		},
		{
			rows:       20,
			patterns:   []string{tmpl},
			tmplCounts: map[string]int{tmpl: 3},
			hllValues:  [][]byte{[]byte("a"), []byte("d"), []byte("e")},
			ddValues:   []float64{5, 6, 7, 8},
		},
	}

	c := hourSeries()
	for i, spec := range perFooter {
		desc := descriptor(fmt.Sprintf("seg%d", i), svc, spec.rows,
			start.Add(time.Duration(i)*time.Minute),
			start.Add(time.Duration(i)*time.Minute+30*time.Second))
		spec.service = svc
		_, err := c.MergeFooter(desc, buildFooter(spec), nil)
		require.NoError(t, err)
	}

	// Direct merge of the same sketches into fresh oracles.
	var wantLines int64
	oracleHLL := sketch.NewHLL()
	oracleDD := sketch.NewDDSketch(sketch.QuantileAccuracy)
	for _, spec := range perFooter {
		wantLines += int64(spec.tmplCounts[tmpl])

		hll := sketch.NewHLL()
		for _, v := range spec.hllValues {
			hll.Add(v)
		}
		require.NoError(t, oracleHLL.Merge(hll))

		dd := sketch.NewDDSketch(sketch.QuantileAccuracy)
		for _, v := range spec.ddValues {
			dd.Add(v)
		}
		require.NoError(t, oracleDD.Merge(dd))
	}

	assert.Equal(t, []summary.CountPoint{{Key: summary.CountKey{PatternID: tmpl, Service: svc, Bin: start.UnixMilli()}, Lines: wantLines}},
		c.CountSlice("", 0, 1<<62), "the series' count must equal the sum of the per-footer pattern frequencies")

	points, err := c.SketchSlice("", start.UnixMilli(), start.Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	require.Len(t, points, 1)
	require.NotNil(t, points[0].Distinct[segment.ColTraceID])
	require.NotNil(t, points[0].Quantiles[segment.ColDurationMs])

	assert.Equal(t, oracleHLL.Estimate(), points[0].Distinct[segment.ColTraceID].Estimate(),
		"the series' distinct-trace estimate must equal the direct merge's")
	for _, q := range []float64{0.5, 0.95, 0.99} {
		assert.InDelta(t, oracleDD.Quantile(q), points[0].Quantiles[segment.ColDurationMs].Quantile(q), 1e-9,
			"the series' quantile(%v) must equal the direct merge's", q)
	}
}

// TestConfigValidate: Validate judges only what a caller stated. Zero means unset and
// takes the package default, so the empty Config — the one every test here builds a
// store from — has to pass; a negative value is a mistake nothing can substitute for.
func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  summary.Config
		want string
	}{
		{name: "zero config takes defaults", cfg: summary.Config{}},
		{
			name: "negative bin width",
			cfg:  summary.Config{BinWidth: -time.Minute},
			want: "summary: bin width -1m0s must be positive",
		},
		{
			name: "negative retention",
			cfg:  summary.Config{Retention: -time.Hour},
			want: "summary: retention -1h0m0s must be positive",
		},
		{
			name: "negative prewarm",
			cfg:  summary.Config{LoadWindow: -time.Hour},
			want: "summary: prewarm -1h0m0s must be positive",
		},
		{
			name: "negative refresh interval",
			cfg:  summary.Config{RefreshInterval: -time.Second},
			want: "summary: refresh interval -1s must be positive",
		},
		{
			name: "prewarm beyond retention",
			cfg:  summary.Config{Retention: 6 * time.Hour, LoadWindow: 7 * time.Hour},
			want: "summary: prewarm 7h0m0s exceeds retention 6h0m0s",
		},
		{
			name: "prewarm beyond a defaulted retention is not a claim about retention",
			cfg:  summary.Config{LoadWindow: 7 * time.Hour},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.want == "" {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tt.want)
		})
	}
}
