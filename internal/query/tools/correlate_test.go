package tools_test

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/stats"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// correlateBase is a minute-aligned instant so the request grid lands on the
// summary's bin boundaries without rounding — Store.CanServe requires it, and an
// unaligned window would silently exercise the scan path instead.
var correlateBase = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

const correlateBinWidth = time.Minute

// pulseAt spikes a lone pattern to 5 in bin binIndex.
func pulseAt(patternID string, binIndex int) summary.Segment {
	binStart := correlateBase.Add(time.Duration(binIndex) * correlateBinWidth).UnixMilli()
	desc := segment.Descriptor{
		ID:     fmt.Sprintf("%s-bin%d", patternID, binIndex),
		Stream: segment.Stream{Service: "app"},
		TimeRange: segment.TimeRange{
			Min: time.UnixMilli(binStart + 1000).UTC(),
			Max: time.UnixMilli(binStart + 2000).UTC(),
		},
		Rows: 5,
	}
	return summary.Segment{Descriptor: desc, Footer: patternFooter("app", patternID, desc.Rows)}
}

// patternFooter is a footer whose rows are all service's and patternID's.
func patternFooter(service, patternID string, rows int) segment.Footer {
	counts := map[segment.ServicePattern]int{{Service: service, Pattern: patternID}: rows}
	return segment.FooterWithServices(segment.Footer{ServicePatternCounts: counts}, map[string]segment.Footer{service: {Rows: rows}})
}

// correlateSnapshot is the pin every hand-seeded correlate request carries. The
// store below declares itself synced at the same epoch, which is what lets the
// tool read it — and lets these tests pass no catalog at all.
const correlateSnapshot = "e1.s1"

// newCorrelateStore returns a minute-binned summary of segs, complete from
// correlateBase so an unseeded bin reads as zero rather than unknown.
func newCorrelateStore(t *testing.T, segs ...summary.Segment) summary.Store {
	t.Helper()
	rs, err := summary.NewStore(summary.Config{
		BinWidth:  correlateBinWidth,
		Retention: 365 * 24 * time.Hour,
	}, "1", correlateBase, segs...)
	require.NoError(t, err)
	return rs
}

// correlateRequest wires an 8-bin window over correlateBase with a pinned
// snapshot, so the tool never has to reach a catalog.
func correlateRequest(a, b string, maxLagSeconds int) tools.CorrelateRequest {
	return tools.CorrelateRequest{
		Selector: selector.Selector{
			Time: window(correlateBase, correlateBase.Add(8*correlateBinWidth)),
		},
		SeriesA:       tools.SeriesRef{PatternID: a},
		SeriesB:       tools.SeriesRef{PatternID: b},
		MaxLagSeconds: maxLagSeconds,
		Snapshot:      correlateSnapshot,
	}
}

// TestCorrelateSummarySeries correlates hand-seeded pulses off a summary alone, so every
// answer is free and carries the pinned snapshot; a peak of 27/28 is ρ normalized over
// the whole series, where samples a two-bin lag shifts past the window end cost score.
func TestCorrelateSummarySeries(t *testing.T) {
	tests := []struct {
		name        string
		pulses      map[string][]int
		a, b        string
		maxLag      int
		want        tools.CorrelateResponse
		wantInvalid bool
	}{
		{
			name:   "recovers shift and direction",
			pulses: map[string][]int{"tmpl-a": {2}, "tmpl-b": {4}},
			a:      "tmpl-a", b: "tmpl-b", maxLag: 300,
			want: tools.CorrelateResponse{Rho: 27.0 / 28.0, LagSeconds: 120, Direction: "a_leads_b"},
		},
		{
			name:   "reversed flips direction",
			pulses: map[string][]int{"tmpl-a": {2}, "tmpl-b": {4}},
			a:      "tmpl-b", b: "tmpl-a", maxLag: 300,
			want: tools.CorrelateResponse{Rho: 27.0 / 28.0, LagSeconds: -120, Direction: "b_leads_a"},
		},
		{
			name:   "constant series is none",
			pulses: map[string][]int{"tmpl-a": {2}, "tmpl-flat": {0, 1, 2, 3, 4, 5, 6, 7}},
			a:      "tmpl-flat", b: "tmpl-a", maxLag: 300,
			want: tools.CorrelateResponse{Direction: "none"},
		},
		{
			name:   "unknown pattern is an all-zero series",
			pulses: map[string][]int{"tmpl-a": {2}},
			a:      "tmpl-a", b: "missing", maxLag: 300,
			want: tools.CorrelateResponse{Direction: "none"},
		},
		{
			name:   "full search recovers a three-bin shift",
			pulses: map[string][]int{"tmpl-a": {1}, "tmpl-b": {4}},
			a:      "tmpl-a", b: "tmpl-b", maxLag: 300,
			want: tools.CorrelateResponse{Rho: 53.0 / 56.0, LagSeconds: 180, Direction: "a_leads_b"},
		},
		{
			name:   "lag is clamped to max lag",
			pulses: map[string][]int{"tmpl-a": {1}, "tmpl-b": {4}},
			a:      "tmpl-a", b: "tmpl-b", maxLag: 60,
			want: tools.CorrelateResponse{Rho: -9.0 / 56.0, LagSeconds: -60, Direction: "b_leads_a"},
		},
		{name: "empty pattern is invalid", a: "", b: "tmpl-b", maxLag: 300, wantInvalid: true},
		{name: "negative max lag is invalid", a: "tmpl-a", b: "tmpl-b", maxLag: -1, wantInvalid: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var segs []summary.Segment
			for id, bins := range tt.pulses {
				for _, bin := range bins {
					segs = append(segs, pulseAt(id, bin))
				}
			}
			store := newCorrelateStore(t, segs...)

			env, err := toolsOver(catalog.Fake{}, segment.Fake{}, store).Correlate(context.Background(), correlateRequest(tt.a, tt.b, tt.maxLag))
			if tt.wantInvalid {
				var invalid *query.InvalidRequestError
				require.ErrorAs(t, err, &invalid)
				return
			}
			require.NoError(t, err)

			assertCorrelation(t, tt.want, env.Data)
			// The summary answers from CountSlice alone — no footer or column reads.
			assert.Equal(t, query.Cost{}, env.Meta.Cost)
			assert.Equal(t, correlateSnapshot, env.Meta.Snapshot)
			assert.Equal(t, 1.0, env.Meta.Coverage)
		})
	}
}

// assertCorrelation compares a whole response, holding ρ to a float tolerance.
func assertCorrelation(t *testing.T, want, got tools.CorrelateResponse) {
	t.Helper()
	assert.InDelta(t, want.Rho, got.Rho, 1e-9)
	got.Rho = want.Rho
	assert.Equal(t, want, got)
}

// seedCorrelationCorpus keeps each segment inside one bin, leaving straddling to
// seedStraddlingCorpus.
func seedCorrelationCorpus(t *testing.T, store storage.Store, cat catalog.Catalog) {
	t.Helper()
	burst := func(binIndex int, shape string) {
		msgs := make([]string, 5)
		for i := range msgs {
			msgs[i] = fmt.Sprintf(shape, i)
		}
		start := correlateBase.Add(time.Duration(binIndex) * 15 * time.Second)
		writeSegment(t, store, cat, "app", messageEvents(start, msgs...))
	}
	burst(2, "alpha finished job %d")
	burst(4, "beta finished job %d")
}

// corpusPatterns returns the two sorted pattern_ids, which are content hashes and so cannot be hard-coded.
func corpusPatterns(t *testing.T, cr catalog.Catalog, rd segment.Reader) (string, string) {
	t.Helper()
	env, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), tools.AggregateRequest{
		Selector: selector.Selector{Time: window(correlateBase, correlateBase.Add(2*time.Minute))},
		Measure:  "count",
		GroupBy:  segment.ColPatternID,
	})
	require.NoError(t, err)
	keys := make([]string, 0, len(env.Data.Series))
	for _, s := range env.Data.Series {
		keys = append(keys, s.Key)
	}
	slices.Sort(keys)
	require.Len(t, keys, 2, "corpus should produce exactly two patterns")
	return keys[0], keys[1]
}

// scanCorrelateRequest correlates the two seeded patterns on a 15s grid over the
// corpus window, against whichever store is passed.
func scanCorrelateRequest(a, b string) tools.CorrelateRequest {
	return tools.CorrelateRequest{
		Selector: selector.Selector{
			Time: window(correlateBase, correlateBase.Add(2*time.Minute)),
		},
		SeriesA:       tools.SeriesRef{PatternID: a},
		SeriesB:       tools.SeriesRef{PatternID: b},
		MaxLagSeconds: 60,
		Bins:          "15s",
	}
}

// seedStraddlingCorpus writes segments whose rows sit at one end of each bin they
// span, so splitting them by time overlap would get the shape wrong.
//
// Each segment covers 24s from a 30s boundary and carries both patterns: alpha only
// in its first 15s bin, beta only in its second, so the two series are exactly opposed.
func seedStraddlingCorpus(t *testing.T, store storage.Store, cat catalog.Catalog) {
	t.Helper()
	for seg := range 4 {
		start := correlateBase.Add(time.Duration(seg) * 30 * time.Second)
		events := make([]segment.Record, 0, 18)
		for _, span := range []struct {
			offset time.Duration
			shape  string
		}{{0, "alpha finished job %d"}, {16 * time.Second, "beta finished job %d"}} {
			for i := range 9 {
				events = append(events, segment.LogRecord(start.Add(span.offset+time.Duration(i)*time.Second), "info", fmt.Sprintf(span.shape, i), nil))
			}
		}
		writeSegment(t, store, cat, "app", events)
	}
}

// TestCorrelateSummaryNeverChangesTheAnswer is the gate on the summary hot path: the same corpus
// and window answered with and without a wired summary must agree, which the summary may
// only make cheaper.
func TestCorrelateSummaryNeverChangesTheAnswer(t *testing.T) {
	tests := []struct {
		name     string
		seed     func(*testing.T, storage.Store, catalog.Catalog)
		summary  time.Duration
		maxLag   int
		want     tools.CorrelateResponse
		wantHot  query.Cost
		wantCold query.Cost
	}{
		{
			// Every segment holds one pattern inside one bin, so the descriptor answers the
			// cold grouped read without an object read either.
			name:    "summary on the grid is a cache",
			seed:    seedCorrelationCorpus,
			summary: 15 * time.Second,
			maxLag:  60,
			want:    tools.CorrelateResponse{Rho: 27.0 / 28.0, LagSeconds: 30, Direction: "a_leads_b"},
		},
		{
			// A minute-binned summary holds no 15s shape, so the 30s lag proves it fell
			// through rather than answering {rho: 0, direction: none}.
			name:    "summary off the grid misses and still answers",
			seed:    seedCorrelationCorpus,
			summary: time.Minute,
			maxLag:  60,
			want:    tools.CorrelateResponse{Rho: 27.0 / 28.0, LagSeconds: 30, Direction: "a_leads_b"},
		},
		{
			// The rows are exactly opposed at lag zero, which the summary counts from them.
			name:     "straddling segments are served from their rows' counts",
			seed:     seedStraddlingCorpus,
			summary:  15 * time.Second,
			maxLag:   0,
			want:     tools.CorrelateResponse{Rho: -1, Direction: "none"},
			wantCold: query.Cost{GETs: 8, BytesRead: 508, Segments: 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, store, cat := newCorpus(t)
			tt.seed(t, store, cat)
			cr, rd := readers(dir)
			a, b := corpusPatterns(t, cr, rd)
			rs := wireSummary(t, cr, rd, summary.Config{BinWidth: tt.summary})
			req := scanCorrelateRequest(a, b)
			req.MaxLagSeconds = tt.maxLag

			hot, err := toolsOver(cr, rd, rs).Correlate(context.Background(), req)
			require.NoError(t, err)
			cold, err := toolsOver(cr, rd, nil).Correlate(context.Background(), req)
			require.NoError(t, err)

			assert.Equal(t, cold.Data, hot.Data, "a warm summary must not change what correlate answers")
			assertCorrelation(t, tt.want, hot.Data)
			assert.Equal(t, tt.wantHot, hot.Meta.Cost)
			assert.Equal(t, tt.wantCold, cold.Meta.Cost)
		})
	}
}

// twoPatternsByRows tells the two patterns apart by row count since their ids are content hashes.
func twoPatternsByRows(t *testing.T, cr catalog.Catalog, rd segment.Reader, w selector.TimeSpec, rowsA, rowsB int) (string, string) {
	t.Helper()
	env, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), tools.AggregateRequest{
		Selector: selector.Selector{Time: w},
		Measure:  "count",
		GroupBy:  segment.ColPatternID,
		Bins:     "1h",
	})
	require.NoError(t, err)
	var a, b string
	for _, s := range env.Data.Series {
		var sum int
		for _, v := range present(s.Values) {
			sum += int(v)
		}
		switch sum {
		case rowsA:
			a = s.Key
		case rowsB:
			b = s.Key
		}
	}
	require.NotEmpty(t, a, "expected a pattern with %d rows", rowsA)
	require.NotEmpty(t, b, "expected a pattern with %d rows", rowsB)
	return a, b
}

// ratioRampCorpus seeds what both ratio-measure correlate oracles read: alpha at a
// constant four requests a bin whose 503 share ramps .25→1.0, and beta at a count
// ramping 1,2,3,4 to match, so alpha's count is flat while its failure rate tracks
// beta exactly. noise adds that many one-line services neither series asks about, so
// a read that fails to narrow to the wanted patterns pays for them.
func ratioRampCorpus(t *testing.T, base time.Time, noise int) (catalog.Catalog, segment.Reader, selector.TimeSpec, string, string) {
	t.Helper()
	dir, store, cat := newCorpus(t)
	var events []segment.Record
	for bin := range 4 {
		at := base.Add(time.Duration(bin) * time.Minute)
		events = append(events, statusEvents(at, "alpha handled a request", 4, bin+1)...)
		for i := range bin + 1 {
			events = append(events, segment.LogRecord(at.Add(time.Duration(30+i)*time.Second), "info", "beta did some work", nil))
		}
	}
	writeSegment(t, store, cat, "svc", events)
	for i := range noise {
		writeSegment(t, store, cat, fmt.Sprintf("noise-%d", i), []segment.Record{segment.LogRecord(base.Add(time.Duration(i)*time.Second), "info", fmt.Sprintf("noise stream %d emitted a line", i), nil)})
	}
	cr, rd := readers(dir)

	w := window(base, base.Add(4*time.Minute))
	// alpha 4×4 = 16 rows across the four bins, beta 1+2+3+4 = 10.
	alpha, beta := twoPatternsByRows(t, cr, rd, w, 16, 10)
	return cr, rd, w, alpha, beta
}

// TestCorrelateSeriesScopeChangesTheAnswer is the oracle for SeriesRef's Measure and
// Where: alpha's whole count is flat while its 503 slice and 503 ratio ramp in lockstep
// with beta, so only a caller who can scope the series sees the relationship.
func TestCorrelateSeriesScopeChangesTheAnswer(t *testing.T) {
	cr, rd, w, alpha, beta := ratioRampCorpus(t, correlateBase.Add(48*time.Hour), 0)
	status503 := []selector.Predicate{{Field: "status", Op: selector.OpEq, Value: "503"}}

	tests := []struct {
		name    string
		seriesA tools.SeriesRef
		want    tools.CorrelateResponse
	}{
		{"whole count is flat", tools.SeriesRef{PatternID: alpha}, tools.CorrelateResponse{Direction: "none"}},
		{"where slice ramps with beta", tools.SeriesRef{PatternID: alpha, Where: status503}, tools.CorrelateResponse{Rho: 1, Direction: "none"}},
		{"ratio ramps with beta", tools.SeriesRef{PatternID: alpha, Measure: "ratio", Where: errStatus}, tools.CorrelateResponse{Rho: 1, Direction: "none"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := toolsOver(cr, rd, nil).Correlate(context.Background(), tools.CorrelateRequest{
				Selector: selector.Selector{Time: w},
				SeriesA:  tt.seriesA,
				SeriesB:  tools.SeriesRef{PatternID: beta},
				Bins:     "1m",
			})
			require.NoError(t, err)
			assertCorrelation(t, tt.want, env.Data)
		})
	}
}

// TestCorrelateRatioSkipsGapBinsRatherThanZeroingThem proves the ratio series is
// densified with NaN gaps, not zeros: alpha has no traffic in the third bin — a 0/0
// gap — while beta keeps an off-trend count there. Correlating the ratio against beta
// must skip that bin, matching the NaN-aware coefficient and not the one a fabricated
// zero would produce, which here even flips the sign.
func TestCorrelateRatioSkipsGapBinsRatherThanZeroingThem(t *testing.T) {
	dir, store, cat := newCorpus(t)
	base := correlateBase.Add(48 * time.Hour)
	// Per bin: alpha requests/errors (the rate) and beta count. The third bin has no
	// alpha traffic (the gap) but an off-ramp beta count, so skipping vs zeroing disagree.
	plan := []struct{ reqs, errs, beta int }{
		{4, 1, 1}, // rate .25
		{4, 2, 3}, // rate .50
		{0, 0, 9}, // gap; beta off the ramp
		{4, 3, 5}, // rate .75
	}
	var events []segment.Record
	for bin, p := range plan {
		at := base.Add(time.Duration(bin) * time.Minute)
		events = append(events, statusEvents(at, "alpha handled a request", p.reqs, p.errs)...)
		for i := range p.beta {
			events = append(events, segment.LogRecord(at.Add(time.Duration(30+i)*time.Second), "info", "beta did some work", nil))
		}
	}
	writeSegment(t, store, cat, "svc", events)
	cr, rd := readers(dir)

	w := window(base, base.Add(4*time.Minute))
	// alpha 4+4+0+4 = 12 rows, beta 1+3+9+5 = 18.
	alpha, beta := twoPatternsByRows(t, cr, rd, w, 12, 18)

	env, err := toolsOver(cr, rd, nil).Correlate(context.Background(), tools.CorrelateRequest{
		Selector:      selector.Selector{Time: w},
		SeriesA:       tools.SeriesRef{PatternID: alpha, Measure: "ratio", Where: errStatus},
		SeriesB:       tools.SeriesRef{PatternID: beta},
		Bins:          "1m",
		MaxLagSeconds: 0,
	})
	require.NoError(t, err)

	rates := []float64{0.25, 0.5, math.NaN(), 0.75}
	betaCounts := []float64{1, 3, 9, 5}
	skipped := stats.CrossCorrelate(rates, betaCounts, time.Minute, 0)
	zeroed := stats.CrossCorrelate([]float64{0.25, 0.5, 0, 0.75}, betaCounts, time.Minute, 0)

	require.False(t, math.IsNaN(env.Data.Rho), "a gap must not poison the coefficient")
	assert.InDelta(t, skipped.Rho, env.Data.Rho, 1e-9, "the gap bin is skipped, as densify's NaN")
	assert.Greater(t, math.Abs(env.Data.Rho-zeroed.Rho), 0.1,
		"and is not read as a fabricated zero — here the two even differ in sign")
}

// TestCorrelateRatioNarrowsToTheWantedPatterns caps a ratio at twice the filtered count's cost,
// which holds only if both passes skip the six unrelated services' segments.
func TestCorrelateRatioNarrowsToTheWantedPatterns(t *testing.T) {
	cr, rd, w, alpha, beta := ratioRampCorpus(t, correlateBase.Add(72*time.Hour), 6)

	cost := func(a tools.SeriesRef) query.Cost {
		env, err := toolsOver(cr, rd, nil).Correlate(context.Background(), tools.CorrelateRequest{
			Selector: selector.Selector{Time: w},
			SeriesA:  a,
			SeriesB:  tools.SeriesRef{PatternID: beta},
			Bins:     "1m",
		})
		require.NoError(t, err)
		return env.Meta.Cost
	}

	filteredCount := cost(tools.SeriesRef{PatternID: alpha, Where: errStatus})
	ratio := cost(tools.SeriesRef{PatternID: alpha, Measure: "ratio", Where: errStatus})

	require.NotZero(t, filteredCount.GETs, "the filtered count must actually read something to bound against")
	assert.LessOrEqual(t, ratio.GETs, 2*filteredCount.GETs,
		"a ratio is two narrowed count passes, not two scans of the window")
}

// TestCorrelateRatioRequiresANumerator: for a ratio series Where is the numerator, so
// an empty one is a constant 1.0 that correlates with nothing for the wrong reason.
// Refused rather than answered.
func TestCorrelateRatioRequiresANumerator(t *testing.T) {
	dir, store, cat := newCorpus(t)
	base := correlateBase.Add(96 * time.Hour)
	writeSegment(t, store, cat, "svc", []segment.Record{
		segment.LogRecord(base, "info", "alpha handled a request", nil),
		segment.LogRecord(base.Add(time.Second), "info", "alpha handled a request", nil),
		segment.LogRecord(base.Add(2*time.Second), "info", "beta did some work", nil),
	})
	cr, rd := readers(dir)

	w := window(base, base.Add(2*time.Minute))
	alpha, beta := twoPatternsByRows(t, cr, rd, w, 2, 1)
	_, err := toolsOver(cr, rd, nil).Correlate(context.Background(), tools.CorrelateRequest{
		Selector: selector.Selector{Time: w},
		SeriesA:  tools.SeriesRef{PatternID: alpha, Measure: "ratio"},
		SeriesB:  tools.SeriesRef{PatternID: beta},
		Bins:     "1m",
	})
	var invalid *query.InvalidRequestError
	require.ErrorAs(t, err, &invalid, "a ratio series with no numerator is the caller's to fix")
	assert.Contains(t, invalid.Error(), "ratio")
}
