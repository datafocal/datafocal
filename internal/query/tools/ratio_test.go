package tools_test

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/summary"
)

// statusEvents builds n events carrying message, one second apart from start, the
// first `errors` of them with status 503 and the rest 200. One message is one
// pattern, so the events differ only by the status a predicate can filter on.
func statusEvents(start time.Time, message string, n, errors int) []segment.Record {
	events := make([]segment.Record, n)
	for i := range events {
		status := "200"
		if i < errors {
			status = "503"
		}
		events[i] = segment.LogRecord(start.Add(time.Duration(i)*time.Second), "info", message, map[string]string{"status": status})
	}
	return events
}

// errStatus is the numeric 5xx numerator predicate the ratio tests filter on.
var errStatus = []selector.Predicate{{Field: "status", Op: selector.OpGe, Value: "500"}}

// TestAggregateRatio is the per-bin ratio oracle: a ratio moves only with the failure fraction where a count moves with traffic too
// (acceptance #1), a 0/0 bin is a gap the ratio omits where a count fills it with a
// real zero, and "ratio:<field>" divides sums over that column rather than row counts.
func TestAggregateRatio(t *testing.T) {
	// Bin A 20 req / 2 err (10%), bin B 40 req / 4 err (10%, traffic doubled), bin C
	// 40 req / 8 err (20%, failures doubled at constant traffic).
	scaling := slices.Concat(
		statusEvents(aggBase, "handled request", 20, 2),
		statusEvents(aggBase.Add(time.Minute), "handled request", 40, 4),
		statusEvents(aggBase.Add(2*time.Minute), "handled request", 40, 8))
	// Bin A has traffic, the middle bin has none, bin C has traffic.
	gapped := slices.Concat(
		statusEvents(aggBase, "handled request", 10, 1),
		statusEvents(aggBase.Add(2*time.Minute), "handled request", 10, 3))
	// Nine fast successes at 10ms and one slow failure at 910ms: 10% of the requests,
	// but 910/1000 of the time.
	weighted := make([]segment.Record, 10)
	for i := range weighted {
		status, ms := "200", 10
		if i == 0 {
			status, ms = "503", 910
		}
		weighted[i] = segment.LogRecord(aggBase.Add(time.Duration(i)*time.Second), "info", fmt.Sprintf("handled request in %d ms", ms), map[string]string{"status": status})
	}

	tests := []struct {
		name      string
		events    []segment.Record
		to        time.Duration
		measure   string
		where     []selector.Predicate
		numerator []selector.Predicate
		want      []*float64
		baselines []*float64
	}{
		{name: "traffic count scales A to B", events: scaling, to: 3 * time.Minute, measure: "count",
			want: []*float64{num(20), num(40), num(40)}},
		{name: "error count rises with traffic and with failures", events: scaling, to: 3 * time.Minute, measure: "count", where: errStatus,
			want: []*float64{num(2), num(4), num(8)}},
		// Correlating against the count would track the traffic, the false signal a ratio removes.
		{name: "ratio is flat where only traffic moved", events: scaling, to: 3 * time.Minute, measure: "ratio", numerator: errStatus,
			want: []*float64{num(0.1), num(0.1), num(0.2)}},
		{name: "count is dense with the empty bin a real zero", events: gapped, to: 3 * time.Minute, measure: "count",
			want: []*float64{num(10), num(0), num(10)}},
		{name: "ratio leaves the 0/0 middle bin null rather than 0, and baselines the bins around it", events: gapped, to: 3 * time.Minute, measure: "ratio", numerator: errStatus,
			want: []*float64{num(0.1), nil, num(0.3)}, baselines: []*float64{num(0.1), nil, num(0.16)}},
		{name: "one request in ten failed", events: weighted, to: time.Minute, measure: "ratio", numerator: errStatus,
			want: []*float64{num(0.1)}},
		{name: "but they took 910ms of the 1000ms spent", events: weighted, to: time.Minute, measure: "ratio:duration_ms", numerator: errStatus,
			want: []*float64{num(0.91)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, store, cat := newCorpus(t)
			writeSegment(t, store, cat, "gateway", tt.events)
			cr, rd := readers(dir)

			env, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), tools.AggregateRequest{
				Selector: selector.Selector{Time: window(aggBase, aggBase.Add(tt.to)), Where: tt.where},
				Measure:  tt.measure, GroupBy: "service", Bins: "1m", Numerator: tt.numerator,
			})
			require.NoError(t, err)

			require.Len(t, env.Data.Series, 1)
			got := env.Data.Series[0]
			assert.Equal(t, aggBase.UnixMilli(), got.Start)
			assert.Equal(t, tt.want, got.Values)
			if tt.baselines != nil {
				assert.Equal(t, tt.baselines, got.Baselines)
			}
		})
	}
}

// TestAggregateRatioScanBounded is acceptance #4: a filtered ratio that would read
// past either scan bound refuses with scan_too_large rather than reading or
// fabricating, while a ratio the summary can serve is never refused for a wide
// window — the bounds are on reads, not on the window.
//
// Segments and bytes are separate limits because a compacted segment is fifty times an
// L0 one, so neither substitutes for the other.
func TestAggregateRatioScanBounded(t *testing.T) {
	dir, store, cat := newCorpus(t)
	for i := range 6 {
		writeSegment(t, store, cat, fmt.Sprintf("svc-%d", i), []segment.Record{segment.LogRecord(aggBase.Add(time.Duration(i)*time.Minute), "info", "handled request", map[string]string{"status": "503"})})
	}
	cr, rd := readers(dir)
	w := window(aggBase, aggBase.Add(10*time.Minute))

	run := func(rs summary.Store, req tools.AggregateRequest) error {
		req.Selector = selector.Selector{Time: w}
		req.Measure, req.GroupBy, req.Bins = "ratio", "service", "1m"
		_, err := toolsOver(cr, rd, rs).Aggregate(context.Background(), req)
		return err
	}

	refusals := []struct {
		name string
		req  tools.AggregateRequest
	}{
		{name: "past_segment_cap", req: tools.AggregateRequest{Numerator: errStatus, MaxScanSegments: 2}},
		// One byte: any real segment is over it, so this bites on size alone with the
		// segment count left at its default.
		{name: "past_byte_cap", req: tools.AggregateRequest{Numerator: errStatus, MaxScanBytes: 1}},
	}
	for _, tt := range refusals {
		t.Run(tt.name, func(t *testing.T) {
			var apiErr *query.APIError
			require.ErrorAs(t, run(nil, tt.req), &apiErr, "a filtered ratio past the bound must refuse")
			assert.Equal(t, query.ErrCodeScanTooLarge, apiErr.Code)
			assert.NotEmpty(t, apiErr.Hint, "the refusal must carry the re-plan")
		})
	}

	t.Run("negative_is_the_opt_out", func(t *testing.T) {
		require.NoError(t, run(nil, tools.AggregateRequest{
			Numerator: errStatus, MaxScanSegments: -1, MaxScanBytes: -1,
		}))
	})

	// Both counts served from the summary: no scan, so no bound engages however wide the
	// window. A numerator of one service= predicate keeps the numerator pass inside the
	// summary's scope (plan.Planner.Precomputed), so neither pass reads a segment.
	t.Run("summary_served_is_never_refused", func(t *testing.T) {
		rs := wireSummary(t, cr, rd, summary.Config{BinWidth: time.Minute})
		require.NoError(t, run(rs, tools.AggregateRequest{
			Numerator:       []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "svc-0"}},
			MaxScanSegments: 1,
			MaxScanBytes:    1,
		}))
	})
}

// TestAggregateRatioRejectsNumeratorMismatch: a ratio with no numerator is 1.0 in
// every bin, and a numerator on any other measure is silently dropped. Both answer a
// question nobody asked, so both are refused rather than computed.
func TestAggregateRatioRejectsNumeratorMismatch(t *testing.T) {
	dir, store, cat := newCorpus(t)
	writeSegment(t, store, cat, "gateway", statusEvents(aggBase, "handled request", 10, 2))
	cr, rd := readers(dir)

	sel := selector.Selector{Time: window(aggBase, aggBase.Add(time.Minute))}
	tests := []struct {
		name      string
		measure   string
		numerator []selector.Predicate
	}{
		{name: "ratio_without_numerator", measure: "ratio"},
		{name: "count_with_numerator", measure: "count", numerator: errStatus},
		{name: "p99_with_numerator", measure: "p99", numerator: errStatus},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), tools.AggregateRequest{
				Selector: sel, Measure: tt.measure, GroupBy: "service", Bins: "1m", Numerator: tt.numerator,
			})
			var invalid *query.InvalidRequestError
			require.ErrorAs(t, err, &invalid, "a numerator/measure mismatch is the caller's to fix")
			assert.Contains(t, invalid.Error(), "ratio")
		})
	}
}

// TestDensifyGapValue pins the gap→NaN bridge a ratio series rides on. A present bin
// keeps its value either way; an absent one takes the caller's missing value — 0 for a
// count, NaN for a ratio's 0/0, which the correlation path skips.
func TestDensifyGapValue(t *testing.T) {
	bins := series.Bins{Width: time.Second, From: time.UnixMilli(0).UTC(), To: time.UnixMilli(3000).UTC()}
	present := map[int64]float64{0: 0.1, 2000: 0.3}

	asRatio := tools.ExportDensify(present, bins, math.NaN())
	require.Len(t, asRatio, 3, "boundaries at 0, 1000, 2000")
	assert.Equal(t, 0.1, asRatio[0])
	assert.True(t, math.IsNaN(asRatio[1]), "the gap bin densifies to NaN")
	assert.Equal(t, 0.3, asRatio[2])

	assert.Equal(t, []float64{0.1, 0, 0.3}, tools.ExportDensify(present, bins, 0),
		"the same map with a count's gap value fills the bin instead")
}
