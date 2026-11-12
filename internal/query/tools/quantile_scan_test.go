package tools_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
)

// TestAggregateScannedQuantileOracle pins acceptance #2 and its siblings: a quantile a
// row-level predicate or group_by forces onto the scan path is exact over just the
// matching rows, one with neither stays on the approximate footer sketches, and a
// filtered distinct scans into an HLL that stays approximate but sees only the matching
// shards (which path each takes is TestAggregateCapabilities' job, this pins the numbers).
func TestAggregateScannedQuantileOracle(t *testing.T) {
	cr, rd := capabilityCorpus(t)
	miss := []selector.Predicate{{Field: "outcome", Op: selector.OpEq, Value: "miss"}}
	hll := 2 * sketch.HLLStandardError

	// The corpus runs duration_ms = i+1 for i in [0,20), shard = s{i%2} and outcome miss on
	// every fourth row, so the misses are {1,5,9,13,17} all on s0, which holds the odds
	// {1,3..19} while s1 holds the evens {2,4..20}.
	tests := []struct {
		name    string
		measure string
		groupBy string
		where   []selector.Predicate
		want    map[string]float64
		// delta is the absolute error allowed: 0 for a scanned quantile, the HLL's for a distinct.
		delta  float64
		approx bool
	}{
		// Nearest-rank p99 of five values is the 5th: the max.
		{name: "row_level_predicate", measure: "p99:duration_ms", groupBy: "service", where: miss, want: map[string]float64{"cache": 17}},
		// Nearest-rank p50 of ten values is the 5th of each group.
		{name: "row_level_group_by", measure: "p50:duration_ms", groupBy: "shard", want: map[string]float64{"s0": 9, "s1": 10}},
		// DDSketch's rank convention lands p99 of twenty on the 19th, and 2% covers its 1% bucket edge.
		{name: "bare_quantile_keeps_the_sketch_path", measure: "p99:duration_ms", groupBy: "service", want: map[string]float64{"cache": 19}, delta: 0.02 * 19, approx: true},
		{name: "distinct_over_both_shards", measure: "distinct:shard", groupBy: "service", want: map[string]float64{"cache": 2}, delta: hll * 2, approx: true},
		// A 2 here would mean the predicate never reached the rows.
		{name: "distinct_honours_the_filter", measure: "distinct:shard", groupBy: "service", where: miss, want: map[string]float64{"cache": 1}, delta: hll, approx: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), tools.AggregateRequest{
				Selector: selector.Selector{Time: window(aggBase, aggBase.Add(time.Minute)), Where: tt.where},
				Measure:  tt.measure, GroupBy: tt.groupBy, Bins: "1m",
			})
			require.NoError(t, err, "a measure the sketches cannot answer must scan, not refuse")
			assert.Equal(t, tt.approx, env.Meta.Approx, "meta.approx")

			got := map[string]float64{}
			for _, s := range env.Data.Series {
				require.Len(t, s.Values, 1)
				got[s.Key] = *s.Values[0]
			}
			assert.InDeltaMapValues(t, tt.want, got, tt.delta)
		})
	}
}
