package stats

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEWMA pins µ_0 = x_0 and µ_t = λ·x_t + (1-λ)·µ_{t-1}.
func TestEWMA(t *testing.T) {
	const eps = 1e-9
	tests := []struct {
		name   string
		values []float64
		lambda float64
		want   []float64
		delta  float64
	}{
		// µ_1 = 0.3*20 + 0.7*10 = 13, µ_2 = 0.3*30 + 0.7*13 = 18.1, and so on.
		{name: "ramp", values: []float64{10, 20, 30, 40, 50}, lambda: 0.3, want: []float64{10, 13, 18.1, 24.67, 32.269}, delta: eps},
		{name: "single point seeds itself", values: []float64{42.5}, lambda: DefaultLambda, want: []float64{42.5}},
		{name: "empty does not panic", values: []float64{}, lambda: DefaultLambda, want: []float64{}},
		{name: "constant input", values: []float64{7.5, 7.5, 7.5, 7.5}, lambda: DefaultLambda, want: []float64{7.5, 7.5, 7.5, 7.5}, delta: eps},
		{name: "zero lambda freezes the seed", values: []float64{5, 100, -50}, lambda: 0, want: []float64{5, 5, 5}, delta: eps},
		{name: "unit lambda does no smoothing", values: []float64{5, 100, -50}, lambda: 1, want: []float64{5, 100, -50}, delta: eps},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.InDeltaSlice(t, tt.want, EWMA(tt.values, tt.lambda), tt.delta)
		})
	}
}

// TestSpikes pins the score, (x_t - µ_{t-1}) / max(σ_{t-1}, √max(µ_{t-1}, 1)), at
// both ends of its denominator and at the threshold.
func TestSpikes(t *testing.T) {
	// flat is seven bins of v, so the series it starts reaches minSupportBins.
	flat := func(v float64, tail ...float64) []float64 {
		return append(slices.Repeat([]float64{v}, 7), tail...)
	}
	tests := []struct {
		name   string
		values []float64
		want   []Spike
	}{
		// A flat history has no variance, so the jump is scored against √4: (20-4)/2.
		{name: "jump off a flat series", values: flat(4, 20), want: []Spike{{Index: 7, Expected: 4, Score: 8}}},
		{name: "rise below the threshold", values: flat(4, 9)},
		{name: "drop is not a spike", values: flat(20, 0)},
		// A baseline under one floors at √1, so a rise of 3 sits exactly on the threshold.
		{name: "small baseline at the threshold", values: flat(0.5, 3.5), want: []Spike{{Index: 7, Expected: 0.5, Score: 3}}},
		{name: "small baseline under the threshold", values: flat(0.5, 3.4)},
		// The first jump leaves σ = √32.1 ≈ 5.67 by index 10, above √6.352, so the second
		// scores (40-6.352)/5.67 rather than 13.4.
		{name: "variance widens the deviation", values: flat(4, 20, 4, 4, 40),
			want: []Spike{{Index: 7, Expected: 4, Score: 8}, {Index: 10, Expected: 6.352, Score: 5.9389}}},
		{name: "lines in too few bins", values: []float64{0, 0, 0, 0, 0, 0, 0, 40}},
		{name: "single value has nothing before it", values: []float64{100}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Spikes(tt.values)
			require.Len(t, got, len(tt.want))
			for i, want := range tt.want {
				assert.Equal(t, want.Index, got[i].Index)
				assert.InDelta(t, want.Expected, got[i].Expected, 1e-9)
				assert.InDelta(t, want.Score, got[i].Score, 1e-4)
			}
		})
	}
}

// TestSupported pins the floor at eight bins with lines, wherever they fall.
func TestSupported(t *testing.T) {
	assert.True(t, Supported([]float64{1, 0, 1, 1, 1, 1, 1, 1, 1}))
	assert.False(t, Supported([]float64{1, 0, 1, 1, 1, 0, 1, 1, 1}))
}

// TestSupportedSplits: the splits it names are those both of whose sides are Supported, on
// series of every density, the empty range included.
func TestSupportedSplits(t *testing.T) {
	for seed := range 200 {
		values := make([]float64, seed%40)
		for i := range values {
			if (i*7+seed*13)%(seed%5+2) == 0 {
				values[i] = float64(i%3 + 1)
			}
		}
		var want []int
		for split := 1; split < len(values); split++ {
			if Supported(values[:split]) && Supported(values[split:]) {
				want = append(want, split)
			}
		}
		first, last := SupportedSplits(values)
		var got []int
		for split := max(first, 1); split <= min(last, len(values)-1); split++ {
			got = append(got, split)
		}
		require.Equal(t, want, got, "%v", values)
	}
}

// TestRoutine: a steady beat is routine; a gap or a swing is not.
func TestRoutine(t *testing.T) {
	assert.True(t, Routine([]float64{3, 3, 2, 4, 3, 3}))
	assert.False(t, Routine([]float64{3, 3, 0, 3, 3, 3}))
	assert.False(t, Routine([]float64{1, 1, 1, 9, 1, 1}))
}
