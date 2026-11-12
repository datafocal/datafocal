package stats

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCrossCorrelateDegenerateInputs(t *testing.T) {
	tests := []struct {
		name string
		a, b []float64
	}{
		{"length mismatch", []float64{1, 2, 3}, []float64{1, 2}},
		{"empty", []float64{}, []float64{}},
		{"single sample", []float64{1}, []float64{1}},
		{"a is constant", []float64{5, 5, 5, 5}, []float64{1, 2, 3, 4}},
		{"b is constant", []float64{1, 2, 3, 4}, []float64{3, 3, 3, 3}},
		{"both constant", []float64{5, 5, 5, 5}, []float64{3, 3, 3, 3}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, CorrelationResult{Direction: "none"}, CrossCorrelate(tc.a, tc.b, time.Second, 5))
		})
	}
}

func TestCrossCorrelateProportionalSeries(t *testing.T) {
	a := []float64{1, 2, 3, 4, 5, 6}
	b := []float64{0.5, 1, 1.5, 2, 2.5, 3}

	got := CrossCorrelate(a, b, time.Second, 2)

	assert.InDelta(t, 1.0, got.Rho, 1e-12, "b = a/2 is a perfect linear relationship")
	assert.Equal(t, CorrelationResult{Rho: got.Rho, LagBins: 0, LagDuration: 0, Direction: "none"}, got,
		"an aligned pair peaks at lag zero, so neither series leads")
}

// TestCrossCorrelateRecoversTheGeneratingShift: the oracle is the shift the test
// itself applied — b is a delayed, jittered copy of a, so the peak must land on
// that delay. It catches the normalization bug that per-overlap ρ hid: at
// |lag| = n-1 the overlap is a single sample pair, which is perfectly collinear
// by construction, so every extreme lag scored |ρ| = 1 and outranked the real
// peak whenever the real peak was anything short of perfect.
func TestCrossCorrelateRecoversTheGeneratingShift(t *testing.T) {
	const shift = 3
	base := []float64{1, 1, 9, 6, 3, 1, 1, 2, 7, 4, 1, 1}
	jitter := []float64{0.2, -0.3, 0.1, 0.4, -0.2, 0.3, -0.1, 0.2, -0.4, 0.1, 0.3, -0.2}

	a := base
	b := make([]float64, len(base))
	for i := range b {
		if i < shift {
			b[i] = 1 + jitter[i]
			continue
		}
		b[i] = base[i-shift] + jitter[i]
	}

	// maxLag reaches the single-pair overlaps at either end, which is exactly
	// where the spurious peaks lived.
	maxLag := len(a) - 1
	got := CrossCorrelate(a, b, time.Second, maxLag)

	assert.Equal(t, shift, got.LagBins, "b is a delayed by %d bins", shift)
	assert.Equal(t, "a_leads_b", got.Direction)
	assert.Equal(t, time.Duration(shift)*time.Second, got.LagDuration)
	assert.Less(t, got.Rho, 1.0, "a jittered copy is not perfectly correlated")

	// Swapping the arguments must mirror the answer, not change it.
	swapped := CrossCorrelate(b, a, time.Second, maxLag)
	assert.InDelta(t, got.Rho, swapped.Rho, 1e-12, "ρ is symmetric in its arguments")
	assert.Equal(t, -got.LagBins, swapped.LagBins, "the lag flips sign")
	assert.Equal(t, "b_leads_a", swapped.Direction)
}

// TestCrossCorrelateTapersWithOverlap: ρ at a lag that leaves two samples
// overlapping must not be able to outrank a lag that leaves all of them. Two
// unrelated series are the sharpest form of it — nothing here should read as a
// strong lead/lag relationship at any lag.
func TestCrossCorrelateTapersWithOverlap(t *testing.T) {
	a := []float64{1, 5, 2, 8, 3, 7}
	b := []float64{4, 4.5, 9, 1, 7, 2}

	got := CrossCorrelate(a, b, time.Second, len(a)-1)

	assert.Less(t, math.Abs(got.Rho), 0.9,
		"unrelated series must not report a near-perfect correlation at any lag")
}

// TestCrossCorrelateSkipsNaNGaps: a NaN marks "no observation at this bin"
// (a ratio gap) and must be excluded from every statistic — not silently
// zeroed. A pair where either series is NaN drops out of the covariance sum,
// and the per-series mean/variance count only that series' own valid points.
func TestCrossCorrelateSkipsNaNGaps(t *testing.T) {
	nan := math.NaN()

	t.Run("perfect correlation preserved through matching gaps", func(t *testing.T) {
		a := []float64{1, 2, nan, 4, 5}
		b := []float64{2, 4, nan, 8, 10}

		got := CrossCorrelate(a, b, time.Second, 0)

		assert.InDelta(t, 1.0, got.Rho, 1e-12,
			"the four valid pairs form a perfect 1:2 line; NaN pairs must not contribute")
	})

	t.Run("aligned NaNs match the gap-free subsequence at lag zero", func(t *testing.T) {
		gappyA := []float64{1, 2, nan, 4, 5, nan, 7, 8}
		gappyB := []float64{2, 4, nan, 7, 11, nan, 15, 17}
		cleanA := []float64{1, 2, 4, 5, 7, 8}
		cleanB := []float64{2, 4, 7, 11, 15, 17}

		gap := CrossCorrelate(gappyA, gappyB, time.Second, 0)
		clean := CrossCorrelate(cleanA, cleanB, time.Second, 0)

		assert.InDelta(t, clean.Rho, gap.Rho, 1e-12,
			"skipping matched gaps must reproduce the gap-free oracle")
	})

	t.Run("gap is skipped not zeroed", func(t *testing.T) {
		// If NaN were coerced to 0, the mean/variance centered on the gap would
		// drag rho far from the value the valid pairs alone imply. Under skip
		// semantics the gap has no effect on either statistic.
		gappy := []float64{10, 20, nan, 40, 50}
		clean := []float64{10, 20, 30, 40, 50}
		asZero := []float64{10, 20, 0, 40, 50}

		gotSkip := CrossCorrelate(gappy, clean, time.Second, 0)
		gotZero := CrossCorrelate(asZero, clean, time.Second, 0)

		assert.InDelta(t, 1.0, gotSkip.Rho, 1e-12,
			"the four valid pairs match the clean series exactly")
		assert.Less(t, gotZero.Rho, gotSkip.Rho-0.01,
			"zero-substitution must produce a strictly worse rho than skipping")
	})

	t.Run("lag search skips lags with no valid overlap", func(t *testing.T) {
		// Every non-zero lag pairs a NaN against a real value, so only lag 0
		// has any valid pairs. The peak must land there rather than on a lag
		// that scored zero because it had nothing to score.
		a := []float64{1, nan, 3, nan, 5}
		b := []float64{2, nan, 6, nan, 10}

		got := CrossCorrelate(a, b, time.Second, 2)

		assert.Equal(t, 0, got.LagBins, "lag 0 is the only lag with valid pairs")
		assert.InDelta(t, 1.0, got.Rho, 1e-12)
	})
}

func TestCrossCorrelateNaNDegenerate(t *testing.T) {
	nan := math.NaN()
	tests := []struct {
		name string
		a, b []float64
	}{
		{"all NaN both", []float64{nan, nan, nan, nan}, []float64{nan, nan, nan, nan}},
		{"all NaN one side", []float64{nan, nan, nan, nan}, []float64{1, 2, 3, 4}},
		{"single valid point", []float64{1, nan, nan, nan}, []float64{1, 2, 3, 4}},
		{"valid points are constant", []float64{5, nan, 5, nan, 5}, []float64{1, 2, 3, 4, 5}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, CorrelationResult{Direction: "none"},
				CrossCorrelate(tc.a, tc.b, time.Second, 2))
		})
	}
}

func TestKLDivergence(t *testing.T) {
	tests := []struct {
		name string
		p, q []float64
		want float64
	}{
		{"self is zero", []float64{0.2, 0.3, 0.5}, []float64{0.2, 0.3, 0.5}, 0},
		{"length mismatch", []float64{0.5, 0.5}, []float64{0.3, 0.3, 0.4}, 0},
		// p[0]=0 skips the term, so only the p[2]/q[2] mismatch contributes.
		{"zero in p skips its term", []float64{0, 0.5, 0.5}, []float64{0.1, 0.5, 0.4}, 0.5 * math.Log(1.25)},
		{"zero in q under positive p", []float64{0.3, 0.3, 0.4}, []float64{0.3, 0, 0.7}, math.Inf(1)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := KLDivergence(tc.p, tc.q)
			if math.IsInf(tc.want, 1) {
				assert.True(t, math.IsInf(got, 1))
				return
			}
			assert.InDelta(t, tc.want, got, 1e-12)
		})
	}
}

// TestKLDivergenceGrowsWithDrift: the closed form is already asserted above, so
// what is left to check is the property callers rely on — a Q further from P
// scores higher.
func TestKLDivergenceGrowsWithDrift(t *testing.T) {
	p := []float64{0.25, 0.25, 0.25, 0.25}
	near := KLDivergence(p, []float64{0.24, 0.25, 0.25, 0.26})
	far := KLDivergence(p, []float64{0.1, 0.2, 0.3, 0.4})

	assert.Greater(t, near, 0.0, "any drift is a positive divergence")
	assert.Greater(t, far, near)
}

func TestMutualInformation(t *testing.T) {
	tests := []struct {
		name  string
		joint [][]float64
		want  float64
	}{
		{"empty", [][]float64{}, 0},
		{"non-rectangular", [][]float64{{0.1, 0.2}, {0.3}}, 0},
		{"independent", [][]float64{{0.25, 0.25}, {0.25, 0.25}}, 0},
		// A diagonal joint is a deterministic pairing: knowing A fixes Y, so the
		// information shared is the full entropy of a fair coin, log 2.
		{"deterministic pairing", [][]float64{{0.5, 0}, {0, 0.5}}, math.Log(2)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.InDelta(t, tc.want, MutualInformation(tc.joint), 1e-12)
		})
	}
}

func TestMutualInformationIsSymmetric(t *testing.T) {
	joint := [][]float64{{0.1, 0.2}, {0.3, 0.4}}
	transposed := [][]float64{{0.1, 0.3}, {0.2, 0.4}}

	require.NotZero(t, MutualInformation(joint), "a dependent joint carries information to compare")
	assert.InDelta(t, MutualInformation(joint), MutualInformation(transposed), 1e-12)
}
