package stats

import (
	"math"
	"time"
)

// CorrelationResult holds the output of a cross-correlation computation.
type CorrelationResult struct {
	Rho         float64       // Signed peak correlation in [-1,1]
	LagBins     int           // Offset (in bins) where peak occurs; positive means b lags a
	LagDuration time.Duration // LagBins * BinWidth
	Direction   string        // One of directionALeadsB, directionBLeadsA, directionNone
}

// CorrelationResult.Direction values.
const (
	directionALeadsB = "a_leads_b"
	directionBLeadsA = "b_leads_a"
	directionNone    = "none"
)

// CrossCorrelate computes the Pearson correlation coefficient ρ_XY(τ) for
// τ in [-maxLag, +maxLag], returning the lag with the highest |ρ|.
// Degenerate inputs (len mismatch, len<2, zero variance) return a zero result
// with Direction="none".
//
// Formula: ρ_XY(τ) = Σ_overlap (x_t - x̄)(y_{t+τ} - ȳ) / √(Σ_all (x_t - x̄)² · Σ_all (y_t - ȳ)²)
//
// The denominator sums over the whole series, not the overlap a given lag leaves.
// Normalizing per-overlap makes lags incomparable: at τ = ±(n-1) the overlap is
// one sample pair and at ±(n-2) two, and one or two points are always perfectly
// collinear, so the extreme lags scored |ρ| = 1 by construction and won every peak
// search that reached them. A constant denominator tapers ρ toward zero as the
// overlap shrinks, which is what makes the peak mean something.
//
// A math.NaN() value marks "no observation at this bin" (a ratio gap): it is
// excluded from the per-series mean and variance, and any lag-pair where either
// side is NaN drops out of the covariance sum. A lag whose overlap contains no
// valid pairs contributes nothing to the peak search.
func CrossCorrelate(a, b []float64, binWidth time.Duration, maxLag int) CorrelationResult {
	if len(a) != len(b) || len(a) < 2 {
		return CorrelationResult{Direction: directionNone}
	}

	meanA, validA := mean(a)
	meanB, validB := mean(b)
	if validA < 2 || validB < 2 {
		return CorrelationResult{Direction: directionNone}
	}
	sumSqA := sumSquares(a, meanA)
	sumSqB := sumSquares(b, meanB)
	if sumSqA == 0 || sumSqB == 0 {
		return CorrelationResult{Direction: directionNone}
	}
	denom := math.Sqrt(sumSqA * sumSqB)

	bestRho := 0.0
	bestLag := 0
	for lag := -maxLag; lag <= maxLag; lag++ {
		cov, pairs := crossCovariance(a, b, meanA, meanB, lag)
		if pairs == 0 {
			continue
		}
		rho := cov / denom
		// Break ties on smaller |lag| so lag-zero perfect correlation wins.
		if math.Abs(rho) > math.Abs(bestRho) ||
			(math.Abs(rho) == math.Abs(bestRho) && max(lag, -lag) < max(bestLag, -bestLag)) {
			bestRho = rho
			bestLag = lag
		}
	}

	direction := directionNone
	switch {
	case bestLag > 0:
		direction = directionALeadsB
	case bestLag < 0:
		direction = directionBLeadsA
	}

	return CorrelationResult{
		Rho:         bestRho,
		LagBins:     bestLag,
		LagDuration: time.Duration(int64(bestLag)) * binWidth,
		Direction:   direction,
	}
}

// mean returns the arithmetic mean of the non-NaN values in v along with the
// count of valid points. A NaN is a "no observation" marker (a ratio gap) and
// is excluded from both the sum and the divisor.
func mean(v []float64) (float64, int) {
	sum := 0.0
	valid := 0
	for _, x := range v {
		if math.IsNaN(x) {
			continue
		}
		sum += x
		valid++
	}
	if valid == 0 {
		return 0, 0
	}
	return sum / float64(valid), valid
}

// sumSquares is Σ(x - m)² over the non-NaN entries of v, the unnormalized
// spread CrossCorrelate divides by.
func sumSquares(v []float64, m float64) float64 {
	sum := 0.0
	for _, x := range v {
		if math.IsNaN(x) {
			continue
		}
		diff := x - m
		sum += diff * diff
	}
	return sum
}

// crossCovariance is Σ(a_t - ā)(b_{t+τ} - b̄) over the samples the lag leaves
// overlapping, with no normalization — CrossCorrelate divides every lag by the
// same constant. Any pair where a[t] or b[t+τ] is NaN is skipped, and the valid-pair
// count returned alongside the sum distinguishes a covariance of exactly zero from a lag
// whose overlap held no observations.
//
// lag > 0 means b is shifted forward (b lags a); lag < 0 means a is shifted
// forward (a lags b).
func crossCovariance(a, b []float64, meanA, meanB float64, lag int) (float64, int) {
	var startA, startB, count int
	if lag >= 0 {
		startA, startB, count = 0, lag, len(a)-lag
	} else {
		startA, startB, count = -lag, 0, len(a)+lag
	}
	if count <= 0 {
		return 0, 0
	}

	sum := 0.0
	pairs := 0
	for i := range count {
		x, y := a[startA+i], b[startB+i]
		if math.IsNaN(x) || math.IsNaN(y) {
			continue
		}
		sum += (x - meanA) * (y - meanB)
		pairs++
	}
	return sum, pairs
}

// KLDivergence computes the discrete Kullback-Leibler divergence
// D_KL(P || Q) = Σ p_i log(p_i / q_i), summing only over indices where p_i > 0 and q_i > 0.
// A zero in q with positive p returns +Inf. Length mismatch returns 0.
func KLDivergence(p, q []float64) float64 {
	if len(p) != len(q) {
		return 0
	}

	kl := 0.0
	for i := range p {
		if p[i] > 0 {
			if q[i] <= 0 {
				return math.Inf(1)
			}
			kl += p[i] * math.Log(p[i]/q[i])
		}
	}
	return kl
}

// MutualInformation computes I(A;Y) = Σ_a Σ_y p(a,y) log(p(a,y)/(p(a)p(y)))
// over a normalized joint distribution matrix. Returns 0 for empty or non-rectangular input.
func MutualInformation(joint [][]float64) float64 {
	if len(joint) == 0 {
		return 0
	}

	width := len(joint[0])
	for _, row := range joint {
		if len(row) != width {
			return 0
		}
	}
	height := len(joint)

	marginalA := make([]float64, height)
	marginalY := make([]float64, width)
	for i := range height {
		for j := range width {
			marginalA[i] += joint[i][j]
			marginalY[j] += joint[i][j]
		}
	}

	mi := 0.0
	for i := range height {
		for j := range width {
			if joint[i][j] > 0 && marginalA[i] > 0 && marginalY[j] > 0 {
				mi += joint[i][j] * math.Log(joint[i][j]/(marginalA[i]*marginalY[j]))
			}
		}
	}
	return mi
}
