package stats

import (
	"math"
	"slices"
)

// DefaultLambda is the EWMA smoothing factor for baselines:
// moderate enough to respond to real shifts without chasing every spike.
const DefaultLambda = 0.3

// spikeThreshold is how many standard deviations above its baseline a value must sit
// to be a spike.
const spikeThreshold = 3.0

// minSupportBins is how many bins a series needs lines in before it counts as evidence:
// fewer coincide with another series at some lag by chance.
const minSupportBins = 8

// Spike is one value standing out from the series before it.
type Spike struct {
	Index    int
	Expected float64
	Score    float64 // standard deviations above the baseline
}

// EWMA returns the exponentially-weighted moving average of values, using
// µ_0 = x_0 and µ_t = λ·x_t + (1-λ)·µ_{t-1}.
func EWMA(values []float64, lambda float64) []float64 {
	out := make([]float64, len(values))
	for i, v := range values {
		if i == 0 {
			out[i] = v
			continue
		}
		out[i] = lambda*v + (1-lambda)*out[i-1]
	}
	return out
}

// Supported reports whether at least minSupportBins of values are above zero.
func Supported(values []float64) bool {
	bins := 0
	for _, v := range values {
		if v > 0 {
			bins++
		}
	}
	return bins >= minSupportBins
}

// SupportedSplits is the splits of values both of whose sides are Supported, [first, last],
// none when first > last: a side's bins with lines only grow as it does, so they are a range.
func SupportedSplits(values []float64) (first, last int) {
	seen := 0
	first, last = len(values), 0
	for i, v := range values {
		if v > 0 {
			if seen++; seen == minSupportBins {
				first = i + 1
			}
		}
	}
	seen = 0
	for i := len(values) - 1; i >= 0; i-- {
		if values[i] > 0 {
			if seen++; seen == minSupportBins {
				last = i
				break
			}
		}
	}
	return first, last
}

// Spikes floors the deviation at the Poisson √expected, so a flat series does not
// turn a one-line rise into an infinite score. A series that is not Supported has none.
func Spikes(values []float64) []Spike {
	if !Supported(values) {
		return nil
	}
	baseline := EWMA(values, DefaultLambda)
	var spikes []Spike
	variance := 0.0
	for i, v := range values {
		if i == 0 {
			continue
		}
		expected := baseline[i-1]
		deviation := v - expected
		score := deviation / math.Max(math.Sqrt(variance), math.Sqrt(math.Max(expected, 1)))
		if score >= spikeThreshold {
			spikes = append(spikes, Spike{Index: i, Expected: expected, Score: score})
		}
		variance = (1 - DefaultLambda) * (variance + DefaultLambda*deviation*deviation)
	}
	return spikes
}

// RoutineVariation is the coefficient of variation under which a series is routine.
const RoutineVariation = 0.5

// Routine reports whether values are a near-constant beat, as health checks and heartbeats
// are: above zero in every bin, with a standard deviation under RoutineVariation of the mean.
func Routine(values []float64) bool {
	if len(values) == 0 || slices.Contains(values, 0) {
		return false
	}
	m, n := mean(values)
	return math.Sqrt(sumSquares(values, m)/float64(n)) < RoutineVariation*m
}

// Drift is how far window's median moved from baseline's in baseline's median absolute
// deviations, floored at a hundredth of its median so a steady baseline does not make any move infinite.
func Drift(baseline, window []float64) float64 {
	if len(baseline) == 0 || len(window) == 0 {
		return 0
	}
	center := median(baseline)
	deviations := make([]float64, len(baseline))
	for i, v := range baseline {
		deviations[i] = math.Abs(v - center)
	}
	spread := math.Max(median(deviations), math.Abs(center)/100)
	if spread == 0 {
		return 0
	}
	return (median(window) - center) / spread
}

func median(values []float64) float64 {
	sorted := slices.Sorted(slices.Values(values))
	n := len(sorted)
	return (sorted[(n-1)/2] + sorted[n/2]) / 2
}
