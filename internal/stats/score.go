// Package stats is the math tools share: correlation, divergence, breakdown and the
// signal score.
package stats

import "math"

// Signal scoring weights sum to 1 with novelty heaviest, so a
// never-before-seen pattern dominates and the score stays in [0,1].
const (
	noveltyWeight      = 0.5
	rateOfChangeWeight = 0.3
	rarityWeight       = 0.2
)

// Evidence is everything the signal formula needs about one pattern, with no
// statement about where it came from — the caller gathers it and Score below turns
// it into a number, so the formula has one implementation however it was measured.
type Evidence struct {
	// IsNew may be set only by a source that can see the data's history (see summary.SeenIndex.Observe).
	IsNew bool
	// Rate and PrevRate are lines per second now and in the preceding period, and
	// FirstObservation marks PrevRate absent rather than zero.
	Rate, PrevRate   float64
	FirstObservation bool
	// Lines and TotalLines are this pattern's and the corpus volume, measured over the same span.
	Lines, TotalLines int64
}

// Score is pure so cold- and warm-cache queriers rank patterns identically.
func Score(ev Evidence) float64 {
	novelty := 0.0
	if ev.IsNew {
		novelty = 1.0
	}

	// An absent previous rate read as zero would make every new pattern a rise from
	// nothing, pegging them all near 1.0 with nothing to separate them.
	rateOfChange := 0.0
	if !ev.FirstObservation {
		rateOfChange = math.Abs(ev.Rate-ev.PrevRate) / math.Max(math.Max(ev.Rate, ev.PrevRate), 1)
	}

	// Both sides of rarity count whole observed volume, since one segment against the all-time
	// total drove the term toward 1 for every pattern as the corpus grew.
	rarity := 0.0
	if ev.TotalLines > 0 {
		rarity = 1 - float64(ev.Lines)/float64(ev.TotalLines)
	}

	score := noveltyWeight*novelty + rateOfChangeWeight*rateOfChange + rarityWeight*rarity
	return math.Max(0, math.Min(1, score))
}
