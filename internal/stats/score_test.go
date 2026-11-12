package stats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScoreNewPatternHighScore(t *testing.T) {
	got := Score(Evidence{
		IsNew: true, Rate: 50.0, FirstObservation: true,
		Lines: 10, TotalLines: 1000,
	})

	// novelty 1 + rate-of-change 0 + rarity (1 - 10/1000).
	assert.InDelta(t, 0.5+0.2*0.99, got, 1e-9)
	assert.Less(t, got, 1.0, "a first observation must leave headroom above it")
}

func TestScoreStablePatternLowScore(t *testing.T) {
	got := Score(Evidence{Rate: 10.0, PrevRate: 10.0, Lines: 900, TotalLines: 1000})

	assert.Less(t, got, 0.2)
}

// A rate spike on an established pattern raises its signal above its
// pre-spike score, with nothing else about it changed.
func TestScoreRateSpikeIncreasesScore(t *testing.T) {
	routine := Evidence{Rate: 10.0, PrevRate: 10.0, Lines: 100, TotalLines: 1000}
	spiked := routine
	spiked.Rate = 100.0

	assert.Greater(t, Score(spiked), Score(routine))
}

// A new pattern outranks a routine one — the ordering list_patterns sort=signal
// exists to produce.
func TestScoreNewOutranksRoutine(t *testing.T) {
	fresh := Score(Evidence{IsNew: true, Rate: 20.0, FirstObservation: true, Lines: 50, TotalLines: 1000})
	busy := Score(Evidence{Rate: 5.0, PrevRate: 5.0, Lines: 900, TotalLines: 1000})

	assert.Greater(t, fresh, busy)
}

// FirstObservation drops the rate-of-change term rather than reading the absent
// previous rate as zero, which would peg every new pattern near 1.
func TestScoreFirstObservationSuppressesRateOfChange(t *testing.T) {
	ev := Evidence{IsNew: true, Rate: 100.0, FirstObservation: true, Lines: 1000, TotalLines: 1000}

	assert.InDelta(t, 0.5, Score(ev), 1e-9)

	ev.FirstObservation = false
	assert.InDelta(t, 0.5+0.3, Score(ev), 1e-9, "with a baseline, the same rate is a rise from zero")
}

func TestScoreRarity(t *testing.T) {
	chatty := Score(Evidence{Lines: 300, TotalLines: 303})
	quiet := Score(Evidence{Lines: 1, TotalLines: 303})

	assert.Less(t, chatty, 0.05)
	assert.Greater(t, quiet, chatty)
	assert.Zero(t, Score(Evidence{Lines: 5}), "no corpus volume means no rarity to report")
}
