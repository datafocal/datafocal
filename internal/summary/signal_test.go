package summary

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignalExistedBeforeIsOneSided(t *testing.T) {
	idx := NewSeenIndex()
	idx.Observe(Observation{PatternID: "t", Bin: 5})

	assert.False(t, idx.ExistedBefore("t", 5), "the observed bin is not before itself")
	assert.True(t, idx.ExistedBefore("t", 6), "a segment folded in an earlier bin is a fact")
	assert.False(t, idx.ExistedBefore("never-folded", 6),
		"an absent entry is not evidence, so the caller must look elsewhere")
}

// Observing a pattern again widens its bin span without moving its first bin
// forward, so a later segment cannot turn a rule-out back into a decline.
func TestSignalObserveWidensBinSpan(t *testing.T) {
	idx := NewSeenIndex()
	idx.Observe(Observation{PatternID: "t", Bin: 10, Text: "took <*> ms"})
	idx.Observe(Observation{PatternID: "t", Bin: 4})
	idx.Observe(Observation{PatternID: "t", Bin: 7})

	got, ok := idx.entry("t")
	require.True(t, ok)
	assert.Equal(t, SignalEntry{FirstBin: 4, LastBin: 10, Text: "took <*> ms"}, got)
}

// Evict bounds the map to the retention window. It narrows what the index can
// rule out and never what it claims: an evicted pattern reads as "cannot say",
// which sends the caller to the manifest that still holds the history.
func TestSignalEvict(t *testing.T) {
	idx := NewSeenIndex()
	idx.Observe(Observation{PatternID: "old", Bin: 0, Text: "old text"})
	idx.Observe(Observation{PatternID: "kept", Bin: 1000, Text: "kept text"})

	text, ok := idx.PatternText("old")
	require.True(t, ok)
	assert.Equal(t, "old text", text)
	idx.Evict(1000)
	_, ok = idx.PatternText("old")
	assert.False(t, ok)
	text, ok = idx.PatternText("kept")
	assert.True(t, ok)
	assert.Equal(t, "kept text", text)

	_, ok = idx.entry("old")
	assert.False(t, ok, "a pattern last seen before the horizon is gone")
	_, ok = idx.entry("kept")
	assert.True(t, ok, "a pattern last seen at the horizon survives")
	assert.False(t, idx.ExistedBefore("old", 2000),
		"an evicted pattern is no longer ruled out, so the caller re-checks the manifest")
}

func (idx *SeenIndex) entry(patternID string) (SignalEntry, bool) {
	e, ok := idx.entries[patternID]
	if !ok {
		return SignalEntry{}, false
	}
	return *e, true
}
