package summary

import "maps"

// Observation is one segment's contribution to the index: a pattern it carried
// and the newest series bin it touched, which is both what novelty is measured
// against and what retention ages the entry out by.
type Observation struct {
	PatternID string
	Bin       int64
	Text      string
}

// SignalEntry holds pattern text and its bin span: FirstBin rules novelty out,
// LastBin drives retention.
type SignalEntry struct {
	FirstBin int64
	LastBin  int64
	Text     string
}

// SeenIndex holds the first- and last-seen bins of each pattern this process has
// added, which can rule novelty out but never in — see ExistedBefore.
type SeenIndex struct {
	entries map[string]*SignalEntry
}

// NewSeenIndex returns an empty seen index.
func NewSeenIndex() *SeenIndex {
	return &SeenIndex{entries: make(map[string]*SignalEntry)}
}

// Observe records text and the bin span, never "first seen", because novelty read from an in-memory
// index would track process uptime and two queriers would disagree.
func (idx *SeenIndex) Observe(o Observation) {
	e, seen := idx.entries[o.PatternID]
	if !seen {
		e = &SignalEntry{FirstBin: o.Bin, Text: o.Text}
		idx.entries[o.PatternID] = e
	}
	e.FirstBin = min(e.FirstBin, o.Bin)
	e.LastBin = max(e.LastBin, o.Bin)
}

// ExistedBefore reports whether the index holds positive evidence that the
// pattern was already being emitted before bin. True is trustworthy: the index
// folded a segment carrying this pattern in an earlier bin, and a sealed segment
// is a fact about the data. False means only that the index cannot say, so a
// caller must consult the manifest before claiming the pattern is new.
//
// This one-sided reading mirrors the membership filter's contract in the other
// direction (segment/filter.go: "false may be trusted"), and it is what keeps
// novelty free in the common case.
func (idx *SeenIndex) ExistedBefore(patternID string, bin int64) bool {
	e, ok := idx.entries[patternID]
	return ok && e.FirstBin < bin
}

// PatternText returns the text of a pattern still held in the index.
func (idx *SeenIndex) PatternText(patternID string) (string, bool) {
	e, ok := idx.entries[patternID]
	if !ok {
		return "", false
	}
	return e.Text, true
}

// Evict drops patterns last seen before the retention start, which only narrows
// what ExistedBefore can rule out since the manifest still holds the history.
func (idx *SeenIndex) Evict(retentionStart int64) {
	maps.DeleteFunc(idx.entries, func(_ string, e *SignalEntry) bool {
		return e.LastBin < retentionStart
	})
}
