package stats

import (
	"cmp"
	"maps"
	"math"
	"slices"
)

// Share is one row of a breakdown: how much of the total a single group
// accounts for. The group is a Key rather than a Service because the dimension
// is the caller's choice — a service, a pattern, a pod, a region.
type Share struct {
	Key   string  `json:"key"`
	Lines int64   `json:"lines"`
	Pct   float64 `json:"pct"`
}

// Breakdown converts a per-group line map into shares of the total, largest
// first and ties broken by key ascending so the same map always renders in the
// same order. Empty input returns an empty (non-nil) slice.
func Breakdown(byGroup map[string]int64) []Share {
	if len(byGroup) == 0 {
		return []Share{}
	}

	total := int64(0)
	for _, lines := range byGroup {
		total += lines
	}

	shares := make([]Share, 0, len(byGroup))
	for group, lines := range byGroup {
		pct := 0.0
		if total > 0 {
			pct = float64(lines) * 100.0 / float64(total)
		}
		shares = append(shares, Share{Key: group, Lines: lines, Pct: pct})
	}

	slices.SortFunc(shares, func(a, b Share) int {
		if a.Pct != b.Pct {
			return cmp.Compare(b.Pct, a.Pct)
		}
		return cmp.Compare(a.Key, b.Key)
	})

	// The per-share divisions each round independently, so the column can miss
	// 100 by a few ULP. Folding the gap into the largest share keeps the printed
	// column summing to 100 and puts the correction where it is proportionally
	// smallest. With no lines at all there is no total to be a share of — every
	// pct is already 0 and forcing the column to sum to 100 would report the
	// first group as the whole of an activity that never happened.
	if total > 0 {
		sum := 0.0
		for _, s := range shares {
			sum += s.Pct
		}
		shares[0].Pct += 100.0 - sum
	}

	return shares
}

// Divergence is the KL divergence of the slice's mix from the baseline's, with every key
// credited one line on both sides so a key the baseline lacks scores large rather than +Inf.
func Divergence(slice, baseline map[string]int64) float64 {
	keys := make(map[string]struct{}, len(slice)+len(baseline))
	for k := range slice {
		keys[k] = struct{}{}
	}
	for k := range baseline {
		keys[k] = struct{}{}
	}
	sorted := slices.Sorted(maps.Keys(keys))
	s, b := make([]int64, len(sorted)), make([]int64, len(sorted))
	for i, k := range sorted {
		s[i], b[i] = slice[k], baseline[k]
	}
	return AlignedDivergence(s, b)
}

// AlignedDivergence is Divergence of two mixes whose i-th counts are one key's, the keys in
// sorted order, for a caller scoring many splits of one set of keys.
func AlignedDivergence(slice, baseline []int64) float64 {
	var sliceTotal, baselineTotal int64
	for i := range slice {
		sliceTotal, baselineTotal = sliceTotal+slice[i], baselineTotal+baseline[i]
	}
	sliceDenom := float64(sliceTotal + int64(len(slice)))
	baselineDenom := float64(baselineTotal + int64(len(slice)))
	// KLDivergence's sum, its terms in its order, each positive for the line both sides are credited.
	kl := 0.0
	for i := range slice {
		p, q := float64(slice[i]+1)/sliceDenom, float64(baseline[i]+1)/baselineDenom
		kl += p * math.Log(p/q)
	}
	return kl
}
