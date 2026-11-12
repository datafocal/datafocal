package segment

import (
	"fmt"
	"time"
)

type TimeRange struct {
	Min time.Time `json:"min"`
	Max time.Time `json:"max"`
}

func (r *TimeRange) Add(ts time.Time) {
	if r.Min.IsZero() || ts.Before(r.Min) {
		r.Min = ts
	}
	if r.Max.IsZero() || ts.After(r.Max) {
		r.Max = ts
	}
}

// maxBins bounds the bins one time range may span, since one corrupt producer timestamp
// can claim centuries.
const maxBins = 8192

// Bin is one bin a time range overlaps: its start in unix ms, and how much of the range
// falls inside it.
type Bin struct {
	Start   int64
	Overlap time.Duration
}

// Bins returns every width-wide bin r overlaps by more than zero, oldest first, where
// a range inside one bin reports one zero overlap meaning all of it.
func (r TimeRange) Bins(width time.Duration) ([]Bin, error) {
	step := width.Milliseconds()
	first := FloorBin(r.Min.UnixMilli(), step)
	last := FloorBin(r.Max.UnixMilli(), step)
	if first >= last {
		return []Bin{{Start: first}}, nil
	}
	if n := (last-first)/step + 1; n > maxBins {
		return nil, fmt.Errorf("time range spans %d bins of %s, over the %d cap: check its timestamps", n, width, maxBins)
	}

	bins := make([]Bin, 0, (last-first)/step+1)
	for b := first; b <= last; b += step {
		binStart := time.UnixMilli(b)
		if w := overlap(r.Min, r.Max, binStart, binStart.Add(width)); w > 0 {
			bins = append(bins, Bin{Start: b, Overlap: w})
		}
	}
	return bins, nil
}

// FloorBin rounds ms down to a multiple of width, correcting Go's toward-zero division
// for pre-epoch timestamps.
func FloorBin(ms, width int64) int64 {
	q := ms / width
	if ms < 0 && q*width != ms {
		q--
	}
	return q * width
}

// overlap returns the length of the intersection of [aMin, aMax] and [bMin, bMax), or
// zero when they do not meet.
func overlap(aMin, aMax, bMin, bMax time.Time) time.Duration {
	start := aMin
	if bMin.After(start) {
		start = bMin
	}
	end := aMax
	if bMax.Before(end) {
		end = bMax
	}
	if !end.After(start) {
		return 0
	}
	return end.Sub(start)
}
