package series

import (
	"regexp"
	"strconv"
	"time"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/selector"
)

// Bins is a window [From, To) divided into equal-width bins of size Width.
type Bins struct {
	Width time.Duration
	From  time.Time
	To    time.Time
}

// MaxBins caps how many bins one answer may carry. It bounds the response a
// caller has to read as much as the allocation behind it.
const MaxBins = 10000

// ParseBins divides from-to by '<n>s', '<n>m', '<n>h' or '<n>d' of at least 15s, 'auto', or
// 'all' or ” (one bin), refusing any other spec and a width that makes more than MaxBins.
func ParseBins(spec string, from, to time.Time) (Bins, error) {
	var width time.Duration

	switch spec {
	case "", "all":
		// A single bin covering the whole window.
		width = to.Sub(from)
	case "auto":
		width = autoBinWidth(to.Sub(from))
	default:
		if m := binSpec.FindStringSubmatch(spec); m != nil {
			n, _ := strconv.Atoi(m[1])
			width = time.Duration(n) * binUnits[m[2]]
		}
		if width < minBinWidth {
			return Bins{}, query.Invalidf("unsupported bin spec %q: give a width of at least 15s such as 10m, or auto or all", spec)
		}
	}

	bins := Bins{
		Width: width,
		From:  from,
		To:    to,
	}
	// Truncating instead would answer a narrower window than the one asked for,
	// under the caller's own From and To — a correlation over the first 41 hours
	// of a month reported as a correlation over the month.
	if n := bins.NumBins(); n > MaxBins {
		return Bins{}, query.Invalidf(
			"bins %q over this window is %d bins, above the %d limit — widen the bins or narrow the window",
			spec, n, MaxBins)
	}
	return bins, nil
}

// binSpec is a bin width as a count of a unit, its units binUnits.
var (
	binSpec  = regexp.MustCompile(`^(\d{1,6})([smhd])$`)
	binUnits = map[string]time.Duration{"s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour}
)

// minBinWidth is the finest bin a spec may name.
const minBinWidth = 15 * time.Second

// autoBinWidth derives a sensible bin width based on the time span, widened as
// far as it takes to keep the grid inside MaxBins.
// Returns: span < 1h → 15s, < 6h → 1m, < 24h → 5m, < 7d → 1h, else → 24h — and a
// day still leaves a multi-year window over the limit, so the ladder is a floor
// on the width rather than the last word on it.
func autoBinWidth(span time.Duration) time.Duration {
	width := binWidthLadder(span)
	if fit := (span + MaxBins - 1) / MaxBins; fit > width {
		return fit
	}
	return width
}

func binWidthLadder(span time.Duration) time.Duration {
	switch {
	case span < time.Hour:
		return 15 * time.Second
	case span < 6*time.Hour:
		return time.Minute
	case span < 24*time.Hour:
		return 5 * time.Minute
	case span < 7*24*time.Hour:
		return time.Hour
	default:
		return 24 * time.Hour
	}
}

// BinStart returns the start time (unix milliseconds) of the bin containing ts.
// Bins are aligned to b.From and have width b.Width.
// Timestamps before b.From clamp to the first bin; timestamps at or after b.To clamp to the last bin.
func (b Bins) BinStart(ts time.Time) int64 {
	if ts.Before(b.From) {
		return b.From.UnixMilli()
	}
	if !ts.Before(b.To) {
		last := max(b.NumBins()-1, 0)
		return b.From.Add(time.Duration(last) * b.Width).UnixMilli()
	}

	// Normal case: ts is in [From, To).
	// Bin index = floor((ts - From) / Width)
	offset := ts.Sub(b.From)
	binIndex := offset / b.Width
	return b.From.Add(binIndex * b.Width).UnixMilli()
}

// NumBins is ceil((To - From) / Width): how many bins the spec divides its
// window into, and so how many points an answer on it carries. Zero for an empty
// or inverted window.
func (b Bins) NumBins() int64 {
	span := b.To.Sub(b.From)
	if span <= 0 || b.Width <= 0 {
		return 0
	}
	n := int64(span / b.Width)
	if span%b.Width != 0 {
		n++
	}
	return n
}

// Boundaries returns the start time (unix milliseconds) of every bin in [From, To), in ascending order.
//
// There is no cap here: the width that decides the count is validated where the
// spec is built (ParseBins, gridFor), so truncating a spec that reached this far
// would drop bins the caller was told they would get.
func (b Bins) Boundaries() []int64 {
	boundaries := make([]int64, b.NumBins())
	for i := range boundaries {
		boundaries[i] = b.From.Add(time.Duration(i) * b.Width).UnixMilli()
	}
	return boundaries
}

// Window is the time window the bins divide.
func (b Bins) Window() selector.Window {
	return selector.Window{From: b.From, To: b.To}
}

// Snap floors From and ceils To to whole bin widths, the grid the summary's points sit
// on (Store.CanServe), so an unsnapped window does not miss the summary for no reason
// in the data.
func (b Bins) Snap() Bins {
	w := b.Width.Milliseconds()
	if w <= 0 {
		return b
	}
	from, to := b.From.UnixMilli(), b.To.UnixMilli()
	from -= from % w
	if rem := to % w; rem != 0 {
		to += w - rem
	}
	b.From = time.UnixMilli(from).UTC()
	b.To = time.UnixMilli(to).UTC()
	return b
}

// GridFor resolves a series' bins from an explicit spec or else width, the summary's
// own, snapped so the summary can serve them.
func GridFor(spec string, width time.Duration, w selector.Window) (Bins, error) {
	if spec != "" {
		bins, err := ParseBins(spec, w.From, w.To)
		if err != nil {
			return Bins{}, err
		}
		return bins.Snap(), nil
	}
	bins := Bins{Width: width, From: w.From, To: w.To}.Snap()
	if bins.NumBins() <= MaxBins {
		return bins, nil
	}
	// A caller naming no width asked for a coarser grid rather than an error when the
	// window is too long for the summary's, which no other width can serve anyway, so this
	// one is left unsnapped.
	return Bins{Width: autoBinWidth(w.To.Sub(w.From)), From: w.From, To: w.To}, nil
}
