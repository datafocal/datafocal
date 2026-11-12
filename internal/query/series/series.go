// Package series computes one measure's value per key per bin over a selector, from
// whichever source can answer it: the summary, the segments' footer sketches, or their
// rows. The caller names the measure and the bins; choosing the source is this
// package's job, so every tool that needs a series gets the same answer from it.
package series

import (
	"context"
	"fmt"
	"slices"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// Values is a measure's value per key per bin start in unix milliseconds.
type Values map[string]map[int64]float64

// Labels is a sequence measure's string value per key per bin, empty where it has none.
type Labels map[string]map[int64]string

// Request is one measure over one selector on one set of bins.
type Request struct {
	// Version is the catalog version read.
	Version catalog.Version
	// Summary lets the summary answer when it holds Version, as the caller's snapshot
	// token allows.
	Summary bool
	// Selector prunes the segments and filters their rows; Window filters the rows by time.
	Selector selector.Resolved
	Window   selector.Window
	Measure  Measure
	// Numerator selects the rows a ratio's numerator counts within Selector.
	Numerator []selector.Predicate
	// GroupBy keys the values; empty keys them by pattern_id, and Total all under one key.
	GroupBy string
	Bins    Bins
	// Patterns, when set, reads only the segments listing one of them or none at all.
	Patterns map[string]struct{}
	Bound    Bound
	// Measures, when set, are read instead of Measure from one scan of the rows.
	Measures []FilteredMeasure
	// OrdinalOf, when set, bins each row by its 1-based position among the rows of its
	// value of this column in time order, counting only the rows OrdinalWhere matches, a
	// position p read as the time p ms after the epoch so Bins divide positions.
	OrdinalOf    string
	OrdinalWhere []selector.Predicate
	// Segments, when set, are read in place of the segments the catalog prunes to.
	Segments []segment.Descriptor
}

// FilteredMeasure is a measure over only the rows Where matches as well as the selector,
// and, when DistinctBy names a column, only the first of them read with each of its values.
// GroupBy, when set, keys its values in place of the request's.
type FilteredMeasure struct {
	Measure
	Where      []selector.Predicate
	DistinctBy string
	GroupBy    string
}

// Result is a measure's values and what answering it read.
type Result struct {
	Values Values
	Labels Labels
	Cost   query.Cost
	// Coverage is the fraction of segments in scope that answered, below 1 only when a
	// footer carries no sketch for a sketch measure.
	Coverage float64
	// Approx is set when a sketch produced the values.
	Approx bool
	// Multiplied is set when a row was counted under each of its group_by list's elements.
	Multiplied bool
	// Measures holds each of Request.Measures' Values and Labels, in order.
	Measures []Result
}

// Total is the GroupBy that keys every row "", a column no row holds: one series of all the
// rows a measure reads.
const Total = "\x00"

// Reader computes measures.
type Reader interface {
	// Read answers req from the summary when it can, then from footer sketches, then
	// from rows, and refuses a row scan past req.Bound.
	Read(ctx context.Context, req Request) (Result, error)
}

type reader struct {
	planner plan.Planner
	scanner scan.Scanner
}

// New returns a Reader planning its reads through planner and reading rows through scanner.
func New(planner plan.Planner, scanner scan.Scanner) Reader {
	return reader{planner: planner, scanner: scanner}
}

func (r reader) Read(ctx context.Context, req Request) (Result, error) {
	if err := req.validate(); err != nil {
		return Result{}, err
	}
	if req.Measure.Kind == KindRatio {
		return r.ratio(ctx, req)
	}
	if len(req.Measures) > 0 {
		return r.eachFromRows(ctx, req)
	}
	if res, ok := r.fromSummary(req); ok {
		return res, nil
	}
	descs, err := r.segments(ctx, req)
	if err != nil {
		return Result{}, err
	}
	if kind := req.Measure.Kind; (kind == KindQuantile || kind == KindDistinct) && req.OrdinalOf == "" && req.Measure.Length == "" &&
		!sketchCannotAnswer(descs, req.Selector.Where, req.GroupBy) && !r.planner.Derived().Has(req.Measure.Field) {
		return r.fromFooters(ctx, descs, req)
	}
	return r.fromRows(ctx, descs, req)
}

// validate refuses a request no tool should build: these are the caller's bugs, not the
// user's, so they are plain errors rather than invalid_request.
func (req Request) validate() error {
	switch {
	case req.Bins.Width <= 0:
		return fmt.Errorf("series: bin width %v is not positive", req.Bins.Width)
	case (req.Measure.Kind == KindRatio) != (len(req.Numerator) > 0):
		return fmt.Errorf("series: a numerator goes with a ratio and only a ratio")
	}
	return nil
}

// segments is the catalog stage, narrowed to the requested patterns.
func (r reader) segments(ctx context.Context, req Request) ([]segment.Descriptor, error) {
	descs := req.Segments
	if descs == nil {
		var err error
		if descs, err = r.planner.Segments(ctx, req.Version, req.Selector); err != nil {
			return nil, fmt.Errorf("prune: %w", err)
		}
	}
	if len(req.Patterns) == 0 {
		return descs, nil
	}
	// A descriptor with no pattern list is kept, since dropping it loses rows.
	kept := make([]segment.Descriptor, 0, len(descs))
	for _, d := range descs {
		if len(d.PatternIDs) == 0 || slices.ContainsFunc(d.PatternIDs, func(id string) bool {
			_, ok := req.Patterns[id]
			return ok
		}) {
			kept = append(kept, d)
		}
	}
	return kept, nil
}

func addCost(a, b query.Cost) query.Cost {
	return query.Cost{GETs: a.GETs + b.GETs, BytesRead: a.BytesRead + b.BytesRead,
		FootersDecoded: a.FootersDecoded + b.FootersDecoded, Segments: a.Segments + b.Segments}
}
