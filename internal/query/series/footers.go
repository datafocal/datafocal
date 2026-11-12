package series

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"slices"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
)

// sketchCannotAnswer reports whether the rows have to be read because a whole-segment
// sketch can be neither subset by a row-level predicate nor split along a dimension
// only the rows carry; without the second test "p95 by shard" refused while "p95 by
// shard where x=y" answered.
func sketchCannotAnswer(descs []segment.Descriptor, where []selector.Predicate, groupBy string) bool {
	if NeedsGroupScan(descs, groupBy) {
		return true
	}
	// A predicate every segment settles was applied by pruning; any other needs the rows.
	for _, p := range where {
		if len(descs) == 0 || NeedsGroupScan(descs, p.Field) {
			return true
		}
	}
	return false
}

// fromFooters answers a quantile or a distinct count from footer sketches alone, a bare
// quantile keeping the column it resolves from the first footer carrying a sketch so
// every bin measures the same one.
func (r reader) fromFooters(ctx context.Context, descs []segment.Descriptor, req Request) (Result, error) {
	if req.Measure.Kind == KindDistinct {
		return footerSketches[*sketch.HLL]{
			columns:  func(footer segment.Footer) iter.Seq[string] { return maps.Keys(footer.DistinctCounts) },
			lookup:   func(footer segment.Footer) (*sketch.HLL, error) { return footer.DistinctCounts[req.Measure.Field], nil },
			newEmpty: sketch.NewHLL,
			reduce:   (*sketch.HLL).Estimate,
		}.read(ctx, r.planner, descs, req)
	}
	field := req.Measure.Field
	return footerSketches[*sketch.DDSketch]{
		columns: func(footer segment.Footer) iter.Seq[string] { return maps.Keys(footer.Distributions) },
		lookup: func(footer segment.Footer) (*sketch.DDSketch, error) {
			if field == "" {
				resolved, ok, err := soleDistribution(footer)
				if !ok {
					return nil, err
				}
				field = resolved
			}
			return footer.Distributions[field], nil
		},
		newEmpty: func() *sketch.DDSketch { return sketch.NewDDSketch(sketch.QuantileAccuracy) },
		reduce:   func(sk *sketch.DDSketch) float64 { return sk.Quantile(req.Measure.Quantile) },
	}.read(ctx, r.planner, descs, req)
}

// mergeable is a sketch that absorbs another of its kind and is comparable so an absent
// one, a nil pointer, can be spotted.
type mergeable[T any] interface {
	comparable
	Merge(T) error
}

// footerSketches merges each segment's footer sketch whole into every bin the segment
// overlaps, the coarse approximation the design trades for reading no rows.
type footerSketches[T mergeable[T]] struct {
	// lookup returns the segment's sketch, or the zero T when the footer carries
	// none — those segments are skipped and count against coverage.
	lookup func(footer segment.Footer) (T, error)
	// columns lists the columns a footer carries this family for, suggested when no
	// segment carried the requested one.
	columns  func(footer segment.Footer) iter.Seq[string]
	newEmpty func() T
	reduce   func(T) float64
}

func (s footerSketches[T]) read(ctx context.Context, planner plan.Planner, descs []segment.Descriptor, req Request) (Result, error) {
	label := req.Measure.Kind.label()
	p, err := planner.Plan(ctx, descs, selector.Resolved{})
	if err != nil {
		return Result{}, fmt.Errorf("aggregate: %s: %w", label, err)
	}
	m := sketchMerge[T]{merged: map[string]map[int64]T{}, available: map[string]struct{}{}}
	for d, footer := range p.Footers() {
		if err := s.merge(d, footer, req, &m); err != nil {
			return Result{}, err
		}
	}
	if m.covered == 0 && len(descs) > 0 {
		return Result{}, noCoverage(label, req.Measure.Field, m.available)
	}
	coverage := 1.0
	if len(descs) > 0 {
		coverage = float64(m.covered) / float64(len(descs))
	}
	return Result{Values: reduceBins(m.merged, s.reduce), Cost: p.Cost, Coverage: coverage, Approx: true}, nil
}

// sketchMerge is what read folds each footer into.
type sketchMerge[T any] struct {
	merged    map[string]map[int64]T
	available map[string]struct{}
	covered   int
}

// merge folds one segment's footer sketch into m, or notes the columns it does carry.
func (s footerSketches[T]) merge(d segment.Descriptor, footer segment.Footer, req Request, m *sketchMerge[T]) error {
	var absent T
	src, err := s.lookup(footer)
	if err != nil {
		return err
	}
	if src == absent {
		for col := range s.columns(footer) {
			m.available[col] = struct{}{}
		}
		return nil
	}
	m.covered++
	// sketchCannotAnswer checked every descriptor settles the group_by value, and merging
	// into a fresh sketch per (key, bin) keeps a cached footer's own sketch reusable.
	key, _ := plan.SegmentValue(d, req.GroupBy)
	for _, b := range segmentBinStarts(d, req.Bins) {
		if err := binSketch(m.merged, key, b, s.newEmpty).Merge(src); err != nil {
			return fmt.Errorf("aggregate: %s merge %s: %w", req.Measure.Kind.label(), d.ID, err)
		}
	}
	return nil
}

// noCoverage refuses a sketch measure no segment can answer, naming the request's
// column and the ones that are there, since an empty series would read as a column that
// held nothing.
func noCoverage(label, field string, available map[string]struct{}) error {
	msg := fmt.Sprintf("aggregate: no segment in the window carries a %s sketch", label)
	if field != "" {
		msg += fmt.Sprintf(" for %q", field)
	}
	return query.NewAPIError(query.ErrCodeUnknownField, msg).WithSuggestedFields(slices.Sorted(maps.Keys(available)))
}

// segmentBinStarts returns the starts of the bins overlapped by d.TimeRange.
func segmentBinStarts(d segment.Descriptor, bins Bins) []int64 {
	first := bins.BinStart(d.TimeRange.Min)
	last := bins.BinStart(d.TimeRange.Max)
	step := bins.Width.Milliseconds()
	if first == last || step <= 0 {
		return []int64{first}
	}
	out := make([]int64, 0, (last-first)/step+1)
	for b := first; b <= last; b += step {
		out = append(out, b)
	}
	return out
}

// binSketch returns the sketch for (key, bin), creating the nested map and the sketch
// on first touch.
func binSketch[S any](merged map[string]map[int64]S, key string, bin int64, newSketch func() S) S {
	byBin := merged[key]
	if byBin == nil {
		byBin = map[int64]S{}
		merged[key] = byBin
	}
	s, ok := byBin[bin]
	if !ok {
		s = newSketch()
		byBin[bin] = s
	}
	return s
}

// reduceBins turns each (key, bin) sketch or sample into its value.
func reduceBins[T any](bins map[string]map[int64]T, reduce func(T) float64) Values {
	values := make(Values, len(bins))
	for key, byBin := range bins {
		row := make(map[int64]float64, len(byBin))
		for bin, v := range byBin {
			row[bin] = reduce(v)
		}
		values[key] = row
	}
	return values
}
