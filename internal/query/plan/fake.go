package plan

import (
	"context"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/summary"
)

// Fake is a Planner whose methods call the matching Fn field; a nil field returns zero
// values.
type Fake struct {
	SegmentsFn func(v catalog.Version, sel selector.Resolved) ([]segment.Descriptor, error)
	PlanFn     func(descs []segment.Descriptor, sel selector.Resolved) (Plan, error)
	PruneFn    func(v catalog.Version, sel selector.Resolved) (Plan, error)
	SummaryFn  func(v catalog.Version) summary.Store
	DerivedFn  func() Derived
}

var _ Planner = Fake{}

func (f Fake) Segments(_ context.Context, v catalog.Version, sel selector.Resolved) ([]segment.Descriptor, error) {
	if f.SegmentsFn == nil {
		return nil, nil
	}
	return f.SegmentsFn(v, sel)
}

func (f Fake) Plan(_ context.Context, descs []segment.Descriptor, sel selector.Resolved) (Plan, error) {
	if f.PlanFn == nil {
		return Plan{}, nil
	}
	return f.PlanFn(descs, sel)
}

func (f Fake) Prune(_ context.Context, v catalog.Version, sel selector.Resolved) (Plan, error) {
	if f.PruneFn == nil {
		return Plan{}, nil
	}
	return f.PruneFn(v, sel)
}

func (f Fake) Summary(v catalog.Version) summary.Store {
	if f.SummaryFn == nil {
		return nil
	}
	return f.SummaryFn(v)
}

func (f Fake) Derived() Derived {
	if f.DerivedFn == nil {
		return Derived{}
	}
	return f.DerivedFn()
}
