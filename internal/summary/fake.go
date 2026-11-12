package summary

import (
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/topology"
)

// Fake is a Store whose methods call the matching Fn field; a nil field returns zero
// values.
type Fake struct {
	CatalogVersionFn func() catalog.Version
	BinWidthFn       func() time.Duration
	CompleteFromFn   func(binMillis int64) bool
	CanServeFn       func(width time.Duration, from time.Time) bool
	CountSliceFn     func(service string, from, to int64) []CountPoint
	SketchSliceFn    func(service string, from, to int64) ([]SketchPoint, error)
	ValueSliceFn     func(column string, from, to int64) []ValuePoint
	HoldsValuesFn    func(column string, from int64) bool
	WidenedFn        func(column string) bool
	ExistedBeforeFn  func(patternID string, t time.Time) bool
	PatternTextFn    func(patternID string) (string, bool)
	HasAddedFn       func(descriptorID string) bool
	TopologyViewFn   func(filter topology.Filter) topology.View
	TopologyConfigFn func() topology.Config
}

var _ Store = Fake{}

func (f Fake) CatalogVersion() catalog.Version {
	if f.CatalogVersionFn == nil {
		return ""
	}
	return f.CatalogVersionFn()
}

func (f Fake) BinWidth() time.Duration {
	if f.BinWidthFn == nil {
		return 0
	}
	return f.BinWidthFn()
}

func (f Fake) CompleteFrom(binMillis int64) bool {
	return f.CompleteFromFn != nil && f.CompleteFromFn(binMillis)
}

func (f Fake) CanServe(width time.Duration, from time.Time) bool {
	return f.CanServeFn != nil && f.CanServeFn(width, from)
}

func (f Fake) CountSlice(service string, from, to int64) []CountPoint {
	if f.CountSliceFn == nil {
		return nil
	}
	return f.CountSliceFn(service, from, to)
}

func (f Fake) SketchSlice(service string, from, to int64) ([]SketchPoint, error) {
	if f.SketchSliceFn == nil {
		return nil, nil
	}
	return f.SketchSliceFn(service, from, to)
}

func (f Fake) ValueSlice(column string, from, to int64) []ValuePoint {
	if f.ValueSliceFn == nil {
		return nil
	}
	return f.ValueSliceFn(column, from, to)
}

func (f Fake) HoldsValues(column string, from int64) bool {
	return f.HoldsValuesFn != nil && f.HoldsValuesFn(column, from)
}

func (f Fake) Widened(column string) bool {
	return f.WidenedFn != nil && f.WidenedFn(column)
}

func (f Fake) ExistedBefore(patternID string, t time.Time) bool {
	return f.ExistedBeforeFn != nil && f.ExistedBeforeFn(patternID, t)
}

func (f Fake) PatternText(patternID string) (string, bool) {
	if f.PatternTextFn == nil {
		return "", false
	}
	return f.PatternTextFn(patternID)
}

func (f Fake) HasAdded(descriptorID string) bool {
	return f.HasAddedFn != nil && f.HasAddedFn(descriptorID)
}

func (f Fake) TopologyView(filter topology.Filter) topology.View {
	if f.TopologyViewFn == nil {
		return topology.View{}
	}
	return f.TopologyViewFn(filter)
}

func (f Fake) TopologyConfig() topology.Config {
	if f.TopologyConfigFn == nil {
		return topology.Config{}
	}
	return f.TopologyConfigFn()
}
