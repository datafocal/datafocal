package segment

import (
	"context"

	"github.com/datafocal/datafocal/internal/sketch"
)

// Fake is a Reader whose methods call the matching Fn field; a nil field returns
// zero values.
type Fake struct {
	GramsFn      func(loc ObjectLocation, footer Footer) (Grams, Cost, error)
	FooterFn     func(loc ObjectLocation) (Footer, Cost, error)
	ColumnsFn    func(loc ObjectLocation, footer Footer, rowGroup int, rows []int, cols []string) (map[string]ColumnChunk, Cost, error)
	MayContainFn func(loc ObjectLocation, footer Footer, column, value string) (bool, bool, Cost, error)
	RecordsFn    func(loc ObjectLocation) ([]Record, Footer, Cost, error)
	GroupFn      func(loc ObjectLocation, footer Footer, rowGroup int) ([]Record, Cost, error)
	RollupFn     func(loc ObjectLocation) ([]byte, Cost, error)
	FiltersFn    func(loc ObjectLocation) (map[string]*sketch.XorFilter, Cost, error)
}

var _ Reader = Fake{}

func (f Fake) Footer(_ context.Context, loc ObjectLocation) (Footer, Cost, error) {
	if f.FooterFn == nil {
		return Footer{}, Cost{}, nil
	}
	return f.FooterFn(loc)
}

func (f Fake) Columns(_ context.Context, loc ObjectLocation, footer Footer, rowGroup int, rows []int, cols ...string) (map[string]ColumnChunk, Cost, error) {
	if f.ColumnsFn == nil {
		return nil, Cost{}, nil
	}
	return f.ColumnsFn(loc, footer, rowGroup, rows, cols)
}

func (f Fake) MayContain(_ context.Context, loc ObjectLocation, footer Footer, column, value string) (filtered, may bool, cost Cost, err error) {
	if f.MayContainFn == nil {
		return false, false, Cost{}, nil
	}
	return f.MayContainFn(loc, footer, column, value)
}

func (f Fake) Records(_ context.Context, loc ObjectLocation) ([]Record, Footer, Cost, error) {
	if f.RecordsFn == nil {
		return nil, Footer{}, Cost{}, nil
	}
	return f.RecordsFn(loc)
}

func (f Fake) RowGroupRecords(_ context.Context, loc ObjectLocation, footer Footer, rowGroup int) ([]Record, Cost, error) {
	if f.GroupFn == nil {
		return nil, Cost{}, nil
	}
	return f.GroupFn(loc, footer, rowGroup)
}

func (f Fake) Rollup(_ context.Context, loc ObjectLocation) ([]byte, Cost, error) {
	if f.RollupFn == nil {
		return nil, Cost{}, nil
	}
	return f.RollupFn(loc)
}

func (f Fake) Filters(_ context.Context, loc ObjectLocation) (map[string]*sketch.XorFilter, Cost, error) {
	if f.FiltersFn == nil {
		return nil, Cost{}, nil
	}
	return f.FiltersFn(loc)
}

// FooterWithFilters is f with a filter section on each of columns, for a test that
// builds a footer by hand and answers MayContain itself.
func FooterWithFilters(f Footer, columns ...string) Footer {
	f.filters = map[string]ByteRange{}
	for _, c := range columns {
		f.filters[c] = ByteRange{}
	}
	return f
}

// FooterWithServices is f with views as its per-service views, for a test that builds
// a footer by hand.
func FooterWithServices(f Footer, views map[string]Footer) Footer {
	f.services = views
	return f
}

func (f Fake) Grams(_ context.Context, loc ObjectLocation, footer Footer) (Grams, Cost, error) {
	if f.GramsFn == nil {
		return nil, Cost{}, nil
	}
	return f.GramsFn(loc, footer)
}
