package collector

import (
	"context"

	"github.com/datafocal/datafocal/internal/segment"
)

// Fake is a Collector whose methods call the matching Fn field; a nil field returns
// zero values.
type Fake struct {
	CollectFn func(r segment.Record) (CollectResult, error)
	CutFn     func() (CollectResult, error)
	FlushFn   func() ([]segment.Descriptor, error)
}

var _ Collector = Fake{}

func (f Fake) Collect(_ context.Context, r segment.Record) (CollectResult, error) {
	if f.CollectFn == nil {
		return CollectResult{}, nil
	}
	return f.CollectFn(r)
}

func (f Fake) Cut(_ context.Context) (CollectResult, error) {
	if f.CutFn == nil {
		return CollectResult{}, nil
	}
	return f.CutFn()
}

func (f Fake) Flush(_ context.Context) ([]segment.Descriptor, error) {
	if f.FlushFn == nil {
		return nil, nil
	}
	return f.FlushFn()
}
