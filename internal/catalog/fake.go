package catalog

import (
	"context"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// Fake is a Catalog and Snapshotter whose methods call the matching Fn field; a nil
// field returns zero values.
type Fake struct {
	AppendFn   func(d segment.Descriptor) error
	VersionFn  func() (Version, error)
	PruneFn    func(v Version, sel selector.Resolved) ([]segment.Descriptor, error)
	SnapshotFn func() error
}

var (
	_ Catalog     = Fake{}
	_ Snapshotter = Fake{}
)

// Discard is a Catalog that drops every descriptor, for a collector that has no
// catalog to publish to.
var Discard Catalog = Fake{}

// Append calls AppendFn with each descriptor in turn.
func (f Fake) Append(_ context.Context, ds ...segment.Descriptor) error {
	if f.AppendFn == nil {
		return nil
	}
	for _, d := range ds {
		if err := f.AppendFn(d); err != nil {
			return err
		}
	}
	return nil
}

func (f Fake) Version(_ context.Context) (Version, error) {
	if f.VersionFn == nil {
		return "", nil
	}
	return f.VersionFn()
}

func (f Fake) Prune(_ context.Context, v Version, sel selector.Resolved) ([]segment.Descriptor, error) {
	if f.PruneFn == nil {
		return nil, nil
	}
	return f.PruneFn(v, sel)
}

func (f Fake) Snapshot(context.Context) error {
	if f.SnapshotFn == nil {
		return nil
	}
	return f.SnapshotFn()
}
