package catalog

import (
	"context"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// Catalog is the append-only list of one tenant's segment descriptors, serving the
// manifest-prune stage of the query pipeline.
type Catalog interface {
	// Append adds descriptors to the catalog in one commit, so a reader sees all of them
	// or none.
	Append(ctx context.Context, ds ...segment.Descriptor) error
	// Version returns the current catalog version.
	Version(ctx context.Context) (Version, error)
	// Prune returns the descriptors at version v that sel's window and predicates cannot rule out.
	Prune(ctx context.Context, v Version, sel selector.Resolved) ([]segment.Descriptor, error)
}

// Version names, opaquely, what the catalog held when it was read; pinning it is
// what makes repeated queries reproducible.
type Version string

// Valid reports whether v is a version a catalog may have read.
func (v Version) Valid() bool { return v.total() >= 0 }

// Before reports whether v held less than w, so a holder of versions keeps the newest.
func (v Version) Before(w Version) bool { return v.total() < w.total() }
