package catalog

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

// TestCatalogCollectsPastTheGrace: as merges supersede raw segments, a warm catalog and the
// snapshot it writes hold the live descriptors and the grace's worth of superseded ones
// rather than every descriptor ever committed; a version pinned inside the grace answers as it
// did, and one past it answers ErrSnapshotExpired, warm and cold.
func TestCatalogCollectsPastTheGrace(t *testing.T) {
	ctx := t.Context()
	store := storage.NewFS(t.TempDir())
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	cfg := Config{Grace: 15 * time.Minute, Now: func() time.Time { return now }}
	cat := NewWithConfig(store, "acme", cfg).(*storeCatalog)
	writer := New(store, "acme")
	pin := func(c Catalog) (Version, []segment.Descriptor) {
		v, err := c.Version(ctx)
		require.NoError(t, err)
		live, err := c.Prune(ctx, v, selector.Resolved{})
		require.NoError(t, err)
		return v, live
	}
	// rounds commits n five-minute rounds of six raw segments and the merge superseding them.
	rounds := func(n int) {
		for range n {
			var raws []string
			for i := range 6 {
				d := segment.Descriptor{ID: fmt.Sprint(now.Unix(), "-", i), TimeRange: segment.TimeRange{Min: now, Max: now.Add(time.Minute)}, Rows: 1}
				require.NoError(t, writer.Append(ctx, d))
				raws = append(raws, d.ID)
			}
			pin(cat)
			merged := segment.Descriptor{ID: fmt.Sprint(now.Unix()), TimeRange: segment.TimeRange{Min: now, Max: now.Add(time.Minute)}, Rows: 6, Supersedes: raws}
			require.NoError(t, writer.Append(ctx, merged))
			pin(cat)
			now = now.Add(5 * time.Minute)
		}
	}

	rounds(4)
	inside, insideLive := pin(cat)
	rounds(2)
	got, err := cat.Prune(ctx, inside, selector.Resolved{})
	require.NoError(t, err, "pinned ten minutes ago, inside the grace")
	assert.Equal(t, insideLive, got)

	rounds(40)
	_, err = cat.Prune(ctx, inside, selector.Resolved{})
	require.ErrorIs(t, err, ErrSnapshotExpired, "pinned past the grace")
	assert.LessOrEqual(t, len(cat.entries), 46+4*7, "the live merges and the grace's rounds, not every raw segment")

	recent, recentLive := pin(cat)
	require.NoError(t, NewSnapshotter(cat, SnapshotConfig{MinCommits: 1}).Snapshot(ctx))
	cold := NewWithConfig(store, "acme", cfg).(*storeCatalog)
	got, err = cold.Prune(ctx, recent, selector.Resolved{})
	require.NoError(t, err)
	assert.Equal(t, recentLive, got)
	assert.LessOrEqual(t, len(cold.entries), 46+4*7, "the snapshot folds away what the warm catalog dropped")
	_, err = cold.Prune(ctx, inside, selector.Resolved{})
	require.ErrorIs(t, err, ErrSnapshotExpired)
}
