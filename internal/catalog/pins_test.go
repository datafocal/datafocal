package catalog_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

// TestCatalogLoadsAV2Snapshot: a catalog a v2 snapshot and v1 commits hold loads and answers as
// it did, its versions still counts, and a snapshot of it is a v3 one that answers the same.
func TestCatalogLoadsAV2Snapshot(t *testing.T) {
	ctx := t.Context()
	fs := storage.NewFS(t.TempDir())
	at := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	raw, err := json.Marshal(map[string]any{"base": 0, "descriptors": []segment.Descriptor{desc("v2-0", "checkout", at, at), desc("v2-1", "checkout", at, at)}})
	require.NoError(t, err)
	enc, _ := zstd.NewWriter(nil)
	key := "acme/catalog/snapshot/v2/00000000000000000001.json.zst"
	require.NoError(t, fs.Put(ctx, key, enc.EncodeAll(raw, nil)))
	require.NoError(t, fs.Put(ctx, "acme/catalog/newest-snapshot", []byte(key)))
	require.NoError(t, fs.Put(ctx, "acme/catalog/v1/00000000000000000002.json", []byte(`{"id":"v1-2"}`)))

	for _, snapshot := range []bool{false, true} {
		cat := catalog.New(fs, "acme")
		v, err := cat.Version(ctx)
		require.NoError(t, err)
		assert.Equal(t, catalog.Version("3"), v)
		got, err := cat.Prune(ctx, catalog.Version("2"), selector.Resolved{})
		require.NoError(t, err)
		assert.Equal(t, []string{"v2-0", "v2-1"}, ids(got))
		if snapshot {
			continue
		}
		require.NoError(t, catalog.NewSnapshotter(cat, catalog.SnapshotConfig{MinCommits: 1}).Snapshot(ctx))
		newest, err := fs.GetSuffix(ctx, "acme/catalog/newest-snapshot", 1<<20)
		require.NoError(t, err)
		assert.Equal(t, "acme/catalog/snapshot/v3/00000000000000000002.json.zst", string(newest))
	}
}

// TestCatalogExpiresAVersionItCannotHold: a version holding commits no load reaches, as a
// writer's this catalog has none of, answers snapshot_expired after a fresh load rather than
// fewer descriptors.
func TestCatalogExpiresAVersionItCannotHold(t *testing.T) {
	ctx := t.Context()
	fs := storage.NewFS(t.TempDir())
	require.NoError(t, catalog.New(fs, "acme").Append(ctx, desc("a", "checkout", time.Time{}, time.Time{})))
	cat := catalog.New(fs, "acme")
	_, err := cat.Prune(ctx, catalog.Version("2"), selector.Resolved{})
	require.ErrorIs(t, err, catalog.ErrSnapshotExpired)
	_, err = cat.Prune(ctx, catalog.Version("0-AARub2RlAQ"), selector.Resolved{})
	require.ErrorIs(t, err, catalog.ErrSnapshotExpired)
	got, err := cat.Prune(ctx, catalog.Version("1"), selector.Resolved{})
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, ids(got))
}
