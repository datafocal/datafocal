package e2e

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/compact"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGramsSize(t *testing.T) {
	ctx, dir := context.Background(), t.TempDir()
	_, err := seedcorpus.Build(ctx, dir, seedcorpus.ScenarioFactory)
	require.NoError(t, err)
	store := storage.NewFS(dir)
	cat := catalog.New(store, tenant)
	require.NoError(t, compact.New(cat, catalog.NewSnapshotter(cat, catalog.SnapshotConfig{}), segment.NewReader(store), store, tenant).Run(ctx, time.Now()))
	var rawBytes, mergedBytes, rawGrams, mergedGrams int64
	keys, err := store.List(ctx, tenant+"/", "")
	require.NoError(t, err)
	for _, key := range keys {
		if !strings.HasSuffix(key, ".seg") {
			continue
		}
		body, err := store.GetSuffix(ctx, key, math.MaxInt64)
		require.NoError(t, err)
		rd, loc := segment.NewReader(store), segment.ObjectLocation{Key: key}
		footer, _, err := rd.Footer(ctx, loc)
		require.NoError(t, err)
		_, cost, err := rd.Grams(ctx, loc, footer)
		require.NoError(t, err)
		if strings.Contains(key, "/raw/") {
			rawBytes += int64(len(body))
			rawGrams += cost.Bytes
		} else {
			mergedBytes += int64(len(body))
			mergedGrams += cost.Bytes
		}
	}
	require.Positive(t, mergedBytes)
	raw := float64(rawGrams) / float64(rawBytes)
	merged := float64(mergedGrams) / float64(mergedBytes)
	whole := float64(rawGrams+mergedGrams) / float64(rawBytes+mergedBytes)
	t.Logf("gram sections: raw %.4f%%, merged %.4f%%, whole %.4f%%", 100*raw, 100*merged, 100*whole)
	assert.LessOrEqual(t, merged, 0.01)
	assert.LessOrEqual(t, whole, 0.03)
}
