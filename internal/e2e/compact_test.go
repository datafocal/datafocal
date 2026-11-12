package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/compact"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// TestCompactKeepsTheFactoryAnswers merges the factory corpus's 10s segments into hours:
// every view request answers the same data, and the summary the same counts, after.
func TestCompactKeepsTheFactoryAnswers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	_, err := seedcorpus.Build(ctx, dir, seedcorpus.ScenarioFactory)
	require.NoError(t, err)
	store := storage.NewFS(dir)
	cat := catalog.New(store, tenant)
	// One stream-hour of the factory's 10s segments, and a row measure over the last 48h.
	var requests []viewRequest
	readJSON(t, filepath.Join("testdata", "compact_requests.json"), &requests)
	answers := func() []map[string]any {
		out := make([]map[string]any, len(requests))
		for i, r := range requests {
			out[i] = answerOf(t, store, r.Tool, r.Req)
		}
		assert.Nil(t, out[len(out)-1]["code"], "a 48h row measure is not too large to scan")
		return out
	}
	counts := func(b summary.Builder) []summary.CountPoint {
		require.NoError(t, b.Build(ctx))
		return b.Store().CountSlice("", 0, 1<<62)
	}
	before, v := answers(), version(t, cat)
	synced := summary.NewBuilder(cat, segment.NewReader(store), summary.Config{})
	wantCounts := counts(synced)
	require.NoError(t, compact.New(cat, catalog.NewSnapshotter(cat, catalog.SnapshotConfig{}), segment.NewReader(store), store, tenant).Run(ctx, time.Now()))
	assert.True(t, v.Before(version(t, cat)), "the merges commit past the version answered before")
	assert.Equal(t, before, answers())
	assert.Equal(t, wantCounts, counts(synced), "a summary that counted the inputs skips their merge")
	assert.Equal(t, wantCounts, counts(summary.NewBuilder(cat, segment.NewReader(store), summary.Config{})))
}

// TestCompactKeepsTheArchetypeAnswers: every BenchmarkQuery archetype answers the same after
// its corpus's hours are merged, rank_fields in the same order of its high-signal fields but
// for what it derives from a field's share of segments, which merging them changes.
func TestCompactKeepsTheArchetypeAnswers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	corpus := collectCorpus(t, dir, 2)
	wide := filepath.Join(dir, "wide")
	_, err := seedcorpus.Build(ctx, wide, seedcorpus.ScenarioWide)
	require.NoError(t, err)
	for path, archetypes := range map[string][]Archetype{filepath.Join(dir, "store"): Archetypes(corpus), wide: wideArchetypes()} {
		store := storage.NewFS(path)
		cat := catalog.New(store, tenant)
		answers := func() []map[string]any {
			out := make([]map[string]any, len(archetypes))
			for i, a := range archetypes {
				body, err := json.Marshal(a.Request)
				require.NoError(t, err)
				out[i] = answerOf(t, store, a.Tool, body)
				if a.Tool == "rank_fields" {
					withoutSegmentShares(out[i])
				}
			}
			return out
		}
		before, v := answers(), version(t, cat)
		require.NoError(t, compact.New(cat, catalog.NewSnapshotter(cat, catalog.SnapshotConfig{}), segment.NewReader(store), store, tenant).Run(ctx, time.Now()))
		assert.True(t, v.Before(version(t, cat)), "the merges commit past the version answered before")
		assert.Equal(t, before, answers(), path)
	}
}

// withoutSegmentShares drops from a rank_fields answer each field's presence, a share of
// segments, and the signal it weighs, keeping the high-signal fields first in their ranked
// order. The rest follow by name: their signals differ by their presence alone, so merging
// segments reorders them.
func withoutSegmentShares(answer map[string]any) {
	data := answer["data"].(map[string]any)
	var high, rest []any
	for _, f := range data["fields"].([]any) {
		delete(f.(map[string]any), "present")
		delete(f.(map[string]any), "signal")
		if f.(map[string]any)["high_signal"] == true {
			high = append(high, f)
		} else {
			rest = append(rest, f)
		}
	}
	slices.SortFunc(rest, func(a, b any) int {
		return strings.Compare(a.(map[string]any)["name"].(string), b.(map[string]any)["name"].(string))
	})
	data["fields"] = append(high, rest...)
}

func version(t *testing.T, cat catalog.Catalog) catalog.Version {
	v, err := cat.Version(context.Background())
	require.NoError(t, err)
	return v
}

// answerOf is store's answer to a request without its meta, its lines sorted and stripped
// of the segment and row that compaction replaces.
func answerOf(t *testing.T, store storage.Store, tool string, req []byte) map[string]any {
	rec := httptest.NewRecorder()
	querier.Open(querier.Options{Catalog: catalog.New(store, tenant), Store: store}).Handler().
		ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+tool, bytes.NewReader(req)))
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	delete(out, "meta")
	data, _ := out["data"].(map[string]any)
	lines, _ := data["lines"].([]any)
	for _, l := range lines {
		delete(l.(map[string]any), "segment")
		delete(l.(map[string]any), "row")
	}
	slices.SortFunc(lines, func(a, b any) int { return strings.Compare(fmt.Sprint(a), fmt.Sprint(b)) })
	return out
}
