package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/ingest"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

type fixedClock struct{}

func (fixedClock) Now() time.Time { return seedcorpus.WideStart() }

// TestIngestSegmentsGolden pins every byte ingest seals from each scenario's logs, size
// cuts and alignment buckets included, so a faster write path must write the same segments.
// Each is pinned under its key by one hash of its object and descriptor JSON, cut to 64 bits as
// it need only detect a change.
func TestIngestSegmentsGolden(t *testing.T) {
	ctx := context.Background()
	got := map[string]map[string]string{}
	for _, scenario := range []seedcorpus.Scenario{seedcorpus.ScenarioIncident, seedcorpus.ScenarioHealthy,
		seedcorpus.ScenarioWide, seedcorpus.ScenarioAccess, seedcorpus.ScenarioFactory} {
		dir := t.TempDir()
		store := storage.NewFS(dir)
		if scenario == seedcorpus.ScenarioFactory {
			_, err := seedcorpus.Build(ctx, dir, scenario)
			require.NoError(t, err)
		} else {
			logs := filepath.Join(dir, "logs")
			_, err := seedcorpus.WriteLogs(logs, scenario, 2, 0)
			require.NoError(t, err)
			cfg := collector.DefaultConfig()
			cfg.Tenant, cfg.MaxRows, cfg.FlushAlignment, cfg.SealInBackground = tenant, 20_000, 5*time.Minute, true
			c, err := collector.New(cfg, collector.Dependencies{Store: store, CatalogWriter: catalog.New(store, tenant), Clock: fixedClock{}})
			require.NoError(t, err)
			in, err := ingest.New(ingest.Config{StateDir: filepath.Join(dir, "state"), Host: "node-1", FlushInterval: time.Hour,
				Sources: []ingest.Source{{Path: filepath.Join(logs, "*.log"), Parse: logformat.Config{Format: "json"}}}}, c)
			require.NoError(t, err)
			require.NoError(t, in.Drain(ctx))
		}
		cat := catalog.New(store, tenant)
		v, err := cat.Version(ctx)
		require.NoError(t, err)
		descs, err := cat.Prune(ctx, v, selector.Resolved{})
		require.NoError(t, err)
		got[string(scenario)] = map[string]string{}
		for _, d := range descs {
			body, err := os.ReadFile(filepath.Join(dir, d.Object.Key))
			require.NoError(t, err)
			desc, err := json.Marshal(normalizeDescriptorPaths(d))
			require.NoError(t, err)
			got[string(scenario)][d.Object.Key] = sha([]byte(sha(body) + sha(desc)))[:16]
		}
	}
	golden(t, "testdata/ingest_segments.golden.json", got)
}

func sha(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
