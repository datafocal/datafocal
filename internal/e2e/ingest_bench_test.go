package e2e

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/ingest"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

// BenchmarkIngest follows one copy of the seeded logs, 230k JSON lines, from files to
// cataloged segments on the fs store: tailing, parsing, collecting, sealing and writing.
func BenchmarkIngest(b *testing.B) {
	logs := filepath.Join(b.TempDir(), "logs")
	written, err := seedcorpus.WriteLogs(logs, seedcorpus.ScenarioAll, 1, 0)
	if err != nil {
		b.Fatal(err)
	}
	lines := 0
	for _, n := range written.Lines {
		lines += n
	}
	b.SetBytes(written.Bytes)
	b.ReportAllocs()
	for b.Loop() {
		ingestAll(b, logs, lines)
	}
	b.ReportMetric(float64(lines)*float64(b.N)/b.Elapsed().Seconds(), "lines/s")
}

func ingestAll(b *testing.B, logs string, lines int) {
	b.Helper()
	dir := b.TempDir()
	store := storage.NewFS(filepath.Join(dir, "store"))
	cfg := collector.DefaultConfig()
	cfg.Tenant = seedcorpus.Tenant
	cfg.SealInBackground = true // as cmd/collector runs it
	c, err := collector.New(cfg, collector.Dependencies{Store: store, CatalogWriter: catalog.New(store, cfg.Tenant)})
	if err != nil {
		b.Fatal(err)
	}
	in, err := ingest.New(ingest.Config{StateDir: filepath.Join(dir, "state"), FlushInterval: 100 * time.Millisecond,
		Sources: []ingest.Source{{Path: filepath.Join(logs, "*.log"), Parse: logformat.Config{Format: "json"}}}}, c)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- in.Run(ctx) }()
	reader := catalog.New(store, cfg.Tenant)
	for cataloged(b, reader) < lines {
		<-time.After(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		b.Fatal(err)
	}
}

func cataloged(b *testing.B, cat catalog.Catalog) int {
	b.Helper()
	v, err := cat.Version(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	descs, err := cat.Prune(context.Background(), v, selector.Resolved{})
	if err != nil {
		b.Fatal(err)
	}
	rows := 0
	for _, d := range descs {
		rows += d.Rows
	}
	return rows
}
