package e2e

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/storage"
)

// BenchmarkCollect collects one copy of the seeded logs, parsed beforehand, into sealed
// and encoded segments that are then dropped: the write path past parsing.
func BenchmarkCollect(b *testing.B) {
	records, bytes := seededRecords(b)
	b.SetBytes(bytes)
	b.ReportAllocs()
	ctx := context.Background()
	for b.Loop() {
		c, err := collector.New(collector.DefaultConfig(), collector.Dependencies{Store: storage.Discard, CatalogWriter: catalog.Discard})
		if err != nil {
			b.Fatal(err)
		}
		for _, r := range records {
			// Collect fills a record's columns in, so each pass takes its own copy.
			if _, err := c.Collect(ctx, segment.Record{Timestamp: r.Timestamp, Columns: append([]segment.Column(nil), r.Columns...)}); err != nil {
				b.Fatal(err)
			}
		}
		if _, err := c.Flush(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func seededRecords(b *testing.B) ([]segment.Record, int64) {
	b.Helper()
	dir := b.TempDir()
	written, err := seedcorpus.WriteLogs(dir, seedcorpus.ScenarioAll, 1, 0)
	if err != nil {
		b.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		b.Fatal(err)
	}
	var records []segment.Record
	for _, path := range paths {
		parser, err := logformat.New(logformat.Config{Format: "json"})
		if err != nil {
			b.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			if r, ok := parser.Parse(scanner.Text()); ok {
				records = append(records, r)
			}
		}
		if err := scanner.Err(); err != nil {
			b.Fatal(err)
		}
		_ = f.Close() // read only
	}
	return records, written.Bytes
}
