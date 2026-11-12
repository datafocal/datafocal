// Package seedcorpus generates a deterministic, reproducible corpus for testing.
// It extracts the event-generation and corpus-build logic from cmd/datafocal-seed
// so it can be imported by tests.
package seedcorpus

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/pattern"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/internal/value"
)

// Scenario selects which corpus to build.
type Scenario string

const (
	ScenarioIncident Scenario = "incident"
	ScenarioHealthy  Scenario = "healthy"
	ScenarioAll      Scenario = "all"
	// ScenarioWide is agent transcripts as wide as flattened JSON makes them, the shape
	// whose columns once cost a segment memory whatever its rows held.
	ScenarioWide Scenario = "wide"
	// ScenarioAccess is one access-log pattern whose faults are in its field values alone.
	ScenarioAccess Scenario = "access"
)

// BuildResult carries the outcome of a corpus build.
type BuildResult struct {
	Descriptors []segment.Descriptor
	Patterns    map[string]string // pattern_id → pattern string
	Elapsed     time.Duration
	Truth       *FactoryTruth // set for ScenarioFactory only
}

// Build generates the deterministic corpus into outDir. Creates outDir if missing.
// Errors on unknown scenario.
func Build(ctx context.Context, outDir string, scenario Scenario) (BuildResult, error) {
	if scenario == ScenarioFactory {
		return buildFactory(ctx, outDir)
	}
	gens, err := streamsFor(scenario)
	if err != nil {
		return BuildResult{}, err
	}
	return buildStreams(ctx, outDir, gens)
}

func streamsFor(scenario Scenario) ([]func() stream, error) {
	switch scenario {
	case ScenarioIncident:
		return incidentStreams(), nil
	case ScenarioHealthy:
		return healthyStreams(), nil
	case ScenarioAll:
		return append(incidentStreams(), healthyStreams()...), nil
	case ScenarioWide:
		return []func() stream{genWide}, nil
	case ScenarioAccess:
		return []func() stream{genAccess}, nil
	}
	return nil, fmt.Errorf("unknown scenario %q (want incident, healthy, all, wide or access)", scenario)
}

// buildStreams is the shared corpus-writing core used by both Build and
// BuildScenario. It creates outDir, seeds every stream, verifies the catalog
// round-trip, and returns the result.
func buildStreams(ctx context.Context, outDir string, gens []func() stream) (BuildResult, error) {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return BuildResult{}, fmt.Errorf("create out dir: %w", err)
	}

	store := storage.NewFS(outDir)
	cat := catalog.New(store, Tenant)
	patterns := map[string]string{}
	quiet := slog.New(slog.DiscardHandler)

	start := time.Now()
	var descriptors []segment.Descriptor
	for _, gen := range gens {
		s := gen()
		if len(s.events) == 0 {
			continue
		}
		descs, err := seedStream(ctx, store, cat, patterns, quiet, s)
		if err != nil {
			return BuildResult{}, fmt.Errorf("seed stream %s: %w", s.service, err)
		}
		descriptors = append(descriptors, descs...)
	}
	elapsed := time.Since(start)

	if err := verifyCatalog(ctx, store, len(descriptors)); err != nil {
		return BuildResult{}, err
	}

	return BuildResult{
		Descriptors: descriptors,
		Patterns:    patterns,
		Elapsed:     elapsed,
	}, nil
}

// LogsResult is what WriteLogs wrote: the lines per service and the bytes in all.
type LogsResult struct {
	Lines map[string]int
	Bytes int64
}

// WriteLogs writes scenario's streams to dir as a JSON-lines file each, every copy an hour
// after the one before so they never overlap, and every copy shift later.
func WriteLogs(dir string, scenario Scenario, copies int, shift time.Duration) (LogsResult, error) {
	gens, err := streamsFor(scenario)
	if err != nil {
		return LogsResult{}, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return LogsResult{}, fmt.Errorf("create logs dir: %w", err)
	}
	result := LogsResult{Lines: map[string]int{}}
	for _, gen := range gens {
		s := gen()
		sort.SliceStable(s.events, func(i, j int) bool {
			return s.events[i].Timestamp.Before(s.events[j].Timestamp)
		})
		n, err := writeStreamLogs(filepath.Join(dir, streamFileName(s)), s, copies, shift)
		if err != nil {
			return LogsResult{}, fmt.Errorf("write stream %s: %w", s.service, err)
		}
		result.Lines[s.service] += len(s.events) * copies
		result.Bytes += n
	}
	return result, nil
}

// streamFileName names a stream's file by its service and label values, which are
// unique per stream; the service a collector records comes from the line, not the name.
func streamFileName(s stream) string {
	parts := []string{s.service}
	for _, k := range slices.Sorted(maps.Keys(s.labels)) {
		parts = append(parts, s.labels[k])
	}
	return strings.Join(parts, "_") + ".log"
}

func writeStreamLogs(path string, s stream, copies int, shift time.Duration) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	var written int64
	for c := range copies {
		for _, ev := range s.events {
			line, err := logLine(s, ev, shift+time.Duration(c)*time.Hour)
			if err != nil {
				return 0, errors.Join(err, f.Close())
			}
			n, err := w.Write(line)
			if err != nil {
				return 0, errors.Join(err, f.Close())
			}
			written += int64(n)
		}
	}
	if err := w.Flush(); err != nil {
		return 0, errors.Join(err, f.Close())
	}
	return written, f.Close()
}

// logLine is ev as one JSON object carrying the stream's service and labels as fields,
// since a file has no stream identity of its own to carry them.
func logLine(s stream, ev segment.Record, shift time.Duration) ([]byte, error) {
	fields := make(map[string]string, len(s.labels)+len(ev.Columns)+2)
	maps.Copy(fields, s.labels)
	for _, col := range ev.Columns {
		fields[col.Name] = value.Format(col.Type, col.Value)
	}
	fields[segment.ColService] = s.service
	fields[segment.ColTimestamp] = ev.Timestamp.Add(shift).UTC().Format(time.RFC3339Nano)
	line, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	return append(line, '\n'), nil
}

// seedStream drives one stream's events through a collector wired to the fs
// store and catalog, aligned on the summary's bin width, so each stream becomes a run
// of segments one bin each, stamped by an event-time clock.
func seedStream(ctx context.Context, store storage.Store, cat catalog.Catalog, patterns map[string]string, logger *slog.Logger, s stream) ([]segment.Descriptor, error) {
	cfg := collector.DefaultConfig()
	cfg.Tenant = Tenant
	cfg.Stream = collector.StreamConfig{Service: s.service, Labels: s.labels}
	// A segment per bin per stream, which an age cut would split, so age is off as size and
	// rows are below.
	cfg.FlushAlignment = cmp.Or(s.segmentWidth, summary.DefaultBinWidth)
	cfg.FlushInterval = 1 << 40
	cfg.FlushBytes = 1 << 40 // effectively disable size-based flush
	cfg.MaxRows = 1 << 30    // effectively disable row-count flush

	// Correlated request pairs are generated from a shared key rather than in
	// emit order, so a stream's events can be slightly out of time order. Sort
	// before ingest: a real collector tolerates that, but a time-ordered segment
	// keeps the corpus reproducible and its ts zone maps tight.
	sort.SliceStable(s.events, func(i, j int) bool {
		return s.events[i].Timestamp.Before(s.events[j].Timestamp)
	})

	clock := &seedClock{now: s.events[0].Timestamp}
	c, err := collector.New(cfg, collector.Dependencies{
		Store:         store,
		CatalogWriter: cat,
		Clock:         clock,
		Logger:        logger,
	})
	if err != nil {
		return nil, err
	}

	var out []segment.Descriptor
	for _, ev := range s.events {
		clock.set(ev.Timestamp)
		recordPattern(patterns, ev)
		res, err := c.Collect(ctx, ev)
		if err != nil {
			return nil, err
		}
		out = append(out, res.Descriptors...)
	}
	final, err := c.Flush(ctx)
	if err != nil {
		return nil, err
	}
	return append(out, final...), nil
}

// seedClock is the event-time clock the collector stamps segments from, so the corpus does
// not depend on when it was built.
type seedClock struct{ now time.Time }

func (c *seedClock) Now() time.Time  { return c.now }
func (c *seedClock) set(t time.Time) { c.now = t }

// recordPattern records the pattern text behind r's pattern_id, which message
// derivation obtains by masking the message alone.
func recordPattern(patterns map[string]string, r segment.Record) {
	for _, col := range r.Columns {
		if col.Name == segment.ColMessage {
			text := pattern.Mask(col.Value.String)
			patterns[pattern.ID(text)] = text
		}
	}
}

// verifyCatalog reads the commits back through the catalog read path and
// confirms they hold exactly the descriptors that were written.
func verifyCatalog(ctx context.Context, store storage.Store, want int) error {
	reader := catalog.New(store, Tenant)
	version, err := reader.Version(ctx)
	if err != nil {
		return fmt.Errorf("catalog version: %w", err)
	}
	descs, err := reader.Prune(ctx, version, selector.Resolved{})
	if err != nil {
		return fmt.Errorf("catalog prune: %w", err)
	}
	if len(descs) != want {
		return fmt.Errorf("catalog round-trip mismatch: wrote %d descriptors, catalog holds %d", want, len(descs))
	}
	return nil
}
