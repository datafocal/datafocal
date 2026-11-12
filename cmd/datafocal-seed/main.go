// Command datafocal-seed generates a real corpus — records driven through the
// collector into sealed segments on the fs object store plus an fs catalog
// manifest — reproducing the seeded incident fixture.
//
// Two scenarios share tenant "acme":
//
//   - incident: a cross-layer degradation with NO shared trace_id across layers
//     (analytics-export write spike → host-7 disk saturation → db-3 write p95
//     40ms→1.2s → jobs queue 0→4200 → gateway/checkout user-create 5xx 0.1→7%),
//     plus a pod→host scheduler log so db-3→host-7 topology is derivable.
//   - healthy: a freshly rolled-out "recommendations" service, ~210 req/s, p95
//     ~41ms, zero errors, three new patterns.
//   - factory: an agent-factory node's container logs and Claude Code transcripts, written
//     under --out/node and read through ingest as the node's collector reads them.
//
// The output under --out is a live corpus the querier, tests, and the frontend
// mockup can all point at. With --logs it instead writes the same events as JSON-lines
// files for a collector process to follow, --copies times over. Usage:
//
//	go run ./cmd/datafocal-seed --out ./.localdata --scenario all
//	go run ./cmd/datafocal-seed --logs ./.localdata/logs --scenario all --copies 4
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
)

func main() {
	out := flag.String("out", "./.localdata", "output directory for segment objects and the catalog manifest")
	scenario := flag.String("scenario", "all", "which scenario to seed: incident | healthy | all | factory")
	logs := flag.String("logs", "", "write the scenario as JSON-lines log files under this directory instead of a corpus")
	copies := flag.Int("copies", 1, "with --logs, how many hour-apart copies of the scenario to write")
	flag.Parse()

	if *logs != "" {
		if err := writeLogs(*logs, *scenario, *copies); err != nil {
			fmt.Fprintln(os.Stderr, "datafocal-seed:", err)
			os.Exit(1)
		}
		return
	}
	if err := run(*out, *scenario); err != nil {
		fmt.Fprintln(os.Stderr, "datafocal-seed:", err)
		os.Exit(1)
	}
}

func run(out, scenario string) error {
	s := seedcorpus.Scenario(scenario)
	result, err := seedcorpus.Build(context.Background(), out, s)
	if err != nil {
		return err
	}

	report(out, result.Descriptors, result.Patterns, result.Elapsed)
	// Build already round-trips the manifest through the catalog read path and
	// fails if it does not hold every descriptor written, so reaching here is the
	// verification result worth reporting.
	fmt.Printf("\nCatalog manifest: %d descriptors, verified via read path.\n", len(result.Descriptors))
	return listOutput(out)
}

func writeLogs(dir, scenario string, copies int) error {
	if copies < 1 {
		return fmt.Errorf("copies must be at least 1, got %d", copies)
	}
	result, err := seedcorpus.WriteLogs(dir, seedcorpus.Scenario(scenario), copies, 0)
	if err != nil {
		return err
	}
	for _, svc := range slices.Sorted(maps.Keys(result.Lines)) {
		slog.Info("wrote service logs", "service", svc, "lines", result.Lines[svc])
	}
	slog.Info("wrote logs", "dir", dir, "bytes", humanBytes(result.Bytes))
	return nil
}

// --- reporting ---

func report(out string, descs []segment.Descriptor, patterns map[string]string, elapsed time.Duration) {
	type agg struct {
		rows     int
		segments int
		labels   map[string]string
		tmpls    map[string]struct{}
		minT     time.Time
		maxT     time.Time
	}
	byService := map[string]*agg{}
	for _, d := range descs {
		a := byService[d.Stream.Service]
		if a == nil {
			a = &agg{labels: d.Stream.Labels, tmpls: map[string]struct{}{}, minT: d.TimeRange.Min, maxT: d.TimeRange.Max}
			byService[d.Stream.Service] = a
		}
		a.rows += d.Rows
		a.segments++
		for _, id := range d.PatternIDs {
			a.tmpls[id] = struct{}{}
		}
		if d.TimeRange.Min.Before(a.minT) {
			a.minT = d.TimeRange.Min
		}
		if d.TimeRange.Max.After(a.maxT) {
			a.maxT = d.TimeRange.Max
		}
	}

	services := slices.Sorted(maps.Keys(byService))

	totalRows, totalSegs := 0, 0
	slog.Info("seeded tenant", "tenant", seedcorpus.Tenant, "out", out, "elapsed", elapsed)
	for _, svc := range services {
		a := byService[svc]
		totalRows += a.rows
		totalSegs += a.segments
		slog.Info("seeded service", "service", svc, "rows", a.rows, "segments", a.segments, "pattern_count", len(a.tmpls),
			"window_start", a.minT.UTC().Format("15:04:05"), "window_end", a.maxT.UTC().Format("15:04:05"),
			"patterns", patternTexts(a.tmpls, patterns))
	}
	slog.Info("seeded total", "rows", totalRows, "segments", totalSegs, "services", len(services))
}

// patternTexts returns the sorted pattern text of each id in ids.
func patternTexts(ids map[string]struct{}, patterns map[string]string) []string {
	texts := make([]string, 0, len(ids))
	for id := range ids {
		if t := patterns[id]; t != "" {
			texts = append(texts, t)
		}
	}
	sort.Strings(texts)
	return texts
}

// listOutput walks the output directory and logs the segment objects and
// catalog commits that were produced.
func listOutput(out string) error {
	var segs, commits int
	var segBytes int64
	err := filepath.WalkDir(out, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ".seg" {
			segs++
			segBytes += info.Size()
		}
		if strings.Contains(filepath.ToSlash(path), "/catalog/v1/") {
			commits++
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("list output: %w", err)
	}
	slog.Info("seeded output", "out", out, "segment_objects", segs, "segment_bytes", humanBytes(segBytes), "catalog_commits", commits)
	return nil
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGT"[exp])
}
