// Command compactor runs a compaction pass over a tenant's segments every interval until it
// is stopped: each pass merges each complete stream-hour into one segment that supersedes its
// inputs and folds the catalog into a snapshot. It is the one process that rewrites what is
// already in the bucket.
//
// Usage:
//
//	go run ./cmd/compactor --root ./.localdata
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/compact"
	"github.com/datafocal/datafocal/internal/config"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/summary"
)

// settings is the compactor's operator surface.
type settings struct {
	Tenant string `json:"tenant" envconfig:"TENANT"`
	config.Store
	// Interval is how often a pass starts; one that runs longer delays the next.
	Interval config.Duration `json:"interval" envconfig:"COMPACT_INTERVAL"`
	// Settle is how long a segment must exist before a pass merges it, so a summary that
	// refreshes more often has counted it and skips the merged segment rather than missing it.
	Settle config.Duration `json:"settle" envconfig:"COMPACT_SETTLE"`
	// Retention is how long after its last row a segment is kept, matching the querier's
	// and the bucket's lifecycle rule; 0 keeps every one.
	Retention config.Duration `json:"retention" envconfig:"RETENTION"`
	// Lookback is how far before settle a pass looks for hours to merge; 0 looks at every hour.
	Lookback config.Duration `json:"lookback" envconfig:"COMPACT_LOOKBACK"`
	// Window is the span of an incomplete hour a pass merges once settled, dividing an hour;
	// 0 merges only whole hours.
	Window config.Duration `json:"window" envconfig:"COMPACT_WINDOW"`
	// Summary is the querier's summary block, whose bin width and sketch columns each rollup is
	// built for, so the querier adds a merged hour from its rollup.
	Summary struct {
		BinWidth      config.Duration `json:"bin_width" envconfig:"BIN_WIDTH"`
		SketchColumns []string        `json:"sketch_columns" envconfig:"SKETCH_COLUMNS"`
	} `json:"summary"`
	// SnapshotMinCommits is the commits past the newest catalog snapshot before a pass
	// writes another; 0 uses the catalog's default.
	SnapshotMinCommits int `json:"catalog_snapshot_min_commits" envconfig:"CATALOG_SNAPSHOT_MIN_COMMITS"`
	// CatalogGrace is how long a pinned catalog version keeps answering once newer ones are read.
	CatalogGrace config.Duration `json:"catalog_grace" envconfig:"CATALOG_GRACE"`
	// MemoryLimit is the container's in bytes, 85% of which the runtime keeps under; 0 is none.
	MemoryLimit int64 `json:"memory_limit" envconfig:"MEMORY_LIMIT"`
}

const defaultConfigPath = "/etc/config/compactor.json"

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Default().Error("run compactor", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := loadConfig(os.Args[1:])
	if err != nil {
		return err
	}
	if cfg.MemoryLimit > 0 {
		// The rest is for what the runtime's limit does not count, such as stacks and the OS.
		debug.SetMemoryLimit(cfg.MemoryLimit / 100 * 85)
	}
	store, err := cfg.Open()
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cat := catalog.NewWithConfig(store, cfg.Tenant, catalog.Config{Retention: time.Duration(cfg.Retention), Grace: time.Duration(cfg.CatalogGrace), Writer: "compactor"})
	snapshotter := catalog.NewSnapshotter(cat, catalog.SnapshotConfig{MinCommits: cfg.SnapshotMinCommits})
	c := compact.NewWithConfig(cat, snapshotter, segment.NewReader(store), store, compact.Config{
		Tenant:   cfg.Tenant,
		Lookback: time.Duration(cfg.Lookback),
		Window:   time.Duration(cfg.Window),
		Summary:  summary.Config{BinWidth: time.Duration(cfg.Summary.BinWidth), SketchColumns: cfg.Summary.SketchColumns},
	})
	loop(ctx, c.Run, time.Duration(cfg.Interval), time.Duration(cfg.Settle))
	return nil
}

// loop runs pass now and then every interval until ctx is done, logging a failed one, since
// a pass retries every hour it has not merged; a pass ctx stops ends at its next hour.
func loop(ctx context.Context, pass func(context.Context, time.Time) error, interval, settle time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		start := time.Now()
		err := pass(ctx, start.Add(-settle))
		switch {
		case ctx.Err() != nil:
			slog.Default().Info("compactor stopping", "reason", context.Cause(ctx))
			return
		case err != nil:
			slog.Default().Error("compact segments", "error", err)
		default:
			slog.Default().Info("compacted segments", "duration_ms", float64(time.Since(start).Microseconds())/1000)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// loadConfig layers the config file, defaultConfigPath or the one DATAFOCAL_CONFIG_FILE
// names, the environment (DATAFOCAL_*) and args over the defaults.
func loadConfig(args []string) (settings, error) {
	cfg := settings{Tenant: "local", Interval: config.Duration(time.Minute), Settle: config.Duration(2 * time.Minute),
		Lookback: config.Duration(24 * time.Hour), Window: config.Duration(5 * time.Minute), CatalogGrace: config.Duration(catalog.DefaultGrace)}
	flags := flag.NewFlagSet("compactor", flag.ContinueOnError)
	cfg.Bind(flags)
	flags.Var(&cfg.Interval, "interval", "how often a pass starts, such as 5m")
	flags.Var(&cfg.Settle, "settle", "how long a segment must exist before a pass merges it, such as 15m")
	flags.Var(&cfg.Lookback, "lookback", "how far before settle a pass looks for hours to merge; 0 looks at every hour")
	flags.Var(&cfg.Window, "window", "span of an incomplete hour a pass merges, dividing an hour, such as 5m; 0 merges only whole hours")
	if err := config.Load(&cfg, defaultConfigPath, flags, args); err != nil {
		return settings{}, err
	}
	if w := time.Duration(cfg.Window); w < 0 || w > 0 && time.Hour%w != 0 {
		return settings{}, fmt.Errorf("compactor: window %s does not divide an hour", w)
	}
	return cfg, nil
}
