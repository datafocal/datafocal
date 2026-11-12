package main

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/ingest"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	if err := run(logger); err != nil {
		logger.Error("run collector", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := loadConfig(os.Args[1:])
	if err != nil {
		return fmt.Errorf("load collector config: %w", err)
	}

	store, err := cfg.Open()
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	// Ingest saves its offsets from the cuts and the segments reported later, so the
	// collector can seal a batch while the next one fills.
	cfg.SealInBackground = true
	// Each node's collector commits to a log of its own, so collectors never race to commit.
	host, _ := os.Hostname()
	c, err := collector.New(cfg.Config, collector.Dependencies{
		Store:         store,
		CatalogWriter: catalog.NewWithConfig(store, cfg.Tenant, catalog.Config{Writer: cmp.Or(cfg.Ingest.Host, host)}),
		Logger:        logger,
	})
	if err != nil {
		return fmt.Errorf("create collector: %w", err)
	}
	cfg.Ingest.FlushInterval = cfg.FlushInterval
	in, err := ingest.New(cfg.Ingest, c)
	if err != nil {
		return fmt.Errorf("create ingest: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	logger.Info("collector following sources", "sources", len(cfg.Ingest.Sources), "root", cfg.Root)
	if err := in.Run(ctx); err != nil {
		return err
	}
	logger.Info("collector stopped", "reason", context.Cause(ctx))
	return nil
}
