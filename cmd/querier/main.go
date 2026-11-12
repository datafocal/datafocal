// Command querier serves the read-side tool API over a local fs corpus: the same
// segment objects and catalog manifest that cmd/datafocal-seed writes. It opens the
// querier rooted at --root and listens on --addr, refreshing its summary on an
// interval. The same tools are exposed over MCP at /mcp (streamable HTTP) and,
// with --mcp-stdio, over stdio for agent hosts that prefer that transport.
//
// Usage:
//
//	go run ./cmd/datafocal-seed --out ./.localdata --scenario all
//	go run ./cmd/querier --root ./.localdata --addr :8080
//	go run ./cmd/querier --root ./.localdata --mcp-stdio
package main

import (
	"cmp"
	"context"
	"expvar"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
	"syscall"
	"time"

	"github.com/datafocal/datafocal/internal/api"
	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	// Stderr, since --mcp-stdio speaks MCP on stdout.
	slog.SetDefault(slog.New(api.LogHandler(slog.NewTextHandler(os.Stderr, nil))))
	cfg, err := NewQuerierConfig(os.Args[1:])
	if err != nil {
		slog.Default().Error("load querier config", "error", err)
		os.Exit(1)
	}

	if err := run(cfg); err != nil {
		slog.Default().Error("run querier", "error", err)
		os.Exit(1)
	}
}

func run(cfg QuerierConfig) error {
	var refreshing sync.WaitGroup
	defer refreshing.Wait()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if limit := cfg.runtimeMemoryLimit(); limit > 0 {
		debug.SetMemoryLimit(limit)
	}
	budget, from := cfg.memBudget()
	slog.Default().Info("request memory budget", "bytes", budget, "from", from, "memory_limit", cfg.MemoryLimit, "max_in_flight", cfg.MaxInFlight)
	summaryCfg := cfg.Summary.Config()
	store, err := cfg.Open()
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	derived, err := plan.NewDerived(cfg.Derived, cfg.Tables)
	if err != nil {
		return err
	}
	cat := catalog.NewWithConfig(store, cfg.Tenant, catalog.Config{Retention: time.Duration(cfg.Retention), Grace: time.Duration(cfg.CatalogGrace),
		MaxAge: time.Duration(cfg.CatalogRefresh)})
	app := querier.Open(querier.Options{
		Catalog: cat, Store: store, Summary: &summaryCfg, Derived: derived, FooterReads: cfg.FooterReads,
		MaxInFlight: cfg.MaxInFlight, RequestMemBudget: budget, MCPResultTokens: cfg.MCPResultTokens,
		Definitions: cfg.Definitions.Dir, SLO: cfg.SLO.Config(cfg.Retention),
	})
	publishAnswerCache(app)

	// In the background because the first build is thousands of serial footer reads
	// and the listener must not wait for it.
	refreshing.Go(func() {
		refresh(ctx, app, cmp.Or(time.Duration(cfg.Summary.RefreshInterval), summary.DefaultRefreshInterval))
	})
	refreshing.Go(func() { reportProcess(ctx, processReportInterval) })
	refreshing.Go(func() {
		if err := app.Watch(ctx); err != nil {
			slog.Default().Error("watch definitions", "error", err)
		}
	})

	if cfg.MCPStdio {
		mcpSrv := api.NewMCPServer(api.NewServer(app.Tools(), api.Limits{}).Registry(), cfg.MCPResultTokens)
		slog.Default().Info("querier serving MCP over stdio", "root", cfg.Root)
		if err := mcpSrv.Run(ctx, &mcp.StdioTransport{}); err != nil {
			return fmt.Errorf("mcp stdio: %w", err)
		}
		return nil
	}

	httpServer := newServer(cfg, app.Handler())

	// Bound before logging, so "listening" is never followed by a failed bind.
	ln, err := net.Listen("tcp", cmp.Or(cfg.Addr, ":http"))
	if err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	slog.Default().Info("querier listening", "addr", cfg.Addr, "root", cfg.Root, "store", cmp.Or(cfg.Kind, "fs"), "tenant", cfg.Tenant)

	errChan := make(chan error, 1)
	go func() {
		if err := httpServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			errChan <- fmt.Errorf("serve: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		slog.Default().Info("shutting down", "reason", context.Cause(ctx))
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		return httpServer.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}

// newServer sets no BaseContext, since one rooted in the shutdown signal would cancel the
// requests Shutdown's grace period lets finish.
func newServer(cfg QuerierConfig, h http.Handler) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/", h)
	// Outside the in-flight limit, so a process at its limit still says what it holds.
	mux.Handle("GET /debug/vars", expvar.Handler())
	// ?gc=1 collects first, so its heap, and /debug/vars after it, hold only what is live.
	mux.Handle("GET /debug/pprof/heap", pprof.Handler("heap"))
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       time.Duration(cfg.ReadTimeout),
		WriteTimeout:      time.Duration(cfg.WriteTimeout),
		IdleTimeout:       time.Duration(cfg.IdleTimeout),
	}
}

// refresh builds the summary and counts the SLOs now and every interval until ctx is done,
// logging a failure, which costs latency rather than availability since the scan path answers.
func refresh(ctx context.Context, app querier.App, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var allocated uint64
	var answers, chunks query.CacheStats
	for {
		if err := app.Refresh(ctx); err != nil {
			slog.Default().Error("build summary", "error", err)
		}
		allocated = releaseBuildMemory(allocated)
		answers, chunks = logCaches(app, answers, chunks)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// releaseBuildMemory collects and returns to the OS the heap a summary build used when the
// process allocated more than its live heap since the bytes allocated it was last passed,
// returning the bytes allocated now. The runtime would otherwise keep a full build's peak as
// the querier's RSS at rest; a refresh that found no new segments allocates too little to pay for a collection.
func releaseBuildMemory(allocated uint64) uint64 {
	sample := []metrics.Sample{{Name: "/gc/heap/allocs:bytes"}, {Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)
	if now := sample[0].Value.Uint64(); now-allocated <= sample[1].Value.Uint64() {
		return allocated
	}
	debug.FreeOSMemory()
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}

// logCaches logs what app's caches hold and what they counted since they had counted answers and
// chunks, returning what they have counted now.
func logCaches(app querier.App, answers, chunks query.CacheStats) (nowAnswers, nowChunks query.CacheStats) {
	nowAnswers, nowChunks = app.Caches()
	slog.Default().Info("caches",
		"answer_entries", nowAnswers.Entries, "answer_bytes", nowAnswers.Bytes, "answer_cap_bytes", nowAnswers.CapBytes,
		"answer_hits", nowAnswers.Hits-answers.Hits, "answer_shared", nowAnswers.Shared-answers.Shared,
		"answer_misses", nowAnswers.Misses-answers.Misses, "answer_evictions", nowAnswers.Evictions-answers.Evictions,
		"answer_not_stored", nowAnswers.NotStored-answers.NotStored,
		"chunk_bytes", nowChunks.Bytes, "chunk_cap_bytes", nowChunks.CapBytes, "chunk_hits", nowChunks.Hits-chunks.Hits,
		"chunk_shared", nowChunks.Shared-chunks.Shared, "chunk_reads", nowChunks.Misses-chunks.Misses,
		"chunk_evictions", nowChunks.Evictions-chunks.Evictions)
	return nowAnswers, nowChunks
}

// publishAnswerCache publishes what app's answer cache holds and has counted beside heap_live_bytes.
func publishAnswerCache(app querier.App) {
	for name, stat := range map[string]func(query.CacheStats) int64{
		"answer_cache_bytes":  func(s query.CacheStats) int64 { return s.Bytes },
		"answer_cache_hits":   func(s query.CacheStats) int64 { return s.Hits },
		"answer_cache_misses": func(s query.CacheStats) int64 { return s.Misses },
	} {
		expvar.Publish(name, expvar.Func(func() any {
			answers, _ := app.Caches()
			return stat(answers)
		}))
	}
}

// The process's memory beside expvar's memstats: the heap the last collection left live, which
// unlike HeapAlloc holds no garbage, so a benchmark or an operator reads what the querier keeps.
func init() {
	expvar.Publish("heap_live_bytes", expvar.Func(func() any {
		sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
		metrics.Read(sample)
		return sample[0].Value.Uint64()
	}))
}

// processReportInterval is how often the querier logs its memory, so the climb before an
// OOM kill is in its logs.
const processReportInterval = 30 * time.Second

// reportProcess logs the heap, the memory taken from the OS and the goroutines every
// interval until ctx is done.
func reportProcess(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		slog.Default().Info("process stats", "heap_bytes", mem.HeapAlloc, "sys_bytes", mem.Sys, "goroutines", runtime.NumGoroutine(), "gc_cycles", mem.NumGC)
	}
}
