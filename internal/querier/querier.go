// Package querier builds the read side's object graph, the one place it is assembled:
// cmd/querier, the end-to-end suites and the RCA bench all open it.
package querier

import (
	"context"
	"net/http"

	"github.com/datafocal/datafocal/internal/api"
	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/definitions"
	"github.com/datafocal/datafocal/internal/llm"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/lang"
	"github.com/datafocal/datafocal/internal/query/lang/logql"
	"github.com/datafocal/datafocal/internal/query/lang/sql"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/slo"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/web"
)

// App is a querier over one corpus.
type App interface {
	// Handler serves the tools over HTTP, over MCP's streamable transport at /mcp, and
	// the UI at /ui/.
	Handler() http.Handler
	// Refresh folds the segments the catalog gained since the last call into the
	// summary, when there is one, and into the SLOs' counts.
	Refresh(ctx context.Context) error
	// Tools is the tool surface Handler serves.
	Tools() tools.Tools
	// Watch reloads the SLO definitions whenever their directory changes, until ctx is done.
	Watch(ctx context.Context) error
	// Caches reports what the tools' answer cache and the column chunk cache hold and have counted.
	Caches() (answers, chunks query.CacheStats)
}

// Options is the corpus a querier reads and the summary it builds over it.
type Options struct {
	Catalog catalog.Catalog
	Store   storage.Store
	// Summary configures the summary; nil answers every request from the segments.
	Summary *summary.Config
	// Derived is the derived columns every tool reads as if stored.
	Derived plan.Derived
	// FooterReads is the footers a plan reads at once; 0 or 1 reads them in series.
	FooterReads int
	// MaxInFlight is the requests served at once, past which one is refused 429; 0 is any.
	MaxInFlight int
	// RequestMemBudget is the bytes one request may hold decoded before it fails too_large; 0 is any.
	RequestMemBudget int64
	// MCPResultTokens is the tokens an MCP tool result holds at most; 0 is api.DefaultMCPResultTokens.
	MCPResultTokens int
	// Definitions is the directory of SLO definitions; empty defines none.
	Definitions string
	SLO         slo.Config
}

type app struct {
	handler  http.Handler
	registry *api.Registry
	reader   segment.CachingReader
	builder  summary.Builder
	tools    tools.Tools
	slos     slo.Store
}

// Open wires the graph over opts, reading its definitions now and no segment until a request
// or Refresh does.
func Open(opts Options) App {
	// Keep footers hot: pruning and boundary column reads reuse cached footers
	// instead of re-reading them from the object per call.
	rd := segment.NewFooterCache(opts.Store)
	a := app{reader: rd}
	var store summary.Store
	if opts.Summary != nil {
		a.builder = summary.NewBuilder(opts.Catalog, rd, *opts.Summary)
		store = a.builder.Store()
	}
	planner := plan.New(plan.Deps{Catalog: opts.Catalog, Reader: rd, Summary: store, Derived: opts.Derived, FooterReads: opts.FooterReads})
	scanner := scan.New(rd, opts.Derived)
	reader := series.New(planner, scanner)
	a.slos = slo.New(definitions.Open(opts.Definitions, opts.SLO.Limits()), opts.Catalog, reader, opts.SLO)
	a.tools = tools.New(tools.Deps{
		Catalog:   opts.Catalog,
		Series:    series.Logged(reader),
		Planner:   planner,
		Scanner:   scan.Logged(scanner),
		Summary:   store,
		Languages: lang.Registry{"logql": logql.New(), "sql": sql.New()},
		Store:     opts.Store,
		SLOs:      a.slos,
	})

	// One MCP handler on both /mcp and /mcp/.
	srv := api.NewServer(a.tools, api.Limits{MaxInFlight: opts.MaxInFlight, MemBudget: opts.RequestMemBudget})
	a.registry = srv.Registry()
	mcpHandler := api.NewMCPHandler(srv.Registry(), opts.MCPResultTokens)
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpHandler)
	mux.Handle("/mcp/", mcpHandler)
	mux.Handle("/ui/", http.StripPrefix("/ui", web.Handler()))
	mux.Handle("/", srv.Handler())
	a.handler = mux
	return a
}

// DefaultDerived is cost_usd and the embedded model prices, as copies a caller's config may
// decode over.
func DefaultDerived() ([]plan.DerivedColumn, map[string]map[string]string) {
	return []plan.DerivedColumn{{Name: "cost_usd", Expr: "coalesce(logged_cost_usd, input_tokens*lookup(model, price_in)/1e6 + coalesce(output_tokens, reasoning_tokens, 0)*lookup(model, price_out)/1e6 + coalesce(cache_read_tokens, 0)*lookup(model, price_cache_read, lookup(model, price_in))/1e6 + coalesce(cache_write_tokens, 0)*lookup(model, price_cache_write, 0)/1e6)"}}, llm.Tables()
}

func (a app) Handler() http.Handler {
	return a.handler
}

func (a app) Refresh(ctx context.Context) error {
	if a.builder != nil {
		if err := a.builder.Build(ctx); err != nil {
			return err
		}
	}
	return a.slos.Refresh(ctx)
}

func (a app) Watch(ctx context.Context) error {
	return a.slos.Watch(ctx)
}

func (a app) Caches() (answers, chunks query.CacheStats) {
	return a.registry.AnswerCache(), a.reader.ChunkCache()
}

func (a app) Tools() tools.Tools {
	return a.tools
}
