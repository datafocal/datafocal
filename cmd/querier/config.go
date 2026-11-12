package main

import (
	"errors"
	"flag"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/config"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/slo"
	"github.com/datafocal/datafocal/internal/summary"
)

const (
	defaultTenant     = "local"
	defaultRoot       = "./.localdata"
	defaultAddr       = ":8080"
	defaultConfigPath = "/etc/config/querier.json"
)

// QuerierConfig is the querier's whole operator surface, its json and envconfig tags the
// file and environment layers config.Load decodes onto the defaults.
type QuerierConfig struct {
	Tenant string `json:"tenant" envconfig:"TENANT"`
	config.Store
	Addr     string          `json:"addr" envconfig:"ADDR"`
	MCPStdio bool            `json:"mcp_stdio" envconfig:"MCP_STDIO"`
	Summary  SummarySettings `json:"summary"`
	// Retention is how long after its last row a segment is read, matching the bucket's
	// lifecycle rule, and replaces the summary's retention; 0 reads every segment.
	Retention config.Duration `json:"retention" envconfig:"RETENTION"`
	// CatalogGrace is how long a pinned catalog version keeps answering once newer ones are read.
	CatalogGrace config.Duration `json:"catalog_grace" envconfig:"CATALOG_GRACE"`
	// CatalogRefresh is how often a request reads the catalog's new commits; one in between
	// answers from the last read, so new segments are queryable within it.
	CatalogRefresh config.Duration `json:"catalog_refresh" envconfig:"CATALOG_REFRESH"`
	// FooterReads is the segment footers a query reads at once; 0 or 1 reads them in series.
	FooterReads int `json:"footer_reads" envconfig:"FOOTER_READS"`
	// As querier.Options, except that a RequestMemBudget of 0 is derived by memBudget.
	MaxInFlight      int   `json:"max_in_flight" envconfig:"MAX_IN_FLIGHT"`
	RequestMemBudget int64 `json:"request_mem_budget" envconfig:"REQUEST_MEM_BUDGET"`
	// MemoryLimit is the container's in bytes, 85% of which the runtime keeps under; 0 is none.
	MemoryLimit  int64           `json:"memory_limit" envconfig:"MEMORY_LIMIT"`
	ReadTimeout  config.Duration `json:"read_timeout" envconfig:"READ_TIMEOUT"`
	WriteTimeout config.Duration `json:"write_timeout" envconfig:"WRITE_TIMEOUT"`
	IdleTimeout  config.Duration `json:"idle_timeout" envconfig:"IDLE_TIMEOUT"`
	// MCPResultTokens is as querier.Options'.
	MCPResultTokens int `json:"mcp_result_tokens" envconfig:"MCP_RESULT_TOKENS"`
	// Derived are columns every tool reads as if stored, computed per row from stored
	// columns and the lookup Tables, such as a cost from token counts and a price table.
	// A file's derived columns are merged over the defaults by name, and its tables
	// replace the default table of the same name.
	Derived []plan.DerivedColumn         `json:"derived" ignored:"true"`
	Tables  map[string]map[string]string `json:"tables" ignored:"true"`
	// Definitions is where the SLO definitions are read from and saved to.
	Definitions DefinitionsSettings `json:"definitions"`
	SLO         SLOSettings         `json:"slo"`
}

// DefinitionsSettings is the definitions directory; an empty Dir defines no SLOs.
type DefinitionsSettings struct {
	Dir string `json:"dir" envconfig:"DIR"`
}

// SLOSettings is the operator-facing spelling of slo.Config.
type SLOSettings struct {
	FineHorizon   config.Duration `json:"fine_horizon" envconfig:"FINE_HORIZON"`
	MaxSeries     int             `json:"max_series" envconfig:"MAX_SERIES"`
	MaxGroups     int             `json:"max_groups" envconfig:"MAX_GROUPS"`
	BackfillBytes int64           `json:"backfill_bytes" envconfig:"BACKFILL_BYTES"`
}

// Config resolves the settings into the SLO store's own config, bounded by retention.
func (s SLOSettings) Config(retention config.Duration) slo.Config {
	return slo.Config{Retention: time.Duration(retention), FineHorizon: time.Duration(s.FineHorizon),
		MaxSeries: s.MaxSeries, MaxGroups: s.MaxGroups, BackfillBytes: s.BackfillBytes}
}

func DefaultQuerierConfig() QuerierConfig {
	cfg := QuerierConfig{
		Tenant:  defaultTenant,
		Store:   config.Store{Root: defaultRoot},
		Addr:    defaultAddr,
		Summary: defaultSummarySettings(),
		// Footer reads wait on the store, so a few beyond the cores still help; 16 caps
		// the requests one query puts in flight.
		FooterReads:    min(runtime.GOMAXPROCS(0), 16),
		MaxInFlight:    8,
		CatalogGrace:   config.Duration(catalog.DefaultGrace),
		CatalogRefresh: config.Duration(5 * time.Second),
		ReadTimeout:    config.Duration(30 * time.Second),
		// A search_logs follow clears its own, so it streams past this.
		WriteTimeout: config.Duration(6 * time.Minute),
		IdleTimeout:  config.Duration(2 * time.Minute),
		SLO: SLOSettings{FineHorizon: config.Duration(slo.DefaultFineHorizon), MaxSeries: slo.DefaultMaxSeries,
			MaxGroups: slo.DefaultMaxGroups, BackfillBytes: slo.DefaultBackfillBytes},
	}
	cfg.Derived, cfg.Tables = querier.DefaultDerived()
	return cfg
}

// NewQuerierConfig layers the config file, the environment (DATAFOCAL_*) and args over the
// defaults, and validates the result.
func NewQuerierConfig(args []string) (QuerierConfig, error) {
	cfg := DefaultQuerierConfig()
	// Cloned, since decoding a list reuses its backing array.
	below := slices.Clone(cfg.Derived)
	flags := flag.NewFlagSet("querier", flag.ContinueOnError)
	cfg.bind(flags)
	if err := config.Load(&cfg, defaultConfigPath, flags, args); err != nil {
		return QuerierConfig{}, err
	}
	cfg.Derived = mergeDerived(below, cfg.Derived)
	if hours := int(time.Duration(cfg.Retention) / time.Hour); hours > 0 {
		cfg.Summary.RetentionHours = hours
		cfg.Summary.LoadWindowHours = min(cfg.Summary.LoadWindowHours, hours)
	}
	if err := cfg.Validate(); err != nil {
		return QuerierConfig{}, err
	}
	return cfg, nil
}

// bind registers a flag for each setting an operator overrides at the command line.
func (cfg *QuerierConfig) bind(flags *flag.FlagSet) {
	flags.StringVar(&cfg.Tenant, "tenant", cfg.Tenant, "tenant whose segments are read")
	cfg.Bind(flags)
	flags.StringVar(&cfg.Addr, "addr", cfg.Addr, "address to listen on")
	flags.BoolVar(&cfg.MCPStdio, "mcp-stdio", cfg.MCPStdio, "serve the MCP surface over stdio instead of starting the HTTP server")
	flags.Var(&cfg.Summary.BinWidth, "summary-bin-width", "summary bin width, such as 15s")
	flags.IntVar(&cfg.Summary.RetentionHours, "summary-retention-hours", cfg.Summary.RetentionHours, "hours of summary points to keep in steady state")
	flags.IntVar(&cfg.Summary.LoadWindowHours, "summary-prewarm-hours", cfg.Summary.LoadWindowHours, "hours of summary points the first build reaches back for")
	flags.Func("summary-sketch-columns", "comma-separated list of sketch columns to retain", func(v string) error {
		cfg.Summary.SketchColumns = parseSketchColumns(v)
		return nil
	})
	flags.Var(&cfg.Summary.RefreshInterval, "summary-refresh-interval", "interval to refresh the summary store, such as 30s")
	flags.IntVar(&cfg.FooterReads, "footer-reads", cfg.FooterReads, "segment footers a query reads at once; 0 or 1 reads them in series")
	flags.Var(&cfg.Retention, "retention", "how long after its last row a segment is read, such as 720h; 0 reads every segment")
}

const (
	// summaryShare is the part of the runtime's memory limit kept for the summary, the
	// filter cache and garbage not yet collected, the summary having no bound of its own:
	// it and the footers it read held 11 MB over five copies of the seeded logs and 66 MB
	// over the 1,100-segment wide scenario.
	summaryShare = 4
	// defaultMemBudget is a request's without a memory limit to derive one from, near the
	// 213 MiB a 3Gi querier derives.
	defaultMemBudget = 224 << 20
	minMemBudget     = 32 << 20
)

// runtimeMemoryLimit is the 85% of the container's limit the runtime keeps under, the rest
// for what it does not count, such as stacks and the OS; 0 is none.
func (cfg QuerierConfig) runtimeMemoryLimit() int64 { return cfg.MemoryLimit / 100 * 85 }

// memBudget is the bytes one request may hold decoded and what set it: request_mem_budget,
// or the runtime's memory limit less the footer and chunk caches' caps and summaryShare, shared by
// MaxInFlight requests, or defaultMemBudget without a limit.
func (cfg QuerierConfig) memBudget() (int64, string) {
	limit := cfg.runtimeMemoryLimit()
	switch {
	case cfg.RequestMemBudget > 0:
		return cfg.RequestMemBudget, "request_mem_budget"
	case limit <= 0:
		return defaultMemBudget, "default, no memory_limit"
	}
	held := segment.FooterCacheBytes + segment.ChunkCacheBytes + int64(max(cfg.MaxInFlight, 1))*segment.ChunkStageBytes + limit/summaryShare
	return max((limit-held)/int64(max(cfg.MaxInFlight, 1)), minMemBudget), "memory_limit"
}

// mergeDerived is file's derived columns and every one of below that file does not name,
// since decoding a list replaces it whole and a file adding a column should not drop cost_usd.
func mergeDerived(below, file []plan.DerivedColumn) []plan.DerivedColumn {
	merged := slices.Clone(file)
	for _, d := range below {
		if !slices.ContainsFunc(file, func(f plan.DerivedColumn) bool { return f.Name == d.Name }) {
			merged = append(merged, d)
		}
	}
	return merged
}

// Validate checks that the config is well-formed. What makes a summary setting
// usable is the summary package's to say, so the whole block is handed to it rather
// than re-checked here in hours — that is how a bad value gets one answer whichever
// binary reads the file.
func (cfg QuerierConfig) Validate() error {
	var errs []error
	if cfg.Tenant == "" {
		errs = append(errs, errors.New("tenant is required"))
	}
	if cfg.Addr == "" {
		errs = append(errs, errors.New("addr is required"))
	}
	errs = append(errs, cfg.Summary.Config().Validate())
	_, err := plan.NewDerived(cfg.Derived, cfg.Tables)
	errs = append(errs, err)
	return errors.Join(errs...)
}

func parseSketchColumns(value string) []string {
	if strings.TrimSpace(value) == "" {
		return []string{}
	}
	var cols []string
	for col := range strings.SplitSeq(value, ",") {
		if col = strings.TrimSpace(col); col != "" {
			cols = append(cols, col)
		}
	}
	slices.Sort(cols)
	cols = slices.Compact(cols)
	return cols
}

// SummarySettings is the operator-facing spelling of summary.Config, with the
// retention and load-window spans in whole hours, the unit an operator chooses.
type SummarySettings struct {
	BinWidth config.Duration `json:"bin_width" envconfig:"BIN_WIDTH"`
	// RetentionHours is how far back the store keeps points in steady state.
	RetentionHours int `json:"retention_hours" envconfig:"RETENTION_HOURS"`
	// LoadWindowHours is how far back the first build reaches, at most RetentionHours.
	LoadWindowHours int             `json:"prewarm_hours" envconfig:"PREWARM_HOURS"`
	SketchColumns   []string        `json:"sketch_columns" envconfig:"SKETCH_COLUMNS"`
	RefreshInterval config.Duration `json:"refresh_interval" envconfig:"REFRESH_INTERVAL"`
}

// defaultSummarySettings is the block a querier told nothing builds, its load window
// the whole retention window so lowering it is opt-in.
func defaultSummarySettings() SummarySettings {
	hours := int(summary.DefaultRetention / time.Hour)
	return SummarySettings{
		BinWidth:        config.Duration(summary.DefaultBinWidth),
		RetentionHours:  hours,
		LoadWindowHours: hours,
		SketchColumns:   []string{},
		RefreshInterval: config.Duration(summary.DefaultRefreshInterval),
	}
}

// Config resolves the settings into the summary's own config.
func (s SummarySettings) Config() summary.Config {
	return summary.Config{
		BinWidth:        time.Duration(s.BinWidth),
		Retention:       time.Duration(s.RetentionHours) * time.Hour,
		LoadWindow:      time.Duration(s.LoadWindowHours) * time.Hour,
		SketchColumns:   s.SketchColumns,
		RefreshInterval: time.Duration(s.RefreshInterval),
	}
}
