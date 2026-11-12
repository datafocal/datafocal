package main

import (
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/config"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/storage"
)

func TestQuerierConfig_Defaults(t *testing.T) {
	want := QuerierConfig{
		Tenant:   "local",
		Store:    config.Store{Root: "./.localdata"},
		Addr:     ":8080",
		MCPStdio: false,
		Summary: SummarySettings{
			BinWidth:        config.Duration(15 * time.Second),
			RetentionHours:  24,
			LoadWindowHours: 24,
			SketchColumns:   []string{},
			RefreshInterval: config.Duration(30 * time.Second),
		},
		FooterReads:    min(runtime.GOMAXPROCS(0), 16),
		MaxInFlight:    8,
		CatalogGrace:   config.Duration(15 * time.Minute),
		CatalogRefresh: config.Duration(5 * time.Second),
		ReadTimeout:    config.Duration(30 * time.Second),
		WriteTimeout:   config.Duration(6 * time.Minute),
		IdleTimeout:    config.Duration(2 * time.Minute),
		SLO:            SLOSettings{FineHorizon: config.Duration(24 * time.Hour), MaxSeries: 2000, MaxGroups: 500, BackfillBytes: 256 << 20},
	}
	want.Derived, want.Tables = querier.DefaultDerived()
	assert.Equal(t, want, DefaultQuerierConfig())
}

func TestQuerierConfig_FileOverridesDefaults(t *testing.T) {
	file := writeConfigFile(t, `{
		"tenant": "prod",
		"root": "/data",
		"addr": ":9000",
		"summary": {
			"bin_width": "30s",
			"retention_hours": 48,
			"prewarm_hours": 6
		}
	}`)

	cfg, err := fromFile(t, file)
	require.NoError(t, err)
	assert.Equal(t, "prod", cfg.Tenant)
	assert.Equal(t, "/data", cfg.Root)
	assert.Equal(t, ":9000", cfg.Addr)
	assert.Equal(t, config.Duration(30*time.Second), cfg.Summary.BinWidth)
	assert.Equal(t, 48, cfg.Summary.RetentionHours)
	assert.Equal(t, 6, cfg.Summary.LoadWindowHours)

	// The file said nothing about the refresh interval, so it keeps the default
	// rather than the zero value decoding a partial object could have left.
	assert.Equal(t, config.Duration(30*time.Second), cfg.Summary.RefreshInterval)
}

func TestQuerierConfig_EnvOverridesFile(t *testing.T) {
	file := writeConfigFile(t, `{
		"tenant": "file-tenant",
		"summary": {"bin_width": "30s"},
		"s3": {"endpoint": "from-file", "bucket": "segments"}
	}`)

	t.Setenv("DATAFOCAL_TENANT", "env-tenant")
	t.Setenv("DATAFOCAL_SUMMARY_BIN_WIDTH", "60s")
	t.Setenv("DATAFOCAL_S3_ENDPOINT", "minio:9000")
	t.Setenv("DATAFOCAL_DEFINITIONS_DIR", "/definitions")
	t.Setenv("DATAFOCAL_SLO_MAX_GROUPS", "50")

	cfg, err := fromFile(t, file)
	require.NoError(t, err)
	assert.Equal(t, "env-tenant", cfg.Tenant)
	assert.Equal(t, DefinitionsSettings{Dir: "/definitions"}, cfg.Definitions)
	assert.Equal(t, 50, cfg.SLO.MaxGroups)
	assert.Equal(t, config.Duration(60*time.Second), cfg.Summary.BinWidth)
	assert.Equal(t, storage.S3Config{Endpoint: "minio:9000", Bucket: "segments"}, cfg.S3)
}

func TestQuerierConfig_FlagsOverrideEnv(t *testing.T) {
	args := []string{
		"--tenant", "flag-tenant",
		"--root", "/flag/path",
		"--addr", ":7000",
		"--summary-bin-width", "45s",
		"--summary-retention-hours", "12",
		"--summary-prewarm-hours", "4",
		"--summary-refresh-interval", "15s",
		"--summary-sketch-columns", "latency,error_rate",
	}
	t.Setenv("DATAFOCAL_TENANT", "env-tenant")
	t.Setenv("DATAFOCAL_ROOT", "/env/path")

	cfg, err := NewQuerierConfig(args)
	require.NoError(t, err)
	assert.Equal(t, "flag-tenant", cfg.Tenant)
	assert.Equal(t, "/flag/path", cfg.Root)
	assert.Equal(t, ":7000", cfg.Addr)
	assert.Equal(t, config.Duration(45*time.Second), cfg.Summary.BinWidth)
	assert.Equal(t, 12, cfg.Summary.RetentionHours)
	assert.Equal(t, 4, cfg.Summary.LoadWindowHours)
	assert.Equal(t, config.Duration(15*time.Second), cfg.Summary.RefreshInterval)
	assert.Equal(t, []string{"error_rate", "latency"}, cfg.Summary.SketchColumns)
}

// TestQuerierConfig_RetentionBoundsTheSummary: the one retention setting is also how long
// the summary keeps points, and how far back its first build reaches at most.
func TestQuerierConfig_RetentionBoundsTheSummary(t *testing.T) {
	cfg, err := NewQuerierConfig([]string{"--retention", "48h", "--summary-retention-hours", "72", "--summary-prewarm-hours", "72"})
	require.NoError(t, err)
	assert.Equal(t, config.Duration(48*time.Hour), cfg.Retention)
	assert.Equal(t, 48, cfg.Summary.RetentionHours)
	assert.Equal(t, 48, cfg.Summary.LoadWindowHours)
}

// TestQuerierConfig_MCPStdioEnvOverridesFile: an operator setting
// DATAFOCAL_MCP_STDIO=false must beat a file that turned it on, even though false
// is also the field's zero value.
func TestQuerierConfig_MCPStdioEnvOverridesFile(t *testing.T) {
	file := writeConfigFile(t, `{"mcp_stdio": true}`)

	t.Setenv("DATAFOCAL_MCP_STDIO", "false")

	cfg, err := fromFile(t, file)
	require.NoError(t, err)
	assert.False(t, cfg.MCPStdio)
}

func TestQuerierConfig_SketchColumnsParsesList(t *testing.T) {
	cfg, err := NewQuerierConfig([]string{"--summary-sketch-columns", "latency,distinct_traces,error_rate"})
	require.NoError(t, err)
	assert.Equal(t, []string{"distinct_traces", "error_rate", "latency"}, cfg.Summary.SketchColumns)
}

// TestQuerierConfig_ValidateFails_NonPositiveSummary: a nonsense summary setting has
// to be refused at startup, and the message has to come from summary.Config.Validate
// — that is what makes this config layer a way of stating the values rather than a
// second opinion on which ones are legal. The wants are phrased in durations and
// bytes for the same reason: hours are this file's spelling, not the store's.
// TestQuerierConfig_DerivedDefaults prices Claude Code's tokens as cost_usd with no config,
// from embedded tables that compile.
func TestQuerierConfig_DerivedDefaults(t *testing.T) {
	cfg := DefaultQuerierConfig()
	assert.Equal(t, []string{"cost_usd"}, derivedNames(cfg.Derived))
	for _, table := range []string{"price_in", "price_out", "price_cache_read", "price_cache_write"} {
		assert.NotEmpty(t, cfg.Tables[table], table)
	}
	_, err := plan.NewDerived(cfg.Derived, cfg.Tables)
	require.NoError(t, err)
}

// TestQuerierConfig_DerivedFromFile merges a file's derived columns over the defaults by name
// and replaces the default table a file names, and refuses an expression naming a table
// neither holds.
func TestQuerierConfig_DerivedFromFile(t *testing.T) {
	file := writeConfigFile(t, `{
		"derived": [{"name": "gb", "expr": "bytes/1e9"}, {"name": "cost_usd", "expr": "tokens/1e6*lookup(model, price_in)"}],
		"tables": {"price_in": {"opus": "15"}}
	}`)
	cfg, err := fromFile(t, file)
	require.NoError(t, err)
	assert.Equal(t, []plan.DerivedColumn{
		{Name: "gb", Expr: "bytes/1e9"},
		{Name: "cost_usd", Expr: "tokens/1e6*lookup(model, price_in)"},
	}, cfg.Derived)
	assert.Equal(t, map[string]string{"opus": "15"}, cfg.Tables["price_in"])
	assert.Equal(t, DefaultQuerierConfig().Tables["price_out"], cfg.Tables["price_out"])

	added := writeConfigFile(t, `{"derived": [{"name": "gb", "expr": "bytes/1e9"}]}`)
	cfg, err = fromFile(t, added)
	require.NoError(t, err)
	assert.Equal(t, []string{"gb", "cost_usd"}, derivedNames(cfg.Derived))

	missing := writeConfigFile(t, `{"derived": [{"name": "tier", "expr": "lookup(model, tiers)"}]}`)
	_, err = fromFile(t, missing)
	assert.ErrorContains(t, err, `derived column tier: unknown lookup table "tiers"`)
}

func derivedNames(defs []plan.DerivedColumn) []string {
	names := make([]string, len(defs))
	for i, d := range defs {
		names[i] = d.Name
	}
	return names
}

func TestQuerierConfig_ValidateFails_NonPositiveSummary(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "bin width", args: []string{"--summary-bin-width", "-1s"}, want: "summary: bin width -1s must be positive"},
		{name: "retention hours", args: []string{"--summary-retention-hours", "-1"}, want: "summary: retention -1h0m0s must be positive"},
		{name: "prewarm hours", args: []string{"--summary-prewarm-hours", "-1"}, want: "summary: prewarm -1h0m0s must be positive"},
		{name: "refresh interval", args: []string{"--summary-refresh-interval", "-1s"}, want: "summary: refresh interval -1s must be positive"},
		{
			name: "prewarm beyond retention",
			args: []string{"--summary-retention-hours", "6", "--summary-prewarm-hours", "7"},
			want: "summary: prewarm 7h0m0s exceeds retention 6h0m0s",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewQuerierConfig(tt.args)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

// TestQuerierConfig_MissingConfigFile_OK: the default config file may be absent.
func TestQuerierConfig_MissingConfigFile_OK(t *testing.T) {
	cfg, err := NewQuerierConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, "local", cfg.Tenant)
}

func TestQuerierConfig_ExplicitMissingConfigFile_Error(t *testing.T) {
	_, err := fromFile(t, "/nonexistent/file")
	require.Error(t, err)
}

func TestQuerierConfig_DefaultsPreservedByNewQuerierConfig(t *testing.T) {
	cfg, err := NewQuerierConfig(nil)
	require.NoError(t, err)
	assert.Equal(t, DefaultQuerierConfig(), cfg, "no env, flag, or file may disturb the defaults")
}

func TestQuerierConfig_EnvOverridesFile_WhenEnvEqualsDefault(t *testing.T) {
	file := writeConfigFile(t, `{"summary": {"bin_width": "30s"}}`)

	t.Setenv("DATAFOCAL_SUMMARY_BIN_WIDTH", "15s")

	cfg, err := fromFile(t, file)
	require.NoError(t, err)
	assert.Equal(t, config.Duration(15*time.Second), cfg.Summary.BinWidth,
		"env layer must win over file even when env value equals the default")
}

// TestQuerierConfig_FileStatedZeroMeansUnset: zero is how the store spells "unset"
// (summary.Config.Validate), so a file stating one is carried through as zero and the
// store substitutes its own default — not refused, and not silently rewritten to 24
// here. A negative value is the one a caller can only have got wrong, and that is what
// startup rejects.
func TestQuerierConfig_FileStatedZeroMeansUnset(t *testing.T) {
	file := writeConfigFile(t, `{"summary": {"retention_hours": 0}}`)

	cfg, err := fromFile(t, file)
	require.NoError(t, err)
	assert.Zero(t, cfg.Summary.Config().Retention)
}

// TestQuerierConfig_FileBadValues_Error: a value of the wrong shape fails startup
// rather than being skipped. Both halves matter — the duration goes through
// Duration.UnmarshalText and the number through encoding/json — and the old
// loader's type assertions dropped either one on the floor.
func TestQuerierConfig_FileBadValues_Error(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{name: "unparseable duration", body: `{"summary": {"bin_width": "1 hour"}}`, want: `parse duration "1 hour"`},
		{name: "number as string", body: `{"summary": {"retention_hours": "12"}}`, want: "cannot unmarshal string"},
		{name: "duration as number", body: `{"summary": {"bin_width": 45}}`, want: "cannot unmarshal number"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := writeConfigFile(t, tt.body)

			_, err := fromFile(t, file)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

// fromFile is NewQuerierConfig over the config file at path, named as an operator names one.
func fromFile(t *testing.T, path string) (QuerierConfig, error) {
	t.Setenv("DATAFOCAL_CONFIG_FILE", path)
	return NewQuerierConfig(nil)
}

func writeConfigFile(t *testing.T, content string) string {
	f, err := os.CreateTemp("", "querier-config-*.json")
	require.NoError(t, err)
	defer f.Close()

	_, err = f.WriteString(content)
	require.NoError(t, err)

	return f.Name()
}

// TestQuerierConfig_MemBudget: request_mem_budget wins; otherwise the runtime's memory limit
// less the footer cache and the summary's share is split among the requests in flight, with
// a floor, and without a limit a request gets the default.
func TestQuerierConfig_MemBudget(t *testing.T) {
	tests := []struct {
		name     string
		limit    int64
		explicit int64
		want     int64
		wantFrom string
	}{
		{"the 3Gi deployment", 3 << 30, 0, (3<<30/100*85 - 256<<20 - 128<<20 - 8*16<<20 - 3<<30/100*85/4) / 8, "memory_limit"},
		{"no limit", 0, 0, 224 << 20, "default, no memory_limit"},
		{"set over a limit", 3 << 30, 64 << 20, 64 << 20, "request_mem_budget"},
		{"a limit below the footer cache", 256 << 20, 0, 32 << 20, "memory_limit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultQuerierConfig()
			cfg.MemoryLimit, cfg.RequestMemBudget = tt.limit, tt.explicit
			got, from := cfg.memBudget()
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantFrom, from)
		})
	}
}
