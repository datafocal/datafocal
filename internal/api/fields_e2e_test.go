package api_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

// TestRankFieldsEndToEnd seeds a corpus with high-signal fields (host, deploy_version
// with multiple distinct values) and low-signal fields (level always 'info'), calls
// POST /v1/rank_fields, and asserts the golden ranking invariant: high-cardinality
// fields rank above low-cardinality constant fields.
func TestRankFieldsEndToEnd(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	seedFieldsCorpus(t, store, cat)
	_, baseURL := serve(t, dir, store)

	env := postTool[tools.FieldsResponse](t, baseURL, "/v1/rank_fields", tools.FieldsRequest{
		Selector: selector.Selector{Time: wideWindow()},
	})

	fieldsByName := make(map[string]tools.FieldStat)
	for _, fs := range env.Data.Fields {
		fieldsByName[fs.Name] = fs
	}

	level, levelOk := fieldsByName["level"]
	if !levelOk {
		t.Fatalf("field 'level' not found in response: %v", env.Data.Fields)
	}
	// level is constant across the corpus, so it sits at the signal floor and is
	// the yardstick both rotating fields must clear.
	if level.HighSignal {
		t.Errorf("level.high_signal = true, want false (signal %f)", level.Signal)
	}
	for _, name := range []string{"host", "deploy_version"} {
		fs, ok := fieldsByName[name]
		if !ok {
			t.Errorf("field %q not found in response: %v", name, env.Data.Fields)
			continue
		}
		if fs.Signal <= level.Signal {
			t.Errorf("%s signal %f should be > level signal %f", name, fs.Signal, level.Signal)
		}
		if !fs.HighSignal {
			t.Errorf("%s.high_signal = false, want true (cardinality %d, signal %f)", name, fs.Cardinality, fs.Signal)
		}
	}

	if !env.Meta.Approx {
		t.Errorf("meta.approx = false, want true (sketches used for cardinality)")
	}
	if env.Meta.Coverage != 1.0 {
		t.Errorf("meta.coverage = %v, want 1.0", env.Meta.Coverage)
	}
	if env.Meta.Snapshot == "" {
		t.Errorf("meta.snapshot is empty, want non-empty")
	}
}

// seedFieldsCorpus seeds three services whose host rotates over 5 values and
// deploy_version over 4, while level stays 'info' — so the first two are
// high-signal and level is the low-signal control.
func seedFieldsCorpus(t *testing.T, store storage.Store, cat catalog.Catalog) {
	t.Helper()
	for _, service := range []string{"api", "db", "cache"} {
		events := make([]segment.Record, 21)
		for i := range events {
			host := fmt.Sprintf("host-%d", i%5)
			events[i] = segment.LogRecord(seedStart.Add(time.Duration(i)*time.Second), "info", fmt.Sprintf("event %d for %s on %s", i, service, host), map[string]string{
				"instance":       service + "-0",
				"host":           host,
				"deploy_version": fmt.Sprintf("v1.%d", i%4),
			})
		}
		seedEvents(t, store, cat, service, events)
	}
}

// TestDescribeSchemaEndToEnd pins describe_schema over the fields corpus: every column with
// its type, kind and cardinality, the one profile that wrote all three segments, footer reads
// only, and the same data again at the same snapshot.
func TestDescribeSchemaEndToEnd(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	seedFieldsCorpus(t, store, cat)
	srv, baseURL := serve(t, dir, store)

	req := tools.FieldsRequest{Selector: selector.Selector{Time: wideWindow()}}
	env := postTool[tools.DescribeSchemaResponse](t, baseURL, "/v1/describe_schema", req)

	want := tools.DescribeSchemaResponse{
		Columns: []tools.SchemaColumn{
			{Name: "deploy_version", Type: "string", Kind: "enum", Cardinality: 4},
			{Name: "host", Type: "string", Kind: "enum", Cardinality: 5},
			{Name: "instance", Type: "string", Kind: "enum", Cardinality: 3},
			{Name: "level", Type: "string", Kind: "enum", Cardinality: 1},
			{Name: "message", Type: "string", Kind: "str", Cardinality: 63},
			{Name: "service", Type: "string", Kind: "enum", Cardinality: 3},
		},
		Producers: []tools.Producer{{Profile: "auto", Version: 1, Degradation: "none", Segments: 3}},
	}
	assert.Equal(t, want, env.Data)
	// Each GET is one of the three footers, so no data was read.
	assert.Equal(t, query.Cost{GETs: 3, FootersDecoded: 3, Segments: 3}, env.Meta.Cost)
	assertMCPMatchesHTTP(t, srv, "describe_schema", req, env)

	req.Snapshot = env.Meta.Snapshot
	again := postTool[tools.DescribeSchemaResponse](t, baseURL, "/v1/describe_schema", req)
	first, err := json.Marshal(env.Data)
	require.NoError(t, err)
	second, err := json.Marshal(again.Data)
	require.NoError(t, err)
	assert.Equal(t, string(first), string(second))
}
