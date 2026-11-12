package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/datafocal/datafocal/internal/api"
	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// Shared fixtures for the end-to-end tests in this package. Each test seeds a
// real corpus through a real collector into an fs store and fs catalog, serves
// internal/api over it, and drives the tool through a transport — so the whole
// vertical slice is under test, not a hand-assembled stand-in for it. seedEvents
// and serve are the single place that scaffolding lives.

const testTenant = "acme"

// seedStart anchors every seeded corpus; wideWindow spans it.
var seedStart = time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)

// fixedClock is a settable event-time clock so the collector rolls exactly one
// segment per service (no age-based flush within the seeded span).
type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }

// newCorpus returns an empty corpus directory and the fs store and catalog
// writer over it. The store doubles as the read seam that serve takes.
func newCorpus(t *testing.T) (string, storage.Store, catalog.Catalog) {
	t.Helper()
	dir := t.TempDir()
	return dir, storage.NewFS(dir), catalog.New(storage.NewFS(dir), testTenant)
}

// serve opens a querier over the corpus in dir plus an httptest listener for it,
// returning the URL for the HTTP leg and a Server over the same tools for the MCP leg.
func serve(t *testing.T, dir string, store storage.Store) (*api.Server, string) {
	t.Helper()
	return serveWithSummary(t, dir, store, nil)
}

// serveWithSummary is serve with a summary built over the corpus first, the path
// cmd/querier takes at startup, for the tools that need the hot artifacts.
func serveWithSummary(t *testing.T, dir string, store storage.Store, cfg *summary.Config) (*api.Server, string) {
	t.Helper()
	open := func() querier.App {
		app := querier.Open(querier.Options{Catalog: catalog.New(storage.NewFS(dir), testTenant), Store: store, Summary: cfg})
		if err := app.Refresh(context.Background()); err != nil {
			t.Fatalf("build summary: %v", err)
		}
		return app
	}
	ts := httptest.NewServer(open().Handler())
	t.Cleanup(ts.Close)
	// A querier of its own, so neither leg warms the footer cache the other bills against.
	return api.NewServer(open().Tools(), api.Limits{}), ts.URL
}

// warm is the summary the version gate serves this corpus from.
var warm = &summary.Config{}

// wideWindow spans an hour either side of seedStart, so every seeded row falls
// inside it and the window itself never decides a test's outcome.
func wideWindow() selector.TimeSpec {
	return selector.TimeSpec{
		From: seedStart.Add(-time.Hour).Format(time.RFC3339),
		To:   seedStart.Add(time.Hour).Format(time.RFC3339),
	}
}

// seedEvents drives events for one service through a real collector and flushes
// them as a single sealed segment. Every implicit flush trigger is raised out of
// reach so the row count a test asks for is the row count the segment gets.
func seedEvents(t *testing.T, store storage.Store, cat catalog.Catalog, service string, events []segment.Record) {
	t.Helper()
	seedTenantEvents(t, store, cat, testTenant, service, events)
}

// seedTenantEvents is seedEvents for a tenant other than testTenant.
func seedTenantEvents(t *testing.T, store storage.Store, cat catalog.Catalog, tenant, service string, events []segment.Record) {
	t.Helper()
	cfg := collector.DefaultConfig()
	cfg.Tenant = tenant
	cfg.Stream = collector.StreamConfig{Service: service, Labels: map[string]string{"instance": service + "-0"}}
	cfg.FlushInterval = time.Hour
	cfg.FlushBytes = 1 << 40
	cfg.MaxRows = 1 << 30

	clock := &fixedClock{now: seedStart}
	c, err := collector.New(cfg, collector.Dependencies{
		Store:         store,
		CatalogWriter: cat,
		Clock:         clock,
	})
	ctx := context.Background()
	for i := 0; err == nil && i < len(events); i++ {
		clock.now = events[i].Timestamp
		_, err = c.Collect(ctx, events[i])
	}
	if err == nil {
		_, err = c.Flush(ctx)
	}
	if err != nil {
		t.Fatalf("seed %q: %v", service, err)
	}
}

// requestEvents builds n synthetic request events one second apart from
// seedStart, each carrying attrs plus the service's instance label.
func requestEvents(service string, n int, attrs map[string]string) []segment.Record {
	events := make([]segment.Record, n)
	for i := range events {
		evAttrs := make(map[string]string, len(attrs)+1)
		for k, v := range attrs {
			evAttrs[k] = v
		}
		evAttrs["instance"] = service + "-0"
		events[i] = segment.LogRecord(seedStart.Add(time.Duration(i)*time.Second), "info", fmt.Sprintf("handled request %d in %d ms", i, 10+i%20), evAttrs)
	}
	return events
}

// seedService seeds one segment of n request rows for service.
func seedService(t *testing.T, store storage.Store, cat catalog.Catalog, service string, n int) {
	t.Helper()
	seedEvents(t, store, cat, service, requestEvents(service, n, nil))
}

// seedServiceWithAttrs is seedService with attrs set on every row.
func seedServiceWithAttrs(t *testing.T, store storage.Store, cat catalog.Catalog, service string, n int, attrs map[string]string) {
	t.Helper()
	seedEvents(t, store, cat, service, requestEvents(service, n, attrs))
}

// postTool POSTs req to a tool endpoint and decodes its envelope. Every tool
// shares this request shape, so the response type is the only thing that varies
// and is the one type parameter callers name.
func postTool[Resp any](t *testing.T, baseURL, path string, req any) query.Envelope[Resp] {
	t.Helper()
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	resp, err := http.Post(baseURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s status = %d, want 200", path, resp.StatusCode)
	}
	var env query.Envelope[Resp]
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatalf("decode %s response: %v", path, err)
	}
	return env
}

// assertMCPMatchesHTTP calls tool over an in-memory MCP transport with the same
// request the HTTP leg already answered in httpEnv, and fails if the two
// envelopes do not marshal to identical bytes. Both transports dispatch the same
// registry ToolDef, so a divergence here is a transport-bridging bug.
func assertMCPMatchesHTTP[Resp any](t *testing.T, srv *api.Server, tool string, req any, httpEnv query.Envelope[Resp]) {
	t.Helper()
	ctx := context.Background()

	st, ct := mcp.NewInMemoryTransports()
	ss, err := api.NewMCPServer(srv.Registry(), 0).Connect(ctx, st, nil)
	if err != nil {
		t.Fatalf("mcp server connect: %v", err)
	}
	defer func() {
		if err := ss.Close(); err != nil {
			t.Errorf("close server session: %v", err)
		}
	}()

	client := mcp.NewClient(&mcp.Implementation{Name: "golden-test"}, nil)
	cs, err := client.Connect(ctx, ct, nil)
	if err != nil {
		t.Fatalf("mcp client connect: %v", err)
	}
	defer func() {
		if err := cs.Close(); err != nil {
			t.Errorf("close client session: %v", err)
		}
	}()

	args, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal args: %v", err)
	}
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: json.RawMessage(args)})
	if err != nil {
		t.Fatalf("CallTool %s: %v", tool, err)
	}
	if res.IsError {
		t.Fatalf("MCP %s returned IsError=true: %+v", tool, res.Content)
	}
	if len(res.Content) != 1 {
		t.Fatalf("MCP content blocks = %d, want 1", len(res.Content))
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content type = %T, want *mcp.TextContent", res.Content[0])
	}

	var mcpEnv query.Envelope[Resp]
	if err := json.Unmarshal([]byte(tc.Text), &mcpEnv); err != nil {
		t.Fatalf("decode MCP envelope: %v", err)
	}
	httpJSON, err := json.Marshal(httpEnv)
	if err != nil {
		t.Fatalf("marshal http envelope: %v", err)
	}
	mcpJSON, err := json.Marshal(mcpEnv)
	if err != nil {
		t.Fatalf("marshal mcp envelope: %v", err)
	}
	if !bytes.Equal(httpJSON, mcpJSON) {
		t.Errorf("HTTP vs MCP %s envelope diverged\n--- http ---\n%s\n--- mcp ---\n%s", tool, httpJSON, mcpJSON)
	}
}
