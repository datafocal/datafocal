package api_test

import (
	"testing"

	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/selector"
)

// TestListPatternsHTTP is an end-to-end round-trip: POST /v1/list_patterns with
// a seeded corpus returns an envelope whose Data.Patterns is non-empty and whose
// Meta.Snapshot is set.
func TestListPatternsHTTP(t *testing.T) {
	t.Parallel()

	dir, store, cat := newCorpus(t)
	for service, n := range map[string]int{"svc-a": 5, "svc-b": 3} {
		seedService(t, store, cat, service, n)
	}
	_, baseURL := serve(t, dir, store)

	env := postTool[tools.PatternsResponse](t, baseURL, "/v1/list_patterns", tools.PatternsRequest{
		Selector: selector.Selector{Time: wideWindow()},
		Sort:     "count",
		Limit:    50,
	})

	if len(env.Data.Patterns) == 0 {
		t.Fatal("expected non-empty patterns")
	}
	if env.Meta.Snapshot == "" {
		t.Error("Meta.Snapshot should be set")
	}
}

// TestMCPMatchesHTTP is acceptance criterion 3: an MCP tools/call must return the
// same envelope as the equivalent POST. Both paths go through the same Registry
// ToolDef, so any divergence here means the transport bridging is wrong.
//
// The two tools are separate subtests rather than a table because each names its
// own response type, which is a type argument and cannot be a table column.
func TestMCPMatchesHTTP(t *testing.T) {
	t.Parallel()

	t.Run("aggregate", func(t *testing.T) {
		t.Parallel()

		dir, store, cat := newCorpus(t)
		for service, n := range map[string]int{"analytics-export": 5, "db": 7, "host": 3} {
			seedService(t, store, cat, service, n)
		}
		srv, baseURL := serve(t, dir, store)

		req := tools.AggregateRequest{
			Selector: selector.Selector{Time: wideWindow()},
			Measure:  "count",
			GroupBy:  "service",
			Bins:     "auto",
		}
		httpEnv := postTool[tools.AggregateResponse](t, baseURL, "/v1/aggregate", req)
		assertMCPMatchesHTTP(t, srv, "aggregate", req, httpEnv)
	})

	t.Run("list_patterns", func(t *testing.T) {
		t.Parallel()

		dir, store, cat := newCorpus(t)
		for service, n := range map[string]int{"svc-a": 5, "svc-b": 3} {
			seedService(t, store, cat, service, n)
		}
		srv, baseURL := serve(t, dir, store)

		req := tools.PatternsRequest{
			Selector: selector.Selector{Time: wideWindow()},
			Sort:     "count",
			Limit:    50,
		}
		httpEnv := postTool[tools.PatternsResponse](t, baseURL, "/v1/list_patterns", req)
		assertMCPMatchesHTTP(t, srv, "list_patterns", req, httpEnv)
	})
}
