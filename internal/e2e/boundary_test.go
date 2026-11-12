package e2e

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/selector"
)

// TestHealthyWindowBoundary is the acceptance criterion "healthy window must
// not return incident data": aggregate, list_patterns, and rank_fields over
// the healthy window must not surface the incident's characteristic services
// or pattern shapes.
func TestHealthyWindowBoundary(t *testing.T) {
	h := newHarness(t, true)

	t.Run("aggregate_no_db_activity_in_healthy_window", func(t *testing.T) {
		req := tools.AggregateRequest{
			Selector: withPredicates(healthySelector(),
				selector.Predicate{Field: "service", Op: selector.OpEq, Value: "db"}),
			Measure: "count",
			GroupBy: "service",
		}
		env := postTool[tools.AggregateResponse](t, h, "aggregate", req)
		// db is an incident-only service in the seed. Pruning drops it at the
		// stream level; the healthy window returns no series for it.
		for _, s := range env.Data.Series {
			assert.NotEqualf(t, "db", s.Key,
				"healthy window must not surface a db series; got Series[%q] with %d bins",
				s.Key, len(s.Values))
		}
	})

	t.Run("list_patterns_no_incident_patterns", func(t *testing.T) {
		req := tools.PatternsRequest{Selector: healthySelector(), Limit: 200}
		env := postTool[tools.PatternsResponse](t, h, "list_patterns", req)
		require.NotEmpty(t, env.Data.Patterns, "healthy window must have its own patterns")
		// The incident-only services are absent from the healthy window; the
		// dominant service for any surfaced pattern must be one of the healthy
		// services.
		healthyServices := map[string]bool{"recommendations": true}
		for _, p := range env.Data.Patterns {
			assert.Truef(t, healthyServices[p.Service],
				"healthy-window pattern %q surfaced from unexpected service %q",
				p.PatternID, p.Service)
		}
	})

	t.Run("rank_fields_no_incident_ids", func(t *testing.T) {
		req := tools.FieldsRequest{Selector: healthySelector()}
		env := postTool[tools.FieldsResponse](t, h, "rank_fields", req)
		require.NotEmpty(t, env.Data.Fields, "healthy window must return some fields")
		byName := map[string]tools.FieldStat{}
		for _, f := range env.Data.Fields {
			byName[f.Name] = f
		}
		// The recommendations service tags host through its pod placement, not
		// on every row, so the incident-specific host column with cardinality
		// > 1 must be absent from the healthy window's fields.
		if host, ok := byName["host"]; ok {
			assert.Equalf(t, int64(1), host.Cardinality,
				"healthy window's host field must not carry the incident's multi-host cardinality; got %d",
				host.Cardinality)
		}
	})
}
