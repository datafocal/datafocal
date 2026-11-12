package rcaeval

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadSample(t *testing.T) {
	cases, err := Load("testdata")
	require.NoError(t, err)
	require.Len(t, cases, 5)

	want := []Case{
		{Suite: "RE2", ID: "re2-001", RootCauseService: "order-service", FaultType: "latency", Candidates: []string{"order-service", "payment-service", "gateway"}},
		{Suite: "RE2", ID: "re2-002", RootCauseService: "db-proxy", FaultType: "error_rate", Candidates: []string{"cache-service", "db-proxy", "auth-service"}},
		{Suite: "RE2", ID: "re2-003", RootCauseService: "auth-service", FaultType: "cpu", Candidates: []string{"gateway", "user-service", "auth-service", "db-proxy"}},
		{Suite: "RE3", ID: "re3-001", RootCauseService: "inventory-service", FaultType: "memory", Candidates: []string{"inventory-service", "catalog-service"}},
		{Suite: "RE3", ID: "re3-002", RootCauseService: "notification-service", FaultType: "latency", Candidates: []string{"email-service", "notification-service", "queue-worker"}},
	}
	assert.Equal(t, want, cases)
}

func TestLoadAbsentDirSkips(t *testing.T) {
	cases, err := Load("nonexistent-dir-that-does-not-exist")
	assert.NoError(t, err)
	assert.Nil(t, cases)
}

func TestMetricParity(t *testing.T) {
	// Hand-computed ranks for the 5-case fixture:
	//   re2-001: order-service @ index 0 → rank 1
	//   re2-002: db-proxy @ index 1      → rank 2
	//   re2-003: auth-service @ index 2  → rank 3
	//   re3-001: inventory-service @ 0   → rank 1
	//   re3-002: notification-service @ 1 → rank 2
	// ranks = [1, 2, 3, 1, 2]
	//
	// AC@1 = 2/5 = 0.4
	// AC@3 = 5/5 = 1.0
	// MRR  = (1 + 1/2 + 1/3 + 1 + 1/2) / 5 = 3.3333.../5 = 0.6667 (rounded to 4dp)
	cases, err := Load("testdata")
	require.NoError(t, err)

	sc := Score(cases)
	assert.Equal(t, 5, sc.Aggregate.N)
	assert.InDelta(t, 0.4, sc.Aggregate.AC1, 1e-9)
	assert.InDelta(t, 1.0, sc.Aggregate.AC3, 1e-9)
	assert.InDelta(t, 1.0, sc.Aggregate.AC5, 1e-9)
	assert.InDelta(t, 10.0/15.0, sc.Aggregate.MRR, 1e-9)
}

func TestScoreProducesScorecard(t *testing.T) {
	cases, err := Load("testdata")
	require.NoError(t, err)

	sc := Score(cases)
	assert.Equal(t, len(cases), sc.Aggregate.N)
	require.Len(t, sc.Scenarios, 5)

	for _, s := range sc.Scenarios {
		assert.NotEmpty(t, s.Name)
		assert.True(t, s.CauseRank > 0, "every sample case should have a hit")
	}
}
