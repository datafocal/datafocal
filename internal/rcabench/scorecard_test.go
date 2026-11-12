package rcabench

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAccuracyAtKOracle pins the ranking metric against hand-computed truth so
// a subtle change to the primitive is caught before it flows into a scorecard.
func TestAccuracyAtKOracle(t *testing.T) {
	tests := []struct {
		name  string
		ranks []int
		k     int
		want  float64
	}{
		{name: "empty is zero", ranks: nil, k: 3, want: 0},
		{name: "k=0 is zero", ranks: []int{1, 2, 3}, k: 0, want: 0},
		{name: "all hit at 1", ranks: []int{1, 1, 1}, k: 1, want: 1.0},
		{name: "none hit at 1", ranks: []int{2, 3, 4}, k: 1, want: 0},
		{name: "two of four hit at 3", ranks: []int{1, 4, 3, 5}, k: 3, want: 0.5},
		{name: "zeros are misses", ranks: []int{0, 0, 1}, k: 3, want: 1.0 / 3.0},
		{name: "at k=5 with mix", ranks: []int{1, 2, 6, 0, 5}, k: 5, want: 3.0 / 5.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.InDelta(t, tt.want, AccuracyAtK(tt.ranks, tt.k), 1e-9)
		})
	}
}

// TestMRROracle pins the MRR primitive against RCAEval's definition:
// mean of 1/rank, with rank 0 contributing 0.
func TestMRROracle(t *testing.T) {
	tests := []struct {
		name  string
		ranks []int
		want  float64
	}{
		{name: "empty is zero", ranks: nil, want: 0},
		{name: "all at rank 1", ranks: []int{1, 1, 1}, want: 1.0},
		{name: "1 and 2", ranks: []int{1, 2}, want: (1.0 + 0.5) / 2.0},
		{name: "1, 2, 4", ranks: []int{1, 2, 4}, want: (1.0 + 0.5 + 0.25) / 3.0},
		{name: "one miss", ranks: []int{1, 0}, want: 0.5},
		{name: "all miss", ranks: []int{0, 0, 0}, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.InDelta(t, tt.want, MRR(tt.ranks), 1e-9)
		})
	}
}

// TestAggregateOverExcludesRefusals: an expected refusal counts toward the
// refusal-accuracy metric, not the ranking metrics; a bogus rank on a refusal
// row would otherwise poison AC@k.
func TestAggregateOverExcludesRefusals(t *testing.T) {
	rows := []ScenarioScore{
		{Name: "ordinary", CauseRank: 1, Found: true},
		{Name: "refuse_ok", RefusalExpected: true, RefusalCorrect: true, CauseRank: 0},
		{Name: "refuse_bad", RefusalExpected: true, RefusalCorrect: false, CauseRank: 0},
	}
	agg := AggregateOver(rows)

	assert.Equal(t, 3, agg.N)
	assert.InDelta(t, 1.0, agg.AC1, 1e-9, "only ordinary row contributes to AC@1")
	assert.InDelta(t, 1.0, agg.MRR, 1e-9, "only ordinary row contributes to MRR")
	assert.InDelta(t, 0.5, agg.RefusalAccuracy, 1e-9, "1 of 2 refusals correct")
	assert.InDelta(t, 0.0, agg.FalseAdmitRate, 1e-9, "no decoys admitted")
}

// TestAggregateFalseAdmitRate: total admitted decoys / total decoys, where
// every scenario carries two.
func TestAggregateFalseAdmitRate(t *testing.T) {
	rows := []ScenarioScore{
		{Name: "clean", CauseRank: 1, Found: true, FalseAdmits: nil},
		{Name: "one_admit", CauseRank: 1, Found: true, FalseAdmits: []string{"decoy-comoves activity "}},
		{Name: "two_admit", CauseRank: 1, Found: true, FalseAdmits: []string{"decoy-comoves activity ", "decoy-mechanism idle "}},
	}
	agg := AggregateOver(rows)
	// 3 admits / 6 total decoys
	assert.InDelta(t, 0.5, agg.FalseAdmitRate, 1e-9)
}

// TestScorecardJSONDeterministic: two invocations of JSON() on the same
// scorecard (even with FalseAdmits and Scenarios in a different order) must
// produce byte-identical output.
func TestScorecardJSONDeterministic(t *testing.T) {
	rows := []ScenarioScore{
		{Name: "b", CauseRank: 1, Found: true, FalseAdmits: []string{"z", "a"}},
		{Name: "a", CauseRank: 2, Found: true, FalseAdmits: []string{"m"}},
	}
	sc1 := Scorecard{Scenarios: rows, Aggregate: AggregateOver(rows)}

	// Shuffle a copy: SortScenarios should normalise both to the same shape.
	shuffled := []ScenarioScore{rows[1], rows[0]}
	shuffled[0].FalseAdmits = []string{"m"}
	shuffled[1].FalseAdmits = []string{"a", "z"}
	sc2 := Scorecard{Scenarios: shuffled, Aggregate: AggregateOver(shuffled)}

	a, err := sc1.JSON()
	require.NoError(t, err)
	b, err := sc2.JSON()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(a, b),
		"same scorecard content in different order must marshal byte-identical:\n%s\nvs\n%s", a, b)
}

// TestRoundFourStable: the round-to-4-decimals primitive is what pins JSON
// output across platforms. A bug in it would break the CI git-diff gate.
func TestRoundFourStable(t *testing.T) {
	tests := []struct {
		in   float64
		want float64
	}{
		{in: 0.5, want: 0.5},
		{in: 0.123456, want: 0.1235},
		{in: 0.123449999, want: 0.1234},
		{in: 1.0 / 3.0, want: 0.3333},
		{in: 2.0 / 3.0, want: 0.6667},
		{in: 0, want: 0},
		{in: -0.12345, want: -0.1235},
	}
	for _, tt := range tests {
		got := round4(tt.in)
		assert.InDelta(t, tt.want, got, 1e-9, "round4(%g) = %g, want %g", tt.in, got, tt.want)
	}
}

// TestMarkdownReadable: the human-readable table must include every scenario
// row and the aggregate metrics header.
func TestMarkdownReadable(t *testing.T) {
	rows := []ScenarioScore{
		{Name: "alpha", Volume: "loud", Mechanism: "topology", LeadTime: "well_separated",
			SymptomShape: "count", CauseRank: 1, Found: true, Pass: true, GETs: 3},
		{Name: "beta", Volume: "loud", Mechanism: "none", LeadTime: "well_separated",
			SymptomShape: "count", RefusalExpected: true, RefusalCorrect: false, Pass: false},
	}
	sc := Scorecard{Scenarios: rows, Aggregate: AggregateOver(rows)}
	md := sc.Markdown()
	assert.Contains(t, md, "# RCA Scorecard")
	assert.Contains(t, md, "alpha")
	assert.Contains(t, md, "beta")
	assert.Contains(t, md, "AC@1")
	assert.Contains(t, md, "MRR")
}
