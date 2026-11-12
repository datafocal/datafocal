package stats

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBreakdown(t *testing.T) {
	tests := []struct {
		name    string
		byGroup map[string]int64
		want    []Share
	}{
		{"empty input", map[string]int64{}, []Share{}},
		{
			// Largest first; svc-a and svc-z tie on pct and break on name.
			"orders by share then key",
			map[string]int64{"svc-z": 100, "svc-a": 100, "svc-m": 200},
			[]Share{
				{Key: "svc-m", Lines: 200, Pct: 50},
				{Key: "svc-a", Lines: 100, Pct: 25},
				{Key: "svc-z", Lines: 100, Pct: 25},
			},
		},
		{
			// No lines at all is not a 100%-of-nothing story: every group holds a
			// zero share of an activity that never happened.
			"all groups at zero",
			map[string]int64{"svc-a": 0, "svc-b": 0},
			[]Share{{Key: "svc-a", Lines: 0, Pct: 0}, {Key: "svc-b", Lines: 0, Pct: 0}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Breakdown(tc.byGroup))
		})
	}
}

// TestBreakdownColumnSumsTo100: the per-share divisions round independently, so
// the printed column is corrected to total 100 exactly.
func TestBreakdownColumnSumsTo100(t *testing.T) {
	shares := Breakdown(map[string]int64{"svc-a": 100, "svc-b": 200, "svc-c": 150})

	sum := 0.0
	for _, s := range shares {
		sum += s.Pct
	}
	assert.Equal(t, 100.0, sum)
}

// TestDivergence: a key absent from one side must not make the divergence +Inf,
// which encoding/json cannot marshal, and a matching mix diverges by nothing.
func TestDivergence(t *testing.T) {
	// Add-one smoothing over keys {a, b}: slice 10+2, baseline 5+2.
	want := 11.0/12*math.Log((11.0/12)/(1.0/7)) + 1.0/12*math.Log((1.0/12)/(6.0/7))
	assert.InDelta(t, want, Divergence(map[string]int64{"a": 10}, map[string]int64{"b": 5}), 1e-12)

	assert.Zero(t, Divergence(map[string]int64{"a": 9, "b": 9}, map[string]int64{"a": 99, "b": 99}))
}
