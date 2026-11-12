package tools

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query/series"
)

// TestCorrelatePairsNeedsSupport: a day of 15s bins holding a handful of lines lines up
// at some lag by chance, so two series correlate only once each has lines in 8 bins.
func TestCorrelatePairsNeedsSupport(t *testing.T) {
	const width = 15 * time.Second
	series := func(n, offset int) []float64 {
		values := make([]float64, 5760)
		for i := 1; i <= n; i++ {
			values[i*100+offset] = 1
		}
		return values
	}
	assert.Empty(t, correlatePairs([]string{"a", "b"}, [][]float64{series(4, 0), series(3, 1)}, width, defaultMinRho))

	edges := correlatePairs([]string{"a", "b"}, [][]float64{series(8, 0), series(8, 1)}, width, defaultMinRho)
	require.Len(t, edges, 1)
	rho := edges[0].Rho
	assert.Equal(t, GraphEdge{From: "a", To: "b", Method: edgeCorrelation, Weight: rho, Confidence: rho, Rho: rho,
		LagSeconds: width.Seconds()}, edges[0])
	assert.InDelta(t, 1, rho, 1e-6)
}

// TestSignalOfAServiceNewToTheWindow: a service absent from the baseline gets no credit for
// rising from nothing, while a new pattern of a service already logging still does.
func TestSignalOfAServiceNewToTheWindow(t *testing.T) {
	s := rcaPatternStats{
		linesByPattern:         map[string]int64{"fresh": 100, "new": 100},
		baselineLinesByPattern: map[string]int64{"old": 100},
		serviceByPattern:       map[string]string{"fresh": "fresh-svc", "new": "old-svc"},
		baselineServices:       map[string]struct{}{"old-svc": {}},
		windowDuration:         100 * time.Second,
	}
	// Rarity is 0.2·(1 - 100/200) for both; only the old service's pattern adds 0.3·1.
	assert.InDelta(t, 0.1, s.signalOf("fresh", 100, 200), 1e-9)
	assert.InDelta(t, 0.4, s.signalOf("new", 100, 200), 1e-9)
}

// TestDetectAnomaliesNeedsSupportAndASecondPattern: a pattern spikes only with lines in 8 bins
// and beside another line of its service, and a new one is still reported as new.
func TestDetectAnomaliesNeedsSupportAndASecondPattern(t *testing.T) {
	from := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	bins := series.Bins{Width: 15 * time.Second, From: from, To: from.Add(16 * 15 * time.Second)}
	linesAt := map[string]func(bin int) float64{
		"rising": func(bin int) float64 { return float64(10 + 30*(bin/8)) },
		"steady": func(int) float64 { return 10 },
		"sparse": func(bin int) float64 { return float64(40 * (bin / 13)) },
	}
	values := series.Values{}
	byPattern := map[string]*patternAgg{}
	for id, at := range linesAt {
		values[id] = map[int64]float64{}
		byPattern[id] = &patternAgg{text: id, byService: map[string]int64{}}
		for bin, start := range bins.Boundaries() {
			values[id][start] = at(bin)
			byPattern[id].count += int64(at(bin))
		}
		byPattern[id].byService["solo"] = byPattern[id].count
	}

	tests := []struct {
		ids  []string
		want []string
	}{
		{ids: []string{"rising"}},
		{ids: []string{"rising", "steady"}, want: []string{"spike rising"}},
		{ids: []string{"sparse", "steady"}, want: []string{"new_pattern sparse"}},
	}
	for _, tt := range tests {
		solo := map[string]*patternAgg{}
		for _, id := range tt.ids {
			solo[id] = byPattern[id]
		}
		var got []string
		for _, a := range detectAnomalies(tt.ids, values, bins, solo, map[string]struct{}{"sparse": {}}) {
			got = append(got, a.Kind+" "+a.PatternID)
		}
		assert.Equal(t, tt.want, got, "solo logging %v", tt.ids)
	}
}
