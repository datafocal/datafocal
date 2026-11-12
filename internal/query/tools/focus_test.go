package tools

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/selector"
)

// TestFocusReportsAFailedSectionBesideTheOthers: explain_spike failing leaves lift out and
// names its error, the other sections answer, and every call reads the pinned snapshot.
func TestFocusReportsAFailedSectionBesideTheOthers(t *testing.T) {
	var (
		mu        sync.Mutex
		snapshots []string
	)
	// The sections are called at once, so the fake notes their snapshots under a lock.
	meta := func(snapshot string, gets int, coverage float64, approx bool) query.Meta {
		mu.Lock()
		defer mu.Unlock()
		snapshots = append(snapshots, snapshot)
		return query.Meta{Snapshot: snapshot, Coverage: coverage, Approx: approx, Cost: query.Cost{GETs: gets, BytesRead: int64(gets) * 100}}
	}
	trend := AggregateResponse{Series: []Series{{Key: ""}}}
	related := PatternsResponse{Patterns: []Pattern{{PatternID: "p1", Pattern: "slow query <*> ms"}}, Total: 1}
	examples := SearchResponse{Mode: "scan", Lines: []LogLine{{PatternID: "p1", Message: "slow query 900 ms"}}}
	fake := Fake{
		AggregateFn: func(r AggregateRequest) (query.Envelope[AggregateResponse], error) {
			return query.Envelope[AggregateResponse]{Data: trend, Meta: meta(r.Snapshot, 1, 1, true)}, nil
		},
		ExplainSpikeFn: func(r ExplainSpikeRequest) (query.Envelope[ExplainSpikeResponse], error) {
			meta(r.Snapshot, 0, 1, false)
			return query.Envelope[ExplainSpikeResponse]{}, errors.New("explain_spike: boom")
		},
		PatternsFn: func(r PatternsRequest) (query.Envelope[PatternsResponse], error) {
			return query.Envelope[PatternsResponse]{Data: related, Meta: meta(r.Snapshot, 2, 0.5, false)}, nil
		},
		SearchLogsFn: func(r SearchRequest) (query.Envelope[SearchResponse], error) {
			return query.Envelope[SearchResponse]{Data: examples, Meta: meta(r.Snapshot, 4, 1, false)}, nil
		},
	}

	slice := []selector.Predicate{{Field: "pattern_id", Op: selector.OpEq, Value: "p1"}}
	got, err := focus(context.Background(), fake, FocusRequest{Slice: slice}, "e3.s3")
	require.NoError(t, err)

	want := FocusResponse{Trend: &trend, Related: &related, Examples: &examples,
		Errors: map[string]string{"lift": "explain_spike: boom"}}
	assert.Equal(t, want, got.Data)
	assert.Equal(t, query.Meta{Snapshot: "e3.s3", Coverage: 0.5, Approx: true,
		Cost: query.Cost{GETs: 7, BytesRead: 700}, Tokens: query.EstimateTokens(want)}, got.Meta)
	assert.Equal(t, []string{"e3.s3", "e3.s3", "e3.s3", "e3.s3"}, snapshots)
}

// TestFocusKeepsWhatItsLinesHaveInCommon: of explain_spike's shifts, Focus keeps a value on
// at least half the lines and three times as common as in the rest.
func TestFocusKeepsWhatItsLinesHaveInCommon(t *testing.T) {
	shifts := []Shift{
		{Field: "tool", Value: "bash", SliceShare: 0.8, BaselineShare: 0.05},
		{Field: "endpoint", Value: "/users", SliceShare: 0.81, BaselineShare: 0.55},
		{Field: "status", Value: "503", SliceShare: 0.4, BaselineShare: 0},
	}
	fake := Fake{ExplainSpikeFn: func(ExplainSpikeRequest) (query.Envelope[ExplainSpikeResponse], error) {
		return query.Envelope[ExplainSpikeResponse]{Data: ExplainSpikeResponse{Shifts: shifts, Lines: 10, Rest: 90}}, nil
	}}
	got, err := focus(context.Background(), fake, FocusRequest{}, "e3.s3")
	require.NoError(t, err)
	assert.Equal(t, &ExplainSpikeResponse{Shifts: shifts[:1], Lines: 10, Rest: 90}, got.Data.Lift,
		"/users is only 1.5 times as common, and 503 is under half the lines")
}
