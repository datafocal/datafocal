package tools_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
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
	"github.com/datafocal/datafocal/internal/summary"
)

// capabilityCorpus seeds one stream carrying every dimension kind the matrix crosses:
// service is stream-level, shard and pattern_id are row-level, outcome is a row-level
// predicate field, and duration_ms is a sketched value column.
func capabilityCorpus(t *testing.T) (catalog.Catalog, segment.Reader) {
	t.Helper()
	dir, store, cat := newCorpus(t)
	events := make([]segment.Record, 20)
	for i := range events {
		events[i] = cacheEvent(aggBase.Add(time.Duration(i)*time.Second), i)
	}
	writeSegment(t, store, cat, "cache", events)
	return readers(dir)
}

// cacheEvent is row i of a capability corpus: every fourth row a miss, and two messages
// so the segment holds two patterns and pattern_id is a real row-level dimension.
func cacheEvent(at time.Time, i int) segment.Record {
	outcome := "hit"
	if i%4 == 0 {
		outcome = "miss"
	}
	msg := fmt.Sprintf("cache lookup took %d ms", i+1)
	if i%2 == 0 {
		msg = fmt.Sprintf("cache store took %d ms", i+1)
	}
	return segment.LogRecord(at, "info", msg, map[string]string{"outcome": outcome, "shard": fmt.Sprintf("s%d", i%2)})
}

// seedStraddlingEvents writes one segment whose rows span a bin boundary and are
// deliberately not spread evenly across it: 80 rows in the first bin and 10 in the
// second, which a summary splitting the segment by time overlap would get wrong.
func seedStraddlingEvents(t *testing.T, store storage.Store, cat catalog.Catalog) {
	t.Helper()
	var events []segment.Record
	// Bin one: 80 rows packed into its first half-minute.
	for i := range 80 {
		events = append(events, cacheEvent(aggBase.Add(time.Duration(i)*350*time.Millisecond), i))
	}
	// Bin two: 10 rows, spread so the segment's range reaches 90s and the overlap split
	// is 60/30 against a true 80/10.
	for i := range 10 {
		events = append(events, cacheEvent(aggBase.Add(60*time.Second+time.Duration(i)*3*time.Second), 80+i))
	}
	writeSegment(t, store, cat, "cache", events)
}

// capabilityMeasures is every measure the capability matrices cross.
var capabilityMeasures = []string{"count", "rate", "ratio", "ratio:duration_ms", "sum:duration_ms", "p95:duration_ms", "distinct:shard"}

// capabilityMiss is the predicate the matrices filter by and take as a ratio's numerator.
var capabilityMiss = []selector.Predicate{{Field: "outcome", Op: selector.OpEq, Value: "miss"}}

// capabilityRequest is one matrix cell's aggregate request over span from aggBase.
func capabilityRequest(measure, groupBy string, filtered bool, span time.Duration) tools.AggregateRequest {
	sel := selector.Selector{Time: window(aggBase, aggBase.Add(span))}
	if filtered {
		sel.Where = capabilityMiss
	}
	req := tools.AggregateRequest{Selector: sel, Measure: measure, GroupBy: groupBy, Bins: "1m"}
	if strings.HasPrefix(measure, "ratio") {
		req.Numerator = capabilityMiss
	}
	return req
}

// capabilityCode names how a call resolved: ok, its API error code, or invalid_request.
func capabilityCode(err error) (string, *query.APIError) {
	var api *query.APIError
	switch {
	case err == nil:
		return "ok", nil
	case errors.As(err, &api):
		return api.Code, api
	default:
		return "invalid_request", nil
	}
}

// TestAggregateCapabilities pins which (measure, group_by, filtered) combinations the
// tool answers and which it refuses, as one table rather than a scatter of single-cell
// tests that each pass against the behaviour they were written for while nothing
// compares them. It also asserts the invariant those cells cannot see on their own —
// that narrowing never un-refuses, since a filter only removes rows.
func TestAggregateCapabilities(t *testing.T) {
	cr, rd := capabilityCorpus(t)

	outcome := func(measure, groupBy string, filtered bool) string {
		_, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), capabilityRequest(measure, groupBy, filtered, time.Minute))
		code, api := capabilityCode(err)
		// Every refusal must carry the re-plan, not just the failure, so the matrix checks
		// it for every cell.
		if api != nil && api.Hint == "" && len(api.SuggestedFields) == 0 {
			t.Errorf("%s by %s (filtered=%v) refused as %q with no hint or suggested fields",
				measure, groupBy, filtered, api.Code)
		}
		return code
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%-18s %-12s %-14s %s\n", "measure", "group_by", "unfiltered", "filtered")
	for _, measure := range capabilityMeasures {
		for _, groupBy := range []string{"service", "shard", "pattern_id"} {
			plain, filtered := outcome(measure, groupBy, false), outcome(measure, groupBy, true)
			fmt.Fprintf(&b, "%-18s %-12s %-14s %s\n", measure, groupBy, plain, filtered)

			// The invariant: a filter removes rows, so it cannot make an unanswerable
			// request answerable.
			if plain != "ok" && filtered == "ok" {
				t.Errorf("%s by %s: refused as %q unfiltered but answered when filtered — "+
					"adding a predicate must not un-refuse a request", measure, groupBy, plain)
			}
		}
	}

	const want = `measure            group_by     unfiltered     filtered
count              service      ok             ok
count              shard        ok             ok
count              pattern_id   ok             ok
rate               service      ok             ok
rate               shard        ok             ok
rate               pattern_id   ok             ok
ratio              service      ok             ok
ratio              shard        ok             ok
ratio              pattern_id   ok             ok
ratio:duration_ms  service      ok             ok
ratio:duration_ms  shard        ok             ok
ratio:duration_ms  pattern_id   ok             ok
sum:duration_ms    service      ok             ok
sum:duration_ms    shard        ok             ok
sum:duration_ms    pattern_id   ok             ok
p95:duration_ms    service      ok             ok
p95:duration_ms    shard        ok             ok
p95:duration_ms    pattern_id   ok             ok
distinct:shard     service      ok             ok
distinct:shard     shard        ok             ok
distinct:shard     pattern_id   ok             ok
`
	assert.Equal(t, want, b.String(), "the capability surface changed; if that is intended, update the table")
}

// TestMeasureVocabularyIsToolIndependent: three tools accept a measure — aggregate in
// Measure, correlate in SeriesRef.Measure, rca_walk in Symptom.Measure — and a measure
// one accepts and another cannot is a hole in the vocabulary, so the table below is the
// declared surface. TestAggregateCapabilities crosses measures with group_by for a
// single tool and cannot see this axis, which is what let "ratio:duration_ms" reach a
// nil accumulator through correlate while every aggregate cell stayed green.
func TestMeasureVocabularyIsToolIndependent(t *testing.T) {
	cr, rd := capabilityCorpus(t)
	w := window(aggBase, aggBase.Add(time.Minute))
	sel := selector.Selector{Time: w}
	ids := patternIDsByRows(t, cr, rd, w)
	require.Len(t, ids, 2, "the capability corpus must hold two patterns")

	code := func(err error) string {
		c, _ := capabilityCode(err)
		return c
	}
	// A ratio carries its numerator in Where; every other measure treats Where as scope.
	whereFor := func(measure string) []selector.Predicate {
		if _, isRatio := strings.CutPrefix(measure, "ratio"); isRatio {
			return capabilityMiss
		}
		return nil
	}

	var b strings.Builder
	fmt.Fprintf(&b, "%-18s %-11s %-11s %s\n", "measure", "aggregate", "correlate", "rca_walk")
	for _, measure := range capabilityMeasures {
		_, aggErr := toolsOver(cr, rd, nil).Aggregate(context.Background(), capabilityRequest(measure, "service", false, time.Minute))

		ref := tools.SeriesRef{PatternID: ids[0], Measure: measure, Where: whereFor(measure)}
		_, corrErr := toolsOver(cr, rd, nil).Correlate(context.Background(), tools.CorrelateRequest{
			Selector: sel, SeriesA: ref, SeriesB: tools.SeriesRef{PatternID: ids[1]}, Bins: "1m",
		})

		// The walk bins at 15s, not 1m: the corpus spans 20 seconds, so a one-minute grid
		// is a single bin and every measure would refuse with unusable_series before its
		// vocabulary was ever exercised.
		_, walkErr := toolsOver(cr, rd, nil).RCAWalk(context.Background(), tools.RCAWalkRequest{
			Selector: sel, Symptom: ref,
			Candidates: []tools.SeriesRef{{PatternID: ids[1]}}, Bins: "15s",
		})

		fmt.Fprintf(&b, "%-18s %-11s %-11s %s\n", measure, code(aggErr), code(corrErr), code(walkErr))
	}

	// The one intended gap: the corpus marks every fourth row a miss, so this pattern's
	// error ratio is exactly 0.5 in both bins. aggregate answers "what is the ratio" and
	// the walk asks "what moved with it", and a constant answers only the first.
	const want = `measure            aggregate   correlate   rca_walk
count              ok          ok          ok
rate               ok          ok          ok
ratio              ok          ok          unusable_series
ratio:duration_ms  ok          ok          ok
sum:duration_ms    ok          ok          ok
p95:duration_ms    ok          ok          ok
distinct:shard     ok          ok          ok
`
	assert.Equal(t, want, b.String(),
		"a measure must resolve the same way through every tool that accepts one; "+
			"if a gap is intended, record it here")

}

// patternIDsByRows returns the window's pattern ids ordered by descending row count.
func patternIDsByRows(t *testing.T, cr catalog.Catalog, rd segment.Reader, w selector.TimeSpec) []string {
	t.Helper()
	env, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), tools.AggregateRequest{
		Selector: selector.Selector{Time: w},
		Measure:  "count", GroupBy: segment.ColPatternID, Bins: "1h",
	})
	require.NoError(t, err)
	type row struct {
		id   string
		rows float64
	}
	rows := make([]row, 0, len(env.Data.Series))
	for _, s := range env.Data.Series {
		var n float64
		for _, v := range present(s.Values) {
			n += v
		}
		rows = append(rows, row{s.Key, n})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].rows != rows[j].rows {
			return rows[i].rows > rows[j].rows
		}
		return rows[i].id < rows[j].id
	})
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.id
	}
	return out
}

// TestAggregateSummaryRouting is the cost model and the correctness of routing as one
// table, over the same cells TestAggregateCapabilities answers. Two invariants no
// single-cell test can state:
//
//   - An answer that claims to be exact is exact. A summary may approximate — a sketch
//     merges whole segments — but then Meta.Approx has to say so.
//     Differing from the scan while claiming exactness is the wrong answer, and only a
//     comparison of the two paths can see it.
//   - A request the summary serves reads nothing, and no request costs more warm than
//     cold. The hot path merges summaries, never rows.
//
// The recorded column is which path served each cell, so a routing change shows up as a
// diff rather than as a cost number someone has to interpret.
func TestAggregateSummaryRouting(t *testing.T) {
	dir, store, cat := newCorpus(t)
	seedStraddlingEvents(t, store, cat)
	// Uncached on both sides: a footer cache would zero the cold path's cost too and
	// leave the comparison unable to tell the hot path from a warm one.
	cat, rd := uncachedReaders(dir)
	rs := wireSummary(t, cat, rd, summary.Config{BinWidth: time.Minute, SketchColumns: []string{"duration_ms", "shard"}})

	var b strings.Builder
	fmt.Fprintf(&b, "%-18s %-12s %-11s %s\n", "measure", "group_by", "unfiltered", "filtered")

	for _, measure := range capabilityMeasures {
		for _, groupBy := range []string{"service", "shard", "pattern_id"} {
			route := [2]string{}
			for i, filtered := range []bool{false, true} {
				req := capabilityRequest(measure, groupBy, filtered, 2*time.Minute)
				cold, coldErr := toolsOver(cat, rd, nil).Aggregate(context.Background(), req)
				warm, warmErr := toolsOver(cat, rd, rs).Aggregate(context.Background(), req)

				route[i] = routeOf(t, measure, groupBy, cold, coldErr, warm, warmErr)
			}
			fmt.Fprintf(&b, "%-18s %-12s %-11s %s\n", measure, groupBy, route[0], route[1])
		}
	}

	const want = `measure            group_by     unfiltered  filtered
count              service      hot         scan
count              shard        scan        scan
count              pattern_id   hot         scan
rate               service      hot         scan
rate               shard        scan        scan
rate               pattern_id   hot         scan
ratio              service      scan        scan
ratio              shard        scan        scan
ratio              pattern_id   scan        scan
ratio:duration_ms  service      scan        scan
ratio:duration_ms  shard        scan        scan
ratio:duration_ms  pattern_id   scan        scan
sum:duration_ms    service      scan        scan
sum:duration_ms    shard        scan        scan
sum:duration_ms    pattern_id   scan        scan
p95:duration_ms    service      hot~        scan
p95:duration_ms    shard        scan        scan
p95:duration_ms    pattern_id   scan        scan
distinct:shard     service      scan        scan
distinct:shard     shard        scan        scan
distinct:shard     pattern_id   scan        scan
`
	assert.Equal(t, want, b.String(),
		"which path serves a request changed; if that is intended, update the table")
}

// routeOf compares one cell's two paths and names which served the warm one. It fails
// the test rather than returning a verdict when the paths disagree, because a summary
// answering differently from the scan is a wrong answer whatever it cost.
func routeOf(t *testing.T, measure, groupBy string,
	cold query.Envelope[tools.AggregateResponse], coldErr error,
	warm query.Envelope[tools.AggregateResponse], warmErr error) string {
	t.Helper()
	cell := fmt.Sprintf("%s by %s", measure, groupBy)

	if (coldErr == nil) != (warmErr == nil) {
		t.Errorf("%s: one path answered and the other refused (cold=%v warm=%v)", cell, coldErr, warmErr)
		return "DISAGREE"
	}
	if coldErr != nil {
		return "refused"
	}
	assert.LessOrEqualf(t, warm.Meta.Cost.GETs, cold.Meta.Cost.GETs,
		"%s: a wired summary must never cost more than no summary at all", cell)
	if !warm.Meta.Approx {
		assert.Equalf(t, cold.Data, warm.Data,
			"%s: an answer reporting Approx=false must match the scan exactly", cell)
	}

	switch {
	case warm.Meta.Cost.GETs > 0 || cold.Meta.Cost.GETs == 0:
		return "scan"
	case warm.Meta.Approx:
		// Served free, and honest that it is an estimate.
		return "hot~"
	default:
		return "hot"
	}
}
