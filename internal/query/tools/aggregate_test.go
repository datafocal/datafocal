package tools_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/stats"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/internal/value"
)

// aggBase is the anchor time for the aggregate oracle tests.
var aggBase = time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)

// seedValues seeds one segment carrying a numeric param column and, optionally, a
// per-row trace_id.
//
// The message shape "value <n>" masks to pattern `value <*>`, so
// `param_1` carries the numeric value verbatim and the codec builds a DDSketch on
// it. `trace_id` goes in Attributes, which highCardColumns includes for the HLL
// count-distinct sketch.
func seedValues(t *testing.T, store storage.Store, cat catalog.Catalog, service string, values []float64, traceIDs []string) {
	t.Helper()
	msgs := make([]string, len(values))
	for i, v := range values {
		msgs[i] = fmt.Sprintf("value %g", v)
	}
	events := messageEvents(aggBase, msgs...)
	for i := range traceIDs {
		events[i] = segment.LogRecord(events[i].Timestamp, "info", msgs[i], map[string]string{"trace_id": traceIDs[i]})
	}
	writeSegment(t, store, cat, service, events)
}

// aggSeq is the values 1..n.
func aggSeq(n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = float64(i + 1)
	}
	return out
}

// aggTraces is n distinct trace ids rendered through format.
func aggTraces(format string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(format, i)
	}
	return out
}

// aggSel is the standard tenant selector over [aggBase-1m, aggBase+to].
func aggSel(to time.Duration) selector.Selector {
	return selector.Selector{Time: window(aggBase.Add(-time.Minute), aggBase.Add(to))}
}

// aggTotals sums every series' points per key.
func aggTotals(env query.Envelope[tools.AggregateResponse]) map[string]float64 {
	got := map[string]float64{}
	for _, s := range env.Data.Series {
		for _, v := range present(s.Values) {
			got[s.Key] += v
		}
	}
	return got
}

// aggTotal sums every point of every series.
func aggTotal(env query.Envelope[tools.AggregateResponse]) float64 {
	total := 0.0
	for _, v := range aggTotals(env) {
		total += v
	}
	return total
}

// aggRecord is one hand-built row for a test whose shape the collector cannot synthesize.
func aggRecord(at time.Time, message, service, pattern string, extra ...segment.Column) segment.Record {
	str := func(name, s string) segment.Column {
		return segment.Column{Name: name, Type: value.TypeString, Value: value.Value{String: s}}
	}
	return segment.Record{Timestamp: at, Columns: append([]segment.Column{
		str(segment.ColLevel, "info"), str(segment.ColMessage, message),
		str(segment.ColService, service), str(segment.ColPatternID, pattern),
	}, extra...)}
}

// runAggregate is a shortcut for calling tools.Aggregate over dir without a summary.
func runAggregate(t *testing.T, dir string, req tools.AggregateRequest) query.Envelope[tools.AggregateResponse] {
	t.Helper()
	cat, rd := readers(dir)
	env, err := toolsOver(cat, rd, nil).Aggregate(context.Background(), req)
	require.NoError(t, err)
	return env
}

// TestAggregateSingleBinMeasures is the per-measure oracle over one seeded segment: the
// single-bin value against the true answer within the measure's error envelope, and the
// envelope's Approx and Coverage, grouped by service
// or not at all.
func TestAggregateSingleBinMeasures(t *testing.T) {
	tests := []struct {
		name    string
		values  []float64
		traces  []string
		to      time.Duration
		measure string
		want    float64
		// delta is the absolute error allowed: 0 for exact measures, the sketch's relative
		// accuracy times the true value for approximate ones.
		delta  float64
		approx bool
	}{
		// DDSketch is α=0.01; 2% covers its mapping against the library's ceiling rank.
		{name: "p95 within the DDSketch envelope", values: aggSeq(100), to: 5 * time.Minute, measure: "p95", want: 95, delta: 0.02 * 95, approx: true},
		// HLL's standard error is ~1.6%, so 3% is generous.
		{name: "distinct within the HLL envelope", values: aggSeq(200), traces: aggTraces("trace-%08d", 200), to: 5 * time.Minute, measure: "distinct:trace_id", want: 200, delta: 0.03 * 200, approx: true},
		// The single bin is the whole 3m window, so rate is 30 / 180s.
		{name: "rate is count over the window width", values: aggSeq(30), to: 2 * time.Minute, measure: "rate", want: 30.0 / 180, delta: 1e-9},
		{name: "sum is exact", values: []float64{1.5, 2.25, 3.75, 4.0, 8.5}, to: 5 * time.Minute, measure: "sum:param_1", want: 20},
		{name: "approx count", values: aggSeq(10), traces: aggTraces("trace-%d", 10), to: 5 * time.Minute, measure: "count", want: 10},
		{name: "approx rate", values: aggSeq(10), traces: aggTraces("trace-%d", 10), to: 5 * time.Minute, measure: "rate", want: 10.0 / 360, delta: 1e-9},
		// p95 of ten samples lands on the ninth by DDSketch's rank convention.
		{name: "approx p95", values: aggSeq(10), traces: aggTraces("trace-%d", 10), to: 5 * time.Minute, measure: "p95", want: 9, delta: 0.02 * 9, approx: true},
		{name: "approx distinct", values: aggSeq(10), traces: aggTraces("trace-%d", 10), to: 5 * time.Minute, measure: "distinct:trace_id", want: 10, delta: 0.03 * 10, approx: true},
		{name: "approx sum", values: aggSeq(10), traces: aggTraces("trace-%d", 10), to: 5 * time.Minute, measure: "sum:param_1", want: 55},
		{name: "coverage count", values: aggSeq(5), to: 5 * time.Minute, measure: "count", want: 5},
		// p95 of five samples lands on the fourth by DDSketch's rank convention.
		{name: "coverage p95", values: aggSeq(5), to: 5 * time.Minute, measure: "p95", want: 4, delta: 0.02 * 4, approx: true},
		{name: "coverage sum", values: aggSeq(5), to: 5 * time.Minute, measure: "sum:param_1", want: 15},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, store, cat := newCorpus(t)
			seedValues(t, store, cat, "svc", tt.values, tt.traces)

			// One service's rows read the same by service as in one series of every row.
			for _, groupBy := range []string{"service", "none"} {
				env := runAggregate(t, dir, tools.AggregateRequest{Selector: aggSel(tt.to), Measure: tt.measure, GroupBy: groupBy})

				require.Len(t, env.Data.Series, 1, groupBy)
				require.NotEmpty(t, env.Data.Series[0].Values, groupBy)
				assert.InDelta(t, tt.want, *env.Data.Series[0].Values[0], tt.delta, "value by %s", groupBy)
				assert.Equal(t, tt.approx, env.Meta.Approx, "meta.approx by %s", groupBy)
				assert.Equal(t, 1.0, env.Meta.Coverage, "meta.coverage: every segment contributed")
			}
		})
	}
}

// TestAggregateBinnedCount pins the binning invariant, that binned
// counts sum to the single-bin total, and that every bin's baseline follows
// µ_0=x_0, µ_t = λ·x_t + (1-λ)·µ_{t-1} at DefaultLambda, to 4 significant digits.
func TestAggregateBinnedCount(t *testing.T) {
	dir, store, cat := newCorpus(t)
	// 60 events spanning 60 seconds straddle 4 x 15s bins.
	seedValues(t, store, cat, "svc", aggSeq(60), nil)
	req := tools.AggregateRequest{Selector: aggSel(2 * time.Minute), Measure: "count", GroupBy: "service"}

	single := runAggregate(t, dir, req)
	require.Len(t, single.Data.Series, 1)
	require.Len(t, single.Data.Series[0].Values, 1)
	assert.Equal(t, map[string]float64{"svc": 60}, aggTotals(single), "single-bin total")

	req.Bins = "15s"
	binned := runAggregate(t, dir, req)
	require.Len(t, binned.Data.Series, 1)
	values, baselines := present(binned.Data.Series[0].Values), present(binned.Data.Series[0].Baselines)
	require.GreaterOrEqual(t, len(values), 2)
	require.Len(t, baselines, len(values))
	assert.Equal(t, map[string]float64{"svc": 60}, aggTotals(binned), "binned counts must sum to the single-bin total")

	round := func(v float64) float64 {
		r, err := strconv.ParseFloat(strconv.FormatFloat(v, 'g', 4, 64), 64)
		require.NoError(t, err)
		return r
	}
	mu := values[0]
	assert.Equal(t, round(mu), baselines[0], "baseline[0] seeds at x_0")
	for i := 1; i < len(values); i++ {
		mu = stats.DefaultLambda*values[i] + (1-stats.DefaultLambda)*mu
		assert.Equal(t, round(mu), baselines[i], "baseline[%d]", i)
	}
}

// TestAggregateDeterminism asserts the same request against the same snapshot
// yields byte-identical envelopes.
func TestAggregateDeterminism(t *testing.T) {
	dir, store, cat := newCorpus(t)
	for _, svc := range []string{"gateway", "checkout", "db"} {
		seedValues(t, store, cat, svc, aggSeq(5), nil)
	}
	req := tools.AggregateRequest{Selector: aggSel(5 * time.Minute), Measure: "count", GroupBy: "service", Bins: "15s"}

	// Take the snapshot once so the second call is pinned to the same catalog view.
	env1 := runAggregate(t, dir, req)
	req.Snapshot = env1.Meta.Snapshot
	env2 := runAggregate(t, dir, req)

	b1, err := json.Marshal(env1)
	require.NoError(t, err)
	b2, err := json.Marshal(env2)
	require.NoError(t, err)
	assert.Equal(t, string(b1), string(b2))
}

// TestAggregateGroupByPatternID groups the rows by their pattern, by default or by name, and
// with none keeps every row in one series.
func TestAggregateGroupByPatternID(t *testing.T) {
	dir, store, cat := newCorpus(t)
	writeSegment(t, store, cat, "svc", messageEvents(aggBase,
		"alpha 1", "alpha 2", "alpha 3", "alpha 4", "alpha 5", "beta 1", "beta 2", "beta 3"))

	for _, tc := range []struct {
		groupBy string
		want    []float64
	}{
		{"pattern_id", []float64{3, 5}},
		{"", []float64{3, 5}},
		{"none", []float64{8}},
	} {
		env := runAggregate(t, dir, tools.AggregateRequest{Selector: aggSel(5 * time.Minute), Measure: "count", GroupBy: tc.groupBy})
		// Series keys are pattern hashes, so assert the sorted totals rather than the pattern_ids.
		assert.Equal(t, tc.want, slices.Sorted(maps.Values(aggTotals(env))), "group_by %q", tc.groupBy)
	}
}

// TestAggregateSummaryFallsThrough covers summaries that must not serve: one built off a
// different, empty catalog names an epoch this snapshot never had, and sum has no sketch
// in any summary, so both answer exactly as the summary-less path does and pay for reads.
func TestAggregateSummaryFallsThrough(t *testing.T) {
	dir, store, cat := newCorpus(t)
	seedValues(t, store, cat, "svc", aggSeq(5), nil)

	tests := []struct {
		name    string
		measure string
		summary func(t *testing.T, cr catalog.Catalog, rd segment.Reader) summary.Store
	}{
		{
			// p95 always reads footers on the fallback path, so cost.gets > 0 proves the summary did not serve.
			name: "p95 against a summary of another catalog", measure: "p95",
			summary: func(t *testing.T, _ catalog.Catalog, _ segment.Reader) summary.Store {
				emptyCat, emptyRd := readers(t.TempDir())
				return wireSummary(t, emptyCat, emptyRd, summary.Config{})
			},
		},
		{
			name: "sum against a summary of this catalog", measure: "sum:param_1",
			summary: func(t *testing.T, cr catalog.Catalog, rd segment.Reader) summary.Store {
				return wireSummary(t, cr, rd, summary.Config{})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := tools.AggregateRequest{Selector: aggSel(5 * time.Minute), Measure: tt.measure, GroupBy: "service"}
			want := runAggregate(t, dir, req)

			cr, rd := readers(dir)
			got, err := toolsOver(cr, rd, tt.summary(t, cr, rd)).Aggregate(context.Background(), req)
			require.NoError(t, err)

			assert.NotZero(t, got.Meta.Cost.GETs, "cost.gets: the request must have fallen through to reading segments")
			// Cost and the snapshot's summary version differ with a summary wired by design, so they are left out.
			want.Meta.Cost, got.Meta.Cost = query.Cost{}, query.Cost{}
			want.Meta.Snapshot, got.Meta.Snapshot = "", ""
			assert.Equal(t, want, got)
		})
	}
}

// TestAggregateSummaryCountIsExact: a summary counts a segment straddling its bins from
// the segment's rows, so it serves the count at zero cost and exact at either width.
func TestAggregateSummaryCountIsExact(t *testing.T) {
	dir, store, cat := newCorpus(t)
	// 20 rows a second apart from a minute boundary: inside one 1m bin, across two
	// 15s ones.
	msgs := make([]string, 20)
	for i := range msgs {
		msgs[i] = fmt.Sprintf("query ok %d ms", i)
	}
	writeSegment(t, store, cat, "svc", messageEvents(aggBase, msgs...))

	catRd, rd := readers(dir)
	for _, tc := range []struct {
		bins       string
		width      time.Duration
		wantApprox bool
	}{
		{bins: "1m", width: time.Minute, wantApprox: false},
		{bins: "15s", width: 15 * time.Second, wantApprox: false},
	} {
		t.Run(tc.bins, func(t *testing.T) {
			rs := wireSummary(t, catRd, rd, summary.Config{BinWidth: tc.width})
			env, err := toolsOver(catRd, rd, rs).Aggregate(context.Background(), tools.AggregateRequest{
				Selector: selector.Selector{Time: window(aggBase, aggBase.Add(time.Minute))},
				Measure:  "count",
				GroupBy:  "service",
				Bins:     tc.bins,
			})
			require.NoError(t, err)
			require.Zero(t, env.Meta.Cost.GETs, "cost.gets: both arms are summary-served")
			assert.Equal(t, tc.wantApprox, env.Meta.Approx, "meta.approx")
			assert.Equal(t, 20.0, aggTotal(env), "the count must not lose or invent lines")
		})
	}
}

// TestAggregateSketchGroupsByStreamLabel: a footer sketch summarizes a whole
// segment, so it can be grouped by any dimension every row of that segment shares
// — which is the stream, service and labels both, not service alone. The collector
// partitions by stream and the labels ride the descriptor through the manifest, so
// p95 by a label is answerable from the same footers the p95-by-service path reads,
// at the same cost.
//
// The seed corpus' "instance" is exactly such a label, and refusing this made p95
// per instance unanswerable — the question a latency investigation actually asks.
func TestAggregateSketchGroupsByStreamLabel(t *testing.T) {
	dir, store, cat := newCorpus(t)
	seed := func(instance string, values ...float64) {
		msgs := make([]string, len(values))
		for i, v := range values {
			msgs[i] = fmt.Sprintf("value %g", v)
		}
		writeLabeledSegment(t, store, cat, "svc", map[string]string{"instance": instance},
			messageEvents(aggBase, msgs...))
	}
	seed("a", 1, 2, 3, 4, 5)
	seed("b", 100, 200, 300, 400, 500)

	env := runAggregate(t, dir, tools.AggregateRequest{
		Selector: selector.Selector{Time: window(aggBase, aggBase.Add(time.Minute))},
		Measure:  "p95:param_1",
		GroupBy:  "instance",
	})

	// One series per instance, and each one's p95 is that instance's own — a shared
	// key would have merged both segments' sketches and reported b's for both. p95
	// of five samples lands on the fourth by DDSketch's rank convention, hence 4
	// and 400 rather than 5 and 500.
	want := map[string]float64{"a": 4, "b": 400}
	if len(env.Data.Series) != len(want) {
		t.Fatalf("series = %+v, want one per instance (%d)", env.Data.Series, len(want))
	}
	for _, s := range env.Data.Series {
		expected, ok := want[s.Key]
		if !ok {
			t.Errorf("unexpected series key %q", s.Key)
			continue
		}
		peak := 0.0
		for _, v := range present(s.Values) {
			peak = math.Max(peak, v)
		}
		// DDSketch is built at 1% relative accuracy; 2% covers the bucket edge.
		if math.Abs(peak-expected) > expected*0.02 {
			t.Errorf("instance %q: p95 = %v, want ~%v", s.Key, peak, expected)
		}
	}
	if env.Meta.Cost.GETs != 2 {
		t.Errorf("cost.gets = %d, want 2 (one footer per segment, no column reads)", env.Meta.Cost.GETs)
	}
	if !env.Meta.Approx {
		t.Error("meta.approx = false, want true (a quantile from sketches is approximate)")
	}
}

// TestAggregateGroupsByTheRowsServiceWhenAStreamHoldsMany is the shape a node-level
// agent produces: it tails several services' files on one host and writes them to one
// stream, so every segment is stamped with the agent's own name and the real service is
// a value on each row.
//
// group_by=service must answer with the services. Deciding groupability from the
// dimension's name — the assumption "service is whatever the stream claims" — keyed the
// whole segment on "node-agent" and returned one series under the agent's name, which
// is a wrong answer rather than a missing one, and disagreed with what the scan path
// would have said for the same request.
func TestAggregateGroupsByTheRowsServiceWhenAStreamHoldsMany(t *testing.T) {
	dir, store, cat := newCorpus(t)
	writeRecords(t, store, cat, "node-agent", []segment.Record{
		aggRecord(aggBase, "handled request", "nginx", "handled-request"),
		aggRecord(aggBase.Add(time.Second), "handled request", "nginx", "handled-request"),
		aggRecord(aggBase.Add(2*time.Second), "handled request", "checkout", "handled-request"),
	})

	env := runAggregate(t, dir, tools.AggregateRequest{Selector: aggSel(time.Minute), Measure: "count", GroupBy: "service", Bins: "1m"})

	assert.Equal(t, map[string]float64{"nginx": 2, "checkout": 1}, aggTotals(env),
		"group_by=service must key on the service the rows name, not the stream that carried them")
}

// TestAggregateGroupsAndFiltersByListElements counts a record once under each distinct
// element of a list column, says the series multiplied it, and matches a predicate on
// the list by its elements under either quantifier.
func TestAggregateGroupsAndFiltersByListElements(t *testing.T) {
	tags := func(elems ...string) segment.Column {
		list := make([]value.Value, len(elems))
		for i, e := range elems {
			list[i] = value.Value{String: e}
		}
		return segment.Column{Name: "tags", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: list}}
	}
	dir, store, cat := newCorpus(t)
	writeRecords(t, store, cat, "gateway", []segment.Record{
		aggRecord(aggBase, "handled request", "gateway", "handled-request", tags("a", "b", "a")),
		aggRecord(aggBase.Add(time.Second), "handled request", "gateway", "handled-request", tags("b")),
		aggRecord(aggBase.Add(2*time.Second), "handled request", "gateway", "handled-request", tags()),
	})

	env := runAggregate(t, dir, tools.AggregateRequest{Selector: aggSel(time.Minute), Measure: "count", GroupBy: "tags", Bins: "1m"})
	assert.Equal(t, map[string]float64{"a": 1, "b": 2, "": 1}, aggTotals(env))
	assert.True(t, env.Data.Multiplied)

	tests := []struct {
		name string
		p    selector.Predicate
		want float64
	}{
		{"any eq", selector.Predicate{Field: "tags", Op: selector.OpEq, Value: "a"}, 1},
		{"any ne", selector.Predicate{Field: "tags", Op: selector.OpNe, Value: "a", Quantifier: selector.QuantifierAny}, 2},
		{"all eq", selector.Predicate{Field: "tags", Op: selector.OpEq, Value: "b", Quantifier: selector.QuantifierAll}, 1},
		{"all ne is false on an empty list", selector.Predicate{Field: "tags", Op: selector.OpNe, Value: "a", Quantifier: selector.QuantifierAll}, 1},
		{"any eq an absent element", selector.Predicate{Field: "tags", Op: selector.OpEq, Value: "c"}, 0},
		{"exists", selector.Predicate{Field: "tags", Op: selector.OpExists}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sel := aggSel(time.Minute)
			sel.Where = []selector.Predicate{tt.p}
			env := runAggregate(t, dir, tools.AggregateRequest{Selector: sel, Measure: "count", Bins: "1m"})
			assert.Equal(t, tt.want, aggTotal(env))
			assert.False(t, env.Data.Multiplied)
		})
	}
}

// TestAggregateMeasuresLengthsByElement sums each content block's text length under its
// own block's type, leaves a block without text out of the sum but not the count, and
// reads bytes apart from characters.
func TestAggregateMeasuresLengthsByElement(t *testing.T) {
	p, err := logformat.New(logformat.Config{Format: "json"})
	require.NoError(t, err)
	transcript := func(at time.Time, line string) segment.Record {
		r, ok := p.Parse(line)
		require.True(t, ok)
		return aggRecord(at, "turn", "agent", "turn", r.Columns...)
	}
	dir, store, cat := newCorpus(t)
	writeRecords(t, store, cat, "agent", []segment.Record{
		transcript(aggBase, `{"message":{"content":[{"type":"text","text":"héllo"},{"type":"tool_use","name":"Edit"},{"type":"text","text":"abc"}]}}`),
		transcript(aggBase.Add(time.Second), `{"message":{"content":[{"type":"thinking","thinking":"hmm"},{"type":"text","text":"日本"}]}}`),
	})
	run := func(measure, groupBy string) query.Envelope[tools.AggregateResponse] {
		return runAggregate(t, dir, tools.AggregateRequest{Selector: aggSel(time.Minute), Measure: measure, GroupBy: groupBy, Bins: "1m"})
	}

	env := run("sum:len(message.content.text)", "message.content.type")
	assert.Equal(t, map[string]float64{"text": 5 + 3 + 2}, aggTotals(env))
	assert.False(t, env.Data.Multiplied, "each block counts once, under its own type")
	assert.Equal(t, env, run("sum:len(message.content.text)", "message.content.type"))
	assert.Equal(t, map[string]float64{"text": 6 + 3 + 6}, aggTotals(run("sum:bytes(message.content.text)", "message.content.type")))
	assert.Equal(t, map[string]float64{"text": 2, "tool_use": 1, "thinking": 1}, aggTotals(run("count", "message.content.type")))
	assert.Equal(t, map[string]float64{"agent": 10}, aggTotals(run("sum:len(message.content.text)", "service")))
	assert.Equal(t, map[string]float64{"info": 8}, aggTotals(run("sum:len(message)", "level")))
	assert.Equal(t, map[string]float64{"text": 5}, aggTotals(run("max:len(message.content.text)", "message.content.type")))
}

// TestAggregateSumsDerivedColumn sums a cost derived from token counts and a price table,
// taking a row's own cost.total where it carries one, and versions the tables in the token; its
// most and least read it as the number it is.
func TestAggregateSumsDerivedColumn(t *testing.T) {
	dir, store, cat := newCorpus(t)
	turn := func(i int, model string, in, out int64, cost ...segment.Column) segment.Record {
		return aggRecord(aggBase.Add(time.Duration(i)*time.Second), "turn", "agent", "turn", append([]segment.Column{
			{Name: "message.model", Type: value.TypeString, Value: value.Value{String: model}},
			{Name: "message.usage.input_tokens", Type: value.TypeInt, Value: value.Value{Int: in}},
			{Name: "message.usage.output_tokens", Type: value.TypeInt, Value: value.Value{Int: out}},
		}, cost...)...)
	}
	writeRecords(t, store, cat, "agent", []segment.Record{
		turn(0, "opus", 2e6, 1e5),
		turn(1, "opus", 1e6, 0, segment.Column{Name: "cost.total", Type: value.TypeFloat, Value: value.Value{Float: 0.5}}),
		turn(2, "haiku", 4e6, 2e6),
	})
	const expr = "coalesce(cost.total, message.usage.input_tokens/1e6*lookup(message.model, price_in) + " +
		"message.usage.output_tokens/1e6*lookup(message.model, price_out))"
	measured := func(measure, opusIn, pin string, where ...selector.Predicate) (query.Envelope[tools.AggregateResponse], error) {
		derived, err := plan.NewDerived([]plan.DerivedColumn{{Name: "cost_usd", Expr: expr}}, map[string]map[string]string{
			"price_in": {"opus": opusIn, "haiku": "0.8"}, "price_out": {"opus": "75", "haiku": "4"}})
		require.NoError(t, err)
		cr, rd := readers(dir)
		planner, scanner := plan.New(plan.Deps{Catalog: cr, Reader: rd, Derived: derived}), scan.New(rd, derived)
		sel := aggSel(time.Minute)
		sel.Where = where
		return tools.New(tools.Deps{Catalog: cr, Series: series.New(planner, scanner), Planner: planner, Scanner: scanner}).
			Aggregate(context.Background(), tools.AggregateRequest{Selector: sel, Measure: measure, GroupBy: "message.model", Bins: "1m", Snapshot: pin})
	}
	aggregate := func(opusIn, pin string, where ...selector.Predicate) (query.Envelope[tools.AggregateResponse], error) {
		return measured("sum:cost_usd", opusIn, pin, where...)
	}

	env, err := aggregate("15", "")
	require.NoError(t, err)
	// opus: 2*15 + 0.1*75 priced, plus its own 0.5; haiku: 4*0.8 + 2*4.
	assert.InDeltaMapValues(t, map[string]float64{"opus": 38, "haiku": 11.2}, aggTotals(env), 1e-9)
	filtered, err := aggregate("15", "", selector.Predicate{Field: "cost_usd", Op: selector.OpGt, Value: "20"})
	require.NoError(t, err)
	assert.InDeltaMapValues(t, map[string]float64{"opus": 37.5}, aggTotals(filtered), 1e-9)

	for _, tc := range []struct {
		measure string
		want    map[string]float64
	}{
		{"max:cost_usd", map[string]float64{"opus": 37.5, "haiku": 11.2}},
		{"min:cost_usd", map[string]float64{"opus": 0.5, "haiku": 11.2}},
	} {
		got, err := measured(tc.measure, "15", "")
		require.NoError(t, err)
		assert.InDeltaMapValues(t, tc.want, aggTotals(got), 1e-9, tc.measure)
	}

	repriced, err := aggregate("5", "")
	require.NoError(t, err)
	assert.NotEqual(t, env.Meta.Snapshot, repriced.Meta.Snapshot)
	_, err = aggregate("5", env.Meta.Snapshot)
	assert.ErrorContains(t, err, "derived-column definitions")
	pinned, err := aggregate("15", env.Meta.Snapshot)
	require.NoError(t, err)
	assert.Equal(t, env.Data, pinned.Data)
}

// TestAggregateDistinctBy sums a model call's usage once however many lines repeat it, a
// minute apart or not, and counts every line that names no call; its own where applies
// first, and a measure without distinct_by beside it still reads every line.
func TestAggregateDistinctBy(t *testing.T) {
	dir, store, cat := newCorpus(t)
	line := func(sec int, model, call string, tokens int64) segment.Record {
		cols := []segment.Column{
			{Name: "message.model", Type: value.TypeString, Value: value.Value{String: model}},
			{Name: "tokens", Type: value.TypeInt, Value: value.Value{Int: tokens}},
		}
		if call != "" {
			cols = append(cols, segment.Column{Name: "message.id", Type: value.TypeString, Value: value.Value{String: call}})
		}
		return aggRecord(aggBase.Add(time.Duration(sec)*time.Second), "turn", "agent", "turn", cols...)
	}
	writeRecords(t, store, cat, "agent", []segment.Record{
		line(0, "opus", "c1", 10), line(1, "opus", "c1", 10), line(70, "opus", "c1", 10),
		line(2, "opus", "c2", 5),
		line(3, "haiku", "c3", 4), line(4, "haiku", "c3", 4),
		line(5, "haiku", "", 7),
	})
	cr, rd := readers(dir)
	env, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), tools.AggregateRequest{
		Selector: aggSel(2 * time.Minute), GroupBy: "message.model", Bins: "all",
		Measures: []selector.MeasureSpec{
			{Measure: "sum:tokens", DistinctBy: "message.id"},
			{Measure: "sum:tokens"},
			{Measure: "count", DistinctBy: "message.id", Where: []selector.Predicate{{Field: "tokens", Op: selector.OpGt, Value: "4"}}},
		},
	})
	require.NoError(t, err)

	got := map[string][]float64{}
	for _, s := range env.Data.Series {
		for _, m := range s.MeasureValues {
			got[s.Key] = append(got[s.Key], *m[0])
		}
	}
	assert.Equal(t, map[string][]float64{
		"opus":  {15, 35, 2},
		"haiku": {11, 15, 1},
	}, got)
}

// filterCorpus is three rows in one segment with known values on every dimension the
// predicate tests exercise: two successes and one failure, with durations chosen so a
// numeric comparison and a lexicographic one disagree ("40" and "45" sort above "100"
// as text and below it as numbers).
func filterCorpus(t *testing.T) (catalog.Catalog, segment.Reader) {
	t.Helper()
	dir, store, cat := newCorpus(t)
	writeSegment(t, store, cat, "gateway", []segment.Record{
		segment.LogRecord(aggBase, "info", "req took 40 ms", map[string]string{"status": "200"}),
		segment.LogRecord(aggBase.Add(time.Second), "info", "req took 45 ms", map[string]string{"status": "200"}),
		segment.LogRecord(aggBase.Add(2*time.Second), "error", "req took 1200 ms", map[string]string{"status": "503"}),
	})
	return readers(dir)
}

// TestAggregateFiltersRowsByPredicate is the oracle for row-level filtering.
//
// Every one of these returned 3 before the scan applied predicates: pruning decides
// only the predicates a descriptor's stream metadata answers, and catalog's Prune says
// so — a predicate over a field the manifest does not carry "never prunes here" and is
// "left for a later, data-level stage". There was no later stage, so a filtered request
// got an unfiltered count with no error.
func TestAggregateFiltersRowsByPredicate(t *testing.T) {
	cr, rd := filterCorpus(t)

	tests := []struct {
		name  string
		where []selector.Predicate
		want  float64
	}{
		{"no predicate", nil, 3},
		{"status eq", []selector.Predicate{{Field: "status", Op: selector.OpEq, Value: "503"}}, 1},
		{"status ne", []selector.Predicate{{Field: "status", Op: selector.OpNe, Value: "503"}}, 2},
		{"level eq", []selector.Predicate{{Field: "level", Op: selector.OpEq, Value: "error"}}, 1},
		{"status exists", []selector.Predicate{{Field: "status", Op: selector.OpExists}}, 3},
		{"message match", []selector.Predicate{{Field: "message", Op: selector.OpMatch, Value: "1200"}}, 1},
		{"message regex", []selector.Predicate{{Field: "message", Op: selector.OpRegex, Value: `took 4\d ms`}}, 2},
		{"message noregex", []selector.Predicate{{Field: "message", Op: selector.OpNoRegex, Value: `took 4\d ms`}}, 1},
		{"status anchored regex", []selector.Predicate{{Field: "status", Op: selector.OpRegex, Value: "^(?:50.)$"}}, 1},
		{"service regex prunes in", []selector.Predicate{{Field: "service", Op: selector.OpRegex, Value: "^(?:gate.*)$"}}, 3},
		{"service regex prunes out", []selector.Predicate{{Field: "service", Op: selector.OpRegex, Value: "^(?:gate)$"}}, 0},
		// The numeric discriminator: as text every duration here is "above" 100.
		{"duration gt is numeric", []selector.Predicate{{Field: segment.ColDurationMs, Op: selector.OpGt, Value: "100"}}, 1},
		{"duration lt is numeric", []selector.Predicate{{Field: segment.ColDurationMs, Op: selector.OpLt, Value: "100"}}, 2},
		// A stream-level predicate is still decided by pruning, and still right.
		{"service eq prunes in", []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "gateway"}}, 3},
		{"service eq prunes out", []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "nope"}}, 0},
		// A field no row carries cannot be satisfied, including by ne.
		{"absent field eq", []selector.Predicate{{Field: "pod", Op: selector.OpEq, Value: "any"}}, 0},
		{"absent field ne", []selector.Predicate{{Field: "pod", Op: selector.OpNe, Value: "any"}}, 0},
		{"absent field exists", []selector.Predicate{{Field: "pod", Op: selector.OpExists}}, 0},
		// Predicates compose: both must hold.
		{"two predicates", []selector.Predicate{
			{Field: "status", Op: selector.OpEq, Value: "200"},
			{Field: "level", Op: selector.OpEq, Value: "error"},
		}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sel := aggSel(time.Minute)
			sel.Where = tt.where
			env, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), tools.AggregateRequest{Selector: sel, Measure: "count", Bins: "1m"})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aggTotal(env), "filtered count")
		})
	}
}

// TestAggregateSumSkipsSegmentsWithoutTheColumn: a window spanning several streams holds
// segments that do not carry the summed column at all — the seed's incident window mixes
// gateway, db, jobs, host and scheduler, and only some emit a duration.
//
// Those segments hold no values of it, so they contribute nothing, which is the exact
// answer and not an approximation of one. Treating the missing chunk as a fatal read
// error instead made sum:<field> a 500 on any mixed window.
func TestAggregateSumSkipsSegmentsWithoutTheColumn(t *testing.T) {
	dir, store, cat := newCorpus(t)
	// Carries duration_ms: masking names the wildcard before the literal "ms".
	writeSegment(t, store, cat, "gateway", []segment.Record{
		segment.LogRecord(aggBase, "info", "req took 40 ms", nil),
		segment.LogRecord(aggBase.Add(time.Second), "info", "req took 60 ms", nil),
	})
	// Carries no duration at all.
	writeSegment(t, store, cat, "scheduler", []segment.Record{segment.LogRecord(aggBase.Add(2*time.Second), "info", "placed a pod somewhere", nil)})

	env := runAggregate(t, dir, tools.AggregateRequest{Selector: aggSel(time.Minute), Measure: "sum:" + segment.ColDurationMs, Bins: "1m"})

	assert.Equal(t, 100.0, aggTotal(env), "40 + 60, with the scheduler segment contributing nothing")
}

// TestAggregateRefusals pins requests that refuse rather than guess (§5a #4): a column no
// segment in the window carries, whether summed or ratioed, and a scanned quantile past
// MaxScanSegments, which would otherwise answer from a subset of the segments.
func TestAggregateRefusals(t *testing.T) {
	tests := []struct {
		name string
		seed func(t *testing.T, store storage.Store, cat catalog.Catalog)
		req  tools.AggregateRequest
		code string
		// suggested is a column the refusal must name as one that does exist.
		suggested string
	}{
		{
			name: "sum of a column nothing carries",
			seed: func(t *testing.T, store storage.Store, cat catalog.Catalog) {
				writeSegment(t, store, cat, "scheduler", []segment.Record{segment.LogRecord(aggBase, "info", "placed a pod somewhere", map[string]string{"host": "host-7"})})
			},
			req:  tools.AggregateRequest{Selector: aggSel(time.Minute), Measure: "sum:nonexistent_column", Bins: "1m"},
			code: query.ErrCodeUnknownField, suggested: "host",
		},
		{
			name: "ratio over a column nothing carries",
			seed: func(t *testing.T, store storage.Store, cat catalog.Catalog) {
				writeSegment(t, store, cat, "gateway", statusEvents(aggBase, "handled request", 10, 2))
			},
			req: tools.AggregateRequest{
				Selector: selector.Selector{Time: window(aggBase, aggBase.Add(time.Minute))},
				Measure:  "ratio:no_such_column", GroupBy: "service", Bins: "1m", Numerator: errStatus,
			},
			code: query.ErrCodeUnknownField, suggested: "status",
		},
		{
			name: "filtered quantile past the segment cap",
			seed: func(t *testing.T, store storage.Store, cat catalog.Catalog) {
				row := func(at time.Time, ms int, endpoint string) segment.Record {
					return aggRecord(at, fmt.Sprintf("req took %d ms", ms), "gateway", "req took <*> ms",
						segment.Column{Name: "endpoint", Type: value.TypeString, Value: value.Value{String: endpoint}},
						segment.Column{Name: segment.ColDurationMs, Type: value.TypeFloat, Value: value.Value{Float: float64(ms)}})
				}
				for i := range 6 {
					start := aggBase.Add(time.Duration(i) * time.Minute)
					writeRecords(t, store, cat, "gateway", []segment.Record{
						row(start, (i+1)*10, "/checkout"), row(start.Add(time.Second), (i+2)*10, "/billing"),
					})
				}
			},
			req: tools.AggregateRequest{
				Selector: selector.Selector{Time: window(aggBase, aggBase.Add(10*time.Minute)),
					Where: []selector.Predicate{{Field: "endpoint", Op: selector.OpEq, Value: "/checkout"}}},
				Measure: "p99", GroupBy: "service", Bins: "1m", MaxScanSegments: 2,
			},
			code: query.ErrCodeScanTooLarge,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, store, cat := newCorpus(t)
			tt.seed(t, store, cat)
			cr, rd := readers(dir)

			_, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), tt.req)

			var apiErr *query.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.code, apiErr.Code)
			if tt.suggested != "" {
				assert.Contains(t, apiErr.SuggestedFields, tt.suggested, "the refusal must name columns that do exist")
			}
		})
	}
}

// TestAggregateReadsItsSeries drives Aggregate through a fake series.Reader: each case is
// a request and the reader's answer in, and the series.Request Aggregate built and the
// envelope or error it returned out.
func TestAggregateReadsItsSeries(t *testing.T) {
	from := aggBase
	level := selector.Predicate{Field: segment.ColLevel, Op: selector.OpEq, Value: "error"}
	window := selector.Window{From: from, To: from.Add(2 * time.Minute)}
	bins := series.Bins{Width: time.Minute, From: window.From, To: window.To}
	b0, b1 := from.UnixMilli(), from.Add(time.Minute).UnixMilli()
	down := errors.New("store down")
	read := series.Result{Values: series.Values{"p1": {b0: 2, b1: 1}}, Cost: query.Cost{GETs: 3, BytesRead: 40}, Coverage: 0.5, Approx: true}
	base := tools.AggregateRequest{
		Selector: selector.Selector{Time: selector.TimeSpec{From: from.Format(time.RFC3339), To: window.To.Format(time.RFC3339)},
			Where: []selector.Predicate{level}},
		Measure: "count",
		Bins:    "1m",
	}
	want := series.Request{
		Version:  catalog.Version("5"),
		Summary:  true,
		Selector: selector.Resolved{Window: window, Where: []selector.Predicate{level}},
		Window:   window,
		Measure:  series.Measure{Kind: series.KindCount},
		Bins:     bins,
		Bound:    series.Bound{Segments: 2048, Bytes: 4 << 30},
	}

	tests := []struct {
		name     string
		mutate   func(*tools.AggregateRequest)
		result   series.Result
		readErr  error
		wantReq  series.Request
		wantData []tools.Series
		wantMeta query.Meta
		wantErr  error
	}{
		{
			name:    "a count reads its series and baselines each key",
			result:  read,
			wantReq: want,
			wantData: []tools.Series{{Key: "p1", Start: b0, Step: time.Minute.Milliseconds(),
				Values: []*float64{num(2), num(1)}, Baselines: []*float64{num(2), num(0.3*1 + 0.7*2)}}},
			wantMeta: query.Meta{Snapshot: "e5.s5", Coverage: 0.5, Approx: true, Cost: query.Cost{GETs: 3, BytesRead: 40}},
		},
		{
			name: "a ratio carries its numerator, reads rows in its bins and leaves a 0/0 bin null",
			mutate: func(r *tools.AggregateRequest) {
				r.Measure, r.Numerator, r.Selector.Where = "ratio", []selector.Predicate{level}, nil
			},
			result: series.Result{Values: series.Values{"p1": {b1: 0.5}}, Coverage: 1},
			wantReq: func() series.Request {
				r := want
				r.Selector.Where, r.Measure, r.Numerator, r.Window = nil, series.Measure{Kind: series.KindRatio}, []selector.Predicate{level}, bins.Window()
				return r
			}(),
			wantData: []tools.Series{{Key: "p1", Start: b0, Step: time.Minute.Milliseconds(),
				Values: []*float64{nil, num(0.5)}, Baselines: []*float64{nil, num(0.5)}}},
			wantMeta: query.Meta{Snapshot: "e5.s5", Coverage: 1},
		},
		{name: "a read failure comes back", readErr: down, wantReq: want, wantErr: down},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := base
			if tt.mutate != nil {
				tt.mutate(&req)
			}
			var got series.Request
			reader := series.Fake{ReadFn: func(r series.Request) (series.Result, error) {
				got = r
				return tt.result, tt.readErr
			}}
			store := summary.Fake{CatalogVersionFn: func() catalog.Version { return "5" }}
			cat := catalog.Fake{VersionFn: func() (catalog.Version, error) { return "5", nil }}

			env, err := tools.New(tools.Deps{Catalog: cat, Series: reader, Planner: plan.Fake{}, Summary: store}).Aggregate(context.Background(), req)

			assert.Equal(t, tt.wantReq, got)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			tt.wantMeta.Tokens = env.Meta.Tokens
			assert.Equal(t, query.Envelope[tools.AggregateResponse]{Data: tools.AggregateResponse{Series: tt.wantData}, Meta: tt.wantMeta}, env)
		})
	}
}

// writeTranscript seals one agent transcript row per pattern, a second apart from start,
// under session and the model the row at each index ran on.
func writeTranscript(t *testing.T, store storage.Store, cat catalog.Catalog, start time.Time, session string,
	patterns []string, model func(i int) string) {
	t.Helper()
	str := func(name, s string) segment.Column {
		return segment.Column{Name: name, Type: value.TypeString, Value: value.Value{String: s}}
	}
	records := make([]segment.Record, len(patterns))
	for i, p := range patterns {
		records[i] = aggRecord(start.Add(time.Duration(i)*time.Second), "step "+p, "agent", p,
			str("session_id", session), str("model", model(i)))
	}
	writeRecords(t, store, cat, "agent", records)
}

// TestAggregateMeasuresEachSession groups a transcript by session in one bin: the looping
// session's longest run, its first and last steps, and its one model switch, read across
// segments written out of time order.
func TestAggregateMeasuresEachSession(t *testing.T) {
	dir, store, cat := newCorpus(t)
	loop := []string{"p-start", "p-tool", "p-tool", "p-plan", "p-tool", "p-tool", "p-tool", "p-tool", "p-tool", "p-tool", "p-tool", "p-end"}
	switched := func(i int) string { return []string{"a", "b"}[min(i/6, 1)] }
	writeTranscript(t, store, cat, aggBase.Add(6*time.Second), "loop", loop[6:], func(int) string { return "b" })
	writeTranscript(t, store, cat, aggBase, "loop", loop[:6], switched)
	writeTranscript(t, store, cat, aggBase, "ok", []string{"p-start", "p-end"}, func(int) string { return "a" })

	// one is a series over the one bin aggSel(time.Minute) makes, holding v and label.
	one := func(v float64, label string) tools.Series {
		s := tools.Series{Start: aggBase.Add(-time.Minute).UnixMilli(), Step: (2 * time.Minute).Milliseconds(),
			Values: []*float64{num(v)}, Baselines: []*float64{num(v)}}
		if label != "" {
			s.Labels = []string{label}
		}
		return s
	}
	for _, tc := range []struct {
		measure string
		want    map[string]tools.Series
	}{
		{"max_run:pattern_id", map[string]tools.Series{"loop": one(7, "p-tool"), "ok": one(1, "p-start")}},
		{"first:pattern_id", map[string]tools.Series{"loop": one(0, "p-start"), "ok": one(0, "p-start")}},
		{"last:pattern_id", map[string]tools.Series{"loop": one(0, "p-end"), "ok": one(0, "p-end")}},
		{"changes:model", map[string]tools.Series{"loop": one(1, ""), "ok": one(0, "")}},
	} {
		t.Run(tc.measure, func(t *testing.T) {
			req := tools.AggregateRequest{Selector: aggSel(time.Minute), Measure: tc.measure, GroupBy: "session_id", Bins: "all"}
			env := runAggregate(t, dir, req)
			got := map[string]tools.Series{}
			for _, s := range env.Data.Series {
				key := s.Key
				s.Key = ""
				got[key] = s
			}
			assert.Equal(t, tc.want, got)
			again, err := json.Marshal(runAggregate(t, dir, req))
			require.NoError(t, err)
			first, err := json.Marshal(env)
			require.NoError(t, err)
			assert.Equal(t, string(first), string(again), "the same request and snapshot must answer byte-identically")
		})
	}
}

// TestAggregatePagesGroups pages the groups one at a time by key.
func TestAggregatePagesGroups(t *testing.T) {
	dir, store, cat := newCorpus(t)
	for i, session := range []string{"s1", "s2"} {
		writeTranscript(t, store, cat, aggBase.Add(time.Duration(i)*time.Second), session, []string{"p-start"}, func(int) string { return "a" })
	}
	req := tools.AggregateRequest{Selector: aggSel(time.Minute), Measure: "count", GroupBy: "session_id", Bins: "all", Sort: "key", Limit: 1}
	var keys []string
	for {
		env := runAggregate(t, dir, req)
		for _, s := range env.Data.Series {
			keys = append(keys, s.Key)
		}
		if req.Cursor = env.Meta.Cursor; req.Cursor == "" {
			break
		}
		require.Less(t, len(keys), 2, "the last page must not carry a cursor")
	}
	assert.Equal(t, []string{"s1", "s2"}, keys)
}

// TestAggregateRefusesUnboundedHighCardinalityGroups groups by a column with more values
// than a footer counts, which without a time range has no bound on its groups.
func TestAggregateRefusesUnboundedHighCardinalityGroups(t *testing.T) {
	dir, store, cat := newCorpus(t)
	records := make([]segment.Record, 1100)
	for i := range records {
		records[i] = aggRecord(aggBase.Add(time.Duration(i)*time.Millisecond), "step", "agent", "p-step",
			segment.Column{Name: "session_id", Type: value.TypeString, Value: value.Value{String: fmt.Sprintf("s-%04d", i)}})
	}
	writeRecords(t, store, cat, "agent", records)

	cr, rd := readers(dir)
	_, err := toolsOver(cr, rd, nil).Aggregate(context.Background(),
		tools.AggregateRequest{Measure: "max_run:pattern_id", GroupBy: "session_id", Bins: "all"})
	var apiErr *query.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, query.ErrCodeUnboundedScan, apiErr.Code)

	env := runAggregate(t, dir, tools.AggregateRequest{Selector: aggSel(time.Minute), Measure: "max_run:pattern_id", GroupBy: "session_id", Bins: "all"})
	assert.Len(t, env.Data.Series, 100, "a bounded request answers, a page of groups at a time")
}

// TestAggregateRollsUpSessions rolls a transcript's sessions up across them, each answer
// against a hand count: two sessions a day, s1 and s3 and s4 looping three steps or more.
func TestAggregateRollsUpSessions(t *testing.T) {
	dir, store, cat := newCorpus(t)
	for _, s := range []struct {
		session  string
		day      int
		patterns []string
		input    []int64
		output   []int64
	}{
		{"s1", 0, []string{"a", "b", "b", "b", "c"}, []int64{100, 300, 200, 50, 10}, []int64{5, 5, 5, 5, 5}},
		{"s2", 0, []string{"a", "b", "c"}, []int64{1000, 20, 30}, []int64{1, 2, 3}},
		{"s3", 1, []string{"a", "a", "a", "a"}, []int64{50, 60, 70, 80}, []int64{10, 10, 10, 10}},
		{"s4", 1, []string{"a", "a", "a"}, []int64{5000, 1, 2}, []int64{100, 0, 0}},
	} {
		records := make([]segment.Record, len(s.patterns))
		for i, p := range s.patterns {
			records[i] = aggRecord(aggBase.Add(time.Duration(s.day)*24*time.Hour+time.Duration(i)*time.Second), "step "+p, "agent", p,
				segment.Column{Name: "session_id", Type: value.TypeString, Value: value.Value{String: s.session}},
				segment.Column{Name: "input_tokens", Type: value.TypeInt, Value: value.Value{Int: s.input[i]}},
				segment.Column{Name: "output_tokens", Type: value.TypeInt, Value: value.Value{Int: s.output[i]}})
		}
		writeRecords(t, store, cat, "agent", records)
	}

	// A rollup's grid is its own bins over the window, a day's or the whole window's.
	day0, day, all := aggBase.Add(-time.Minute).UnixMilli(), (24 * time.Hour).Milliseconds(), (48*time.Hour + time.Minute).Milliseconds()
	for _, tc := range []struct {
		name string
		req  tools.AggregateRequest
		want []tools.Series
	}{
		{"histogram of each session's peak input", tools.AggregateRequest{Measure: "max:input_tokens",
			Rollup: &selector.Rollup{Measure: "values[0]", Buckets: []float64{100, 1000}}},
			[]tools.Series{
				{Key: "[-Inf,100)", Start: day0, Step: all, Values: []*float64{num(1)}, Baselines: []*float64{num(1)}},
				{Key: "[100,1000)", Start: day0, Step: all, Values: []*float64{num(1)}, Baselines: []*float64{num(1)}},
				{Key: "[1000,+Inf)", Start: day0, Step: all, Values: []*float64{num(2)}, Baselines: []*float64{num(2)}},
			}},
		{"share of sessions looping per day", tools.AggregateRequest{Measures: []selector.MeasureSpec{{Measure: "max_run:pattern_id"}},
			Rollup: &selector.Rollup{Measure: "ratio", Where: []selector.HavingClause{{Index: 0, Op: selector.OpGe, Value: 3}}, Bins: "1d"}},
			[]tools.Series{{Start: day0, Step: day, Values: []*float64{num(0.5), num(1), nil},
				Baselines: []*float64{num(0.5), num(0.65), nil}}}},
		{"median session's output", tools.AggregateRequest{Measure: "sum:output_tokens", Rollup: &selector.Rollup{Measure: "p50:values[0]"}},
			[]tools.Series{{Start: day0, Step: all, Values: []*float64{num(25)}, Baselines: []*float64{num(25)}}}},
		{"sessions by their first step", tools.AggregateRequest{Measure: "first:pattern_id",
			Rollup: &selector.Rollup{Measure: "count", GroupBy: "label[0]", Bins: "1d"}},
			[]tools.Series{{Key: "a", Start: day0, Step: day, Values: []*float64{num(2), num(2), nil}, Baselines: []*float64{num(2), num(2), nil}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.req.Selector, tc.req.GroupBy, tc.req.Bins = aggSel(48*time.Hour), "session_id", "all"
			env := runAggregate(t, dir, tc.req)
			assert.Equal(t, tc.want, env.Data.Series)
			first, err := json.Marshal(env)
			require.NoError(t, err)
			again, err := json.Marshal(runAggregate(t, dir, tc.req))
			require.NoError(t, err)
			assert.Equal(t, string(first), string(again), "the same request and snapshot must answer byte-identically")
		})
	}
}

// TestAggregateBinsByPosition bins a transcript by user turn: session k has its turns' tokens
// k*1000+n on the assistant row answering turn n, after a reply to no turn, which counts
// at no position. s4 is a turn short of 40.
func TestAggregateBinsByPosition(t *testing.T) {
	dir, store, cat := newCorpus(t)
	turns := map[string]int{"s1": 41, "s2": 40, "s3": 45, "s4": 39}
	for k, session := range []string{"s4", "s3", "s2", "s1"} {
		str := func(name, s string) segment.Column {
			return segment.Column{Name: name, Type: value.TypeString, Value: value.Value{String: s}}
		}
		reply := func(i int, tokens int64) segment.Record {
			return aggRecord(aggBase.Add(time.Duration(7*k+i)*time.Second), "reply", "agent", "reply", str("session_id", session), str("type", "assistant"),
				segment.Column{Name: "message.usage.input_tokens", Type: value.TypeInt, Value: value.Value{Int: tokens}})
		}
		records := []segment.Record{reply(0, 1e6)}
		for n := 1; n <= turns[session]; n++ {
			records = append(records, aggRecord(aggBase.Add(time.Duration(7*k+2*n-1)*time.Second), "ask", "agent", "ask", str("session_id", session), str("type", "user")),
				reply(2*n, int64(1000*int(session[1]-'0')+n)))
		}
		writeRecords(t, store, cat, "agent", records)
	}

	req := tools.AggregateRequest{Selector: aggSel(time.Hour), Measure: "p50:message.usage.input_tokens", Bins: "1",
		OrdinalOf: "session_id", OrdinalWhere: []selector.Predicate{{Field: "type", Op: selector.OpEq, Value: "user"}}}
	env := runAggregate(t, dir, req)
	require.Len(t, env.Data.Series, 1)
	s := env.Data.Series[0]
	assert.Equal(t, []int64{1, 1}, []int64{s.Start, s.Step}, "position 1 on, a position a bin")
	require.Len(t, s.Values, 45, "a bin per position up to the longest session's last")
	assert.Equal(t, 2040.0, *s.Values[39], "the median of s1, s2 and s3's 40th turn")
	assert.Equal(t, 1041.0, *s.Values[40], "s1 and s3 reach turn 41, s2 and s4 add nothing")
	assert.Equal(t, 3045.0, *s.Values[44])
	first, err := json.Marshal(env)
	require.NoError(t, err)
	again, err := json.Marshal(runAggregate(t, dir, req))
	require.NoError(t, err)
	assert.Equal(t, string(first), string(again), "the same request and snapshot must answer byte-identically")

	req.Measure, req.GroupBy, req.Bins = "count", "session_id", "10"
	env = runAggregate(t, dir, req)
	i := slices.IndexFunc(env.Data.Series, func(s tools.Series) bool { return s.Key == "s4" })
	require.GreaterOrEqual(t, i, 0)
	s = env.Data.Series[i]
	assert.Equal(t, []int64{1, 10}, []int64{s.Start, s.Step})
	assert.Equal(t, []float64{20, 20, 20, 18, 0}, present(s.Values),
		"a turn and its reply per position, ten positions a bin, the bins running to the longest session's")
}

// TestAggregateSpanOfTsBoundsATracesDuration measures each trace's extent, its last line's
// time less its first's, with span:ts, and keeps by having only the traces of 400ms or more.
func TestAggregateSpanOfTsBoundsATracesDuration(t *testing.T) {
	dir, store, cat := newCorpus(t)
	var events []segment.Record
	for i := range 5 { // t-i's 3 lines are i*100ms apart, so it lasts i*200ms
		for line := range 3 {
			at := aggBase.Add(time.Duration(i*1000+line*i*100) * time.Millisecond)
			events = append(events, segment.LogRecord(at, "info", "step", map[string]string{"trace_id": fmt.Sprintf("t-%d", i)}))
		}
	}
	writeSegment(t, store, cat, "api", events)
	req := tools.AggregateRequest{Selector: aggSel(time.Minute), Measure: "span:ts", GroupBy: "trace_id", Bins: "all"}
	assert.Equal(t, map[string]float64{"t-0": 0, "t-1": 200, "t-2": 400, "t-3": 600, "t-4": 800}, aggTotals(runAggregate(t, dir, req)))
	req.Having = []selector.HavingClause{{Index: 0, Op: selector.OpGe, Value: 400}}
	assert.Equal(t, map[string]float64{"t-2": 400, "t-3": 600, "t-4": 800}, aggTotals(runAggregate(t, dir, req)))
}
