package logql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query/lang"
	"github.com/datafocal/datafocal/internal/selector"
)

func agg(kind, measure, groupBy, bins string, where ...selector.Predicate) selector.Query {
	return selector.Query{Where: where, Stage: &selector.Stage{Kind: kind, Measure: measure, GroupBy: groupBy, Bins: bins}}
}

// rollup is a per-session max of input_tokens, or for a count of pattern_id's max_run,
// rolled up by measure in bins.
func rollup(measure, bins string) selector.Query {
	inner := "max:input_tokens"
	if measure == "count" {
		inner = "max_run:pattern_id"
	}
	q := agg(selector.StageAggregate, inner, "session_id", "all", selector.Predicate{Field: "job", Op: selector.OpEq, Value: "agent"})
	q.Stage.Rollup = &selector.Rollup{Measure: measure, Bins: bins}
	return q
}

// TestParseRoundTrips parses each construct and Loki documentation queries inside the
// subset, then requires the canonical form to parse back to the same query.
func TestParseRoundTrips(t *testing.T) {
	mysql := selector.Predicate{Field: "job", Op: selector.OpEq, Value: "mysql"}
	app := selector.Predicate{Field: "app", Op: selector.OpEq, Value: "x"}
	top := agg(selector.StageTop, "rate", "name", "5m", selector.Predicate{Field: "region", Op: selector.OpEq, Value: "us-east1"})
	top.Stage.K = 10
	for _, tc := range []struct {
		q    string
		want selector.Query
	}{
		{`{service="checkout", host=~"host-"}`, selector.Query{Where: []selector.Predicate{
			{Field: "service", Op: selector.OpEq, Value: "checkout"}, {Field: "host", Op: selector.OpRegex, Value: "^(?:host-)$"}}}},
		{`{job="mysql"} |= "error" != "timeout" |~ "x" !~ "y"`, selector.Query{Where: []selector.Predicate{mysql,
			{Field: "message", Op: selector.OpMatch, Value: "error"}, {Field: "message", Op: selector.OpNoMatch, Value: "timeout"},
			{Field: "message", Op: selector.OpRegex, Value: "x"}, {Field: "message", Op: selector.OpNoRegex, Value: "y"}}}},
		{"{name!~`mysql.+`} | json | logfmt | unpack | status >= 500, size < 2.5", selector.Query{Where: []selector.Predicate{
			{Field: "name", Op: selector.OpNoRegex, Value: "^(?:mysql.+)$"}, {Field: "status", Op: selector.OpGe, Value: "500"}, {Field: "size", Op: selector.OpLt, Value: "2.5"}}}},
		{`{job="mysql"} | level="error" or level=="warn" | host != "a"`, selector.Query{Where: []selector.Predicate{mysql,
			{Any: []selector.Predicate{{Field: "level", Op: selector.OpEq, Value: "error"}, {Field: "level", Op: selector.OpEq, Value: "warn"}}},
			{Field: "host", Op: selector.OpNe, Value: "a"}}}},
		{`{job="mysql"} | patterns`, selector.Query{Where: []selector.Predicate{mysql}, Stage: &selector.Stage{Kind: selector.StagePatterns}}},
		{`{job="mysql"} | level="error" | explain`, selector.Query{Where: []selector.Predicate{mysql, {Field: "level", Op: selector.OpEq, Value: "error"}},
			Stage: &selector.Stage{Kind: selector.StageExplain}}},
		{`{trace_id="abc"} | trace`, selector.Query{Where: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: "abc"}},
			Stage: &selector.Stage{Kind: selector.StageTrace}}},
		{`count_over_time({job="mysql"}[5m])`, agg(selector.StageAggregate, "count", "", "5m", mysql)},
		{`count_over_time(({job="mysql"} | json)[5m])`, agg(selector.StageAggregate, "count", "", "5m", mysql)},
		{`sum by (host) (rate({job="mysql"} |= "error" | json | duration > 10 [1m]))`, agg(selector.StageAggregate, "rate", "host", "1m",
			mysql, selector.Predicate{Field: "message", Op: selector.OpMatch, Value: "error"}, selector.Predicate{Field: "duration", Op: selector.OpGt, Value: "10"})},
		{`sum(count_over_time({job="mysql"}[5m])) by (level)`, agg(selector.StageAggregate, "count", "level", "5m", mysql)},
		{`quantile_over_time(0.95, {job="mysql"} | unwrap duration_ms [5m]) by (route)`,
			agg(selector.StageAggregate, "p95:duration_ms", "route", "5m", mysql)},
		{`sum_over_time({job="mysql"} | unwrap bytes [1h])`, agg(selector.StageAggregate, "sum:bytes", "", "1h", mysql)},
		{`sum_over_time({job="agent"} | unwrap len(text) [1h]) by (type)`, agg(selector.StageAggregate, "sum:len(text)", "type", "1h", selector.Predicate{Field: "job", Op: selector.OpEq, Value: "agent"})},
		{`quantile_over_time(0.5, {job="agent"} | unwrap bytes(body) [1h])`, agg(selector.StageAggregate, "p50:bytes(body)", "", "1h", selector.Predicate{Field: "job", Op: selector.OpEq, Value: "agent"})},
		{`topk(10,sum(rate({region="us-east1"}[5m])) by (name))`, top},
		{`max_run_over_time({job="agent"} | unwrap pattern_id [all]) by (session_id)`,
			agg(selector.StageAggregate, "max_run:pattern_id", "session_id", "all", selector.Predicate{Field: "job", Op: selector.OpEq, Value: "agent"})},
		{`first_over_time({job="mysql"} | unwrap level [5m])`, agg(selector.StageAggregate, "first:level", "", "5m", mysql)},
		{`sort_desc(sum by (trace_id) (count_over_time({job="mysql"} | level="error" [all])) > 0), max_over_time({job="mysql"} | unwrap ts [all]) by (trace_id)`,
			selector.Query{Where: []selector.Predicate{mysql}, Stage: &selector.Stage{Kind: selector.StageAggregate, GroupBy: "trace_id", Bins: "all",
				Measures: []selector.MeasureSpec{{Measure: "count", Where: []selector.Predicate{{Field: "level", Op: selector.OpEq, Value: "error"}}}, {Measure: "max:ts", Where: []selector.Predicate{}}},
				Having:   []selector.HavingClause{{Op: selector.OpGt}}, Sort: "0:desc"}}},
		{`quantile_over_time(0.5, max_over_time({job="agent"} | unwrap input_tokens [all]) by (session_id))`, rollup("p50:values[0]", "")},
		{`count_over_time(max_run_over_time({job="agent"} | unwrap pattern_id [all]) by (session_id) [1d])`, rollup("count", "1d")},
		{`quantile_over_time(0.5, {job="agent"} | ordinal by (session_id) | unwrap input_tokens [1])`, selector.Query{
			Where: []selector.Predicate{{Field: "job", Op: selector.OpEq, Value: "agent"}},
			Stage: &selector.Stage{Kind: selector.StageAggregate, Measure: "p50:input_tokens", Bins: "1", OrdinalOf: "session_id"}}},
		{`{app="x"} | message.role="user"`, selector.Query{Where: []selector.Predicate{app, {Field: "message.role", Op: selector.OpEq, Value: "user"}}}},
		{`{app="x"} | compact-revision > 3`, selector.Query{Where: []selector.Predicate{app, {Field: "compact-revision", Op: selector.OpGt, Value: "3"}}}},
		{`{service=~".+"}`, selector.Query{}},
		{`{service=~".*"} |= "x"`, selector.Query{Where: []selector.Predicate{{Field: "message", Op: selector.OpMatch, Value: "x"}}}},
		{`{service=~"api.+"}`, selector.Query{Where: []selector.Predicate{{Field: "service", Op: selector.OpRegex, Value: "^(?:api.+)$"}}}},
		{`{k8s.pod="api-1"}`, selector.Query{Where: []selector.Predicate{{Field: "k8s.pod", Op: selector.OpEq, Value: "api-1"}}}},
		{`sum by (message.role) (count_over_time({app="x"}[1m]))`, agg(selector.StageAggregate, "count", "message.role", "1m", app)},
		{`sum_over_time({app="x"} | unwrap message.usage.output_tokens [5m])`, agg(selector.StageAggregate, "sum:message.usage.output_tokens", "", "5m", app)},
	} {
		t.Run(tc.q, func(t *testing.T) {
			got, err := New().Parse(tc.q)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			canonical, err := New().Format(got)
			require.NoError(t, err)
			again, err := New().Parse(canonical)
			require.NoError(t, err, canonical)
			assert.Equal(t, got, again, canonical)
		})
	}
}

// TestParseRejects requires each unsupported construct's error to point at it and name it.
func TestParseRejects(t *testing.T) {
	for _, tc := range []struct {
		q    string
		want lang.ParseError
	}{
		{`{a="b"} | line_format "{{.x}}"`, lang.ParseError{Offset: 10, Msg: "the line_format stage is not supported"}},
		{`{a="b"} | label_format x=y`, lang.ParseError{Offset: 10, Msg: "the label_format stage is not supported"}},
		{`{a="b"} | pattern "<_> x"`, lang.ParseError{Offset: 10, Msg: "the pattern stage is not supported"}},
		{`sum by (a, b) (rate({a="b"}[5m]))`, lang.ParseError{Offset: 9, Msg: "more than one grouping label is not supported"}},
		{`rate({a="b"}[5m]) / rate({a="c"}[5m])`, lang.ParseError{Offset: 18, Msg: "binary operations between queries are not supported"}},
		{`{a="b"} | patterns | explain`, lang.ParseError{Offset: 19, Msg: `"|" follows the last stage; stages do not chain`}},
		{`count_over_time({a="b"} | patterns [5m])`, lang.ParseError{Offset: 26, Msg: "the patterns stage cannot be inside a metric query"}},
		{`{a>"b"}`, lang.ParseError{Offset: 2, Msg: `expected =, !=, =~ or !~ in a stream selector, found ">"`}},
		{`avg_over_time({a="b"}[5m])`, lang.ParseError{Offset: 0, Msg: "the avg_over_time function is not supported"}},
		{`{a=~"("}`, lang.ParseError{Offset: 4, Msg: "invalid regex: error parsing regexp: missing closing ): `(`"}},
		{`{a="b`, lang.ParseError{Offset: 5, Msg: "literal not terminated"}},
		{`{a="b"} | ordinal by (c)`, lang.ParseError{Offset: 24, Msg: "unwrap and ordinal are only allowed inside a range aggregation"}},
		{`count_over_time({a="b"} | ordinal [1])`, lang.ParseError{Offset: 34, Msg: `expected by (<label>) after ordinal, found "["`}},
		{`max_over_time(count_over_time({a="b"}[5m]))`, lang.ParseError{Offset: 0, Msg: "the max_over_time function cannot roll up an aggregation"}},
		{`count_over_time(count_over_time(count_over_time({a="b"}[5m])))`, lang.ParseError{Offset: 0, Msg: "more than two levels of aggregation are not supported"}},
		{`sort(count_over_time(count_over_time({a="b"}[5m])))`, lang.ParseError{Offset: 0, Msg: "a rollup cannot be sorted, compared or listed with other measures"}},
	} {
		t.Run(tc.q, func(t *testing.T) {
			_, err := New().Parse(tc.q)
			var perr *lang.ParseError
			require.ErrorAs(t, err, &perr)
			assert.Equal(t, tc.want, *perr)
		})
	}
}

// FuzzParseRoundTrips requires every query Parse accepts to print, and its canonical form
// to parse back to the same query.
func FuzzParseRoundTrips(f *testing.F) {
	for _, q := range []string{
		`{service="checkout", host=~"host-"} |= "error" != "timeout" | json | status >= 500, size < 2.5`,
		`{job="mysql"} | level="error" or level=~"warn|info" | message!~"(?i)debug" | explain`,
		"{trace_id=`abc`} | trace",
		`sum by (host) (rate({job="mysql"} |~ "x" !~ "y" [1m]))`,
		`topk(10, quantile_over_time(0.95, {job="mysql"} | unwrap duration_ms [5m]) by (route))`,
		`sum by (type) (sum_over_time({job="agent"} | unwrap len(text) [1h]))`,
	} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, text string) {
		parsed, err := New().Parse(text)
		if err != nil {
			return
		}
		canonical, err := New().Format(parsed)
		require.NoError(t, err)
		again, err := New().Parse(canonical)
		require.NoError(t, err, canonical)
		assert.Equal(t, parsed, again, canonical)
	})
}

// TestFormatWidensUnanchoredLabelMatches: a label's substring match or unanchored regex,
// which Parse never builds, prints as the anchored regex that selects the same values.
func TestFormatWidensUnanchoredLabelMatches(t *testing.T) {
	got, err := New().Format(selector.Query{Where: []selector.Predicate{
		{Field: "host", Op: selector.OpMatch, Value: "a.b"}, {Field: "pod", Op: selector.OpNoRegex, Value: "x|y"}}})
	require.NoError(t, err)
	assert.Equal(t, `{host=~"(?s:.*)a\\.b(?s:.*)", pod!~"(?s:.*)(?:x|y)(?s:.*)"}`, got)
}
