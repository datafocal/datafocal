package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query/lang"
	"github.com/datafocal/datafocal/internal/query/lang/logql"
	"github.com/datafocal/datafocal/internal/selector"
)

func agg(kind, measure, groupBy, bins string, where ...selector.Predicate) selector.Query {
	return selector.Query{Where: where, Stage: &selector.Stage{Kind: kind, Measure: measure, GroupBy: groupBy, Bins: bins}}
}

// TestParseRoundTrips parses each construct, the LogQL tests' queries among them, then
// requires the canonical form to parse back to the same query.
func TestParseRoundTrips(t *testing.T) {
	mysql := selector.Predicate{Field: "job", Op: selector.OpEq, Value: "mysql"}
	top := agg(selector.StageTop, "rate", "name", "5m", selector.Predicate{Field: "region", Op: selector.OpEq, Value: "us-east1"})
	top.Stage.K = 10
	for _, tc := range []struct {
		q    string
		want selector.Query
	}{
		{`SELECT * FROM logs`, selector.Query{}},
		{`select * from logs where service = 'checkout' and regexp_like(host, '^(?:host-)$') order by ts desc limit 100`, selector.Query{Where: []selector.Predicate{
			{Field: "service", Op: selector.OpEq, Value: "checkout"}, {Field: "host", Op: selector.OpRegex, Value: "^(?:host-)$"}}}},
		{`SELECT * FROM logs WHERE job = 'mysql' AND message LIKE '%error%' AND message NOT LIKE '%timeout%' AND regexp_like(message, 'x') AND NOT regexp_like(message, 'y')`,
			selector.Query{Where: []selector.Predicate{mysql, {Field: "message", Op: selector.OpMatch, Value: "error"}, {Field: "message", Op: selector.OpNoMatch, Value: "timeout"},
				{Field: "message", Op: selector.OpRegex, Value: "x"}, {Field: "message", Op: selector.OpNoRegex, Value: "y"}}}},
		{`SELECT * FROM logs WHERE NOT regexp_like(name, 'mysql.+') AND status >= 500 AND size < 2.5 AND n <> '007'`, selector.Query{Where: []selector.Predicate{
			{Field: "name", Op: selector.OpNoRegex, Value: "mysql.+"}, {Field: "status", Op: selector.OpGe, Value: "500"},
			{Field: "size", Op: selector.OpLt, Value: "2.5"}, {Field: "n", Op: selector.OpNe, Value: "007"}}}},
		{`SELECT * FROM logs WHERE job = 'mysql' AND (level = 'error' OR level = 'warn') AND host != 'a'`, selector.Query{Where: []selector.Predicate{mysql,
			{Any: []selector.Predicate{{Field: "level", Op: selector.OpEq, Value: "error"}, {Field: "level", Op: selector.OpEq, Value: "warn"}}},
			{Field: "host", Op: selector.OpNe, Value: "a"}}}},
		{`SELECT * FROM logs WHERE level IN ('error', 'warn') OR host LIKE 'web-_.%' AND msg NOT LIKE '%it''s%' AND trace_id IS NOT NULL`, selector.Query{Where: []selector.Predicate{
			{Any: []selector.Predicate{{Field: "level", Op: selector.OpEq, Value: "error"}, {Field: "level", Op: selector.OpEq, Value: "warn"},
				{Field: "host", Op: selector.OpRegex, Value: `(?s)^web-.\..*$`}}},
			{Field: "msg", Op: selector.OpNoMatch, Value: "it's"}, {Field: "trace_id", Op: selector.OpExists}}}},
		{`SELECT * FROM logs WHERE regexp_like(m, '^a.b$')`, selector.Query{Where: []selector.Predicate{{Field: "m", Op: selector.OpRegex, Value: `^a.b$`}}}},
		{`SELECT * FROM patterns(logs) WHERE job = 'mysql'`, selector.Query{Where: []selector.Predicate{mysql}, Stage: &selector.Stage{Kind: selector.StagePatterns}}},
		{`SELECT * FROM explain(logs) WHERE job = 'mysql' AND level = 'error'`, selector.Query{Where: []selector.Predicate{mysql, {Field: "level", Op: selector.OpEq, Value: "error"}},
			Stage: &selector.Stage{Kind: selector.StageExplain}}},
		{`SELECT * FROM traces(logs) WHERE trace_id = 'abc'`, selector.Query{Where: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: "abc"}},
			Stage: &selector.Stage{Kind: selector.StageTrace}}},
		{`SELECT count(*) FROM logs WHERE job = 'mysql' GROUP BY bin(ts, 5m)`, agg(selector.StageAggregate, "count", "", "5m", mysql)},
		{`SELECT host, rate(*) FROM logs WHERE job = 'mysql' AND message LIKE '%error%' AND duration > 10 GROUP BY host, bin(ts, 1m)`,
			agg(selector.StageAggregate, "rate", "host", "1m", mysql, selector.Predicate{Field: "message", Op: selector.OpMatch, Value: "error"},
				selector.Predicate{Field: "duration", Op: selector.OpGt, Value: "10"})},
		{`SELECT route, quantile(0.95)(duration_ms) FROM logs WHERE job = 'mysql' GROUP BY route, bin(ts, 5m)`,
			agg(selector.StageAggregate, "p95:duration_ms", "route", "5m", mysql)},
		{`SELECT sum(bytes) FROM logs WHERE job = 'mysql' GROUP BY bin(ts, 1h)`, agg(selector.StageAggregate, "sum:bytes", "", "1h", mysql)},
		{`SELECT name, rate(*) FROM logs WHERE region = 'us-east1' GROUP BY name, bin(ts, 5m) ORDER BY rate(*) DESC LIMIT 10`, top},
		{`SELECT session_id, max_run(pattern_id) FROM logs WHERE job = 'agent' GROUP BY session_id`,
			agg(selector.StageAggregate, "max_run:pattern_id", "session_id", "all", selector.Predicate{Field: "job", Op: selector.OpEq, Value: "agent"})},
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

// TestLogQLRoundTrips requires a LogQL query printed as SQL to parse to the same query,
// and printed back as LogQL to parse to it again.
func TestLogQLRoundTrips(t *testing.T) {
	for _, q := range []string{
		`{job="mysql", host=~"h"} |= "error" != "timeout" |~ "x.+" !~ "y" | status >= 500 | level="error" or level="warn"`,
		`{trace_id="abc"} | trace`,
		`sum by (host) (rate({job="mysql"} |= "error" [1m]))`,
		`quantile_over_time(0.99, {job="mysql"} | unwrap duration_ms [5m]) by (route)`,
		`topk(3, sum(count_over_time({job="mysql"}[1h])) by (level))`,
	} {
		t.Run(q, func(t *testing.T) {
			want, err := logql.New().Parse(q)
			require.NoError(t, err)
			s, err := New().Format(want)
			require.NoError(t, err)
			got, err := New().Parse(s)
			require.NoError(t, err, s)
			assert.Equal(t, want, got, s)
			l, err := logql.New().Format(got)
			require.NoError(t, err)
			back, err := logql.New().Parse(l)
			require.NoError(t, err, l)
			assert.Equal(t, want, back, l)
		})
	}
}

// TestParseRejects requires each unsupported construct's error to point at it and name it.
func TestParseRejects(t *testing.T) {
	for _, tc := range []struct {
		q    string
		want lang.ParseError
	}{
		{`SELECT * FROM logs JOIN spans ON a = b`, lang.ParseError{Offset: 19, Msg: "joins are not supported"}},
		{`SELECT * FROM logs, spans`, lang.ParseError{Offset: 18, Msg: "joins are not supported"}},
		{`SELECT * FROM (SELECT * FROM logs)`, lang.ParseError{Offset: 15, Msg: "subqueries are not supported"}},
		{`SELECT * FROM logs WHERE a IN (SELECT b FROM logs)`, lang.ParseError{Offset: 31, Msg: "subqueries are not supported"}},
		{`SELECT count(*) OVER (PARTITION BY a) FROM logs`, lang.ParseError{Offset: 16, Msg: "window functions are not supported"}},
		{`SELECT a, b, count(*) FROM logs GROUP BY a, b`, lang.ParseError{Offset: 10, Msg: "more than one grouping column is not supported"}},
		{`SELECT a, count(*) FROM logs GROUP BY a, b`, lang.ParseError{Offset: 41, Msg: "more than one grouping column is not supported"}},
		{`SELECT count(*), sum(x) FROM logs`, lang.ParseError{Offset: 17, Msg: "more than one measure is not supported"}},
		{`SELECT * FROM spans`, lang.ParseError{Offset: 14, Msg: "unknown table spans; the one table is logs"}},
		{`SELECT * FROM entities(logs, user_id)`, lang.ParseError{Offset: 14, Msg: "unknown table entities; the one table is logs"}},
		{`SELECT count(*) FROM patterns(logs)`, lang.ParseError{Offset: 21, Msg: `the patterns stage cannot be aggregated`}},
		{`SELECT a, count(*) FROM logs GROUP BY b`, lang.ParseError{Offset: 7, Msg: "the selected column must be the GROUP BY column"}},
		{`SELECT * FROM logs WHERE (a = 1 AND b = 2)`, lang.ParseError{Offset: 32, Msg: `expected ), found "AND"`}},
		{`SELECT * FROM logs WHERE a = 'b`, lang.ParseError{Offset: 31, Msg: "string not terminated"}},
		{`SELECT * FROM logs WHERE regexp_like(a, '(')`, lang.ParseError{Offset: 40, Msg: "invalid regex: error parsing regexp: missing closing ): `(`"}},
	} {
		t.Run(tc.q, func(t *testing.T) {
			_, err := New().Parse(tc.q)
			var perr *lang.ParseError
			require.ErrorAs(t, err, &perr)
			assert.Equal(t, tc.want, *perr)
		})
	}
}
