package definitions

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "rewrite the normalized goldens under testdata")

var limits = Limits{Retention: 90 * 24 * time.Hour, FineHorizon: 24 * time.Hour}

// TestParseNormalizes pins each file's normalized form, which parses back to the same
// documents, and the rewrites a Loki conversion notes.
func TestParseNormalizes(t *testing.T) {
	for _, tc := range []struct {
		file, golden string
		notes        [][]string
		unbound      bool
	}{
		{file: "event-ratio.yaml", golden: "event-ratio.golden.yaml", notes: [][]string{nil, nil, nil}},
		// Files already normalized, so each is its own golden.
		{file: "latency.yaml", golden: "latency.yaml", notes: [][]string{nil, nil}},
		{file: "timeslices.yaml", golden: "timeslices.yaml", notes: [][]string{nil, nil}},
		{file: "absence.yaml", golden: "absence.yaml", notes: [][]string{nil}},
		{file: "nobl9-loki.yaml", golden: "nobl9-loki.golden.yaml", notes: [][]string{{
			`dropped line_format "{{.log}}" before | json, since ingest already strips a container runtime's JSON wrapper`,
			"dropped the outer count(…) and the range aggregation, counting the log query's events: Loki's count counts series, which as a ratio is no SLI",
			"added the total query's predicates to the good query, so it narrows total",
		}}},
		// A foreign source is kept verbatim, so its file is already normalized.
		{file: "prometheus.yaml", golden: "prometheus.yaml", notes: [][]string{nil}, unbound: true},
		// Every panel kind, variables with and without All, and a hand-set grid that overlaps.
		{file: "view.yaml", golden: "view.golden.yaml", notes: [][]string{nil, nil}},
	} {
		t.Run(tc.file, func(t *testing.T) {
			content, err := os.ReadFile(filepath.Join("testdata", tc.file))
			require.NoError(t, err)
			docs, errs := Parse(content, nil, limits)
			require.Empty(t, errs)
			got, err := Format(docs, FormatYAML)
			require.NoError(t, err)
			golden := filepath.Join("testdata", tc.golden)
			if *update && tc.golden != tc.file {
				require.NoError(t, os.WriteFile(golden, got, 0o644))
			}
			want, err := os.ReadFile(golden)
			require.NoError(t, err)
			assert.Equal(t, string(want), string(got))

			var notes [][]string
			for _, d := range docs {
				notes = append(notes, d.Notes)
			}
			assert.Equal(t, tc.notes, notes)
			assert.Equal(t, tc.unbound, docs[0].SLO.Unbound)

			again, errs := Parse(got, nil, limits)
			require.Empty(t, errs)
			for i := range again {
				again[i].Notes = docs[i].Notes
			}
			assert.Equal(t, docs, again, "the normalized form parses to the same documents")
		})
	}
}

// TestParseRejects: each invalid document is rejected on its own, naming its field, and the
// valid one beside it loads.
func TestParseRejects(t *testing.T) {
	valid, err := os.ReadFile(filepath.Join("testdata", "latency.yaml"))
	require.NoError(t, err)
	slo := func(window, objective, query, extra string) string {
		return `apiVersion: openslo/v1
kind: SLO
metadata: {name: bad}
spec:
  service: checkout
  timeWindow: [` + window + `]
  objectives: [` + objective + `]
  indicator:
    spec:
      thresholdMetric:
        metricSource: {type: DataFocal, spec: {query: '` + query + `'}}
` + extra
	}
	const (
		rolling   = "{duration: 1d, isRolling: true}"
		objective = "{op: lt, value: 300, target: 0.99, timeSliceWindow: 1m}"
		query     = `quantile_over_time(0.99, {service="checkout"} | unwrap duration_ms [1m])`
	)
	timeslices := "  budgetingMethod: Timeslices\n"
	policy := func(lookback string) string {
		return timeslices + "  alertPolicies:\n  - kind: AlertPolicy\n    metadata: {name: burn}\n    spec:\n      conditions:\n      - kind: AlertCondition\n" +
			"        metadata: {name: slow}\n        spec: {condition: {kind: burnrate, op: gte, threshold: 1, lookbackWindow: " + lookback + "}}\n"
	}
	view := func(time, variables, panel string) string {
		return "apiVersion: datafocal/v1\nkind: View\nmetadata: {name: bad}\nspec:\n  time: {from: " + time + "}\n  variables: [" + variables + "]\n  panels: [" + panel + "]\n"
	}
	const (
		service = "{name: service, column: service}"
		grid    = "grid: {x: 0, y: 0, w: 4, h: 4}"
	)
	lines := func(query string) string { return "{kind: lines, query: '" + query + "', " + grid + "}" }
	for _, tc := range []struct {
		name, doc string
		want      Error
	}{
		{"a panel of no kind", view("now-1h", "", "{kind: pie, query: '{service=\"a\"}', "+grid+"}"),
			Error{Name: "bad", Field: "spec.panels[0].kind", Message: `"pie" is not one of slo or [lines patterns stat table timeseries]`}},
		{"a query that does not fit its kind", view("now-1h", "", "{kind: stat, query: '{service=\"a\"}', "+grid+"}"),
			Error{Name: "bad", Field: "spec.panels[0].query", Message: `"{service=\"a\"}" is not an ungrouped aggregate of one measure, which a stat panel draws`}},
		{"a panel query that does not parse", view("now-1h", "", lines(`{service="a"} | line_format "x"`)),
			Error{Name: "bad", Field: "spec.panels[0].query", Message: "at offset 16: the line_format stage is not supported"}},
		{"an slo panel with a query", view("now-1h", "", "{kind: slo, slo: checkout-latency, query: '{service=\"a\"}', "+grid+"}"),
			Error{Name: "bad", Field: "spec.panels[0].query", Message: "an slo panel takes no query"}},
		{"an slo panel naming no SLO", view("now-1h", "", "{kind: slo, "+grid+"}"),
			Error{Name: "bad", Field: "spec.panels[0].slo", Message: "names no SLO"}},
		{"an slo panel naming a missing SLO", view("now-1h", "", "{kind: slo, slo: missing, "+grid+"}"),
			Error{Name: "bad", Field: "spec.panels[0].slo", Message: `names no SLO "missing"`}},
		{"a grid past 12 columns", view("now-1h", "", "{kind: slo, slo: checkout-latency, grid: {x: 10, y: 0, w: 4, h: 4}}"),
			Error{Name: "bad", Field: "spec.panels[0].grid", Message: "falls outside the 12 columns"}},
		{"a grid of no height", view("now-1h", "", "{kind: slo, slo: checkout-latency, grid: {x: 0, y: 0, w: 4, h: 0}}"),
			Error{Name: "bad", Field: "spec.panels[0].grid", Message: "needs a positive w and h"}},
		{"two variables of one name", view("now-1h", service+", "+service, ""),
			Error{Name: "bad", Field: "spec.variables[1].name", Message: `"service" is declared twice`}},
		{"a variable of no column", view("now-1h", "{name: service}", ""),
			Error{Name: "bad", Field: "spec.variables[0].column", Message: "is empty"}},
		{"a $var in a regex", view("now-1h", service, lines(`{service=~"$service"}`)),
			Error{Name: "bad", Field: "spec.panels[0].query", Message: `"{service=~\"$service\"}" uses a $variable other than as the whole value of an = or != predicate`}},
		{"an undeclared $var", view("now-1h", service, lines(`{service="$other"}`)),
			Error{Name: "bad", Field: "spec.panels[0].query", Message: `"{service=\"$other\"}" uses an undeclared variable: $other has no value`}},
		{"a time that does not parse", view("yesterday", "", ""),
			Error{Name: "bad", Field: "spec.time", Message: `time.from: cannot parse time "yesterday" (want RFC3339 or now[±duration])`}},
		{"a forecast that is no duration", strings.Replace(slo(rolling, objective, query, timeslices),
			"metadata: {name: bad}", "metadata: {name: bad, annotations: {datafocal/forecast: soon}}", 1),
			Error{Name: "bad", Field: "metadata.annotations.datafocal/forecast", Message: `"soon" is not a duration such as 30m or 1h`}},
		{"a window longer than retention", slo("{duration: 91d, isRolling: true}", objective, query, timeslices),
			Error{Name: "bad", Field: "spec.timeWindow[0].duration", Message: "91d is longer than the querier's retention of 2160h0m0s"}},
		{"a lookback beyond the fine tier for its resolution", slo(rolling, objective, query, policy("1530m")),
			Error{Name: "bad", Field: "spec.alertPolicies[0].conditions[0].condition.lookbackWindow", Message: "1530m is beyond the 24h0m0s fine horizon and so must be whole hours"}},
		{"a timeSliceWindow that does not divide an hour", slo(rolling, "{op: lt, value: 300, target: 0.99, timeSliceWindow: 7m}", query, timeslices),
			Error{Name: "bad", Field: "spec.objectives[0].timeSliceWindow", Message: `"7m" does not divide an hour`}},
		{"a calendar window outside UTC", slo(`{duration: 1M, calendar: {startTime: "2026-01-01 00:00:00", timeZone: America/New_York}}`, objective, query, timeslices),
			Error{Name: "bad", Field: "spec.timeWindow[0].calendar.timeZone", Message: `"America/New_York" is not UTC, the only calendar time zone`}},
		{"a calendar window starting off the hour", slo(`{duration: 1M, calendar: {startTime: "2026-01-01 00:30:00"}}`, objective, query, timeslices),
			Error{Name: "bad", Field: "spec.timeWindow[0].calendar.startTime", Message: `"2026-01-01 00:30:00" and the window's duration must fall on an hour`}},
		{"a $var", slo(rolling, objective, `quantile_over_time(0.99, {service="$service"} | unwrap duration_ms [1m])`, timeslices),
			Error{Name: "bad", Field: "spec.indicator.spec.thresholdMetric.metricSource.spec.query",
				Message: `"quantile_over_time(0.99, {service=\"$service\"} | unwrap duration_ms [1m])" uses a $variable, which only a view's panel predicate value may`}},
		{"an alertPolicyRef naming nothing", slo(rolling, objective, query, timeslices+"  alertPolicies: [{alertPolicyRef: missing}]\n"),
			Error{Name: "bad", Field: "spec.alertPolicies[0].alertPolicyRef", Message: `names no AlertPolicy "missing"`}},
		{"a query that does not parse", slo(rolling, objective, `{service="checkout"} | line_format "x"`, timeslices),
			Error{Name: "bad", Field: "spec.indicator.spec.thresholdMetric.metricSource.spec.query", Message: "at offset 23: the line_format stage is not supported"}},
		{"a good query not narrowing total", strings.NewReplacer("checkout-latency", "bad", "checkout-fast", "bad-sli",
			`{service="checkout"} | duration_ms`, `{service="cart"} | duration_ms`).Replace(string(valid)),
			Error{Name: "bad", Field: "spec.indicator.spec.ratioMetric", Message: "the good or bad query must hold every predicate of the total query"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			docs, errs := Parse([]byte(string(valid)+"---\n"+tc.doc), nil, limits)
			assert.Equal(t, []Error{tc.want}, errs)
			var names []string
			for _, d := range docs {
				names = append(names, d.Name)
			}
			assert.Contains(t, names, "checkout-fast", "the valid document loads")
		})
	}
}

// TestParseResolvesAgainstDir: a document naming one only the directory holds fails alone
// and imports against the directory.
func TestParseResolvesAgainstDir(t *testing.T) {
	latency, err := os.ReadFile(filepath.Join("testdata", "latency.yaml"))
	require.NoError(t, err)
	policy := "apiVersion: openslo/v1\nkind: AlertPolicy\nmetadata: {name: burn}\nspec: {conditions: []}\n"
	for _, tc := range []struct{ name, dir, doc, field string }{
		{"a view's slo panel", string(latency), "apiVersion: datafocal/v1\nkind: View\nmetadata: {name: v}\nspec:\n  panels: [{kind: slo, slo: checkout-latency, grid: {x: 0, y: 0, w: 4, h: 4}}]\n", "spec.panels[0].slo"},
		{"an SLO's alertPolicyRef", policy, strings.Replace(string(latency), "  service: checkout\n", "  service: checkout\n  alertPolicies: [{alertPolicyRef: burn}]\n", 1), "spec.alertPolicies[0].alertPolicyRef"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, errs := Parse([]byte(tc.doc), nil, limits)
			require.Len(t, errs, 1)
			assert.Equal(t, tc.field, errs[0].Field, "alone, it names something missing")
			dir, errs := Parse([]byte(tc.dir), nil, limits)
			require.Empty(t, errs)
			_, errs = Parse([]byte(tc.doc), dir, limits)
			assert.Empty(t, errs)
		})
	}
}

// TestDirectory: a duplicate across files rejects both, naming both files; a save writes
// <kind>/<name>.yaml that loads back as it was; a change is reported once it settles.
func TestDirectory(t *testing.T) {
	root := t.TempDir()
	content, err := os.ReadFile(filepath.Join("testdata", "prometheus.yaml"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "a.yaml"), content, 0o644))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "team"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "team", "b.yml"), content, 0o644))
	dir := Open(root, limits)
	docs, errs := dir.Load()
	assert.Empty(t, docs)
	message := `SLO api-latency-prometheus is defined in both "a.yaml" and "team/b.yml"`
	assert.Equal(t, []Error{{Path: "a.yaml", Name: "api-latency-prometheus", Field: "metadata.name", Message: message},
		{Path: "team/b.yml", Name: "api-latency-prometheus", Field: "metadata.name", Message: message}}, errs)

	require.NoError(t, os.Remove(filepath.Join(root, "a.yaml")))
	require.NoError(t, os.Remove(filepath.Join(root, "team", "b.yml")))
	parsed, _ := Parse(content, nil, limits)
	changed := make(chan struct{}, 8)
	go func() { _ = dir.Watch(t.Context(), func() { changed <- struct{}{} }) }()
	require.True(t, dir.Writable())
	// Saved until a change is reported, since the watch registers on its own goroutine.
	require.Eventually(t, func() bool {
		require.NoError(t, dir.Save(parsed))
		select {
		case <-changed:
			return true
		default:
			return false
		}
	}, 5*time.Second, 2*settle)
	saved, err := os.ReadFile(filepath.Join(root, "slo", "api-latency-prometheus.yaml"))
	require.NoError(t, err)
	assert.Equal(t, string(content), string(saved))
	docs, errs = dir.Load()
	assert.Empty(t, errs)
	parsed[0].Path = filepath.Join("slo", "api-latency-prometheus.yaml")
	assert.Equal(t, parsed, docs)
}
