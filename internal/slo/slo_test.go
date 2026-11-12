package slo

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/definitions"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// segments are one 1-byte segment named s<n> in each of the given hours after hour0.
func segments(hours ...int) []segment.Descriptor {
	descs := make([]segment.Descriptor, len(hours))
	for i, h := range hours {
		from := hour0.Add(time.Duration(h) * time.Hour)
		descs[i] = segment.Descriptor{ID: fmt.Sprintf("s%d", i), EstimatedSize: 1,
			TimeRange: segment.TimeRange{Min: from, Max: from.Add(time.Hour - time.Second)}}
	}
	return descs
}

// held is a catalog holding *descs.
func held(descs *[]segment.Descriptor) catalog.Catalog {
	return catalog.Fake{
		VersionFn: func() (catalog.Version, error) { return "1", nil },
		PruneFn:   func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) { return *descs, nil },
	}
}

// directory is a definitions directory holding each of files in a file of its own.
func directory(t *testing.T, files ...string) definitions.Directory {
	dir := t.TempDir()
	for i, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, fmt.Sprintf("slo-%d.yaml", i)), []byte(content), 0o644))
	}
	return definitions.Open(dir, definitions.Limits{FineHorizon: DefaultFineHorizon})
}

// every is value at each bin of req under key, where it has one.
func every(req series.Request, key string, value func(bin time.Time) (float64, bool)) series.Values {
	values := series.Values{key: {}}
	for _, b := range req.Bins.Boundaries() {
		if v, ok := value(time.UnixMilli(b)); ok {
			values[key][b] = v
		}
	}
	return values
}

func constant(v float64) func(time.Time) (float64, bool) {
	return func(time.Time) (float64, bool) { return v, true }
}

// until is before up to the minute m after an hour's start and after from then.
func until(m int, before, after float64) func(time.Time) (float64, bool) {
	return func(bin time.Time) (float64, bool) {
		if bin.Minute() < m {
			return before, true
		}
		return after, after >= 0
	}
}

// sloDoc is an SLO document named name over window budgeted by method against target.
func sloDoc(name, window, method string, target float64, objective, indicator string) string {
	return fmt.Sprintf(`apiVersion: openslo/v1
kind: SLO
metadata: {name: %s}
spec:
  service: checkout
  budgetingMethod: %s
  timeWindow: [{duration: %s, isRolling: true}]
  objectives: [{target: %v%s}]
  indicator:
    spec:
%s`, name, method, window, target, objective, indicator)
}

func ratio(narrower, query string) string {
	return fmt.Sprintf(`      ratioMetric:
        total: {metricSource: {type: DataFocal, spec: {query: '{service="checkout"}'}}}
        %s: {metricSource: {type: DataFocal, spec: {query: '{service="checkout"} | %s'}}}
`, narrower, query)
}

// forecastOf is the forecast a store answers for want's counts at to: the window holds all the
// cases' data, so its burn over the two hours before to is the burn over all of it, as the
// 6h burn is, carried an hour on across the window doc names.
func forecastOf(want Status, doc string, to time.Time) *Forecast {
	hours, _ := strconv.Atoi(regexp.MustCompile(`duration: (\d+)h`).FindStringSubmatch(doc)[1])
	window := time.Duration(hours) * time.Hour
	f := &Forecast{Horizon: "1h", Lookback: "2h", BurnRate: want.BurnRate6h}
	if want.BurnRate6h == nil || want.BudgetRemaining == nil {
		return f
	}
	burn, budget := *want.BurnRate6h, *want.BudgetRemaining
	f.BudgetRemaining = ptr(budget - burn*float64(time.Hour)/float64(window))
	if burn > 0 && budget > 0 {
		f.ExhaustedAt = ptr(to.Add(time.Duration(budget / burn * float64(window))).UTC())
	}
	return f
}

func TestStoreCountsEachShape(t *testing.T) {
	// status's budget holds across the window unless trend says how it got there.
	status := func(sli, budget, burn1h, burn6h float64, target float64, trend ...[]float64) Status {
		points := slices.Concat(trend...)
		if len(points) == 0 {
			points = slices.Repeat([]float64{budget}, trendPoints)
		}
		return Status{Name: "checkout", Service: "checkout", State: StateOK, Objective: target, SLI: &sli, BudgetRemaining: &budget,
			BurnRate1h: &burn1h, BurnRate6h: &burn6h, Coverage: 1, WindowStart: hour0, BudgetTrend: ptrs(points)}
	}
	// twoMeasures keys by pattern, as an ungrouped event ratio reads.
	twoMeasures := func(total, narrower func(time.Time) (float64, bool)) func(series.Request) series.Result {
		return func(req series.Request) series.Result {
			key := ""
			if req.Bins.Width == time.Minute {
				key = "p1"
			}
			return series.Result{Measures: []series.Result{{Values: every(req, key, total)}, {Values: every(req, key, narrower)}}}
		}
	}
	for _, tc := range []struct {
		name  string
		doc   string
		hours []int
		read  func(series.Request) series.Result
		want  Status
		// bad is the events slo_status names as bad, which the SLO page investigates.
		bad []selector.Predicate
	}{
		{"event ratio", sloDoc("checkout", "1h", "Occurrences", 0.75, "", ratio("good", "status < 500")), []int{0},
			twoMeasures(constant(8), constant(7)), status(7.0/8, 0.5, 0.5, 0.5, 0.75), bad(where("status", selector.OpGe, "500"))},
		// Lines repeating an event, as a transcript's blocks repeat a call's usage, count it once
		// by the field the definition names: read without it, the 8 events are 16 lines.
		{"event ratio of distinct events", strings.Replace(sloDoc("checkout", "1h", "Occurrences", 0.75, "", ratio("good", "status < 500")),
			"metadata: {name: checkout}", "metadata: {name: checkout, annotations: {datafocal/distinct-by: message.id}}", 1), []int{0},
			func(req series.Request) series.Result {
				if req.Measures[0].DistinctBy != "message.id" || req.Measures[1].DistinctBy != "message.id" {
					return twoMeasures(constant(16), constant(10))(req)
				}
				return twoMeasures(constant(8), constant(7))(req)
			}, status(7.0/8, 0.5, 0.5, 0.5, 0.75), bad(where("status", selector.OpGe, "500"))},
		{"latency threshold", sloDoc("checkout", "1h", "Occurrences", 0.75, "", ratio("good", "duration_ms <= 300")), []int{0},
			twoMeasures(constant(8), constant(7)), status(7.0/8, 0.5, 0.5, 0.5, 0.75), bad(where("duration_ms", selector.OpGt, "300"))},
		// One equality on a counted column reads as the total keyed by that column.
		{"event ratio by one equality", sloDoc("checkout", "1h", "Occurrences", 0.75, "", ratio("good", `level="info"`)), []int{0},
			func(req series.Request) series.Result {
				values := every(req, "info", constant(7))
				values["error"] = every(req, "error", constant(1))["error"]
				return series.Result{Measures: []series.Result{{Values: values}}}
			}, status(7.0/8, 0.5, 0.5, 0.5, 0.75), bad(where("level", selector.OpNe, "info"))},
		{"event ratio by bad events", sloDoc("checkout", "1h", "Occurrences", 0.75, "", ratio("bad", "status >= 500")), []int{0},
			twoMeasures(constant(8), constant(1)), status(7.0/8, 0.5, 0.5, 0.5, 0.75), bad(where("status", selector.OpGe, "500"))},
		{"time-slice", sloDoc("checkout", "1h", "Timeslices", 0.5, ", op: lt, value: 300, timeSliceWindow: 1m", `      thresholdMetric:
        metricSource: {type: DataFocal, spec: {query: 'quantile_over_time(0.99, {service="checkout"} | unwrap duration_ms [1m])'}}
`), []int{0}, func(req series.Request) series.Result {
			return series.Result{Measures: []series.Result{{Values: every(req, "", until(45, 200, 400))}}}
		}, status(0.75, 0.5, 0.5, 0.5, 0.5,
			// Good to the 45th minute, the 36th of 48 steps, then each bad slice spending more.
			slices.Repeat([]float64{1}, 36), []float64{0.9148936170212766, 0.875, 0.8367346938775511, 0.8, 0.7307692307692308, 0.6981132075471699, 0.6666666666666667, 0.6363636363636364, 0.5789473684210527, 0.5517241379310345, 0.5254237288135593, 0.5}),
			// The requests past the threshold, which push a minute's p99 over it.
			bad(where("duration_ms", selector.OpGe, "300"))},
		// Slices of 20 events, 2 bad from the half hour, which is under 0.95 good.
		{"RatioTimeslices", sloDoc("checkout", "1h", "RatioTimeslices", 0.75, ", op: gte, value: 0.95, timeSliceWindow: 5m", ratio("bad", `level="error"`)), []int{0},
			twoMeasures(constant(20), until(30, 0, 2)), status(0.5, -1, 2, 2, 0.75,
				slices.Repeat([]float64{1}, 24),
				slices.Repeat([]float64{0.4285714285714286}, 4),
				slices.Repeat([]float64{0}, 4),
				slices.Repeat([]float64{-0.33333333333333326}, 4),
				slices.Repeat([]float64{-0.6000000000000001}, 4),
				slices.Repeat([]float64{-0.8181818181818181}, 4),
				slices.Repeat([]float64{-1}, 4)), bad(where("level", selector.OpEq, "error"))},
		// Slices with nothing logged count against the budget, but not an hour holding no segment,
		// in which nothing was collected.
		{"absence", sloDoc("checkout", "2h", "Timeslices", 0.75, ", op: gt, value: 0, timeSliceWindow: 5m", `      thresholdMetric:
        metricSource: {type: DataFocal, spec: {query: 'count_over_time({service="checkout"} [5m])'}}
`), []int{1}, func(req series.Request) series.Result {
			return series.Result{Measures: []series.Result{{Values: every(req, "", until(30, 3, -1))}, {Values: every(req, "", constant(5))}}}
		}, status(0.5, -1, 2, 2, 0.75,
			slices.Repeat([]float64{math.NaN()}, 24),
			slices.Repeat([]float64{1}, 12),
			slices.Repeat([]float64{0.4285714285714286}, 2),
			slices.Repeat([]float64{0}, 2),
			slices.Repeat([]float64{-0.33333333333333326}, 2),
			slices.Repeat([]float64{-0.6000000000000001}, 2),
			slices.Repeat([]float64{-0.8181818181818181}, 2),
			slices.Repeat([]float64{-1}, 2)),
			// No one event makes a count's slice bad.
			nil},
		// Collection stopping at hour 0's half hour leaves its slices after, nothing of any
		// service logged in them, out rather than bad: 6 of hour 0's slices and 12 of hour 1's,
		// 6 of them bad.
		{"absence while collection stopped", sloDoc("checkout", "2h", "Timeslices", 0.75, ", op: gt, value: 0, timeSliceWindow: 5m", `      thresholdMetric:
        metricSource: {type: DataFocal, spec: {query: 'count_over_time({service="checkout"} [5m])'}}
`), []int{0, 1}, func(req series.Request) series.Result {
			collected := constant(5)
			if req.Bins.From.Equal(hour0) {
				collected = until(30, 5, -1)
			}
			return series.Result{Measures: []series.Result{{Values: every(req, "", until(30, 3, -1))}, {Values: every(req, "", collected)}}}
		}, status(0.6666666666666666, -0.33333333333333326, 2, 1.3333333333333333, 0.75,
			slices.Repeat([]float64{1}, 36),
			slices.Repeat([]float64{0.6923076923076923}, 2),
			slices.Repeat([]float64{0.4285714285714286}, 2),
			slices.Repeat([]float64{0.19999999999999996}, 2),
			slices.Repeat([]float64{0}, 2),
			slices.Repeat([]float64{-0.17647058823529416}, 2),
			slices.Repeat([]float64{-0.33333333333333326}, 2)), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			descs := segments(tc.hours...)
			rd := series.Fake{ReadFn: func(req series.Request) (series.Result, error) { return tc.read(req), nil }}
			s := New(directory(t, tc.doc), held(&descs), rd, Config{})
			require.NoError(t, s.Refresh(t.Context()))
			got, _, err := s.List(t.Context(), "1")
			require.NoError(t, err)
			want := tc.want
			want.Forecast = forecastOf(want, tc.doc, hour0.Add(time.Duration(slices.Max(tc.hours)+1)*time.Hour))
			assert.Equal(t, []Status{want}, got)
			d, err := s.Detail(t.Context(), "1", "checkout", "", selector.Window{})
			require.NoError(t, err)
			assert.Equal(t, tc.bad, d.BadEvents)
		})
	}
}

// TestStoreRefresh: a new segment of a counted hour is read alone for an event ratio, and the
// whole hour for a time-slice SLO; backfill stops at BackfillBytes, the event ratios' shared
// read of an hour and each time-slice SLO's own read each spending it, shown as partial until done;
// and a reload keeps the cells of an SLO whose SLI is unchanged.
func TestStoreRefresh(t *testing.T) {
	docs := []string{sloDoc("checkout", "2h", "Occurrences", 0.75, "", ratio("good", "status < 500")),
		sloDoc("heartbeat", "2h", "Timeslices", 0.5, ", op: gt, value: 0, timeSliceWindow: 1m", `      thresholdMetric:
        metricSource: {type: DataFocal, spec: {query: 'count_over_time({service="checkout"} [1m])'}}
`)}
	descs := segments(0, 1)
	var reads [][]string
	rd := series.Fake{ReadFn: func(req series.Request) (series.Result, error) {
		var ids []string
		for _, d := range req.Segments {
			ids = append(ids, d.ID)
		}
		reads = append(reads, ids)
		measures := make([]series.Result, len(req.Measures))
		for i := range measures {
			measures[i].Values = every(req, "p1", constant(float64(len(req.Segments))))
		}
		return series.Result{Measures: measures}, nil
	}}
	s := New(directory(t, docs...), held(&descs), rd, Config{BackfillBytes: 1})
	list := func() []Status {
		got, _, err := s.List(t.Context(), "1")
		require.NoError(t, err)
		return got
	}

	require.NoError(t, s.Refresh(t.Context()))
	assert.Equal(t, [][]string{{"s1"}}, reads, "the newest hour first, the event ratio's read spending the backfill bytes")
	assert.Equal(t, []string{StatePartial, StatePartial}, states(list()))
	reads = nil
	require.NoError(t, s.Refresh(t.Context()))
	assert.Equal(t, [][]string{{"s1"}}, reads, "the time-slice SLO's own read of the hour, spending them again")
	for range 2 {
		require.NoError(t, s.Refresh(t.Context()))
	}
	assert.Equal(t, []string{StateOK, StateOK}, states(list()))

	reads = nil
	late := segments(1)[0]
	late.ID = "s2"
	descs = append(descs, late)
	require.NoError(t, s.Refresh(t.Context()))
	assert.ElementsMatch(t, [][]string{{"s1", "s2"}, {"s2"}}, reads, "the time-slice SLO rereads the hour, the event ratio its new segment")

	reads = nil
	incremental := list()
	rebuilt := New(directory(t, docs...), held(&descs), rd, Config{})
	require.NoError(t, rebuilt.Refresh(t.Context()))
	got, _, err := rebuilt.List(t.Context(), "1")
	require.NoError(t, err)
	assert.Equal(t, got, incremental)

	reads = nil
	_, err = s.Import(t.Context(), "1", []byte(docs[0]+"---\n"+docs[1]), true)
	require.NoError(t, err)
	assert.Equal(t, incremental, list())
	assert.Empty(t, reads, "the reload kept the unchanged SLOs' cells")
}

func states(statuses []Status) []string {
	out := make([]string, len(statuses))
	for i, st := range statuses {
		out[i] = st.State
	}
	return out
}

// TestStoreFoldsGroups: each grouped SLO keeps its groups with the most valid events, up to
// slo.max_groups and what slo.max_series leaves after the SLOs before it, folding the rest.
// TestStoreRefreshPastHoursWithoutTheField: an hour none of whose segments carry a quantile's
// field counts as one without data, and the refresh goes on to the hours before it.
func TestStoreRefreshPastHoursWithoutTheField(t *testing.T) {
	doc := sloDoc("checkout", "3h", "Timeslices", 0.5, ", op: lt, value: 300, timeSliceWindow: 1m", `      thresholdMetric:
        metricSource: {type: DataFocal, spec: {query: 'quantile_over_time(0.99, {service="checkout"} | unwrap duration_ms [1m])'}}
`)
	descs := segments(0, 1, 2)
	rd := series.Fake{ReadFn: func(req series.Request) (series.Result, error) {
		if req.Bins.From.Equal(hour0.Add(time.Hour)) {
			return series.Result{}, query.NewAPIError(query.ErrCodeUnknownField, `aggregate: no segment in the window carries a "duration_ms" column for quantile`)
		}
		return series.Result{Measures: []series.Result{{Values: every(req, "", constant(200))}}}, nil
	}}
	s := New(directory(t, doc), held(&descs), rd, Config{})
	require.NoError(t, s.Refresh(t.Context()))
	got, _, err := s.List(t.Context(), "1")
	require.NoError(t, err)
	assert.Equal(t, []string{StateOK}, states(got))
	assert.Equal(t, 1.0, got[0].Coverage, "hour 0, before the hour without the field, counted too")
	assert.Equal(t, 1.0, *got[0].SLI, "hours 0 and 2's minutes, each good")
}

func TestStoreFoldsGroups(t *testing.T) {
	grouped := func(name string) string {
		return strings.Replace(sloDoc(name, "1h", "Occurrences", 0.75, "", ratio("good", "status < 500")),
			"metadata: {name: "+name+"}", "metadata: {name: "+name+", annotations: {datafocal/group-by: route}}", 1)
	}
	descs := segments(0)
	rd := series.Fake{ReadFn: func(req series.Request) (series.Result, error) {
		byRoute := series.Values{}
		for route, n := range map[string]float64{"r1": 3, "r2": 2, "r3": 1} {
			byRoute[route] = every(req, route, constant(n))[route]
		}
		out := make([]series.Result, len(req.Measures))
		for i, m := range req.Measures {
			out[i].Values = byRoute
			if m.GroupBy == "" {
				out[i].Values = every(req, "", constant(6))
			}
		}
		return series.Result{Measures: out}, nil
	}}
	// The ungrouped SLO, first by name, takes none of the grouped SLOs' MaxSeries.
	ungrouped := sloDoc("a", "1h", "Occurrences", 0.75, "", ratio("good", "status < 500"))
	s := New(directory(t, ungrouped, grouped("b"), grouped("c")), held(&descs), rd, Config{MaxGroups: 2, MaxSeries: 3})
	require.NoError(t, s.Refresh(t.Context()))
	got, _, err := s.List(t.Context(), "1")
	require.NoError(t, err)
	type folded struct {
		Groups    []string
		Untracked int
	}
	var kept []folded
	for _, st := range got {
		f := folded{Untracked: st.Untracked}
		for _, g := range st.Groups {
			f.Groups = append(f.Groups, g.Group)
		}
		kept = append(kept, f)
	}
	assert.Equal(t, []folded{{}, {Groups: []string{"r1", "r2", "untracked"}, Untracked: 1}, {Groups: []string{"r1", "untracked"}, Untracked: 2}}, kept)
}

// TestStoreImport classifies each document against the directory's, previews each SLO, saves
// into a writable directory only, and exports what a re-import finds unchanged.
func TestStoreImport(t *testing.T) {
	checkout := sloDoc("checkout", "1h", "Occurrences", 0.75, "", ratio("good", "status < 500"))
	prometheus := strings.NewReplacer("DataFocal", "Prometheus", "checkout", "api").Replace(checkout)
	changed := strings.Replace(checkout, "status < 500", "status < 400", 1)
	descs := segments(0)
	rd := series.Fake{ReadFn: func(req series.Request) (series.Result, error) {
		return series.Result{Measures: []series.Result{{Values: every(req, "p1", constant(8))}, {Values: every(req, "p1", constant(7))}}}, nil
	}}
	dir := t.TempDir()
	s := New(definitions.Open(dir, definitions.Limits{}), held(&descs), rd, Config{})
	imported, err := s.Import(t.Context(), "1", []byte(checkout+"---\n"+prometheus+"---\nkind: SLO\nmetadata: {name: Bad_Name}\n"), true)
	require.NoError(t, err)
	classes := func(imported []Imported) []string {
		out := make([]string, len(imported))
		for i, im := range imported {
			out[i] = im.Name + " " + im.Class + " " + im.Error
		}
		return out
	}
	assert.Equal(t, []string{"checkout new ", "api unbound ", `Bad_Name invalid Bad_Name: metadata.name: "Bad_Name" is not a DNS label`}, classes(imported))
	preview := Status{Name: "checkout", Service: "checkout", State: StateOK, Objective: 0.75, SLI: ptr(7.0 / 8), BudgetRemaining: ptr(0.5),
		BurnRate1h: ptr(0.5), BurnRate6h: ptr(0.5), Coverage: 1, WindowStart: hour0.Add(-23 * time.Hour)}
	preview.Forecast = forecastOf(preview, "duration: 24h", hour0.Add(time.Hour))
	assert.Equal(t, &preview, imported[0].Preview, "a status over the last 24 hours")

	exported, err := s.Export(nil, definitions.FormatYAML)
	require.NoError(t, err)
	// In path order, slo/api.yaml first.
	normalized, _ := definitions.Parse([]byte(prometheus+"---\n"+checkout), nil, definitions.Limits{})
	want, err := definitions.Format(normalized, definitions.FormatYAML)
	require.NoError(t, err)
	assert.Equal(t, string(want), string(exported))
	again, err := s.Import(t.Context(), "1", exported, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"api unbound ", "checkout unchanged "}, classes(again))
	again, err = s.Import(t.Context(), "1", []byte(changed), false)
	require.NoError(t, err)
	assert.Equal(t, []string{"checkout changed "}, classes(again))
	view := "apiVersion: datafocal/v1\nkind: View\nmetadata: {name: v}\nspec: {panels: [{kind: slo, slo: checkout, grid: {x: 0, y: 0, w: 4, h: 4}}]}\n"
	again, err = s.Import(t.Context(), "1", []byte(view), false)
	require.NoError(t, err)
	assert.Equal(t, []string{"v new "}, classes(again), "a view naming the directory's SLO")
	assert.Equal(t, []string{"slo"}, again[0].Panels, "its panels previewed")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "both.yaml"), []byte(sloDoc("one", "1h", "Occurrences", 0.75, "", ratio("good", "status < 500"))+
		"---\n"+sloDoc("two", "1h", "Occurrences", 0.75, "", ratio("good", "status < 500"))), 0o644))
	_, err = s.Import(t.Context(), "1", []byte(sloDoc("one", "1h", "Occurrences", 0.9, "", ratio("good", "status < 500"))), true)
	assert.EqualError(t, err, "import_definitions: cannot save SLO one: both.yaml holds other documents too, so export it instead")

	readOnly := t.TempDir()
	require.NoError(t, os.Chmod(readOnly, 0o555))
	_, err = New(definitions.Open(readOnly, definitions.Limits{}), held(&descs), rd, Config{}).Import(t.Context(), "1", []byte(checkout), true)
	assert.EqualError(t, err, "import_definitions: cannot save slo/checkout.yaml: the definitions directory is read-only, so export it and commit it instead")
}

// TestStoreBudgets pins what one SLO-group pair holds over a 28-day window, a day of it in
// minutes, and that listing it costs little more than listing a day, each a day of minutes
// and the 28 days' rest one bin an hour.
func TestStoreBudgets(t *testing.T) {
	const groups = 50
	hours := make([]int, 28*24)
	for i := range hours {
		hours[i] = i
	}
	descs := segments(hours...)
	rd := series.Fake{ReadFn: func(req series.Request) (series.Result, error) {
		values := series.Values{}
		for g := range groups {
			key := fmt.Sprintf("route-%d", g)
			values[key] = every(req, key, constant(2))[key]
		}
		return series.Result{Measures: slices.Repeat([]series.Result{{Values: values}}, len(req.Measures))}, nil
	}}
	open := func(window string) Store {
		doc := strings.Replace(sloDoc("checkout", window, "Occurrences", 0.75, "", ratio("good", "status < 500")),
			"metadata: {name: checkout}", "metadata: {name: checkout, annotations: {datafocal/group-by: route}}", 1)
		return New(directory(t, doc), held(&descs), rd, Config{})
	}

	before := heapAfterGC()
	month := open("28d")
	require.NoError(t, month.Refresh(t.Context()))
	perPair := (heapAfterGC() - before) / groups
	// Measured at 107 KB: 24 hours of minutes, 648 of one bin, and a map entry an hour.
	assert.LessOrEqual(t, perPair, uint64(128<<10), "bytes per SLO-group pair")

	day := open("1d")
	require.NoError(t, day.Refresh(t.Context()))
	fastest := func(s Store) time.Duration {
		best := time.Hour
		for range 5 {
			start := time.Now()
			_, _, err := s.List(t.Context(), "1")
			require.NoError(t, err)
			best = min(best, time.Since(start))
		}
		return best
	}
	// Measured at 2.4 times, the 27 days' hours being 648 cells a group beside the day's 24.
	assert.Less(t, fastest(month), 4*fastest(day), "listing 28 days against listing a day")
	runtime.KeepAlive(month)
}

// TestStoreScalesWithSLOs: reading one SLO costs the same however many are defined, and a
// read at a catalog version already refreshed neither prunes the catalog nor lists again.
func TestStoreScalesWithSLOs(t *testing.T) {
	hours := make([]int, 24)
	for i := range hours {
		hours[i] = i
	}
	descs := segments(hours...)
	rd := series.Fake{ReadFn: func(req series.Request) (series.Result, error) {
		key := ""
		if req.Bins.Width == time.Minute {
			key = "p1"
		}
		return series.Result{Measures: slices.Repeat([]series.Result{{Values: every(req, key, constant(8))}}, len(req.Measures))}, nil
	}}
	open := func(n int) (Store, *int) {
		docs := make([]string, n)
		for i := range docs {
			docs[i] = sloDoc(fmt.Sprintf("slo-%03d", i), "1d", "Occurrences", 0.99, "", ratio("good", "status < 500"))
		}
		prunes := 0
		cat := catalog.Fake{
			VersionFn: func() (catalog.Version, error) { return "1", nil },
			PruneFn: func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) {
				prunes++
				return descs, nil
			},
		}
		s := New(directory(t, docs...), cat, rd, Config{})
		require.NoError(t, s.Refresh(t.Context()))
		return s, &prunes
	}
	detail := func(s Store, name string) float64 {
		return testing.AllocsPerRun(5, func() {
			_, err := s.Detail(t.Context(), "1", name, "", selector.Window{})
			require.NoError(t, err)
		})
	}
	few, _ := open(20)
	many, prunes := open(400)
	// Measured equal, at 2,961 allocations; 22,578 for the 400th when each read folded every SLO.
	assert.Less(t, detail(many, "slo-399"), 1.5*detail(few, "slo-019"), "reading the last of 400 SLOs against the last of 20")

	before := *prunes
	listed, _, err := many.List(t.Context(), "1")
	require.NoError(t, err)
	again := testing.AllocsPerRun(5, func() {
		_, _, err := many.List(t.Context(), "1")
		require.NoError(t, err)
	})
	assert.Equal(t, before, *prunes, "reads at the refreshed version prune nothing")
	assert.LessOrEqual(t, again, 2.0, "listing again at the same snapshot copies the listing")
	assert.Len(t, listed, 400)
}

// bad is the checkout service's events, and p.
func bad(p selector.Predicate) []selector.Predicate {
	return []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "checkout"}, p}
}

func where(field string, op selector.Op, value string) selector.Predicate {
	return selector.Predicate{Field: field, Op: op, Value: value}
}

// ptrs is a pointer to each of vs, nil for NaN, a point without valid events.
func ptrs(vs []float64) []*float64 {
	out := make([]*float64, len(vs))
	for i := range vs {
		if !math.IsNaN(vs[i]) {
			out[i] = &vs[i]
		}
	}
	return out
}

func heapAfterGC() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}
