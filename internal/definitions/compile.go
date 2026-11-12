package definitions

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/datafocal/datafocal/internal/selector"
)

// The OpenSLO v1 fields an SLO compiles from, matched to the JSON names case-insensitively.
type (
	sloSpec struct {
		Service         string
		BudgetingMethod string
		TimeWindow      []timeWindow
		Objectives      []struct {
			Target          float64
			Op              string
			Value           float64
			TimeSliceWindow duration
		}
		Indicator *struct {
			Spec sliSpec
		}
		IndicatorRef  string
		AlertPolicies []struct {
			AlertPolicyRef string
			Metadata       meta
			Spec           policySpec
		}
	}
	timeWindow struct {
		Duration  duration
		IsRolling bool
		Calendar  *struct{ StartTime, TimeZone string }
	}
	sliSpec struct {
		RatioMetric *struct {
			Good, Bad, Total *metric
		} `json:"ratioMetric"`
		ThresholdMetric *metric `json:"thresholdMetric"`
	}
	metric struct {
		MetricSource struct {
			Type string
			Spec map[string]any
		}
	}
	policySpec struct {
		AlertWhenNoData bool
		Conditions      []struct {
			ConditionRef string
			Metadata     meta
			Spec         conditionSpec
		}
	}
	conditionSpec struct {
		Severity  string
		Condition struct {
			Kind, Op                   string
			Threshold                  float64
			LookbackWindow, AlertAfter duration
		}
	}
)

// Budgeting methods.
const (
	Occurrences     = "Occurrences"
	Timeslices      = "Timeslices"
	RatioTimeslices = "RatioTimeslices"
)

// Ops are OpenSLO's comparison operators.
var Ops = []string{"lt", "lte", "gt", "gte"}

// compile rejects both documents of a duplicate identity and every SLO or View that does
// not compile, each on its own, resolving references against dir's documents and docs.
func compile(docs, dir []Document, limits Limits) ([]Document, []Error) {
	paths := map[string][]string{}
	for _, d := range docs {
		paths[d.Kind+"/"+d.Name] = append(paths[d.Kind+"/"+d.Name], d.Path)
	}
	index := map[string]Document{}
	for _, d := range append(slices.Clip(dir), docs...) {
		index[d.Kind+"/"+d.Name] = d
	}
	var kept []Document
	var errs []Error
	for _, d := range docs {
		if p := paths[d.Kind+"/"+d.Name]; len(p) > 1 {
			errs = append(errs, Error{Path: d.Path, Name: d.Name, Field: "metadata.name",
				Message: fmt.Sprintf("%s %s is defined in both %q and %q", d.Kind, d.Name, p[0], p[1])})
			continue
		}
		var err *Error
		switch {
		case d.Kind == kindSLO && d.Raw["apiVersion"] == apiVersion:
			d.SLO, err = compileSLO(d, index, limits)
		case d.Kind == kindView && d.Raw["apiVersion"] == viewAPIVersion:
			d.View, err = compileView(d, index)
		}
		if err != nil {
			err.Path, err.Name = d.Path, d.Name
			errs = append(errs, *err)
			continue
		}
		kept = append(kept, d)
	}
	return kept, errs
}

func invalid(field, format string, args ...any) *Error {
	return &Error{Field: field, Message: fmt.Sprintf(format, args...)}
}

func compileSLO(d Document, index map[string]Document, limits Limits) (*SLO, *Error) {
	var doc struct {
		Metadata meta
		Spec     sloSpec
	}
	if err := remarshal(d.Raw, &doc); err != nil {
		return nil, invalid("spec", "%v", err)
	}
	spec := doc.Spec
	s := &SLO{Name: d.Name, Service: spec.Service, Method: cmp.Or(spec.BudgetingMethod, Occurrences),
		GroupBy:    doc.Metadata.Annotations[annotationPrefix+"group-by"],
		DistinctBy: doc.Metadata.Annotations[annotationPrefix+"distinct-by"]}
	if len(spec.TimeWindow) != 1 {
		return nil, invalid("spec.timeWindow", "needs exactly one window")
	}
	forecast, ferr := forecastOf(doc.Metadata.Annotations[annotationPrefix+"forecast"])
	if ferr != nil {
		return nil, ferr
	}
	s.Forecast = forecast
	window, err := compileWindow(spec.TimeWindow[0], limits)
	if err != nil {
		return nil, err
	}
	s.Window = window
	if len(spec.Objectives) != 1 {
		return nil, invalid("spec.objectives", "needs exactly one objective")
	}
	o := spec.Objectives[0]
	s.Target, s.Op, s.Value = o.Target, o.Op, o.Value
	if s.Target <= 0 || s.Target >= 1 {
		return nil, invalid("spec.objectives[0].target", "%v is not between 0 and 1", s.Target)
	}
	sli, err := indicator(spec, index)
	if err != nil {
		return nil, err
	}
	if err := s.compileIndicator(sli, o.TimeSliceWindow); err != nil {
		return nil, err
	}
	s.Policies, err = compilePolicies(spec, index, limits)
	return s, err
}

// indicator is the SLO's inline SLI, or the one its indicatorRef names.
func indicator(spec sloSpec, index map[string]Document) (sliSpec, *Error) {
	if spec.IndicatorRef == "" && spec.Indicator == nil {
		return sliSpec{}, invalid("spec.indicator", "is missing")
	}
	if spec.IndicatorRef == "" {
		return spec.Indicator.Spec, nil
	}
	sli, err := resolve[struct{ Spec sliSpec }](index, "SLI", spec.IndicatorRef, "spec.indicatorRef")
	return sli.Spec, err
}

// resolve is the document of kind named name decoded, or an error at field naming it.
func resolve[T any](index map[string]Document, kind, name, field string) (T, *Error) {
	var out T
	doc, ok := index[kind+"/"+name]
	if !ok {
		return out, invalid(field, "names no %s %q", kind, name)
	}
	_ = remarshal(doc.Raw, &out)
	return out, nil
}

func compilePolicies(spec sloSpec, index map[string]Document, limits Limits) ([]Policy, *Error) {
	var policies []Policy
	for i, ref := range spec.AlertPolicies {
		field, name, policy := fmt.Sprintf("spec.alertPolicies[%d]", i), ref.Metadata.Name, ref.Spec
		if ref.AlertPolicyRef != "" {
			doc, err := resolve[struct{ Spec policySpec }](index, "AlertPolicy", ref.AlertPolicyRef, field+".alertPolicyRef")
			if err != nil {
				return nil, err
			}
			name, policy = ref.AlertPolicyRef, doc.Spec
		}
		compiled, err := compilePolicy(name, policy, field, index, limits)
		if err != nil {
			return nil, err
		}
		policies = append(policies, compiled)
	}
	return policies, nil
}

var durationText = regexp.MustCompile(`^(\d+)([smhdwMQY])$`)

// duration is an OpenSLO duration as written: a span, or for M, Q and Y calendar months.
type duration struct {
	text   string
	span   time.Duration
	months int
}

func (d *duration) UnmarshalJSON(b []byte) error {
	var text string
	err := json.Unmarshal(b, &text)
	if err == nil {
		*d, err = parseDuration(text)
	}
	return err
}

func parseDuration(text string) (duration, error) {
	m := durationText.FindStringSubmatch(text)
	if m == nil {
		return duration{}, fmt.Errorf("%q is not a duration such as 5m, 1h or 28d", text)
	}
	n, _ := strconv.Atoi(m[1])
	units := map[string]time.Duration{"s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour, "w": 7 * 24 * time.Hour}
	return duration{text: text, span: time.Duration(n) * units[m[2]], months: n * map[string]int{"M": 1, "Q": 3, "Y": 12}[m[2]]}, nil
}

// spanAt is d as a span, refusing calendar months.
func (d duration) spanAt(field string) (time.Duration, *Error) {
	if d.months > 0 {
		return 0, invalid(field, "%q is in calendar months, which only a calendar window takes", d.text)
	}
	return d.span, nil
}

// forecastOf is the datafocal/forecast annotation's horizon, an hour when it names none.
func forecastOf(text string) (time.Duration, *Error) {
	if text == "" {
		return time.Hour, nil
	}
	d, err := parseDuration(text)
	if err != nil || d.months > 0 || d.span <= 0 {
		return 0, invalid("metadata.annotations."+annotationPrefix+"forecast", "%q is not a duration such as 30m or 1h", text)
	}
	return d.span, nil
}

func compileWindow(w timeWindow, limits Limits) (Window, *Error) {
	const field = "spec.timeWindow[0]"
	d, months := w.Duration.span, w.Duration.months
	if limits.Retention > 0 && max(d, time.Duration(months)*31*24*time.Hour) > limits.Retention {
		return Window{}, invalid(field+".duration", "%s is longer than the querier's retention of %s", w.Duration.text, limits.Retention)
	}
	if w.IsRolling {
		d, err := w.Duration.spanAt(field + ".duration")
		return Window{Rolling: true, Duration: d}, err
	}
	return calendarWindow(w, d, months)
}

// calendarWindow is a calendar window of d or months from its UTC startTime on an hour.
func calendarWindow(w timeWindow, d time.Duration, months int) (Window, *Error) {
	const field = "spec.timeWindow[0]"
	if w.Calendar == nil {
		return Window{}, invalid(field+".calendar", "a calendar window needs a startTime")
	}
	if tz := w.Calendar.TimeZone; tz != "" && tz != "UTC" && tz != "Etc/UTC" {
		return Window{}, invalid(field+".calendar.timeZone", "%q is not UTC, the only calendar time zone", tz)
	}
	start, err := time.Parse(time.DateTime, w.Calendar.StartTime)
	if err != nil {
		start, err = time.Parse(time.RFC3339, w.Calendar.StartTime)
	}
	if err != nil || !start.Truncate(time.Hour).Equal(start) || d%time.Hour != 0 {
		return Window{}, invalid(field+".calendar.startTime", "%q and the window's duration must fall on an hour", w.Calendar.StartTime)
	}
	return Window{Duration: d, Months: months, Start: start.UTC()}, nil
}

func (s *SLO) compileIndicator(sli sliSpec, slice duration) *Error {
	if err := s.compileSlices(slice); err != nil {
		return err
	}
	switch {
	case sli.ThresholdMetric != nil && s.Method != Timeslices:
		return invalid("spec.budgetingMethod", "a thresholdMetric is budgeted by Timeslices, not %s", s.Method)
	case sli.ThresholdMetric != nil:
		return s.compileThreshold(sli.ThresholdMetric, slice)
	case sli.RatioMetric == nil:
		return invalid(indicatorField, "needs a ratioMetric or a thresholdMetric")
	case s.Method == Timeslices:
		return invalid("spec.budgetingMethod", "a ratioMetric is budgeted by Occurrences or RatioTimeslices")
	}
	return s.compileRatio(sli.RatioMetric.Total, sli.RatioMetric.Good, sli.RatioMetric.Bad)
}

const indicatorField = "spec.indicator.spec"

// compileSlices reads a time-slice SLO's slice window, which must divide an hour, and op.
func (s *SLO) compileSlices(slice duration) *Error {
	if s.Method == Occurrences {
		return nil
	}
	var err *Error
	s.SliceWindow, err = slice.spanAt("spec.objectives[0].timeSliceWindow")
	if err != nil || s.SliceWindow < 15*time.Second || time.Hour%s.SliceWindow != 0 {
		return cmp.Or(err, invalid("spec.objectives[0].timeSliceWindow", "%q does not divide an hour", slice.text))
	}
	if !slices.Contains(Ops, s.Op) {
		return invalid("spec.objectives[0].op", "%q is not one of %v", s.Op, Ops)
	}
	return nil
}

func (s *SLO) compileThreshold(m *metric, slice duration) *Error {
	var err *Error
	if s.Metric, err = s.query(indicatorField+".thresholdMetric", m, true); err != nil || s.Metric == nil {
		return err
	}
	if bins, perr := time.ParseDuration(s.Metric.Stage.Bins); perr != nil || bins != s.SliceWindow {
		return invalid(indicatorField+".thresholdMetric.metricSource.spec.query", "its range %q must equal timeSliceWindow %s", s.Metric.Stage.Bins, slice.text)
	}
	return nil
}

func (s *SLO) compileRatio(total, good, bad *metric) *Error {
	const field = indicatorField + ".ratioMetric"
	if total == nil || (good == nil) == (bad == nil) {
		return invalid(field, "needs total and one of good or bad")
	}
	var err *Error
	if s.Total, err = s.query(field+".total", total, false); err != nil {
		return err
	}
	if s.Good, err = s.query(field+".good", good, false); err != nil {
		return err
	}
	if s.Bad, err = s.query(field+".bad", bad, false); err != nil {
		return err
	}
	if narrower := cmp.Or(s.Good, s.Bad); narrower != nil && s.Total != nil &&
		slices.ContainsFunc(s.Total.Where, func(p selector.Predicate) bool { return !slices.ContainsFunc(narrower.Where, equal(p)) }) {
		return invalid(field, "the good or bad query must hold every predicate of the total query")
	}
	return nil
}

// variable is a view's $name, which an SLO has none of to substitute.
var variable = regexp.MustCompile(`\$[A-Za-z_]\w*`)

// query compiles a DataFocal source's query, a metric query when metricQuery is set, or
// marks s unbound by any other source.
func (s *SLO) query(field string, m *metric, metricQuery bool) (*selector.Query, *Error) {
	if m == nil {
		return nil, nil
	}
	source := m.MetricSource
	if source.Type != nativeSource {
		s.unbind(source.Spec)
		return nil, nil
	}
	field += ".metricSource.spec.query"
	text, _ := source.Spec["query"].(string)
	q, err := parseQuery(text)
	if err != nil {
		return nil, invalid(field, "%v", err)
	}
	if fault := queryFault(q, text, metricQuery); fault != "" {
		return nil, invalid(field, "%q %s", text, fault)
	}
	return &q, nil
}

// unbind marks s unbound, keeping its foreign source's first query in key order.
func (s *SLO) unbind(spec map[string]any) {
	s.Unbound = true
	for _, k := range slices.Sorted(maps.Keys(spec)) {
		if q, ok := spec[k].(string); ok && s.Source == "" {
			s.Source = q
		}
	}
}

// queryFault is why q, parsed from text, cannot be a metric query when metricQuery is set,
// or a log query otherwise, or empty when it can.
func queryFault(q selector.Query, text string, metricQuery bool) string {
	switch {
	case metricQuery && (q.Stage == nil || q.Stage.Kind != selector.StageAggregate || q.Stage.Rollup != nil || len(q.Stage.Measures) > 0):
		return "is not a metric query of one measure"
	case !metricQuery && q.Stage != nil:
		return "is not a log query"
	case variable.MatchString(text):
		return "uses a $variable, which only a view's panel predicate value may"
	}
	return ""
}

func compilePolicy(name string, spec policySpec, field string, index map[string]Document, limits Limits) (Policy, *Error) {
	p := Policy{Name: name, AlertWhenNoData: spec.AlertWhenNoData}
	for i, ref := range spec.Conditions {
		field, m, c := fmt.Sprintf("%s.conditions[%d]", field, i), ref.Metadata, ref.Spec
		if ref.ConditionRef != "" {
			doc, err := resolve[struct {
				Metadata meta
				Spec     conditionSpec
			}](index, "AlertCondition", ref.ConditionRef, field+".conditionRef")
			if err != nil {
				return p, err
			}
			m, c = doc.Metadata, doc.Spec
		}
		cond, err := compileCondition(m, c, field+".condition", limits)
		if err != nil {
			return p, err
		}
		p.Conditions = append(p.Conditions, cond)
	}
	return p, nil
}

func compileCondition(m meta, spec conditionSpec, field string, limits Limits) (Condition, *Error) {
	c := spec.Condition
	cond := Condition{Name: m.Name, Severity: spec.Severity, Op: c.Op, Threshold: c.Threshold}
	switch {
	case c.Kind != "burnrate":
		return cond, invalid(field+".kind", "%q is not burnrate", c.Kind)
	case !slices.Contains(Ops, c.Op):
		return cond, invalid(field+".op", "%q is not one of %v", c.Op, Ops)
	}
	var err *Error
	if cond.Lookback, err = lookback(field+".lookbackWindow", c.LookbackWindow, limits); err != nil {
		return cond, err
	}
	cond.Short = max(time.Minute, (cond.Lookback / 12).Round(time.Minute))
	if short := m.Annotations[annotationPrefix+"short-window"]; short != "" {
		const field = "metadata.annotations." + annotationPrefix + "short-window"
		d, derr := parseDuration(short)
		if derr != nil {
			return cond, invalid(field, "%v", derr)
		}
		if cond.Short, err = lookback(field, d, limits); err != nil {
			return cond, err
		}
	}
	cond.AlertAfter, err = c.AlertAfter.spanAt(field + ".alertAfter")
	return cond, err
}

// lookback is a burn rate's window, in whole minutes within the fine horizon and whole
// hours past it, since those are the bins that serve it.
func lookback(field string, w duration, limits Limits) (time.Duration, *Error) {
	d, err := w.spanAt(field)
	s := w.text
	switch {
	case err != nil:
		return 0, err
	case d < time.Minute || d%time.Minute != 0:
		return 0, invalid(field, "%s is not whole minutes", s)
	case d > limits.FineHorizon && d%time.Hour != 0:
		return 0, invalid(field, "%s is beyond the %s fine horizon and so must be whole hours", s, limits.FineHorizon)
	case limits.Retention > 0 && d > limits.Retention:
		return 0, invalid(field, "%s is longer than the querier's retention of %s", s, limits.Retention)
	}
	return d, nil
}
