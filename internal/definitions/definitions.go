// Package definitions owns the definitions directory: it reads, validates and normalizes
// OpenSLO v1 files and saved views, converts Loki sources, and writes the files back.
package definitions

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	"sigs.k8s.io/yaml"

	"github.com/datafocal/datafocal/internal/query/lang/logql"
	"github.com/datafocal/datafocal/internal/selector"
)

const (
	apiVersion = "openslo/v1"
	// annotationPrefix is the one spelling of the datafocal/ annotations.
	annotationPrefix = "datafocal/"
	nativeSource     = "DataFocal"
	kindSLO          = "SLO"
	// FormatYAML and FormatJSON are the formats Format writes.
	FormatYAML = "yaml"
	FormatJSON = "json"
)

// Document is one definition as its file holds it, normalized.
type Document struct {
	Kind, Name, Path, Version string
	// SLO is set for kind SLO, and View for an datafocal/v1 View.
	SLO  *SLO
	View *View
	// Raw is the normalized document.
	Raw map[string]any
	// Notes are the rewrites a Loki conversion made.
	Notes []string
}

// SLO is an SLO compiled for evaluation.
type SLO struct {
	Name, Service string
	// Method is Occurrences, Timeslices or RatioTimeslices.
	Method string
	Window Window
	Target float64
	// Op and Value decide whether a slice of SliceWindow is good, for a time-slice SLO.
	Op          string
	Value       float64
	SliceWindow time.Duration
	// Total and Good or Bad are a ratio's log queries; Metric is a threshold's metric query.
	Total, Good, Bad, Metric *selector.Query
	GroupBy                  string
	// DistinctBy names the field an event is one of, so lines repeating an event, as a
	// transcript repeats a model call's usage on each of its blocks, count it once.
	DistinctBy string
	// Forecast is how far ahead the budget is projected, at its burn over twice as long before:
	// the datafocal/forecast annotation, an hour by default.
	Forecast time.Duration
	Policies []Policy
	// Unbound is set when the SLI's source is foreign: Source is its query, kept and never evaluated.
	Unbound bool
	Source  string
}

// Window is an SLO's time window: rolling, or calendar-aligned in UTC from Start.
type Window struct {
	Rolling bool
	// Duration is the window's length, or Months its length in calendar months.
	Duration time.Duration
	Months   int
	Start    time.Time
}

// From is the start of the window that ends at at.
func (w Window) From(at time.Time) time.Time {
	switch {
	case w.Rolling:
		return at.Add(-w.Duration)
	case w.Months > 0:
		from := w.Start
		for !from.AddDate(0, w.Months, 0).After(at) {
			from = from.AddDate(0, w.Months, 0)
		}
		return from
	}
	into := at.Sub(w.Start) % w.Duration
	return at.Add(-into)
}

// Length is the window's span ending at or after at: its duration, or its months from where
// the window holding at began.
func (w Window) Length(at time.Time) time.Duration {
	if w.Months > 0 {
		from := w.From(at)
		return from.AddDate(0, w.Months, 0).Sub(from)
	}
	return w.Duration
}

// Policy is an alert policy's conditions.
type Policy struct {
	Name            string
	AlertWhenNoData bool
	Conditions      []Condition
}

// Condition holds when the burn rate meets Op Threshold over both Lookback and Short.
type Condition struct {
	Name, Severity, Op          string
	Threshold                   float64
	Lookback, Short, AlertAfter time.Duration
}

// Error is a document rejected on its own, at the field that rejected it.
type Error struct{ Path, Name, Field, Message string }

func (e Error) Error() string {
	return strings.Join(slices.DeleteFunc([]string{e.Path, e.Name, e.Field, e.Message}, func(s string) bool { return s == "" }), ": ")
}

// Limits are the querier's bounds a definition must fit.
type Limits struct{ Retention, FineHorizon time.Duration }

// Parse reads the YAML or JSON documents in content, resolving their references against
// dir's documents and theirs, theirs winning.
func Parse(content []byte, dir []Document, limits Limits) ([]Document, []Error) {
	docs, errs := decode(content, "")
	docs, more := compile(docs, dir, limits)
	return docs, append(errs, more...)
}

// Format writes docs as one multi-document YAML file or a JSON array.
func Format(docs []Document, format string) ([]byte, error) {
	raws := make([]map[string]any, len(docs))
	for i, d := range docs {
		raws[i] = d.Raw
	}
	switch format {
	case FormatJSON:
		b, err := json.MarshalIndent(raws, "", "  ")
		return append(b, '\n'), err
	case FormatYAML, "":
		var out bytes.Buffer
		for i, raw := range raws {
			b, err := yaml.Marshal(raw)
			if err != nil {
				return nil, err
			}
			if i > 0 {
				out.WriteString("---\n")
			}
			out.Write(b)
		}
		return out.Bytes(), nil
	}
	return nil, fmt.Errorf("format %q is neither yaml nor json", format)
}

var (
	separator = regexp.MustCompile(`(?m)^---[ \t]*$`)
	dnsLabel  = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

// decode splits content into documents and normalizes each, rejecting one it cannot read.
func decode(content []byte, path string) ([]Document, []Error) {
	var docs []Document
	var errs []Error
	for _, chunk := range separator.Split(string(content), -1) {
		j, err := yaml.YAMLToJSON([]byte(chunk))
		var v any
		if err == nil {
			err = json.Unmarshal(j, &v)
		}
		if err != nil {
			errs = append(errs, Error{Path: path, Message: err.Error()})
			continue
		}
		raws, ok := v.([]any)
		if !ok && v != nil {
			raws = []any{v}
		}
		for _, r := range raws {
			doc, err := normalized(r, path)
			if err != nil {
				errs = append(errs, *err)
				continue
			}
			docs = append(docs, doc)
		}
	}
	return docs, errs
}

// normalized is v as a document, its metric sources converted and their queries formatted.
func normalized(v any, path string) (Document, *Error) {
	raw, _ := v.(map[string]any)
	var h header
	if err := remarshal(raw, &h); err != nil {
		return Document{}, &Error{Path: path, Message: err.Error()}
	}
	doc := Document{Kind: h.Kind, Name: h.Metadata.Name, Path: path, Raw: raw}
	invalid := func(field, format string, args ...any) *Error {
		return &Error{Path: path, Name: doc.Name, Field: field, Message: fmt.Sprintf(format, args...)}
	}
	switch {
	case doc.Kind == "":
		return doc, invalid("kind", "is missing")
	case !dnsLabel.MatchString(doc.Name):
		return doc, invalid("metadata.name", "%q is not a DNS label", doc.Name)
	}
	if h.APIVersion == apiVersion {
		if _, field, err := normalize(raw["spec"], "spec", false, &doc.Notes); err != nil {
			return doc, invalid(field, "%v", err)
		}
		// Good and total rewritten alike note it once.
		seen := map[string]bool{}
		doc.Notes = slices.DeleteFunc(doc.Notes, func(n string) bool {
			dup := seen[n]
			seen[n] = true
			return dup
		})
	}
	if h.APIVersion == viewAPIVersion && doc.Kind == kindView {
		normalizeView(raw)
	}
	b, _ := json.Marshal(raw)
	sum := sha256.Sum256(b)
	doc.Version = hex.EncodeToString(sum[:8])
	return doc, nil
}

type header struct {
	APIVersion string
	Kind       string
	Metadata   meta
}

type meta struct {
	Name, DisplayName string
	Annotations       map[string]string
}

// remarshal decodes v, a decoded JSON value, into out.
func remarshal(v any, out any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

// normalize converts each metric source under v in place, noting each rewrite, where ratio
// is whether v is under a ratioMetric; it reports whether it converted a Loki source, or the
// field a query failed at.
func normalize(v any, field string, ratio bool, notes *[]string) (bool, string, error) {
	loki := false
	for _, c := range children(v, field, ratio) {
		converted, failed, err := c.normalize(notes)
		if err != nil {
			return false, failed, err
		}
		loki = loki || converted
	}
	if m, ok := v.(map[string]any); ok && ratio && loki && m["total"] != nil {
		narrow(m, notes)
		if counter, _ := m["counter"].(bool); counter {
			*notes = append(*notes, "ignored counter: true, since a count over a range is not a cumulative counter")
		}
	}
	return loki, "", nil
}

type child struct {
	v     any
	field string
	ratio bool
}

// normalize converts c when it is a metric source, and what it holds otherwise.
func (c child) normalize(notes *[]string) (bool, string, error) {
	if source, ok := c.v.(map[string]any); ok && strings.HasSuffix(c.field, ".metricSource") {
		converted, err := convert(source, c.ratio, notes)
		return converted, c.field + ".spec.query", err
	}
	return normalize(c.v, c.field, c.ratio, notes)
}

// children are v's elements or values, in key order, each under its field.
func children(v any, field string, ratio bool) []child {
	var out []child
	switch v := v.(type) {
	case []any:
		for i, e := range v {
			out = append(out, child{e, fmt.Sprintf("%s[%d]", field, i), ratio})
		}
	case map[string]any:
		for _, k := range slices.Sorted(maps.Keys(v)) {
			out = append(out, child{v[k], field + "." + k, ratio || k == "ratioMetric"})
		}
	}
	return out
}

// lokiSources are the metric source types the import converts from Loki, compared folded.
var lokiSources = []string{"grafanaLoki", "loki"}

// convert formats a DataFocal source's query and rewrites a Loki one into a DataFocal one,
// reporting whether it did, and leaves one whose query still does not parse, or any other
// source, as it is.
func convert(source map[string]any, ratio bool, notes *[]string) (bool, error) {
	typ, _ := source["type"].(string)
	spec, _ := source["spec"].(map[string]any)
	if typ == nativeSource {
		q, _ := spec["query"].(string)
		parsed, err := parseQuery(q)
		if err == nil {
			spec["query"], err = logqlLang.Format(parsed)
		}
		return false, err
	}
	if !slices.ContainsFunc(lokiSources, func(s string) bool { return strings.EqualFold(s, typ) }) {
		return false, nil
	}
	q, _ := cmp.Or(spec["logql"], spec["query"]).(string)
	query, rewrites, err := fromLoki(q, ratio)
	if err != nil {
		*notes = append(*notes, fmt.Sprintf("kept the %s source unbound: %v", typ, err))
		return false, nil
	}
	source["type"], source["spec"] = nativeSource, map[string]any{"query": query}
	*notes = append(*notes, rewrites...)
	return true, nil
}

func sourceQuery(ratio map[string]any, key string) (map[string]any, selector.Query, bool) {
	m, _ := ratio[key].(map[string]any)
	source, _ := m["metricSource"].(map[string]any)
	spec, _ := source["spec"].(map[string]any)
	q, _ := spec["query"].(string)
	parsed, err := parseQuery(q)
	return spec, parsed, source["type"] == nativeSource && err == nil
}

// narrow adds to a converted good or bad query the total's predicates it lacks, since a
// Loki good query need not repeat them and a DataFocal one must.
func narrow(ratio map[string]any, notes *[]string) {
	_, total, ok := sourceQuery(ratio, "total")
	for _, k := range []string{"good", "bad"} {
		spec, q, isQuery := sourceQuery(ratio, k)
		missing := slices.DeleteFunc(slices.Clone(total.Where), func(p selector.Predicate) bool { return slices.ContainsFunc(q.Where, equal(p)) })
		if !ok || !isQuery || len(missing) == 0 {
			continue
		}
		q.Where = append(q.Where, missing...)
		spec["query"], _ = logqlLang.Format(q)
		*notes = append(*notes, fmt.Sprintf("added the total query's predicates to the %s query, so it narrows total", k))
	}
}

var (
	logqlLang = logql.New()
	// lineFormatLog is the line_format that unwraps a container runtime's JSON wrapper.
	lineFormatLog    = regexp.MustCompile(`\|\s*line_format\s+"\{\{\s*\.log\s*\}\}"\s*(\|\s*json\b)`)
	outerAggregation = regexp.MustCompile(`^\s*(count|sum)\s*\(`)
)

// fromLoki rewrites a Loki query to the LogQL the parser reads, a ratio's to its log query,
// and returns the rewrites it made.
func fromLoki(q string, ratio bool) (string, []string, error) {
	q, outer, notes := unwrapLoki(q)
	parsed, err := parseQuery(q)
	if err == nil {
		err = lokiFault(parsed, q, ratio)
	}
	switch {
	case err != nil:
		return "", nil, err
	case !ratio && outer != "":
		notes = append(notes, fmt.Sprintf("dropped the outer %s(…), keeping its metric query", outer))
	case ratio:
		notes, parsed.Stage = append(notes, eventsNote(outer)), nil
	}
	formatted, err := logqlLang.Format(parsed)
	return formatted, notes, err
}

// lokiFault is why parsed, a Loki query q, cannot convert: it is no metric query, or a
// ratio's counts no events.
func lokiFault(parsed selector.Query, q string, ratio bool) error {
	switch {
	case parsed.Stage == nil || parsed.Stage.Kind != selector.StageAggregate || parsed.Stage.Rollup != nil:
		return fmt.Errorf("%q is not a metric query", q)
	case ratio && parsed.Stage.Measure != "count" && parsed.Stage.Measure != "rate":
		return fmt.Errorf("%q counts no events", q)
	}
	return nil
}

// unwrapLoki drops the line_format "{{.log}}" before | json, and the outer count or sum,
// which it returns, noting the first.
func unwrapLoki(q string) (query, outer string, notes []string) {
	if lineFormatLog.MatchString(q) {
		q = lineFormatLog.ReplaceAllString(q, "$1")
		notes = append(notes, `dropped line_format "{{.log}}" before | json, since ingest already strips a container runtime's JSON wrapper`)
	}
	if m := outerAggregation.FindStringSubmatch(q); m != nil && strings.HasSuffix(strings.TrimSpace(q), ")") {
		return strings.TrimSuffix(strings.TrimSpace(q), ")")[len(m[0]):], m[1], notes
	}
	return q, "", notes
}

// eventsNote is the rewrite of a ratio's range aggregation, under outer when it has one.
func eventsNote(outer string) string {
	switch outer {
	case "":
		return "dropped the range aggregation, counting the log query's events"
	case "count":
		return "dropped the outer count(…) and the range aggregation, counting the log query's events: Loki's count counts series, which as a ratio is no SLI"
	}
	return fmt.Sprintf("dropped the outer %s(…) and the range aggregation, counting the log query's events", outer)
}

func parseQuery(q string) (selector.Query, error) { return logqlLang.Parse(q) }

func equal(a selector.Predicate) func(selector.Predicate) bool {
	return func(b selector.Predicate) bool { return reflect.DeepEqual(a, b) }
}
