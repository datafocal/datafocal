// Package selector holds the request scope every tool call carries: time window and
// typed predicates. No free text reaches the engine, which is what
// keeps the surface injection-safe.
package selector

import (
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Op is a typed predicate operator.
type Op string

const (
	OpEq      Op = "eq"
	OpNe      Op = "ne"
	OpLt      Op = "lt"
	OpLe      Op = "le"
	OpGt      Op = "gt"
	OpGe      Op = "ge"
	OpExists  Op = "exists"
	OpMatch   Op = "match"   // substring/regex-lite — SCAN ONLY, never a hot-path predicate
	OpNoMatch Op = "nomatch" // OpMatch negated, and SCAN ONLY likewise
	OpRegex   Op = "regex"   // an RE2 regexp matching anywhere in the value, SCAN ONLY
	OpNoRegex Op = "noregex" // OpRegex negated, and SCAN ONLY likewise
)

// AllOps is every operator in schema-advertised order, the single source of the
// published schema's op enum.
var AllOps = []Op{OpEq, OpNe, OpLt, OpLe, OpGt, OpGe, OpExists, OpMatch, OpNoMatch, OpRegex, OpNoRegex}

// Quantifier says which of a list column's elements a predicate must match; a scalar
// column ignores it, and an empty one is QuantifierAny.
type Quantifier string

const (
	QuantifierAny Quantifier = "any"
	QuantifierAll Quantifier = "all"
)

// AllQuantifiers is every quantifier, the single source of the published schema's enum.
var AllQuantifiers = []Quantifier{QuantifierAny, QuantifierAll}

// Predicate is one typed filter over a single field, or a group of alternatives in Any
// that matches when one of them does, with Field, Op and Value empty.
type Predicate struct {
	Field      string      `json:"field,omitempty"`
	Op         Op          `json:"op,omitempty"`
	Value      string      `json:"value,omitempty"`
	Quantifier Quantifier  `json:"quantifier,omitempty"`
	Any        []Predicate `json:"any,omitempty"`
}

// ParsedPredicate carries the value parsed once, where Numeric holds only for a finite
// number, Regexp only for a regex operator's valid pattern, and Any shadows Predicate.Any.
type ParsedPredicate struct {
	Predicate
	Number  float64
	Numeric bool
	Regexp  *regexp.Regexp
	Any     []ParsedPredicate
	// Equal is Any as each field's values when every alternative is a field's equality, as a
	// batch of traces' ids is, so a row reads each field once rather than once an alternative.
	Equal map[string]map[string]struct{} `json:"-"`
}

// Parse parses the predicate's value, or each of a group's alternatives.
func (p Predicate) Parse() ParsedPredicate {
	parsed := ParsedPredicate{Predicate: p}
	if len(p.Any) > 0 {
		parsed.Any = ParsePredicates(p.Any)
		parsed.Equal = equalities(parsed.Any)
		return parsed
	}
	if v, err := strconv.ParseFloat(p.Value, 64); err == nil && !math.IsNaN(v) && !math.IsInf(v, 0) {
		parsed.Number, parsed.Numeric = v, true
	}
	if p.Op == OpRegex || p.Op == OpNoRegex {
		// Compile rejects an invalid pattern before a read is planned, so a nil one selects nothing.
		parsed.Regexp, _ = regexp.Compile(p.Value)
	}
	return parsed
}

// equalities is alternatives as each field's values, nil unless every one is a field's equality.
func equalities(alternatives []ParsedPredicate) map[string]map[string]struct{} {
	equal := map[string]map[string]struct{}{}
	for _, a := range alternatives {
		if len(a.Any) > 0 || a.Op != OpEq || a.Quantifier == QuantifierAll {
			return nil
		}
		if equal[a.Field] == nil {
			equal[a.Field] = map[string]struct{}{}
		}
		equal[a.Field][a.Value] = struct{}{}
	}
	return equal
}

// ParsePredicates parses each predicate in where.
func ParsePredicates(where []Predicate) []ParsedPredicate {
	parsed := make([]ParsedPredicate, len(where))
	for i, p := range where {
		parsed[i] = p.Parse()
	}
	return parsed
}

var variable = regexp.MustCompile(`^\$([A-Za-z_]\w*)$`)

// Substitute replaces each = or != predicate whose whole value is a view's $name with
// vars[name], dropping it, or the group of alternatives holding it, when that is All ("*"),
// and refusing a name vars lacks.
func Substitute(where []Predicate, vars map[string]string) ([]Predicate, error) {
	var out []Predicate
	for _, p := range where {
		p, keep, err := p.substitute(vars)
		if err != nil {
			return nil, err
		}
		if keep {
			out = append(out, p)
		}
	}
	return out, nil
}

// substitute is p with its variable substituted, and whether it is kept.
func (p Predicate) substitute(vars map[string]string) (Predicate, bool, error) {
	if len(p.Any) > 0 {
		alternatives, err := Substitute(p.Any, vars)
		kept := len(alternatives) == len(p.Any)
		p.Any = alternatives
		return p, kept, err
	}
	m := variable.FindStringSubmatch(p.Value)
	if m == nil || (p.Op != OpEq && p.Op != OpNe) {
		return p, true, nil
	}
	value, ok := vars[m[1]]
	if !ok {
		return p, false, fmt.Errorf("$%s has no value", m[1])
	}
	p.Value = value
	return p, value != "*", nil
}

// Matches is the row-level decision that catalog and plan pruning must never be stricter
// than.
func (p ParsedPredicate) Matches(value string, present bool) bool {
	if p.Op == OpExists {
		return present
	}
	if !present {
		// As LogQL's level="", so a group_by's "" key selects the rows it counted.
		return p.Op == OpEq && p.Value == ""
	}
	switch p.Op {
	case OpEq:
		return value == p.Value
	case OpNe:
		return value != p.Value
	case OpMatch:
		return strings.Contains(value, p.Value)
	case OpNoMatch:
		return !strings.Contains(value, p.Value)
	case OpRegex:
		return p.Regexp != nil && p.Regexp.MatchString(value)
	case OpNoRegex:
		return p.Regexp != nil && !p.Regexp.MatchString(value)
	case OpLt, OpLe, OpGt, OpGe:
		return p.comparesOrdered(value)
	default:
		// Compile rejects unknown operators before a read is planned, so one reaching
		// here selects nothing.
		return false
	}
}

// MatchesList is Matches over a present list's elements: any of them, or every one under
// QuantifierAll, which an empty list never satisfies.
func (p ParsedPredicate) MatchesList(elems []string) bool {
	if p.Op == OpExists {
		return true
	}
	if p.Quantifier == QuantifierAll {
		return len(elems) > 0 && !slices.ContainsFunc(elems, func(e string) bool { return !p.Matches(e, true) })
	}
	return slices.ContainsFunc(elems, func(e string) bool { return p.Matches(e, true) })
}

// comparesOrdered applies an ordered operator to value against the predicate's value.
func (p ParsedPredicate) comparesOrdered(value string) bool {
	cmp := p.compare(value)
	switch p.Op {
	case OpLt:
		return cmp < 0
	case OpLe:
		return cmp <= 0
	case OpGt:
		return cmp > 0
	case OpGe:
		return cmp >= 0
	default:
		return false
	}
}

// compare orders value against the predicate's value, numerically where both are numbers.
func (p ParsedPredicate) compare(value string) int {
	got, err := strconv.ParseFloat(value, 64)
	switch {
	case !p.Numeric || err != nil:
		return strings.Compare(value, p.Value)
	case got < p.Number:
		return -1
	case got > p.Number:
		return 1
	default:
		return 0
	}
}

// TimeSpec is a caller's unresolved time bounds, each RFC3339 or relative ("now-1h"),
// and not named TimeRange because segment.TimeRange is the range of data already on disk.
type TimeSpec struct {
	From string `json:"from,omitempty" jsonschema:"RFC3339 or relative (now-1h)"`
	To   string `json:"to,omitempty" jsonschema:"RFC3339 or relative (now)"`
}

// Selector is the wire form of the Lens every tool call carries; Resolve turns it into
// the Resolved form the read path takes.
type Selector struct {
	Time  TimeSpec    `json:"time,omitempty"`
	Where []Predicate `json:"where,omitempty"`
}

// Resolved is a Selector with its time bounds resolved to an absolute window.
type Resolved struct {
	Window Window
	Where  []Predicate
}

// Query is the typed form every query language parses into: predicates, then at most one
// stage naming the tool that answers them, where no stage searches.
type Query struct {
	Where []Predicate `json:"where"`
	Stage *Stage      `json:"stage,omitempty"`
}

// Stage kinds, each naming one tool's operation.
const (
	StagePatterns  = "patterns"
	StageAggregate = "aggregate"
	StageTop       = "top"
	StageExplain   = "explain"
	StageTrace     = "trace"
)

// Stage is a query's operation over its predicates, where a top stage keeps the K largest of its aggregate's series.
type Stage struct {
	Kind    string `json:"kind"`
	Measure string `json:"measure,omitempty"`
	GroupBy string `json:"group_by,omitempty"`
	Bins    string `json:"bins,omitempty"`
	K       int    `json:"k,omitempty"`
	// Measures, Having and Sort are as tools.AggregateRequest carries them.
	Measures []MeasureSpec  `json:"measures,omitempty"`
	Having   []HavingClause `json:"having,omitempty"`
	Sort     string         `json:"sort,omitempty"`
	Rollup   *Rollup        `json:"rollup,omitempty"`
	// OrdinalOf bins by a row's position within its value of this column, as
	// tools.AggregateRequest carries it.
	OrdinalOf string `json:"ordinal_of,omitempty"`
}

// Rollup aggregates an aggregate's groups a second time, each group one row of its
// measures' totals and labels, binned by the time of its first row.
type Rollup struct {
	Measure string         `json:"measure" jsonschema:"count|ratio|sum:values[i]|p<NN>:values[i], or values[i] with buckets"`
	Where   []HavingClause `json:"where,omitempty" jsonschema:"the comparisons of a group's measure totals a ratio's numerator groups satisfy"`
	GroupBy string         `json:"group_by,omitempty" jsonschema:"label[i], the label of a group's measure i, or empty for one series"`
	Bins    string         `json:"bins,omitempty" jsonschema:"<n>s|<n>m|<n>h|<n>d of at least 15s, such as 10m|auto|all"`
	Buckets []float64      `json:"buckets,omitempty" jsonschema:"ascending histogram edges, counting the groups whose values[i] falls between each two instead of a measure"`
}

// MeasureSpec is one of an aggregate's several measures, over only the rows Where also matches.
// DistinctBy counts each value of its column once, at the first row read with it.
type MeasureSpec struct {
	Measure    string      `json:"measure"`
	Where      []Predicate `json:"where,omitempty"`
	DistinctBy string      `json:"distinct_by,omitempty" jsonschema:"count each value of this column once, at the first row read with it, as for a model call's usage repeated on each line of its answer; rows without the column all count"`
}

// HavingClause keeps the groups whose measure at Index compares to Value by Op.
type HavingClause struct {
	Index int     `json:"index"`
	Op    Op      `json:"op" jsonschema:"eq|ne|lt|le|gt|ge"`
	Value float64 `json:"value"`
}
