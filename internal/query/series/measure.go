package series

import (
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/segment"
)

// Kind is what a measure computes per bin.
type Kind int

const (
	KindCount Kind = iota
	KindRate
	KindQuantile
	KindDistinct
	KindSum
	KindRatio
	// The sequence kinds read a group's values in time order, so only a scan answers them.
	KindFirst
	KindLast
	KindMin
	KindMax
	KindMaxRun
	KindChanges
	KindSpan
)

// fieldPolicy declares whether a measure takes a ":<field>" suffix.
type fieldPolicy int

const (
	fieldOptional  fieldPolicy = iota // may qualify a field, e.g. p95 or p95:duration
	fieldForbidden                    // bare measure, e.g. count, rate
	fieldRequired                     // must qualify a field, e.g. sum, distinct
)

// Measure is a parsed measure: what it computes, the column it reads, whether it reads
// the column's length in place of its value, and for a quantile which one.
type Measure struct {
	Kind  Kind
	Field string
	// Length is "len" for a value's length in characters and "bytes" for its length
	// encoded, read in place of the value, or empty for the value itself.
	Length   string
	Quantile float64
}

// measureSpec is everything that varies between measures: how it is spelled on
// the wire, whether it takes a field, and whether its answer is approximate.
type measureSpec struct {
	// name is the wire spelling. It is empty for the quantile family, which is
	// parsed rather than enumerated (see parseQuantile) and so is unreachable by
	// name lookup.
	name        string
	fieldPolicy fieldPolicy
	approx      bool
	// lengths is whether the field may be a len(<col>) or bytes(<col>).
	lengths bool
}

// measureTable is the registry of measures, keyed by the kind ParseMeasure
// resolves a request to.
var measureTable = map[Kind]measureSpec{
	KindCount:    {name: "count", fieldPolicy: fieldForbidden},
	KindRate:     {name: "rate", fieldPolicy: fieldForbidden},
	KindSum:      {name: "sum", fieldPolicy: fieldRequired, lengths: true},
	KindDistinct: {name: "distinct", fieldPolicy: fieldRequired, approx: true},
	KindQuantile: {fieldPolicy: fieldOptional, approx: true, lengths: true},
	// The field is optional and picks what is being divided: bare "ratio" is a quotient
	// of row counts, "ratio:bytes" a quotient of sums over that column.
	KindRatio:   {name: "ratio", fieldPolicy: fieldOptional},
	KindFirst:   {name: "first", fieldPolicy: fieldRequired},
	KindLast:    {name: "last", fieldPolicy: fieldRequired},
	KindMin:     {name: "min", fieldPolicy: fieldRequired, lengths: true},
	KindMax:     {name: "max", fieldPolicy: fieldRequired, lengths: true},
	KindMaxRun:  {name: "max_run", fieldPolicy: fieldRequired},
	KindChanges: {name: "changes", fieldPolicy: fieldRequired},
	KindSpan:    {name: "span", fieldPolicy: fieldRequired},
}

// String is the kind's wire name, empty for the quantile family, which is parsed.
func (k Kind) String() string { return measureTable[k].name }

// Sequence reports whether k reads each group's values in time order.
func (k Kind) Sequence() bool { return k >= KindFirst }

// label names the kind in an error: the quantile family by its family, and a rate by the
// count it scans for.
func (k Kind) label() string {
	switch k {
	case KindQuantile:
		return "quantile"
	case KindRate:
		return KindCount.String()
	}
	return k.String()
}

// specByName resolves a wire measure name to its kind and spec.
func specByName(name string) (Kind, measureSpec, bool) {
	for kind, spec := range measureTable {
		if spec.name != "" && spec.name == name {
			return kind, spec, true
		}
	}
	return 0, measureSpec{}, false
}

// ParseMeasure parses a measure string into its kind, field, and quantile.
// Fixed measures come from measureTable ('count', 'rate', 'sum:<field>',
// 'distinct:<field>', 'first:<field>', ...); quantiles are the open 'p<NN>' family ('p50', 'p95',
// 'p99', 'p999', ..., optionally ':<field>'). A sum, quantile, min or max may read
// 'len(<col>)' or 'bytes(<col>)' in place of a column. Returns InvalidRequestError for
// unrecognized formats.
func ParseMeasure(s string) (Measure, error) {
	if s == "" {
		return Measure{}, query.Invalidf("measure is empty")
	}

	name, field, hasField := strings.Cut(s, ":")
	if hasField && field == "" {
		return Measure{}, query.Invalidf("measure field is empty in %q", s)
	}
	// A field must not itself contain a colon (rejects "measure:field:extra").
	if strings.Contains(field, ":") {
		return Measure{}, query.Invalidf("malformed measure %q", s)
	}

	// Quantile family (open set): parse rather than enumerate so arbitrary
	// percentiles work. Field is optional — a bare quantile resolves the sole
	// sketch column downstream (soleDistribution).
	q, isQuantile, err := parseQuantile(name)
	if err != nil {
		return Measure{}, err
	}
	kind, spec := KindQuantile, measureTable[KindQuantile]
	if !isQuantile {
		var ok bool
		if kind, spec, ok = specByName(name); !ok {
			return Measure{}, query.Invalidf("unsupported measure %q", s)
		}
	}

	if err := spec.fieldPolicy.check(name, s, hasField); err != nil {
		return Measure{}, err
	}
	length, field := parseLength(field)
	if length != "" && !spec.lengths {
		return Measure{}, query.Invalidf("measure %q does not take a %s(<field>)", name, length)
	}
	return Measure{Kind: kind, Field: field, Length: length, Quantile: q}, nil
}

// parseLength splits a len(<col>) or bytes(<col>) field into the length and the column,
// and leaves any other field whole.
func parseLength(field string) (length, column string) {
	for _, fn := range []string{"len", "bytes"} {
		inner, ok := strings.CutPrefix(field, fn+"(")
		if col, closed := strings.CutSuffix(inner, ")"); ok && closed && col != "" {
			return fn, col
		}
	}
	return "", field
}

// check refuses a measure s, named name, whose field suffix the policy does not allow.
func (p fieldPolicy) check(name, s string, hasField bool) error {
	switch {
	case p == fieldRequired && !hasField:
		return query.Invalidf("measure %q requires a field (%s:<field>)", name, name)
	case p == fieldForbidden && hasField:
		return query.Invalidf("measure %q does not take a field (got %q)", name, s)
	}
	return nil
}

// parseQuantile interprets the open 'p<NN>' quantile family. The digits after
// 'p' are read as a fraction: p50→0.50, p95→0.95, p999→0.999 (value =
// digits / 10^len(digits)), which reproduces the classic p50/p95/p99 and
// extends cleanly to tail percentiles. It returns ok=false (not an error) for
// anything that is not a 'p<digits>' token, so the caller falls through to the
// fixed-measure table; it returns an error only for a well-formed quantile
// whose value is out of the open interval (0,1), e.g. 'p0'.
func parseQuantile(name string) (q float64, ok bool, err error) {
	if len(name) < 2 || name[0] != 'p' {
		return 0, false, nil
	}
	digits := name[1:]
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, false, nil // e.g. "python", "p95x" — not a quantile
		}
	}
	n, convErr := strconv.Atoi(digits)
	if convErr != nil {
		return 0, false, nil // absurdly long digit run; treat as not-a-quantile
	}
	q = float64(n) / math.Pow(10, float64(len(digits)))
	if q <= 0 || q >= 1 {
		return 0, false, query.Invalidf("quantile %q out of range (0,1)", name)
	}
	return q, true, nil
}

// NeedsGroupScan reports whether the grouping dimension has to be read out of the
// rows rather than taken once per segment.
//
// It asks the data, not the dimension's name. Comparing the name against "service"
// assumed that a segment's service is whatever the stream that wrote it claims, which
// holds for a sidecar or an in-process library and fails for the ordinary case of one
// agent tailing many services on a host: every segment is stamped with the agent, the
// real service is a value on each row, and the two paths answer the same question
// differently — the descriptor path with the agent's name, the scan with the service's.
// Neither errors, so the wrong one is indistinguishable from the right one.
//
// One dissenting segment forces the scan for all of them, because the accumulator
// keys every segment the same way. That is conservative in cost and never in the
// answer.
func NeedsGroupScan(descs []segment.Descriptor, groupBy string) bool {
	// An empty group_by means group by pattern_id, which is per-row by construction; Total
	// keys every row "", which no segment needs reading to say.
	if groupBy == "" {
		return true
	}
	if groupBy == Total {
		return false
	}
	for _, d := range descs {
		if _, ok := plan.SegmentValue(d, groupBy); !ok {
			return true
		}
	}
	return false
}

// isFinite checks whether a float64 is finite (not NaN or Inf).
func isFinite(f float64) bool {
	return !math.IsNaN(f) && !math.IsInf(f, 0)
}

// soleDistribution is the column a bare quantile measures in footer, ok false when the
// footer has no distribution and an error suggesting them all when it has several.
func soleDistribution(footer segment.Footer) (column string, ok bool, err error) {
	columns := slices.Sorted(maps.Keys(footer.Distributions))
	switch len(columns) {
	case 0:
		return "", false, nil
	case 1:
		return columns[0], true, nil
	default:
		return "", false, query.NewAPIError(query.ErrCodeInvalidRequest, "ambiguous quantile target: multiple numeric columns").WithSuggestedFields(columns)
	}
}
