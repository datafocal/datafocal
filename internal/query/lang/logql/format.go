package logql

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// opText and lineFilterText print what ops and lineFilters parse, and regexOf is the regex
// operator a substring match prints as on a label.
var (
	opText = map[selector.Op]string{
		selector.OpEq: "=", selector.OpNe: "!=", selector.OpRegex: "=~", selector.OpNoRegex: "!~",
		selector.OpLt: "<", selector.OpLe: "<=", selector.OpGt: ">", selector.OpGe: ">=",
	}
	regexOf        = map[selector.Op]selector.Op{selector.OpMatch: selector.OpRegex, selector.OpNoMatch: selector.OpNoRegex}
	lineFilterText = map[selector.Op]string{
		selector.OpMatch: " |= ", selector.OpNoMatch: " != ", selector.OpRegex: " |~ ", selector.OpNoRegex: " !~ ",
	}
)

// Format prints the leading run of predicates a stream selector can hold inside it, and
// each later one as its own filter, so the predicates parse back in order.
func (logql) Format(q selector.Query) (string, error) {
	log, err := formatLog(q.Where)
	if err != nil || q.Stage == nil {
		return log, err
	}
	switch q.Stage.Kind {
	case selector.StagePatterns, selector.StageExplain, selector.StageTrace:
		return log + " | " + q.Stage.Kind, nil
	case selector.StageAggregate:
		if q.Stage.Rollup != nil {
			return formatRollup(log, *q.Stage)
		}
		if len(q.Stage.Measures) > 0 {
			return formatMeasures(q.Where, *q.Stage)
		}
		return formatAggregation(log, *q.Stage)
	case selector.StageTop:
		agg, err := formatAggregation(log, *q.Stage)
		return fmt.Sprintf("topk(%d, %s)", q.Stage.K, agg), err
	default:
		return "", fmt.Errorf("logql has no %q stage", q.Stage.Kind)
	}
}

func formatLog(where []selector.Predicate) (string, error) {
	stream := 0
	for stream < len(where) && inStream(where[stream]) {
		stream++
	}
	matchers := make([]string, stream)
	for i, p := range where[:stream] {
		op, value, _ := labelOperand(p)
		matchers[i] = p.Field + op + value
	}
	log := "{" + strings.Join(matchers, ", ") + "}"
	for _, p := range where[stream:] {
		filter, err := formatFilter(p)
		if err != nil {
			return "", err
		}
		log += filter
	}
	return log, nil
}

func inStream(p selector.Predicate) bool {
	op, _, ok := labelOperand(p)
	_, inSelector := streamOps[op]
	return ok && inSelector && len(p.Any) == 0 && p.Field != segment.ColMessage && p.Quantifier == ""
}

func formatFilter(p selector.Predicate) (string, error) {
	if filter, ok := lineFilterText[p.Op]; ok && p.Field == segment.ColMessage && p.Quantifier == "" {
		return filter + strconv.Quote(p.Value), nil
	}
	alternatives := []selector.Predicate{p}
	if len(p.Any) > 0 {
		alternatives = p.Any
	}
	comparisons := make([]string, len(alternatives))
	for i, alt := range alternatives {
		op, value, ok := labelOperand(alt)
		if !ok || len(alt.Any) > 0 || alt.Quantifier == selector.QuantifierAll {
			return "", fmt.Errorf("logql cannot express the predicate %+v", alt)
		}
		comparisons[i] = alt.Field + op + value
	}
	return " | " + strings.Join(comparisons, " or "), nil
}

// labelOperand prints a label comparison's operator and value: a regex without the anchors
// Parse wraps it in, and a substring match as the regex that finds it.
func labelOperand(p selector.Predicate) (op, value string, ok bool) {
	switch p.Op {
	case selector.OpMatch, selector.OpNoMatch:
		return opText[regexOf[p.Op]], strconv.Quote(unanchored + regexp.QuoteMeta(p.Value) + unanchored), true
	case selector.OpRegex, selector.OpNoRegex:
		return opText[p.Op], strconv.Quote(unanchor(p.Value)), true
	}
	op, ok = opText[p.Op]
	return op, formatValue(p), ok
}

// unanchor strips the anchors Parse adds to a label's regex, or widens a regex that has
// none to match anywhere in the value.
func unanchor(re string) string {
	inner, prefixed := strings.CutPrefix(re, anchorPrefix)
	inner, suffixed := strings.CutSuffix(inner, anchorSuffix)
	if _, err := regexp.Compile(inner); prefixed && suffixed && err == nil {
		return inner
	}
	return unanchored + "(?:" + re + ")" + unanchored
}

// formatValue prints a number compared by order bare, as Loki requires, and anything else
// quoted.
func formatValue(p selector.Predicate) string {
	_, err := strconv.ParseFloat(p.Value, 64)
	ordered := p.Op == selector.OpLt || p.Op == selector.OpLe || p.Op == selector.OpGt || p.Op == selector.OpGe
	if ordered && err == nil && strings.Trim(p.Value, "0123456789.") == "" {
		return p.Value
	}
	return strconv.Quote(p.Value)
}

// formatAggregation prints a count, rate or sum summed by its group, and any other
// measure grouped after it, as Loki groups an unwrapped range aggregation.
func formatAggregation(log string, st selector.Stage) (string, error) {
	name, field, _ := strings.Cut(st.Measure, ":")
	if st.OrdinalOf != "" {
		log += " | ordinal by (" + st.OrdinalOf + ")"
	}
	if field != "" {
		log += " | unwrap " + field
	}
	for fn, measure := range rangeMeasures {
		if measure != name || measure == quantilePrefix {
			continue
		}
		agg := fmt.Sprintf("%s(%s [%s])", fn, log, st.Bins)
		// A count or rate unwraps no field, and sum_over_time is the one unwrapped
		// aggregation Loki sums rather than groups.
		if st.GroupBy != "" && (field == "" || fn == "sum_over_time") {
			return "sum by (" + st.GroupBy + ") (" + agg + ")", nil
		}
		return groupedAfter(agg, st.GroupBy), nil
	}
	percent, err := strconv.ParseFloat(strings.TrimPrefix(name, quantilePrefix), 64)
	if err != nil || !strings.HasPrefix(name, quantilePrefix) {
		return "", fmt.Errorf("logql has no measure %q", st.Measure)
	}
	agg := fmt.Sprintf("quantile_over_time(%s, %s [%s])", strconv.FormatFloat(percent/100, 'f', -1, 64), log, st.Bins)
	return groupedAfter(agg, st.GroupBy), nil
}

// formatRollup prints the rollup's measure as a range aggregation over the inner one,
// which a rollup of the first measure's count, sum or quantile, ungrouped, can be.
func formatRollup(log string, st selector.Stage) (string, error) {
	r := *st.Rollup
	name, field, _ := strings.Cut(r.Measure, ":")
	if (field != "values[0]") == (name != countMeasure) || r.Where != nil || r.GroupBy != "" || r.Buckets != nil || len(st.Measures) > 0 {
		return "", fmt.Errorf("logql cannot express the rollup %+v", r)
	}
	st.Rollup = nil
	inner, err := formatAggregation(log, st)
	if err != nil {
		return "", err
	}
	return formatAggregation(inner, selector.Stage{Measure: name, Bins: r.Bins})
}

// formatMeasures prints each measure as an aggregation over the shared predicates and its
// own, compared and sorted as the stage says.
func formatMeasures(shared []selector.Predicate, st selector.Stage) (string, error) {
	sorts := strings.Split(st.Sort, ",")
	terms := make([]string, len(st.Measures))
	for i, m := range st.Measures {
		log, err := formatLog(slices.Concat(shared, m.Where))
		if err != nil {
			return "", err
		}
		if terms[i], err = formatAggregation(log, selector.Stage{Measure: m.Measure, GroupBy: st.GroupBy, Bins: st.Bins, OrdinalOf: st.OrdinalOf}); err != nil {
			return "", err
		}
		for _, h := range st.Having {
			if h.Index == i {
				terms[i] += " " + opText[h.Op] + " " + strconv.FormatFloat(h.Value, 'f', -1, 64)
			}
		}
		switch {
		case slices.Contains(sorts, strconv.Itoa(i)+":desc"):
			terms[i] = "sort_desc(" + terms[i] + ")"
		case slices.Contains(sorts, strconv.Itoa(i)+":asc"):
			terms[i] = "sort(" + terms[i] + ")"
		}
	}
	return strings.Join(terms, ", "), nil
}

// groupedAfter is agg grouped by group after it, or agg alone without one.
func groupedAfter(agg, group string) string {
	if group == "" {
		return agg
	}
	return agg + " by (" + group + ")"
}
