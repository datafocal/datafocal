package sql

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

var opText = map[selector.Op]string{
	selector.OpEq: "=", selector.OpNe: "!=", selector.OpLt: "<", selector.OpLe: "<=", selector.OpGt: ">", selector.OpGe: ">=",
}

func (sql) Format(q selector.Query) (string, error) {
	where, err := formatWhere(q.Where)
	if err != nil || q.Stage == nil {
		return "SELECT * FROM " + table + where, err
	}
	st := *q.Stage
	for fn, kind := range tableFunctions {
		if kind == st.Kind {
			return "SELECT * FROM " + fn + "(" + table + ")" + where, nil
		}
	}
	if (st.Kind != selector.StageAggregate && st.Kind != selector.StageTop) || len(st.Measures) > 0 || st.Rollup != nil || st.OrdinalOf != "" {
		return "", fmt.Errorf("sql cannot express the %q stage %+v", st.Kind, st)
	}
	return formatAggregate(st, where), nil
}

// formatAggregate prints an aggregate or top stage over the printed where clause.
func formatAggregate(st selector.Stage, where string) string {
	measure := formatMeasure(st.Measure)
	columns, groups := measure, []string{}
	if st.GroupBy != "" {
		columns, groups = st.GroupBy+", "+measure, append(groups, st.GroupBy)
	}
	if st.Bins != "all" {
		groups = append(groups, "bin("+segment.ColTimestamp+", "+st.Bins+")")
	}
	s := "SELECT " + columns + " FROM " + table + where
	if len(groups) > 0 {
		s += " GROUP BY " + strings.Join(groups, ", ")
	}
	if st.Kind == selector.StageTop {
		s += fmt.Sprintf(" ORDER BY %s DESC LIMIT %d", measure, st.K)
	}
	return s
}

// formatMeasure prints a measure as the call measure parses to it.
func formatMeasure(m string) string {
	name, field, _ := strings.Cut(m, ":")
	field = cmp.Or(field, "*")
	if percent, err := strconv.ParseFloat(strings.TrimPrefix(name, quantilePrefix), 64); err == nil && strings.HasPrefix(name, quantilePrefix) {
		return fmt.Sprintf("quantile(%s)(%s)", strconv.FormatFloat(percent/100, 'f', -1, 64), field)
	}
	return name + "(" + field + ")"
}

func formatWhere(where []selector.Predicate) (string, error) {
	if len(where) == 0 {
		return "", nil
	}
	conditions := make([]string, len(where))
	for i, p := range where {
		var err error
		if conditions[i], err = formatPredicate(p); err != nil {
			return "", err
		}
	}
	return " WHERE " + strings.Join(conditions, " AND "), nil
}

// formatPredicate prints a group as its alternatives joined by OR, which IN parses to as
// well.
func formatPredicate(p selector.Predicate) (string, error) {
	if len(p.Any) == 0 {
		return formatCondition(p)
	}
	alternatives := make([]string, len(p.Any))
	for i, alt := range p.Any {
		var err error
		if alternatives[i], err = formatCondition(alt); err != nil {
			return "", err
		}
	}
	return "(" + strings.Join(alternatives, " OR ") + ")", nil
}

func formatCondition(p selector.Predicate) (string, error) {
	if len(p.Any) > 0 || p.Quantifier != "" {
		return "", fmt.Errorf("sql cannot express the predicate %+v", p)
	}
	switch p.Op {
	case selector.OpExists:
		return p.Field + " IS NOT NULL", nil
	case selector.OpMatch, selector.OpNoMatch, selector.OpRegex, selector.OpNoRegex:
		return formatMatch(p)
	}
	op, ok := opText[p.Op]
	if !ok {
		return "", fmt.Errorf("sql has no operator %q", p.Op)
	}
	return p.Field + " " + op + " " + formatValue(p.Value), nil
}

// formatValue prints a plain number in its canonical form bare, so it scans back to the
// same text, and anything else quoted.
func formatValue(v string) string {
	if f, err := strconv.ParseFloat(v, 64); err == nil && strings.Trim(v, "0123456789.") == "" && strconv.FormatFloat(f, 'f', -1, 64) == v {
		return v
	}
	return quote(v)
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// formatMatch prints a match as the LIKE that parses back to it, or a regex without one
// as regexp_like.
func formatMatch(p selector.Predicate) (string, error) {
	negated := p.Op == selector.OpNoMatch || p.Op == selector.OpNoRegex
	not := map[bool]string{true: "NOT "}[negated]
	pattern := "%" + p.Value + "%"
	if p.Op == selector.OpRegex || p.Op == selector.OpNoRegex {
		pattern = likePattern(p.Value)
	}
	if like := likePredicate(p.Field, pattern, negated); like.Op == p.Op && like.Value == p.Value {
		return p.Field + " " + not + "LIKE " + quote(pattern), nil
	}
	if p.Op == selector.OpMatch || p.Op == selector.OpNoMatch {
		return "", fmt.Errorf("sql cannot express a substring holding %% or _: %q", p.Value)
	}
	return not + "regexp_like(" + p.Field + ", " + quote(p.Value) + ")", nil
}

// likePattern is the LIKE pattern re would be if likeRegexp made it.
func likePattern(re string) string {
	inner := strings.TrimSuffix(strings.TrimPrefix(re, likeAnchor), "$")
	var b strings.Builder
	for i := 0; i < len(inner); i++ {
		switch {
		case strings.HasPrefix(inner[i:], ".*"):
			b.WriteByte('%')
			i++
		case inner[i] == '.':
			b.WriteByte('_')
		case inner[i] == '\\' && i+1 < len(inner):
			i++
			b.WriteByte(inner[i])
		default:
			b.WriteByte(inner[i])
		}
	}
	return b.String()
}
