// Package sql is the lang.Language for a subset of SQL over one table, logs, plus the
// table functions patterns(logs), explain(logs) and traces(logs) for the DataFocal stages
// SQL has no form for.
package sql

import (
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/datafocal/datafocal/internal/query/lang"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

const (
	likeAnchor     = "(?s)^"
	table          = "logs"
	quantilePrefix = "p"
)

type sql struct{}

// New returns the SQL language.
func New() lang.Language { return sql{} }

func (sql) Name() string { return "sql" }

var ops = map[string]selector.Op{
	"=": selector.OpEq, "!=": selector.OpNe, "<>": selector.OpNe,
	"<": selector.OpLt, "<=": selector.OpLe, ">": selector.OpGt, ">=": selector.OpGe,
}

// matchOps and regexOps are the substring and regex operators, or under NOT their negation.
var (
	matchOps = map[bool]selector.Op{false: selector.OpMatch, true: selector.OpNoMatch}
	regexOps = map[bool]selector.Op{false: selector.OpRegex, true: selector.OpNoRegex}
)

// tableFunctions maps each table function to the stage it names.
var tableFunctions = map[string]string{
	"patterns": selector.StagePatterns, "explain": selector.StageExplain, "traces": selector.StageTrace,
}

// parser reads SQL's clauses, where GROUP BY and ORDER BY fill in the aggregate stage the
// SELECT list starts.
type parser struct {
	*lang.Lexer
	stage *selector.Stage
}

func (sql) Parse(q string) (parsed selector.Query, err error) {
	defer lang.Recover(&err)
	p := parser{Lexer: lang.NewLexer(q, scanner.ScanIdents|scanner.ScanInts|scanner.ScanFloats, nil, "!<>", "=>")}
	parsed = p.query()
	if p.Tok != scanner.EOF {
		p.Fail("unexpected %q", p.Text)
	}
	return parsed, nil
}

func (p *parser) is(keyword string) bool {
	return p.Tok == scanner.Ident && strings.EqualFold(p.Text, keyword)
}

func (p *parser) expect(text string) {
	if p.Text != text && !p.is(text) {
		p.Fail("expected %s, found %q", text, p.Text)
	}
	p.Next()
}

// open reads a "(" that does not start a subquery.
func (p *parser) open() {
	p.expect("(")
	if p.is("SELECT") {
		p.Fail("subqueries are not supported")
	}
}

func (p *parser) ident() string {
	if p.Tok != scanner.Ident {
		p.Fail("expected a column, found %q", p.Text)
	}
	name := p.Text
	p.Next()
	return name
}

// str reads a single-quoted string, where two quotes are one.
func (p *parser) str() string {
	if p.Tok != '\'' {
		p.Fail("expected a string, found %q", p.Text)
	}
	var b strings.Builder
	for {
		switch r := p.S.Next(); {
		case r == scanner.EOF:
			p.FailAt(len(p.Src), "string not terminated")
		case r == '\'' && p.S.Peek() == '\'':
			b.WriteRune(p.S.Next())
		case r == '\'':
			p.Next()
			return b.String()
		default:
			b.WriteRune(r)
		}
	}
}

func (p *parser) query() selector.Query {
	p.expect("SELECT")
	columnAt := p.Pos
	column, measure := p.columns()
	p.expect("FROM")
	kind, kindAt := p.source()
	var q selector.Query
	if p.is("WHERE") {
		p.Next()
		q.Where = p.conditions()
	}
	if measure == "" {
		if kind != "" {
			q.Stage = &selector.Stage{Kind: kind}
		}
		p.searchTail()
		return q
	}
	if kind != "" {
		p.FailAt(kindAt, "the %s stage cannot be aggregated", kind)
	}
	p.stage = &selector.Stage{Kind: selector.StageAggregate, Measure: measure, Bins: "all"}
	if p.is("GROUP") {
		p.Next()
		p.expect("BY")
		p.groupBy()
	}
	if p.stage.GroupBy != column {
		p.FailAt(columnAt, "the selected column must be the GROUP BY column")
	}
	if p.is("ORDER") {
		p.top()
	}
	q.Stage = p.stage
	return q
}

// columns reads * or one measure, beside at most one grouping column.
func (p *parser) columns() (column, measure string) {
	if p.Text == "*" {
		p.Next()
		return "", ""
	}
	at := p.Pos
	name := p.ident()
	if p.Text != "(" {
		column = name
		p.expect(",")
		at = p.Pos
		name = p.ident()
	}
	if p.Text != "(" {
		p.FailAt(at, "more than one grouping column is not supported")
	}
	measure = p.measure(name)
	if p.is("OVER") {
		p.Fail("window functions are not supported")
	}
	if p.Text == "," {
		p.Next()
		p.Fail("more than one measure is not supported")
	}
	return column, measure
}

// measure reads fn(*) as the measure fn, fn(f) as fn:f and quantile(q)(f) as the
// percentile's measure over f.
func (p *parser) measure(fn string) string {
	name := strings.ToLower(fn)
	p.open()
	if name == "quantile" {
		name = quantilePrefix + strconv.FormatFloat(math.Round(p.Number()*1e6)/1e4, 'f', -1, 64)
		p.expect(")")
		p.expect("(")
	}
	if p.Text == "*" {
		p.Next()
		p.expect(")")
		return name
	}
	field := p.ident()
	p.expect(")")
	return name + ":" + field
}

// source reads the logs table or a table function over it, returning the function's stage.
func (p *parser) source() (kind string, at int) {
	at = p.Pos
	if p.Text == "(" {
		p.open()
	}
	if kind = tableFunctions[strings.ToLower(p.Text)]; kind != "" {
		p.Next()
		p.open()
		p.table()
		p.expect(")")
	} else {
		p.table()
	}
	if p.Text == "," || slices.ContainsFunc([]string{"JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS"}, p.is) {
		p.Fail("joins are not supported")
	}
	return kind, at
}

func (p *parser) table() {
	at := p.Pos
	if name := p.ident(); !strings.EqualFold(name, table) {
		p.FailAt(at, "unknown table %s; the one table is %s", name, table)
	}
}

// searchTail reads a search's ORDER BY ts DESC, its only order, and LIMIT, which the
// query request's limit replaces.
func (p *parser) searchTail() {
	if p.is("ORDER") {
		p.Next()
		p.expect("BY")
		p.expect(segment.ColTimestamp)
		p.expect("DESC")
	}
	if p.is("LIMIT") {
		p.limit()
	}
}

// limit reads LIMIT and its positive whole number.
func (p *parser) limit() int {
	p.expect("LIMIT")
	at := p.Pos
	k := p.Number()
	if k < 1 || k != math.Trunc(k) {
		p.FailAt(at, "LIMIT takes a positive whole number")
	}
	return int(k)
}

// groupBy reads at most one column beside a bin(ts, <width>).
func (p *parser) groupBy() {
	for {
		at := p.Pos
		name := p.ident()
		switch {
		case strings.EqualFold(name, "bin") && p.Text == "(":
			p.open()
			p.expect(segment.ColTimestamp)
			p.expect(",")
			from := p.Pos
			for p.Text != ")" && p.Tok != scanner.EOF {
				p.Next()
			}
			p.stage.Bins = strings.TrimSpace(p.Src[from:p.Pos])
			p.expect(")")
		case p.stage.GroupBy != "":
			p.FailAt(at, "more than one grouping column is not supported")
		default:
			p.stage.GroupBy = name
		}
		if p.Text != "," {
			return
		}
		p.Next()
	}
}

// top reads ORDER BY <measure> DESC LIMIT k, which makes the aggregate a top stage.
func (p *parser) top() {
	p.Next()
	p.expect("BY")
	at := p.Pos
	if p.measure(p.ident()) != p.stage.Measure {
		p.FailAt(at, "ORDER BY must name the selected measure")
	}
	p.expect("DESC")
	p.stage.Kind, p.stage.K = selector.StageTop, p.limit()
}

// conditions reads conditions joined by AND.
func (p *parser) conditions() []selector.Predicate {
	where := []selector.Predicate{p.alternatives()}
	for p.is("AND") {
		p.Next()
		where = append(where, p.alternatives())
	}
	return where
}

// alternatives reads conditions joined by OR as one predicate's Any.
func (p *parser) alternatives() selector.Predicate {
	var alts []selector.Predicate
	for {
		var pred selector.Predicate
		if p.Text == "(" {
			p.open()
			pred = p.alternatives()
			p.expect(")")
		} else {
			pred = p.condition()
		}
		if len(pred.Any) > 0 {
			alts = append(alts, pred.Any...)
		} else {
			alts = append(alts, pred)
		}
		if !p.is("OR") {
			break
		}
		p.Next()
	}
	if len(alts) == 1 {
		return alts[0]
	}
	return selector.Predicate{Any: alts}
}

func (p *parser) condition() selector.Predicate {
	negated := p.is("NOT")
	if negated {
		p.Next()
	}
	at := p.Pos
	field := p.ident()
	if strings.EqualFold(field, "regexp_like") && p.Text == "(" {
		return p.regexpLike(negated)
	}
	if negated {
		p.FailAt(at, "a leading NOT applies only to regexp_like")
	}
	switch {
	case p.is("IS"):
		p.Next()
		p.expect("NOT")
		p.expect("NULL")
		return selector.Predicate{Field: field, Op: selector.OpExists}
	case p.is("IN"):
		return p.in(field)
	case p.is("LIKE") || p.is("NOT"):
		return p.like(field)
	}
	op, ok := ops[p.Text]
	if !ok {
		p.Fail("expected a comparison after %s, found %q", field, p.Text)
	}
	p.Next()
	return selector.Predicate{Field: field, Op: op, Value: p.value()}
}

// regexpLike reads regexp_like's column and regular expression as a match.
func (p *parser) regexpLike(negated bool) selector.Predicate {
	p.open()
	pred := selector.Predicate{Field: p.ident(), Op: regexOps[negated]}
	p.expect(",")
	at := p.Pos
	pred.Value = p.str()
	if _, err := regexp.Compile(pred.Value); err != nil {
		p.FailAt(at, "invalid regex: %v", err)
	}
	p.expect(")")
	return pred
}

// like reads [NOT] LIKE's pattern as a match on field.
func (p *parser) like(field string) selector.Predicate {
	negated := p.is("NOT")
	if negated {
		p.Next()
	}
	p.expect("LIKE")
	return likePredicate(field, p.str(), negated)
}

// in reads IN's list as a group of equalities on field.
func (p *parser) in(field string) selector.Predicate {
	p.Next()
	p.open()
	alts := []selector.Predicate{{Field: field, Op: selector.OpEq, Value: p.value()}}
	for p.Text == "," {
		p.Next()
		alts = append(alts, selector.Predicate{Field: field, Op: selector.OpEq, Value: p.value()})
	}
	p.expect(")")
	return selector.Predicate{Any: alts}
}

// value reads a number as its text, or a string.
func (p *parser) value() string {
	if p.Tok == scanner.Int || p.Tok == scanner.Float {
		v := p.Text
		p.Next()
		return v
	}
	return p.str()
}

// likePredicate is a substring match for %text%, which is what LIKE means there, and
// otherwise a match of the anchored regex the pattern is.
func likePredicate(field, pattern string, negated bool) selector.Predicate {
	inner := strings.TrimSuffix(strings.TrimPrefix(pattern, "%"), "%")
	if len(inner)+2 == len(pattern) && !strings.ContainsAny(inner, "%_") {
		return selector.Predicate{Field: field, Op: matchOps[negated], Value: inner}
	}
	return selector.Predicate{Field: field, Op: regexOps[negated], Value: likeRegexp(pattern)}
}

// likeRegexp is the anchored regular expression a LIKE pattern matches, where % and _
// match a newline as they do in SQL.
func likeRegexp(pattern string) string {
	var b strings.Builder
	b.WriteString(likeAnchor)
	for _, r := range pattern {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	return b.String() + "$"
}
