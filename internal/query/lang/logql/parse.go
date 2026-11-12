// Package logql is the lang.Language for a subset of Loki's LogQL: stream selectors, line
// and label filters and metric queries, plus | patterns, | explain and | trace, which are
// DataFocal stages and not part of Loki.
package logql

import (
	"cmp"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/datafocal/datafocal/internal/query/lang"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

const (
	quantilePrefix = "p"
	countMeasure   = "count"
	sumMeasure     = "sum"
	// A label's regex matches the whole value, as in Loki and Prometheus, and a line's
	// matches anywhere in it; unanchored is the pattern that matches any text.
	anchorPrefix = "^(?:"
	anchorSuffix = ")$"
	unanchored   = "(?s:.*)"
)

type logql struct{}

// New returns the LogQL language.
func New() lang.Language { return logql{} }

func (logql) Name() string { return "logql" }

// streamOps are the stream selector's operators, and ops the label filter's.
var streamOps = map[string]selector.Op{
	"=": selector.OpEq, "!=": selector.OpNe, "=~": selector.OpRegex, "!~": selector.OpNoRegex,
}

var ops = map[string]selector.Op{
	"=": selector.OpEq, "==": selector.OpEq, "!=": selector.OpNe, "=~": selector.OpRegex, "!~": selector.OpNoRegex,
	"<": selector.OpLt, "<=": selector.OpLe, ">": selector.OpGt, ">=": selector.OpGe,
}

// lineFilters are the line filter operators, each a match on the message.
var lineFilters = map[string]selector.Op{
	"|=": selector.OpMatch, "!=": selector.OpNoMatch, "|~": selector.OpRegex, "!~": selector.OpNoRegex,
}

// rangeMeasures maps each range aggregation to the aggregate measure it computes, where a
// quantile's is quantilePrefix and its percentile. max_run_over_time and changes_over_time
// are DataFocal extensions, the latter after Prometheus's changes().
var rangeMeasures = map[string]string{
	"count_over_time": countMeasure, "rate": "rate", "sum_over_time": sumMeasure, "quantile_over_time": quantilePrefix,
	"first_over_time": "first", "last_over_time": "last", "min_over_time": "min", "max_over_time": "max",
	"max_run_over_time": "max_run", "changes_over_time": "changes",
}

var binaryOps = []string{"+", "-", "*", "/", "%", "^", "==", "!=", "<", "<=", ">", ">=", "and", "or", "unless"}

// parser is a Lexer with the grammar's productions.
type parser struct{ *lang.Lexer }

// logExpr is a log query: its predicates, its DataFocal stage and a metric query's
// unwrapped field and the column it bins by position within.
type logExpr struct {
	where   []selector.Predicate
	stage   string
	stageAt int
	unwrap  string
	ordinal string
}

func (logql) Parse(q string) (parsed selector.Query, err error) {
	defer lang.Recover(&err)
	p := parser{lang.NewLexer(q, scanner.ScanIdents|scanner.ScanInts|scanner.ScanFloats|scanner.ScanStrings|scanner.ScanRawStrings, isIdentRune, "|!=<>", "=~")}
	if p.Text == "{" {
		parsed = p.logQuery()
	} else {
		parsed = p.metricQuery()
	}
	if p.Tok != scanner.EOF {
		if slices.Contains(binaryOps, p.Text) {
			p.FailAt(p.Pos, "binary operations between queries are not supported")
		}
		p.FailAt(p.Pos, "unexpected %q", p.Text)
	}
	return parsed, nil
}

// isIdentRune lets a label hold '.' and '-' after its first rune, as a JSON field's name does.
func isIdentRune(ch rune, i int) bool {
	return ch == '_' || unicode.IsLetter(ch) || (i > 0 && (unicode.IsDigit(ch) || ch == '.' || ch == '-'))
}

func (p *parser) expect(text string) {
	if p.Text != text {
		p.FailAt(p.Pos, "expected %q, found %q", text, p.Text)
	}
	p.Next()
}

func (p *parser) ident() string {
	if p.Tok != scanner.Ident {
		p.FailAt(p.Pos, "expected a label, found %q", p.Text)
	}
	name := p.Text
	p.Next()
	return name
}

func (p *parser) str() string {
	if p.Tok != scanner.String && p.Tok != scanner.RawString {
		p.FailAt(p.Pos, "expected a string, found %q", p.Text)
	}
	s, err := strconv.Unquote(p.Text)
	if err != nil {
		p.FailAt(p.Pos, "%v", err)
	}
	p.Next()
	return s
}

func (p *parser) logQuery() selector.Query {
	log := p.logExpr()
	if log.unwrap != "" || log.ordinal != "" {
		p.FailAt(p.Pos, "unwrap and ordinal are only allowed inside a range aggregation")
	}
	q := selector.Query{Where: log.where}
	if log.stage != "" {
		q.Stage = &selector.Stage{Kind: log.stage}
	}
	return q
}

func (p *parser) logExpr() logExpr {
	var log logExpr
	p.expect("{")
	for p.Text != "}" {
		if len(log.where) > 0 {
			p.expect(",")
		}
		label := p.ident()
		at := p.Pos
		op, ok := streamOps[p.Text]
		if !ok {
			p.FailAt(at, "expected =, !=, =~ or !~ in a stream selector, found %q", p.Text)
		}
		p.Next()
		pred := selector.Predicate{Field: label, Op: op, Value: p.pattern(op, true)}
		// Every row has a service, so matching any is no predicate, which the summary answers.
		if !matchesAnyService(pred) {
			log.where = append(log.where, pred)
		}
	}
	p.Next()
	for p.Text == "|" || lineFilters[p.Text] != "" {
		if log.stage != "" || log.unwrap != "" {
			p.FailAt(p.Pos, "%q follows the last stage; stages do not chain", p.Text)
		}
		p.pipelineStage(&log)
	}
	return log
}

// matchesAnyService is whether pred is service=~".+" or service=~".*".
func matchesAnyService(pred selector.Predicate) bool {
	return pred.Field == segment.ColService && pred.Op == selector.OpRegex &&
		(pred.Value == anchorPrefix+".+"+anchorSuffix || pred.Value == anchorPrefix+".*"+anchorSuffix)
}

func (p *parser) pipelineStage(log *logExpr) {
	if op := lineFilters[p.Text]; op != "" {
		p.Next()
		log.where = append(log.where, selector.Predicate{Field: segment.ColMessage, Op: op, Value: p.pattern(op, false)})
		return
	}
	p.Next()
	at := p.Pos
	switch name := p.ident(); name {
	case "json", "logfmt", "unpack":
		// Every format is parsed at ingest, so a parser stage has nothing to do.
	case selector.StagePatterns, selector.StageExplain, selector.StageTrace:
		log.stage, log.stageAt = name, at
	case "unwrap":
		if log.unwrap = p.ident(); (log.unwrap == "len" || log.unwrap == "bytes") && p.Text == "(" {
			p.Next()
			log.unwrap += "(" + p.ident() + ")"
			p.expect(")")
		}
	case "ordinal":
		// A DataFocal extension: the metric query bins by position within the label's value.
		if log.ordinal = p.groupBy(); log.ordinal == "" {
			p.FailAt(p.Pos, "expected by (<label>) after ordinal, found %q", p.Text)
		}
	default:
		if _, ok := ops[p.Text]; !ok {
			p.FailAt(at, "the %s stage is not supported", name)
		}
		log.where = append(log.where, p.labelFilter(name)...)
	}
}

// labelFilter reads comparisons joined by one of and, "," or or, where or makes them
// alternatives of one predicate.
func (p *parser) labelFilter(label string) []selector.Predicate {
	preds := []selector.Predicate{p.comparison(label)}
	joiner := ""
	for p.Text == "and" || p.Text == "," || p.Text == "or" {
		if joiner != "" && (joiner == "or") != (p.Text == "or") {
			p.FailAt(p.Pos, "a label filter cannot mix and with or")
		}
		joiner = p.Text
		p.Next()
		preds = append(preds, p.comparison(p.ident()))
	}
	if joiner == "or" {
		return []selector.Predicate{{Any: preds}}
	}
	return preds
}

func (p *parser) comparison(label string) selector.Predicate {
	op, ok := ops[p.Text]
	if !ok {
		p.FailAt(p.Pos, "expected a comparison after %s, found %q", label, p.Text)
	}
	p.Next()
	if (p.Tok == scanner.Int || p.Tok == scanner.Float) && op != selector.OpRegex && op != selector.OpNoRegex {
		value := p.Text
		p.Next()
		return selector.Predicate{Field: label, Op: op, Value: value}
	}
	return selector.Predicate{Field: label, Op: op, Value: p.pattern(op, true)}
}

// pattern reads a string operand, refusing a regex RE2 cannot compile and anchoring a
// label's.
func (p *parser) pattern(op selector.Op, label bool) string {
	at := p.Pos
	s := p.str()
	if op != selector.OpRegex && op != selector.OpNoRegex {
		return s
	}
	if _, err := regexp.Compile(s); err != nil {
		p.FailAt(at, "invalid regex: %v", err)
	}
	if label {
		return anchorPrefix + s + anchorSuffix
	}
	return s
}

func (p *parser) metricQuery() selector.Query {
	if p.Text != "topk" {
		return p.measures()
	}
	p.Next()
	p.expect("(")
	at := p.Pos
	k := p.Number()
	if k < 1 || k != math.Trunc(k) {
		p.FailAt(at, "topk takes a positive whole number of series")
	}
	p.expect(",")
	q := p.aggregation(selector.StageTop)
	q.Stage.K = int(k)
	p.expect(")")
	return q
}

// measureTerm is one of a query's comma-separated aggregations, with the order sort or
// sort_desc gives it and the comparison it is filtered by.
type measureTerm struct {
	q      selector.Query
	sort   string
	having *selector.HavingClause
}

// measures reads one or more measure terms, folding any but one bare aggregation into one
// stage's Measures.
func (p *parser) measures() selector.Query {
	at := p.Pos
	terms := []measureTerm{p.measureTerm()}
	for p.Text == "," {
		p.Next()
		terms = append(terms, p.measureTerm())
	}
	if len(terms) == 1 && terms[0].sort == "" && terms[0].having == nil {
		return terms[0].q
	}
	shared := sharedWhere(terms)
	stage := &selector.Stage{Kind: selector.StageAggregate, GroupBy: terms[0].q.Stage.GroupBy, Bins: terms[0].q.Stage.Bins, OrdinalOf: terms[0].q.Stage.OrdinalOf}
	var sorts []string
	for i, t := range terms {
		if fault := termFault(t, stage); fault != "" {
			p.FailAt(at, "%s", fault)
		}
		own := slices.DeleteFunc(slices.Clone(t.q.Where), func(pred selector.Predicate) bool { return slices.ContainsFunc(shared, equal(pred)) })
		stage.Measures = append(stage.Measures, selector.MeasureSpec{Measure: t.q.Stage.Measure, Where: own})
		if t.having != nil {
			t.having.Index = i
			stage.Having = append(stage.Having, *t.having)
		}
		if t.sort != "" {
			sorts = append(sorts, fmt.Sprintf("%d:%s", i, t.sort))
		}
	}
	stage.Sort = strings.Join(sorts, ",")
	return selector.Query{Where: shared, Stage: stage}
}

// termFault is why t cannot be one of stage's several measures, or empty when it can.
func termFault(t measureTerm, stage *selector.Stage) string {
	if t.q.Stage.Rollup != nil {
		return "a rollup cannot be sorted, compared or listed with other measures"
	}
	if t.q.Stage.GroupBy != stage.GroupBy || t.q.Stage.Bins != stage.Bins || t.q.Stage.OrdinalOf != stage.OrdinalOf {
		return "several measures must share one grouping and one range"
	}
	return ""
}

func (p *parser) measureTerm() measureTerm {
	sorted := map[string]string{"sort": "asc", "sort_desc": "desc"}[p.Text]
	if sorted != "" {
		p.Next()
		p.expect("(")
	}
	t := measureTerm{q: p.aggregation(selector.StageAggregate), sort: sorted}
	if op, ok := ops[p.Text]; ok && op != selector.OpRegex && op != selector.OpNoRegex {
		p.Next()
		t.having = &selector.HavingClause{Op: op, Value: p.Number()}
	}
	if sorted != "" {
		p.expect(")")
	}
	return t
}

// sharedWhere is the predicates of the first term every other term also has.
func sharedWhere(terms []measureTerm) []selector.Predicate {
	var shared []selector.Predicate
	for _, pred := range terms[0].q.Where {
		if !slices.ContainsFunc(terms, func(t measureTerm) bool { return !slices.ContainsFunc(t.q.Where, equal(pred)) }) {
			shared = append(shared, pred)
		}
	}
	return shared
}

func equal(a selector.Predicate) func(selector.Predicate) bool {
	return func(b selector.Predicate) bool { return reflect.DeepEqual(a, b) }
}

// aggregation reads a range aggregation, alone or under sum, grouped by at most one
// label, where a rollup is grouped by its inner aggregation's.
func (p *parser) aggregation(kind string) selector.Query {
	if p.Text != "sum" {
		q := p.rangeAggregation(kind)
		if q.Stage.Rollup == nil {
			q.Stage.GroupBy = p.groupBy()
		}
		return q
	}
	p.Next()
	at := p.Pos
	group := p.groupBy()
	p.expect("(")
	q := p.rangeAggregation(kind)
	p.expect(")")
	if q.Stage.Rollup != nil {
		p.FailAt(at, "a rollup cannot be summed")
	}
	q.Stage.GroupBy = cmp.Or(group, p.groupBy())
	return q
}

func (p *parser) groupBy() string {
	if p.Text != "by" {
		return ""
	}
	p.Next()
	p.expect("(")
	label := p.ident()
	if p.Text == "," {
		p.FailAt(p.Pos, "more than one grouping label is not supported")
	}
	p.expect(")")
	return label
}

func (p *parser) rangeAggregation(kind string) selector.Query {
	at, fn := p.Pos, p.Text
	measure, ok := rangeMeasures[fn]
	if p.Tok != scanner.Ident {
		p.FailAt(p.Pos, "expected a log or metric query, found %q", fn)
	}
	if !ok {
		p.FailAt(at, "the %s function is not supported", fn)
	}
	p.Next()
	p.expect("(")
	if measure == quantilePrefix {
		measure += strconv.FormatFloat(math.Round(p.Number()*1e6)/1e4, 'f', -1, 64)
		p.expect(",")
	}
	// A parenthesized log query, (Q)[r], is the same log query.
	parenthesized := p.Text == "("
	if parenthesized {
		p.Next()
	} else if p.Text != "{" {
		return p.rollup(kind, fn, at, measure)
	}
	log := p.logExpr()
	if parenthesized {
		p.expect(")")
	}
	if log.stage != "" {
		p.FailAt(log.stageAt, "the %s stage cannot be inside a metric query", log.stage)
	}
	if log.unwrap != "" {
		measure += ":" + log.unwrap
	}
	bins := p.bins()
	p.expect(")")
	return selector.Query{Where: log.where, Stage: &selector.Stage{Kind: kind, Measure: measure, Bins: bins, OrdinalOf: log.ordinal}}
}

// rollup reads fn over an inner aggregation and an optional range as the inner
// aggregation rolled up across its groups by fn's measure of their first one.
func (p *parser) rollup(kind, fn string, at int, measure string) selector.Query {
	q := p.aggregation(kind)
	switch {
	case q.Stage.Rollup != nil:
		p.FailAt(at, "more than two levels of aggregation are not supported")
	case kind == selector.StageTop:
		p.FailAt(at, "topk cannot rank a rollup's series")
	case measure == sumMeasure || strings.HasPrefix(measure, quantilePrefix):
		measure += ":values[0]"
	case measure != countMeasure:
		p.FailAt(at, "the %s function cannot roll up an aggregation", fn)
	}
	q.Stage.Rollup = &selector.Rollup{Measure: measure}
	if p.Text == "[" {
		q.Stage.Rollup.Bins = p.bins()
	}
	p.expect(")")
	return q
}

// bins reads a range, the text between its brackets.
func (p *parser) bins() string {
	p.expect("[")
	from := p.Pos
	for p.Text != "]" && p.Tok != scanner.EOF {
		p.Next()
	}
	bins := strings.TrimSpace(p.Src[from:p.Pos])
	p.expect("]")
	return bins
}
