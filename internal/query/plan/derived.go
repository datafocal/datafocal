package plan

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"text/scanner"
	"unicode"

	"github.com/datafocal/datafocal/internal/selector"
)

// DerivedColumn is a column computed per row from stored columns.
type DerivedColumn struct {
	Name string `json:"name"` // e.g. "cost_usd"
	Expr string `json:"expr"` // e.g. "coalesce(cost.total, message.usage.input_tokens/1e6*lookup(message.model, price_in))"
}

// Derived is the derived columns a querier defines, compiled; the zero value defines none.
type Derived struct {
	exprs   map[string]derivedExpr
	version string
}

type derivedExpr struct {
	eval    eval
	columns []string // the stored columns eval reads
	numeric bool     // its outermost operation is arithmetic, so every value it has is a number
}

// eval computes an expression from a row's column values, false where it has no value.
type eval func(value func(column string) ([]byte, bool)) (result, bool)

// result is an expression's value: text, or the number arithmetic made, formatted only once
// read as text, since parsing each operand's formatted result was most of a derived column.
type result struct {
	text  string
	num   float64
	isNum bool
}

func (r result) String() string {
	if r.isNum {
		return strconv.FormatFloat(r.num, 'f', -1, 64)
	}
	return r.text
}

// NewDerived compiles defs, whose lookups read tables, refusing an expression that does
// not parse or reads another derived column.
func NewDerived(defs []DerivedColumn, tables map[string]map[string]string) (Derived, error) {
	if len(defs) == 0 {
		return Derived{}, nil
	}
	d := Derived{exprs: make(map[string]derivedExpr, len(defs))}
	for _, def := range defs {
		e, err := parseDerived(def.Expr, tables)
		if err != nil {
			return Derived{}, fmt.Errorf("derived column %s: %w", def.Name, err)
		}
		d.exprs[def.Name] = e
	}
	for _, def := range defs {
		if i := slices.IndexFunc(d.exprs[def.Name].columns, d.Has); i >= 0 {
			return Derived{}, fmt.Errorf("derived column %s reads derived column %s", def.Name, d.exprs[def.Name].columns[i])
		}
	}
	sorted := slices.SortedFunc(slices.Values(defs), func(a, b DerivedColumn) int { return cmp.Compare(a.Name, b.Name) })
	encoded, err := json.Marshal(struct {
		Defs   []DerivedColumn
		Tables map[string]map[string]string
	}{sorted, tables})
	sum := sha256.Sum256(encoded)
	d.version = hex.EncodeToString(sum[:6])
	return d, err
}

// parseDerived compiles one definition's expression, whose lookups read tables.
func parseDerived(expr string, tables map[string]map[string]string) (derivedExpr, error) {
	p := exprParser{tables: tables}
	p.s.Init(strings.NewReader(expr))
	p.s.IsIdentRune = func(ch rune, i int) bool {
		return ch == '_' || unicode.IsLetter(ch) || (i > 0 && (ch == '.' || unicode.IsDigit(ch)))
	}
	p.s.Error = func(*scanner.Scanner, string) {} // a bad token fails the parse
	p.next()
	e, err := p.expr(0)
	if err == nil && p.tok != scanner.EOF {
		err = fmt.Errorf("unexpected %s", p.s.TokenText())
	}
	return derivedExpr{eval: e, columns: p.columns, numeric: p.arithmetic}, err
}

// DerivedField is a derived column as rank_fields reports one: the stored columns it reads,
// and whether every value it has is a number.
type DerivedField struct {
	Name    string
	Reads   []string
	Numeric bool
}

// Fields is every derived column, by name.
func (d Derived) Fields() []DerivedField {
	fields := make([]DerivedField, 0, len(d.exprs))
	for name, e := range d.exprs {
		fields = append(fields, DerivedField{Name: name, Reads: e.columns, Numeric: e.numeric})
	}
	slices.SortFunc(fields, func(a, b DerivedField) int { return cmp.Compare(a.Name, b.Name) })
	return fields
}

// Has reports whether column is derived.
func (d Derived) Has(column string) bool {
	_, ok := d.exprs[column]
	return ok
}

// Version identifies the definitions, "" when there are none.
func (d Derived) Version() string { return d.version }

// Stored is columns with each derived column replaced by the stored columns it reads.
func (d Derived) Stored(columns []string) []string {
	if len(d.exprs) == 0 {
		return columns
	}
	out := make([]string, 0, len(columns))
	for _, col := range columns {
		if e, ok := d.exprs[col]; ok {
			out = append(out, e.columns...)
			continue
		}
		out = append(out, col)
	}
	return out
}

// Column is a derived column's value on each of n rows, nil where a row has none, computed
// from value's stored columns of row i; false when column is not derived.
func (d Derived) Column(column string, n int, value func(column string, i int) ([]byte, bool)) ([][]byte, bool) {
	e, ok := d.exprs[column]
	if !ok {
		return nil, false
	}
	values := make([][]byte, n)
	for i := range values {
		if v, ok := e.eval(func(stored string) ([]byte, bool) { return value(stored, i) }); ok {
			values[i] = []byte(v.String())
		}
	}
	return values, true
}

// prunable drops the predicates reading a derived column, which no descriptor or footer
// describes, so a derived column never prunes.
func (d Derived) prunable(where []selector.Predicate) []selector.Predicate {
	if len(d.exprs) == 0 {
		return where
	}
	reads := func(p selector.Predicate) bool { return d.Has(p.Field) }
	return slices.DeleteFunc(slices.Clone(where), func(p selector.Predicate) bool {
		return reads(p) || slices.ContainsFunc(p.Any, reads)
	})
}

// exprParser parses expr := term {(+|-) term}, term := operand {(*|/) operand},
// operand := number | string | column | (expr) | lookup(expr, table[, expr]) | coalesce(expr, ...).
type exprParser struct {
	s       scanner.Scanner
	tok     rune
	tables  map[string]map[string]string
	columns []string
	// calls is how many function calls deep the parser is, and arithmetic whether it built an
	// operator outside every call, which makes the whole expression's value a number.
	calls      int
	arithmetic bool
}

func (p *exprParser) next() { p.tok = p.s.Scan() }

func (p *exprParser) expect(tok rune) error {
	if p.tok != tok {
		return fmt.Errorf("want %s at %q", scanner.TokenString(tok), p.s.TokenText())
	}
	p.next()
	return nil
}

// levels are the arithmetic operators by precedence, loosest first.
var levels = []string{"+-", "*/"}

// expr parses operands joined left to right by the operators of level and tighter.
func (p *exprParser) expr(level int) (eval, error) {
	if level == len(levels) {
		return p.operand()
	}
	left, err := p.expr(level + 1)
	for err == nil && strings.ContainsRune(levels[level], p.tok) {
		op := p.tok
		p.next()
		var right eval
		if right, err = p.expr(level + 1); err == nil {
			left = arithmetic(op, left, right)
			p.arithmetic = p.arithmetic || p.calls == 0
		}
	}
	return left, err
}

func (p *exprParser) operand() (eval, error) {
	tok, text := p.tok, p.s.TokenText()
	p.next()
	switch {
	case tok == scanner.Int || tok == scanner.Float:
		return constant(text), nil
	case tok == scanner.String:
		s, err := strconv.Unquote(text)
		return constant(s), err
	case tok == '(':
		e, err := p.expr(0)
		return e, cmp.Or(err, p.expect(')'))
	case tok == scanner.Ident && p.tok == '(':
		p.next()
		return p.call(text)
	case tok == scanner.Ident:
		p.columns = append(p.columns, text)
		return column(text), nil
	}
	return nil, fmt.Errorf("unexpected %q", text)
}

func (p *exprParser) call(name string) (eval, error) {
	p.calls++
	defer func() { p.calls-- }()
	switch name {
	case "lookup":
		return p.lookup()
	case "coalesce":
		return p.coalesce()
	}
	return nil, fmt.Errorf("unknown function %s", name)
}

func (p *exprParser) lookup() (eval, error) {
	key, err := p.expr(0)
	if err = cmp.Or(err, p.expect(',')); err != nil {
		return nil, err
	}
	table, ok := p.tables[p.s.TokenText()]
	if p.tok != scanner.Ident || !ok {
		return nil, fmt.Errorf("unknown lookup table %q", p.s.TokenText())
	}
	p.next()
	fallback := eval(func(func(string) ([]byte, bool)) (result, bool) { return result{}, false })
	if p.tok == ',' {
		p.next()
		if fallback, err = p.expr(0); err != nil {
			return nil, err
		}
	}
	return func(value func(string) ([]byte, bool)) (result, bool) {
		k, ok := key(value)
		if v, found := table[k.String()]; ok && found {
			return result{text: v}, true
		}
		return fallback(value)
	}, p.expect(')')
}

func (p *exprParser) coalesce() (eval, error) {
	var args []eval
	for {
		arg, err := p.expr(0)
		if err != nil {
			return nil, err
		}
		if args = append(args, arg); p.tok != ',' {
			break
		}
		p.next()
	}
	return func(value func(string) ([]byte, bool)) (result, bool) {
		for _, arg := range args {
			if v, ok := arg(value); ok {
				return v, true
			}
		}
		return result{}, false
	}, p.expect(')')
}

func constant(v string) eval {
	return func(func(string) ([]byte, bool)) (result, bool) { return result{text: v}, true }
}

// column reads a stored column, which a row holding it empty does not have.
func column(name string) eval {
	return func(value func(string) ([]byte, bool)) (result, bool) {
		v, ok := value(name)
		return result{text: string(v)}, ok && len(v) > 0
	}
}

// arithmetic has a value only where both operands are numbers and the result is finite.
func arithmetic(op rune, left, right eval) eval {
	return func(value func(string) ([]byte, bool)) (result, bool) {
		a, okA := number(left, value)
		b, okB := number(right, value)
		var v float64
		switch op {
		case '+':
			v = a + b
		case '-':
			v = a - b
		case '*':
			v = a * b
		default:
			v = a / b
		}
		if !okA || !okB || math.IsInf(v, 0) || math.IsNaN(v) {
			return result{}, false
		}
		return result{num: v, isNum: true}, true
	}
}

// number is e's value as a number. An absent value is parsed for none, since a failed parse
// allocates its error, and most rows lack most of an expression's columns.
func number(e eval, value func(string) ([]byte, bool)) (float64, bool) {
	r, ok := e(value)
	if r.isNum || !ok || r.text == "" {
		return r.num, ok && r.isNum
	}
	f, err := strconv.ParseFloat(r.text, 64)
	return f, err == nil
}
