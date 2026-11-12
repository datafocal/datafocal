// Package lang defines what a query language is, so each one is a package behind
// Language and the rest of the system sees only a selector.Query.
package lang

import (
	"fmt"
	"strconv"
	"strings"
	"text/scanner"

	"github.com/datafocal/datafocal/internal/selector"
)

// Language parses its text into a typed query and prints one back, where
// Format(Parse(q)) parses to the same query.
type Language interface {
	Name() string
	Parse(q string) (selector.Query, error) // the error is a *ParseError
	Format(q selector.Query) (string, error)
}

// ParseError is a query the language cannot read, at the byte offset of the fault.
type ParseError struct {
	Offset int
	Msg    string
}

func (e *ParseError) Error() string { return fmt.Sprintf("at offset %d: %s", e.Offset, e.Msg) }

// Registry is the languages a querier serves, keyed by name.
type Registry map[string]Language

// Lexer reads one token ahead for a language's parser and panics with a *ParseError, which
// Recover turns back into an error, so each production reads as the grammar does.
type Lexer struct {
	Src  string
	S    *scanner.Scanner
	Tok  rune
	Text string
	Pos  int
	// joins and joinsNext are the runes text/scanner splits off a two-character operator.
	joins, joinsNext string
}

// NewLexer reads q's first token, scanning in mode with isIdentRune (nil: Go's rule) and
// joining a rune in joins with a following rune in joinsNext into one operator.
func NewLexer(q string, mode uint, isIdentRune func(ch rune, i int) bool, joins, joinsNext string) *Lexer {
	s := new(scanner.Scanner)
	s.Init(strings.NewReader(q))
	s.Mode, s.IsIdentRune = mode, isIdentRune
	l := &Lexer{Src: q, S: s, joins: joins, joinsNext: joinsNext}
	s.Error = func(s *scanner.Scanner, msg string) { l.FailAt(s.Pos().Offset, "%s", msg) }
	l.Next()
	return l
}

// Recover sets *err to the *ParseError a Lexer panicked with; Parse defers it.
func Recover(err *error) {
	if r := recover(); r != nil {
		perr, ok := r.(*ParseError)
		if !ok {
			panic(r)
		}
		*err = perr
	}
}

// Fail fails at the current token.
func (l *Lexer) Fail(format string, args ...any) { l.FailAt(l.Pos, format, args...) }

func (l *Lexer) FailAt(offset int, format string, args ...any) {
	panic(&ParseError{Offset: offset, Msg: fmt.Sprintf(format, args...)})
}

// Next reads one token, joining the two-character operators text/scanner splits.
func (l *Lexer) Next() {
	l.Tok = l.S.Scan()
	l.Pos, l.Text = l.S.Offset, l.S.TokenText()
	if l.Tok == scanner.EOF {
		l.Pos = len(l.Src)
	}
	if strings.ContainsRune(l.joins, l.Tok) && strings.ContainsRune(l.joinsNext, l.S.Peek()) {
		l.Text += string(l.S.Next())
	}
}

func (l *Lexer) Number() float64 {
	v, err := strconv.ParseFloat(l.Text, 64)
	if err != nil || (l.Tok != scanner.Int && l.Tok != scanner.Float) {
		l.FailAt(l.Pos, "expected a number, found %q", l.Text)
	}
	l.Next()
	return v
}
