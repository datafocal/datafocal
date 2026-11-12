package pattern

import "github.com/datafocal/datafocal/internal/segment"

// Fake is a Pattern whose methods call the matching Fn field; a nil field returns
// zero values.
type Fake struct {
	DeriveFn func(columns []segment.Column) segment.ParsedPattern
	FooterFn func() segment.PatternProfile
}

var _ Pattern = Fake{}

func (f Fake) Derive(columns []segment.Column) segment.ParsedPattern {
	if f.DeriveFn == nil {
		return segment.ParsedPattern{}
	}
	return f.DeriveFn(columns)
}

func (f Fake) Footer() segment.PatternProfile {
	if f.FooterFn == nil {
		return segment.PatternProfile{}
	}
	return f.FooterFn()
}
