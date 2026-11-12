package lang

import "github.com/datafocal/datafocal/internal/selector"

// Fake is a Language whose methods call the matching Fn field; a nil field returns zero
// values.
type Fake struct {
	NameFn   func() string
	ParseFn  func(q string) (selector.Query, error)
	FormatFn func(q selector.Query) (string, error)
}

var _ Language = Fake{}

func (f Fake) Name() string {
	if f.NameFn == nil {
		return ""
	}
	return f.NameFn()
}

func (f Fake) Parse(q string) (selector.Query, error) {
	if f.ParseFn == nil {
		return selector.Query{}, nil
	}
	return f.ParseFn(q)
}

func (f Fake) Format(q selector.Query) (string, error) {
	if f.FormatFn == nil {
		return "", nil
	}
	return f.FormatFn(q)
}
