package logformat

import "github.com/datafocal/datafocal/internal/segment"

// Fake is a Parser of Fn fields; a nil ParseFn or ParseAllFn returns the zero record, a nil
// FlushFn no record and a nil HeldFn 0.
type Fake struct {
	ParseFn    func(line string) (segment.Record, bool)
	ParseAllFn func(line string) []segment.Record
	FlushFn    func() (segment.Record, bool)
	HeldFn     func() int
}

var _ Parser = Fake{}

func (f Fake) Parse(line string) (segment.Record, bool) {
	if f.ParseFn == nil {
		return segment.Record{}, true
	}
	return f.ParseFn(line)
}

func (f Fake) ParseAll(line string) []segment.Record {
	if f.ParseAllFn == nil {
		return []segment.Record{{}}
	}
	return f.ParseAllFn(line)
}

func (f Fake) Flush() (segment.Record, bool) {
	if f.FlushFn == nil {
		return segment.Record{}, false
	}
	return f.FlushFn()
}

func (f Fake) Held() int {
	if f.HeldFn == nil {
		return 0
	}
	return f.HeldFn()
}
