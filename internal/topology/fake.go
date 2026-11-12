package topology

import "github.com/datafocal/datafocal/internal/segment"

// Fake is a Graph whose methods call the matching Fn field; a nil field returns zero
// values.
type Fake struct {
	AddFn   func(desc segment.Descriptor, footer segment.Footer) error
	EdgesFn func(filter Filter) View
	EvictFn func(retentionStart int64)
}

var _ Graph = Fake{}

func (f Fake) Add(desc segment.Descriptor, footer segment.Footer) error {
	if f.AddFn == nil {
		return nil
	}
	return f.AddFn(desc, footer)
}

func (f Fake) Edges(filter Filter) View {
	if f.EdgesFn == nil {
		return View{}
	}
	return f.EdgesFn(filter)
}

func (f Fake) Evict(retentionStart int64) {
	if f.EvictFn != nil {
		f.EvictFn(retentionStart)
	}
}
