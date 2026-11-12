package series

import "context"

// Fake is a Reader whose Read calls ReadFn; a nil ReadFn returns zero values.
type Fake struct {
	ReadFn func(req Request) (Result, error)
}

var _ Reader = Fake{}

func (f Fake) Read(_ context.Context, req Request) (Result, error) {
	if f.ReadFn == nil {
		return Result{}, nil
	}
	return f.ReadFn(req)
}
