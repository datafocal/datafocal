package scan

import (
	"context"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
)

// Fake is a Scanner whose Scan returns a cursor over what ScanFn returns; a nil ScanFn
// returns no rows.
type Fake struct {
	ScanFn func(p plan.Plan, q Query) Canned
}

// Canned is what a Fake scan yields: its rows, what reading them cost, and the failure
// that stops the cursor after them.
type Canned struct {
	Rows []Row
	Cost query.Cost
	Err  error
}

var _ Scanner = Fake{}

func (f Fake) Scan(_ context.Context, p plan.Plan, q Query) *Rows {
	if f.ScanFn == nil {
		return &Rows{}
	}
	c := f.ScanFn(p, q)
	return &Rows{batch: c.Rows, cost: c.Cost, err: c.Err}
}
