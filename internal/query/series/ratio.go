package series

import (
	"context"
	"slices"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/selector"
)

// ratio divides the total over Selector plus Numerator by the total over Selector per
// (key, bin), leaving a 0/0 bin absent rather than fabricating it.
func (r reader) ratio(ctx context.Context, req Request) (Result, error) {
	totals, err := r.ratioTotals(ctx, req)
	if err != nil {
		return Result{Cost: totals.cost}, err
	}
	values := Values{}
	for key, byBin := range totals.den {
		for b, d := range byBin {
			if d == 0 {
				continue
			}
			// A key the numerator matched nowhere reads as 0 — the real "no failures in a
			// bin that had traffic" zero.
			addTo(values, key, b, totals.num[key][b]/d)
		}
	}
	return Result{Values: values, Cost: totals.cost, Coverage: 1, Multiplied: totals.multiplied}, nil
}

// ratioTotals are the two totals a ratio divides and what reading them cost.
type ratioTotals struct {
	den, num   Values
	cost       query.Cost
	multiplied bool
}

// ratioTotals reads a count ratio's two totals from the summary when it can, since no
// sum over a column derives from counts, and otherwise both from one fused scan rather
// than two passes over the same rows.
func (r reader) ratioTotals(ctx context.Context, req Request) (ratioTotals, error) {
	count := req
	count.Measure, count.Numerator = Measure{Kind: KindCount}, nil
	if den, ok := r.fromSummary(count); ok && req.Measure.Field == "" {
		// Cloned so the numerator never leaks into the caller's selector.
		count.Selector.Where = append(slices.Clone(req.Selector.Where), req.Numerator...)
		num, err := r.Read(ctx, count)
		return ratioTotals{den: den.Values, num: num.Values, cost: num.Cost}, err
	}

	descs, err := r.segments(ctx, req)
	if err != nil {
		return ratioTotals{}, err
	}
	field := req.Measure.Field
	m := binTotals{binKey: newBinKey(req), field: field, numerator: selector.ParsePredicates(req.Numerator),
		all: Values{}, matched: Values{}}
	columns := []string{field}
	for _, p := range req.Numerator {
		columns = append(columns, p.Field)
	}
	cost, err := r.scanRows(ctx, descs, req, m, columns...)
	if err == nil && field != "" {
		err = r.uncarried(descs, field, "to take a ratio over")
	}
	return ratioTotals{den: m.all, num: m.matched, cost: cost, multiplied: *m.multiplied}, err
}
