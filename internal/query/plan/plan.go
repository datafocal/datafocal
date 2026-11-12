package plan

import (
	"context"
	"fmt"
	"iter"
	"slices"
	"sync"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/internal/value"
)

// Planner is the one read path every tool takes: it chooses between
// the summary and the segments, and prunes the segments catalog → footer → row group.
type Planner interface {
	// Segments is the catalog stage: the segments at version v that sel may match, read
	// without a GET.
	Segments(ctx context.Context, v catalog.Version, sel selector.Resolved) ([]segment.Descriptor, error)
	// Plan reads and bills each of descs' footers once and keeps the row groups sel may
	// match; a segment its descriptor rules out is dropped before its footer is read.
	Plan(ctx context.Context, descs []segment.Descriptor, sel selector.Resolved) (Plan, error)
	// Prune is Segments then Plan, for a caller narrowing nothing in between.
	Prune(ctx context.Context, v catalog.Version, sel selector.Resolved) (Plan, error)
	// Summary is the summary when it holds exactly version v, nil otherwise.
	Summary(v catalog.Version) summary.Store
	// Derived is the derived columns every tool reads as if stored.
	Derived() Derived
}

// Deps is what a Planner reads through.
type Deps struct {
	Catalog catalog.Catalog
	Reader  segment.Reader
	// Summary is the summary the planner may answer from; nil sends every request to the segments.
	Summary summary.Store
	Derived Derived
	// FooterReads is the footers Plan reads at once; 0 or 1 reads them in series.
	FooterReads int
}

type planner struct {
	deps Deps
}

// New returns a Planner reading through deps.
func New(deps Deps) Planner {
	return planner{deps: deps}
}

// WorkItem is one surviving unit of scan work: which segment, its footer as pruning
// read and billed it, and which row group inside it.
type WorkItem struct {
	Segment  segment.Descriptor
	Footer   segment.Footer
	RowGroup int
}

// Plan is Planner.Plan's return value, with no coverage fraction because what
// query.Meta.Coverage measures is known only to the tool consuming the plan.
type Plan struct {
	Items []WorkItem
	Cost  query.Cost
}

// Footers yields each planned segment and its footer once, in plan order.
func (p Plan) Footers() iter.Seq2[segment.Descriptor, segment.Footer] {
	return func(yield func(segment.Descriptor, segment.Footer) bool) {
		for i, wi := range p.Items {
			if i > 0 && p.Items[i-1].Segment.ID == wi.Segment.ID {
				continue
			}
			if !yield(wi.Segment, wi.Footer) {
				return
			}
		}
	}
}

func (p planner) Segments(ctx context.Context, v catalog.Version, sel selector.Resolved) ([]segment.Descriptor, error) {
	sel.Where = p.deps.Derived.prunable(sel.Where)
	descs, err := p.deps.Catalog.Prune(ctx, v, sel)
	if err != nil {
		return nil, fmt.Errorf("plan: prune: %w", err)
	}
	return descs, nil
}

func (p planner) Plan(ctx context.Context, descs []segment.Descriptor, sel selector.Resolved) (Plan, error) {
	preds, err := Compile(sel.Where, sel.Window)
	if err != nil {
		return Plan{}, err // typed *query.APIError (incl. unbounded_scan) propagated as-is
	}
	if prunable := p.deps.Derived.prunable(sel.Where); len(prunable) < len(sel.Where) {
		sel.Where = prunable
		preds, _ = Compile(prunable, sel.Window) // a subset of what compiled
	}
	where := selector.ParsePredicates(sel.Where)

	slots, err := p.probeAll(ctx, descs, where, preds)
	if err != nil {
		return Plan{}, err
	}
	var (
		items []WorkItem
		cost  query.Cost
	)
	for i, s := range slots {
		cost.GETs += s.cost.GETs
		cost.BytesRead += s.cost.BytesRead
		cost.FootersDecoded += s.cost.FootersDecoded
		if !s.may {
			continue
		}
		cost.Segments++
		for rg := range s.footer.RowGroups {
			if (s.groups != nil && !s.groups[rg]) ||
				(rg < len(s.footer.RowGroupZoneMaps) && !mayMatchRowGroup(s.footer.RowGroupZoneMaps[rg], preds)) {
				continue
			}
			items = append(items, WorkItem{Segment: descs[i], Footer: s.footer, RowGroup: rg})
		}
	}

	return Plan{Items: items, Cost: cost}, nil
}

func (p planner) Prune(ctx context.Context, v catalog.Version, sel selector.Resolved) (Plan, error) {
	descs, err := p.Segments(ctx, v, sel)
	if err != nil {
		return Plan{}, err
	}
	return p.Plan(ctx, descs, sel)
}

func (p planner) Derived() Derived { return p.deps.Derived }

func (p planner) Summary(v catalog.Version) summary.Store {
	if p.deps.Summary == nil || p.deps.Summary.CatalogVersion() != v {
		return nil
	}
	return p.deps.Summary
}

// SegmentValue returns the one value every row of d has for column, named by the stream
// or proven by the zone map, whose agreeing extremes alone allow a row without the
// column (ZoneMap.SoleValue).
func SegmentValue(d segment.Descriptor, column string) (string, bool) {
	if column == "" {
		return "", false
	}
	if zm, ok := d.ZoneMaps[column]; ok {
		sole, uniform := zm.SoleValue(d.Rows)
		if !uniform {
			return "", false
		}
		return value.Format(zm.Type, sole), true
	}
	claimed, named := d.Stream.Labels[column]
	return claimed, named
}

// mayMatchDescriptor drops a segment whose descriptor settles a predicate false, since
// the answer is the same for every row.
func mayMatchDescriptor(d segment.Descriptor, where []selector.ParsedPredicate) bool {
	for _, p := range where {
		if !mayMatchDescriptorBy(d, p) {
			return false
		}
	}
	return true
}

// mayMatchDescriptorBy is mayMatchDescriptor for one predicate, or any of a group's alternatives.
func mayMatchDescriptorBy(d segment.Descriptor, p selector.ParsedPredicate) bool {
	if len(p.Any) > 0 {
		return slices.ContainsFunc(p.Any, func(alt selector.ParsedPredicate) bool { return mayMatchDescriptorBy(d, alt) })
	}
	v, ok := SegmentValue(d, p.Field)
	return !ok || p.Matches(v, true)
}

// mayMatchRowGroup applies every predicate to one row group's zone maps, its only
// index, so a present-set or membership check falls back to the column's text bounds.
func mayMatchRowGroup(zoneMaps segment.ZoneMaps, preds []CompiledPredicate) bool {
	group := segment.Footer{ZoneMaps: zoneMaps}
	for _, p := range preds {
		if !mayMatchRowGroupBy(group, p) {
			return false
		}
	}
	return true
}

// mayMatchRowGroupBy applies one predicate, or any of a group's alternatives, to a row group.
func mayMatchRowGroupBy(group segment.Footer, p CompiledPredicate) bool {
	switch p.Kind {
	case KindAny:
		return slices.ContainsFunc(p.Any, func(alt CompiledPredicate) bool { return mayMatchRowGroupBy(group, alt) })
	case KindPresentSet:
		p.Kind = KindZoneMapString
	}
	return MayMatchSegment(group, p)
}

// probed is one segment's probe: its footer, whether it may match, and what reading it cost.
type probed struct {
	footer segment.Footer
	groups []bool
	may    bool
	cost   query.Cost
}

// probeAll probes each of descs its descriptor does not rule out into the slot of the
// same index, up to FooterReads at once, and stops starting probes at the first error or
// when ctx ends; it returns once every probe it started has.
func (p planner) probeAll(ctx context.Context, descs []segment.Descriptor, where []selector.ParsedPredicate, preds []CompiledPredicate) ([]probed, error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	slots := make([]probed, len(descs))
	inFlight := make(chan struct{}, max(p.deps.FooterReads, 1))
	var wg sync.WaitGroup
	for i, desc := range descs {
		if !mayMatchDescriptor(desc, where) {
			continue
		}
		select {
		case inFlight <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
		wg.Go(func() {
			defer func() { <-inFlight }()
			var err error
			slots[i].footer, slots[i].may, err = p.probe(ctx, desc, preds, &slots[i].cost)
			if err == nil && slots[i].may {
				slots[i].groups, slots[i].may, err = p.gramsAdmit(ctx, desc, slots[i].footer, preds, &slots[i].cost)
			}
			if err != nil {
				cancel(err)
			}
		})
	}
	wg.Wait()
	return slots, context.Cause(ctx)
}

// probe reads desc's footer, billing it to cost, and reports whether its checks and
// filters let the segment match preds.
func (p planner) probe(ctx context.Context, desc segment.Descriptor, preds []CompiledPredicate, cost *query.Cost) (segment.Footer, bool, error) {
	if may, err := p.rollupAdmits(ctx, desc, preds, cost); err != nil || !may {
		return segment.Footer{}, false, err
	}
	footer, read, err := p.deps.Reader.Footer(ctx, desc.Object)
	if err != nil {
		return segment.Footer{}, false, fmt.Errorf("plan: probe %s: %w", desc.ID, err)
	}
	cost.GETs += read.GETs
	if read.GETs > 0 { // a footer the cache held cost no GET
		cost.FootersDecoded++
	}
	if !mayMatchAll(footer, preds) {
		return footer, false, nil
	}
	may, err := p.filtersAdmit(ctx, desc, footer, preds, cost)
	return footer, may, err
}

// rollupAdmits asks a merged hour's filters whether the hour may hold each value an equality
// predicate looks up in a column they filter, an id by its name or its values, billing the read to
// cost, so an hour that cannot is dropped before its footer is read.
func (p planner) rollupAdmits(ctx context.Context, desc segment.Descriptor, preds []CompiledPredicate, cost *query.Cost) (bool, error) {
	needles := slices.DeleteFunc(slices.Clone(preds), func(pred CompiledPredicate) bool {
		return pred.Op != selector.OpEq || pred.Value == ""
	})
	if desc.RollupFilters.Key == "" || len(needles) == 0 {
		return true, nil
	}
	filters, read, err := p.deps.Reader.Filters(ctx, desc.RollupFilters)
	cost.GETs += read.GETs
	cost.BytesRead += read.Bytes
	if err != nil {
		return false, fmt.Errorf("plan: rollup filters %s: %w", desc.ID, err)
	}
	return !slices.ContainsFunc(needles, func(pred CompiledPredicate) bool {
		filter, ok := filters[pred.Field]
		return ok && !filter.MayContain(pred.Value)
	}), nil
}

// filtersAdmit asks the membership filter of each equality predicate's column that has
// one whether the segment may hold the value, billing the reads to cost; a group is
// admitted when any alternative is.
func (p planner) filtersAdmit(ctx context.Context, desc segment.Descriptor, footer segment.Footer, preds []CompiledPredicate, cost *query.Cost) (bool, error) {
	for _, pred := range preds {
		if pred.Kind == KindAny {
			may, err := p.anyFilterAdmits(ctx, desc, footer, pred.Any, cost)
			if err != nil || !may {
				return false, err
			}
			continue
		}
		if pred.Op != selector.OpEq || pred.Value == "" || !footer.HasFilter(pred.Field) {
			continue
		}
		_, may, read, err := p.deps.Reader.MayContain(ctx, desc.Object, footer, pred.Field, pred.Value)
		cost.GETs += read.GETs
		cost.BytesRead += read.Bytes
		if err != nil {
			return false, fmt.Errorf("plan: filter %s: %w", desc.ID, err)
		}
		if !may {
			return false, nil
		}
	}
	return true, nil
}

// anyFilterAdmits reports whether the filters admit at least one of alts, reading no
// further filter once one does.
func (p planner) anyFilterAdmits(ctx context.Context, desc segment.Descriptor, footer segment.Footer, alts []CompiledPredicate, cost *query.Cost) (bool, error) {
	for _, alt := range alts {
		may, err := p.filtersAdmit(ctx, desc, footer, []CompiledPredicate{alt}, cost)
		if err != nil || may {
			return may, err
		}
	}
	return false, nil
}

// mayMatchAll applies every compiled predicate to footer as a logical AND.
func mayMatchAll(footer segment.Footer, preds []CompiledPredicate) bool {
	for _, p := range preds {
		if !MayMatchSegment(footer, p) {
			return false
		}
	}
	return true
}

func (p planner) gramsAdmit(ctx context.Context, desc segment.Descriptor, footer segment.Footer, preds []CompiledPredicate, cost *query.Cost) ([]bool, bool, error) {
	needles := slices.DeleteFunc(slices.Clone(preds), func(pred CompiledPredicate) bool {
		return pred.Kind == KindAny || pred.Field != segment.ColMessage || pred.Op != selector.OpMatch || len(pred.Value) < 3
	})
	if len(needles) == 0 {
		return nil, true, nil
	}
	grams, read, err := p.deps.Reader.Grams(ctx, desc.Object, footer)
	cost.GETs += read.GETs
	cost.BytesRead += read.Bytes
	if err != nil {
		return nil, false, fmt.Errorf("plan: grams %s: %w", desc.ID, err)
	}
	groups, may := make([]bool, footer.RowGroups), false
	for rg := range groups {
		groups[rg] = !slices.ContainsFunc(needles, func(pred CompiledPredicate) bool { return !grams.Admits(rg, pred.Value) })
		may = may || groups[rg]
	}
	return groups, may, nil
}
