package plan

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/value"
)

// Micro-benchmarks for the planner hot paths. They establish a baseline for the
// CI bench-smoke stage and are the input to a benchstat regression gate. The
// zero-allocation invariant that these baselines protect is asserted separately
// in TestMayMatchSegment_ZeroAlloc.
// Run: go test -bench=. -benchmem ./internal/query/plan

// benchFooter builds a footer with every summary structure populated so the
// MayMatchSegment benchmarks exercise the real lookup path, not the missing-
// structure early return.
func benchFooter() segment.Footer {
	ps := segment.NewPresentSet()
	ps.Add("tmpl_123")

	sk := sketch.NewDDSketch(0.01)
	for _, v := range []float64{10, 20, 30, 40, 50} {
		sk.Add(v)
	}

	return segment.Footer{
		Patterns: ps,
		ZoneMaps: segment.ZoneMaps{
			"level": {
				Type: value.TypeString,
				Min:  value.Value{String: "debug"},
				Max:  value.Value{String: "warning"},
			},
		},
		Distributions: map[string]*sketch.DDSketch{
			"duration_ms": sk,
		},
	}
}

// BenchmarkMayMatchSegment covers each compiled predicate kind — the per-footer
// hot path that must stay allocation-free (see MayMatchSegment doc comment).
func BenchmarkMayMatchSegment(b *testing.B) {
	footer := benchFooter()

	cases := []struct {
		name string
		pred CompiledPredicate
	}{
		{
			name: "PresentSet",
			pred: CompiledPredicate{Field: "pattern_id", Op: selector.OpEq, Kind: KindPresentSet, Value: "tmpl_123"},
		},
		{
			name: "ZoneMapString",
			pred: CompiledPredicate{Field: "level", Op: selector.OpEq, Kind: KindZoneMapString, Value: "error"},
		},
		{
			name: "NumericRange",
			pred: CompiledPredicate{Field: "duration_ms", Op: selector.OpLt, Kind: KindNumericRange, NumericOK: true, NumericValue: 35},
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = MayMatchSegment(footer, tc.pred)
			}
		})
	}
}

// BenchmarkCompile measures the one-shot predicate compilation over a mixed
// selector exercising every Kind selection branch.
func BenchmarkCompile(b *testing.B) {
	where := []selector.Predicate{
		{Field: "pattern_id", Op: selector.OpEq, Value: "tmpl_123"},
		{Field: "level", Op: selector.OpEq, Value: "error"},
		{Field: "trace_id", Op: selector.OpEq, Value: "trace_abc"},
		{Field: "duration_ms", Op: selector.OpLt, Value: "35"},
		{Field: "message", Op: selector.OpMatch, Value: "timeout"},
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Compile(where, boundedWindow(true)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRoute measures the volume-sensitive planner path as a function of the
// number of catalog-pruned descriptors — the per-descriptor footer probe loop
// plus WorkItem generation (one item per surviving row group). It uses in-memory
// catalog and footer fakes so the benchmark isolates CPU/allocation cost and does
// NOT measure footer-GET I/O (the real Prune bottleneck, accounted separately via
// Plan.Cost.GETs). Every footer matches every predicate, so all descriptors
// survive and the WorkItem slice grows to descriptors * rowGroupsPerSegment —
// the worst case for allocation.
func BenchmarkPrune(b *testing.B) {
	const rowGroupsPerSegment = 8

	// A selector whose predicates all match benchFooter, so no segment is pruned.
	sel := selector.Resolved{
		Window: boundedWindow(true),
		Where: []selector.Predicate{
			{Field: "pattern_id", Op: selector.OpEq, Value: "tmpl_123"},
			{Field: "level", Op: selector.OpEq, Value: "error"},
			{Field: "trace_id", Op: selector.OpEq, Value: "trace_abc"},
			{Field: "duration_ms", Op: selector.OpLt, Value: "100"},
		},
	}

	for _, n := range []int{100, 1_000, 10_000} {
		descs := make([]segment.Descriptor, n)
		footers := make(map[string]segment.Footer, n)
		for i := range descs {
			key := fmt.Sprintf("seg%d", i)
			descs[i] = descWithKey(key, key)
			f := benchFooter()
			f.RowGroups = rowGroupsPerSegment
			footers[key] = f
		}
		cat := catalog.Fake{PruneFn: func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) { return descs, nil }}
		b.Run(fmt.Sprintf("segments=%d", n), func(b *testing.B) {
			benchPrune(b, Deps{Catalog: cat, Reader: footersByKey(footers)}, sel, n*rowGroupsPerSegment)
		})
		if n > 100 {
			continue // a footer wait per segment makes a wider window seconds long
		}
		// Each footer read waits out a store GET's latency, which concurrent reads overlap.
		waiting := segment.Fake{FooterFn: func(loc segment.ObjectLocation) (segment.Footer, segment.Cost, error) {
			<-time.After(100 * time.Microsecond)
			return footers[loc.Key], segment.Cost{GETs: 1}, nil
		}}
		for _, reads := range []int{1, 16} {
			b.Run(fmt.Sprintf("segments=%d/footer_wait/reads=%d", n, reads), func(b *testing.B) {
				benchPrune(b, Deps{Catalog: cat, Reader: waiting, FooterReads: reads}, sel, n*rowGroupsPerSegment)
			})
		}
	}
}

func benchPrune(b *testing.B, deps Deps, sel selector.Resolved, wantItems int) {
	b.ReportAllocs()
	for b.Loop() {
		p, err := New(deps).Prune(context.Background(), catalog.Version("0"), sel)
		if err != nil {
			b.Fatal(err)
		}
		if len(p.Items) != wantItems {
			b.Fatalf("Items = %d, want %d", len(p.Items), wantItems)
		}
	}
}
