package tools

import (
	"cmp"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/summary"
)

// addCost combines measured work from successive reads or child tools.
func addCost(costs ...query.Cost) query.Cost {
	var total query.Cost
	for _, c := range costs {
		total.GETs += c.GETs
		total.BytesRead += c.BytesRead
		total.FootersDecoded += c.FootersDecoded
		total.Segments += c.Segments
	}
	return total
}

// validateSelector resolves relative times into an absolute window before planning,
// leaving predicates to the scan executor, and marks ctx's call when that read the clock.
func validateSelector(ctx context.Context, toolName string, sel selector.Selector) (selector.Resolved, error) {
	if sel.Time.Relative() {
		query.CallOf(ctx).ReadClock.Store(true)
	}
	resolved, err := sel.Resolve(time.Now())
	if err != nil {
		return selector.Resolved{}, query.Invalidf("%s: %v", toolName, err)
	}
	return resolved, nil
}

// snapshot is the catalog snapshot a request reads, the summary version its token names,
// which must agree for the summary to serve, and the derived-column definitions it reads.
type snapshot struct {
	catalog catalog.Version
	summary catalog.Version
	derived string
}

// String names no version as 0, as tokens did when versions were counts.
func (s snapshot) String() string {
	catalogPart, summaryPart := cmp.Or(string(s.catalog), "0"), cmp.Or(string(s.summary), "0")
	if s.derived != "" {
		return fmt.Sprintf("e%s.s%s.d%s", catalogPart, summaryPart, s.derived)
	}
	return fmt.Sprintf("e%s.s%s", catalogPart, summaryPart)
}

// Snapshot names the current snapshot, the one a call pinning none reads.
func (t toolSet) Snapshot(ctx context.Context) (string, error) {
	snap, err := t.resolveSnapshot(ctx, t.Summary, "", "")
	return snap.String(), err
}

// resolveSnapshot parses a pin unvalidated (audit finding F01), or takes the current
// catalog snapshot named with store's version so the response says which summary served it.
func (t toolSet) resolveSnapshot(ctx context.Context, store summary.Store, toolName, pin string) (snapshot, error) {
	if pin == "" {
		version, err := t.Catalog.Version(ctx)
		if err != nil {
			return snapshot{}, fmt.Errorf("%s: snapshot: %w", toolName, err) // infra
		}
		resolved := snapshot{catalog: version, derived: t.Planner.Derived().Version()}
		if store != nil {
			resolved.summary = store.CatalogVersion()
		}
		return resolved, nil
	}
	var pinned snapshot
	base, derived, _ := strings.Cut(pin, ".d")
	pinned.derived = derived
	catalogPart, summaryPart, ok := strings.Cut(strings.TrimPrefix(base, "e"), ".s")
	pinned.catalog, pinned.summary = catalog.Version(catalogPart), catalog.Version(summaryPart)
	if !ok || !strings.HasPrefix(base, "e") || !pinned.catalog.Valid() || !pinned.summary.Valid() || pinned.String() != pin {
		return snapshot{}, query.Invalidf("%s: invalid snapshot %q", toolName, pin)
	}
	// Only the definitions in force can be evaluated, so a pin naming others is refused
	// rather than answered differently.
	if derived != t.Planner.Derived().Version() {
		return snapshot{}, query.Invalidf("%s: snapshot %q reads derived-column definitions this querier no longer holds", toolName, pin)
	}
	return pinned, nil
}

// summaryServes reports whether the token lets the summary answer: only when the
// summary version it names is the catalog version it pins.
func (s snapshot) summaryServes() bool {
	return s.summary == s.catalog
}

// store is the summary when snap's token lets it serve and it holds snap.
func (t toolSet) store(snap snapshot) summary.Store {
	if !snap.summaryServes() {
		return nil
	}
	return t.Planner.Summary(snap.catalog)
}

// summaryWidth is the configured summary's bin width, the default without one, so a
// series lands on the grid the summary can serve.
func (t toolSet) summaryWidth() time.Duration {
	if t.Summary == nil {
		return summary.DefaultBinWidth
	}
	return t.Summary.BinWidth()
}

// footers reads every footer of descs once, pruning nothing.
func (t toolSet) footers(ctx context.Context, descs []segment.Descriptor) (plan.Plan, error) {
	return t.Planner.Plan(ctx, descs, selector.Resolved{})
}
