// Package plan implements the query planner: footer-level pruning and row-group work-item generation.
package plan

import (
	"cmp"
	"regexp"
	"slices"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// PredicateKind is the discriminant for compiled predicate checks, selected at
// compile time so the MayMatchSegment dispatch is a single switch with zero
// per-predicate closures or heap allocations on the hot path.
type PredicateKind uint8

const (
	KindPresentSet PredicateKind = iota
	KindZoneMapString
	KindNumericRange
	KindScanOnly
	KindAny
)

// CompiledPredicate is a selector.Predicate ready for footer checks; NumericValue is
// valid only when NumericOK, and Any holds a KindAny group's alternatives.
type CompiledPredicate struct {
	Field        string
	Op           selector.Op
	Value        string
	NumericValue float64
	NumericOK    bool
	Quantifier   selector.Quantifier
	Kind         PredicateKind
	Any          []CompiledPredicate
}

// Compile translates a slice of selector.Predicate values into compiled predicates
// ready for footer evaluation. w is the query's resolved time window; if any
// predicate is a text match (scanOnlyOps) and w is not Bounded,
// Compile returns a *query.APIError with Code == query.ErrCodeUnboundedScan.
func Compile(where []selector.Predicate, w selector.Window) ([]CompiledPredicate, error) {
	result := make([]CompiledPredicate, 0, len(where))
	bounded := w.Bounded()

	for _, p := range where {
		compile := compilePredicate
		if len(p.Any) > 0 {
			compile = compileGroup
		}
		cp, err := compile(p, bounded)
		if err != nil {
			return nil, err
		}
		result = append(result, cp)
	}

	return result, nil
}

// compileGroup compiles each of a group's alternatives, refusing a group inside one.
func compileGroup(p selector.Predicate, bounded bool) (CompiledPredicate, error) {
	if p.Field != "" || p.Op != "" || p.Value != "" || p.Quantifier != "" {
		return CompiledPredicate{}, query.Invalidf("a predicate with any alternatives takes no field, op, value or quantifier")
	}
	cp := CompiledPredicate{Kind: KindAny, Any: make([]CompiledPredicate, 0, len(p.Any))}
	for _, alt := range p.Any {
		if len(alt.Any) > 0 {
			return CompiledPredicate{}, query.Invalidf("groups nest one level only: an alternative cannot itself have alternatives")
		}
		compiled, err := compilePredicate(alt, bounded)
		if err != nil {
			return CompiledPredicate{}, err
		}
		cp.Any = append(cp.Any, compiled)
	}
	return cp, nil
}

// scanOnlyOps are the text matches no footer can prune by.
var scanOnlyOps = []selector.Op{selector.OpMatch, selector.OpNoMatch, selector.OpRegex, selector.OpNoRegex}

// checkScanOnly refuses a text match over an unbounded window, since it reads every row,
// and a regex that does not compile.
func checkScanOnly(p selector.Predicate, bounded bool) error {
	if !bounded {
		return query.NewAPIError(query.ErrCodeUnboundedScan,
			"match predicates require a bounded time range; unbounded full-text search is not supported")
	}
	if p.Op != selector.OpRegex && p.Op != selector.OpNoRegex {
		return nil
	}
	if _, err := regexp.Compile(p.Value); err != nil {
		return query.Invalidf("invalid regex for %s: %v", p.Field, err)
	}
	return nil
}

// compilePredicate chooses how one predicate is checked against a footer.
func compilePredicate(p selector.Predicate, bounded bool) (CompiledPredicate, error) {
	if slices.Contains(scanOnlyOps, p.Op) {
		if err := checkScanOnly(p, bounded); err != nil {
			return CompiledPredicate{}, err
		}
	}
	if p.Quantifier != "" && !slices.Contains(selector.AllQuantifiers, p.Quantifier) {
		return CompiledPredicate{}, query.Invalidf("unknown quantifier: %s", p.Quantifier)
	}
	cp := CompiledPredicate{
		Field:      p.Field,
		Op:         p.Op,
		Value:      p.Value,
		Quantifier: p.Quantifier,
	}

	// Determine the Kind based on operator and field type.
	switch p.Op {
	case selector.OpMatch, selector.OpNoMatch, selector.OpRegex, selector.OpNoRegex:
		// A match with a time bound is allowed, but is scan-only.
		cp.Kind = KindScanOnly

	case selector.OpExists:
		// Exists checks are via zone-map (is the field's range non-empty?).
		cp.Kind = KindZoneMapString

	case selector.OpEq, selector.OpNe:
		cp.Kind = equalityKind(p.Field)

	case selector.OpLt, selector.OpLe, selector.OpGt, selector.OpGe:
		// Numeric range checks.
		cp.Kind = KindNumericRange
		// The scan's matcher reads the same parse, so both treat the value as a number or neither does.
		parsed := p.Parse()
		cp.NumericValue, cp.NumericOK = parsed.Number, parsed.Numeric

	default:
		return CompiledPredicate{}, query.Invalidf("unknown operator: %s", p.Op)
	}

	return cp, nil
}

// equalityKind checks pattern_id against the present-set and every other field against
// its zone map; Plan asks a filtered column's membership filter after both.
func equalityKind(field string) PredicateKind {
	if field == segment.ColPatternID {
		return KindPresentSet
	}
	return KindZoneMapString
}

// MayMatchSegment evaluates one compiled predicate against a segment footer,
// returning true if the segment could contain matching rows. False is definitive
// (safe to skip); a true return may include false positives. This function is
// the hot path and must allocate zero on the heap.
func MayMatchSegment(f segment.Footer, p CompiledPredicate) bool {
	// A list column's bounds are over its elements, so they rule out only an element
	// some row must hold, which is an equality under any.
	if zm, ok := f.ZoneMaps[p.Field]; ok && zm.Type.Repeated() && (p.Op != selector.OpEq || p.Quantifier == selector.QuantifierAll) {
		return true
	}
	// Only equality can rule a segment out of the set-shaped checks: Ne may match other
	// values, and Exists holds wherever the zone map does.
	switch p.Kind {
	case KindAny:
		for _, alt := range p.Any {
			if MayMatchSegment(f, alt) {
				return true
			}
		}
		return false
	case KindPresentSet:
		return p.Op != selector.OpEq || f.Patterns.Contains(p.Value)
	case KindZoneMapString:
		// Eq "" also matches a row without the column, which no bound describes.
		zm, ok := f.ZoneMaps[p.Field]
		return !ok || p.Op != selector.OpEq || p.Value == "" || zm.MayContainString(p.Value)
	case KindNumericRange:
		return mayMatchOrdered(f, p)
	default:
		// KindScanOnly: the filter happens at scan time.
		return true
	}
}

// mayMatchOrdered bounds an ordered comparison. It has to admit every segment
// selector.ParsedPredicate.comparesOrdered would match a row in, and that matcher
// compares as text unless both sides parse as numbers — so which bound applies follows
// the column's sealed type, not the operator.
func mayMatchOrdered(f segment.Footer, p CompiledPredicate) bool {
	zm, haveZoneMap := f.ZoneMaps[p.Field]

	if !p.NumericOK {
		// The literal is not a number, so every row compares as text.
		minimum, maximum, ok := zm.StringBounds()
		return !haveZoneMap || !ok || mayMatchOrderedBounds(minimum, maximum, p.Value, p.Op)
	}

	// The zone map's bounds are exact, so prefer them. The quantile sketch
	// reconstructs its extremes to a relative accuracy and can report them inside
	// the true range, which prunes a segment holding the boundary value.
	if haveZoneMap {
		minimum, maximum, ok := zm.NumericBounds()
		// A text column: some rows may still compare numerically, and a lexicographic
		// bound says nothing about numeric extremes ({"9","10"} spans 9..10 but orders
		// "10" first), so nothing here can prune.
		return !ok || mayMatchOrderedBounds(minimum, maximum, p.NumericValue, p.Op)
	}

	if sk, ok := f.Distributions[p.Field]; ok {
		return mayMatchOrderedBounds(sk.Quantile(0), sk.Quantile(1), p.NumericValue, p.Op)
	}

	// No zone map or sketch for this field; conservative: may match.
	return true
}

// mayMatchOrderedBounds relaxes an ordered comparison to a pair of bounds: only the
// bound the operator looks at can rule a segment out. The direction table mirrors
// selector.ParsedPredicate.comparesOrdered, which is the matcher this must never contradict.
func mayMatchOrderedBounds[T cmp.Ordered](minimum, maximum, want T, op selector.Op) bool {
	switch op {
	case selector.OpLt:
		return minimum < want
	case selector.OpLe:
		return minimum <= want
	case selector.OpGt:
		return maximum > want
	case selector.OpGe:
		return maximum >= want
	default:
		return true
	}
}
