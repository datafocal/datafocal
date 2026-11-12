package plan

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/value"
)

// boundedWindow maps a table's hasTime column onto the window Compile takes:
// closed on both ends when true, fully open when false. The instants themselves
// never matter — only whether Bounded() holds.
func boundedWindow(hasTime bool) selector.Window {
	if !hasTime {
		return selector.Window{}
	}
	return selector.Window{From: time.Unix(0, 0).UTC(), To: time.Unix(3600, 0).UTC()}
}

// TestCompile_AllOps covers every selector.Op with representative fields and time bounds.
func TestCompile_AllOps(t *testing.T) {
	cases := []struct {
		name       string
		predicates []selector.Predicate
		hasTime    bool
		wantErr    bool
		wantKind   []PredicateKind
		checkFunc  func(t *testing.T, p CompiledPredicate)
	}{
		{
			name:       "OpEq on pattern_id",
			predicates: []selector.Predicate{{Field: "pattern_id", Op: selector.OpEq, Value: "tmpl_123"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindPresentSet},
		},
		{
			name:       "OpNe on pattern_id",
			predicates: []selector.Predicate{{Field: "pattern_id", Op: selector.OpNe, Value: "tmpl_123"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindPresentSet},
		},
		{
			name:       "OpExists on any field",
			predicates: []selector.Predicate{{Field: "some_field", Op: selector.OpExists}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindZoneMapString},
		},
		{
			name:       "OpEq on trace_id (high-cardinality identifier)",
			predicates: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: "trace_abc123"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindZoneMapString},
		},
		{
			name:       "OpEq on user_id (high-cardinality identifier)",
			predicates: []selector.Predicate{{Field: "user_id", Op: selector.OpEq, Value: "user_789"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindZoneMapString},
		},
		{
			name:       "OpNe on user_id",
			predicates: []selector.Predicate{{Field: "user_id", Op: selector.OpNe, Value: "user_789"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindZoneMapString},
		},
		{
			name:       "OpEq on request_id (suffix _id routed to membership)",
			predicates: []selector.Predicate{{Field: "request_id", Op: selector.OpEq, Value: "req_abc"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindZoneMapString},
		},
		{
			name:       "OpEq on low-cardinality string field",
			predicates: []selector.Predicate{{Field: "level", Op: selector.OpEq, Value: "error"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindZoneMapString},
		},
		{
			name:       "OpLt on numeric field with valid float",
			predicates: []selector.Predicate{{Field: "duration_ms", Op: selector.OpLt, Value: "100.5"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindNumericRange},
			checkFunc: func(t *testing.T, p CompiledPredicate) {
				if !p.NumericOK || p.NumericValue != 100.5 {
					t.Errorf("numeric parse: got NumericOK=%v, NumericValue=%v, want true, 100.5", p.NumericOK, p.NumericValue)
				}
			},
		},
		{
			name:       "OpLe on numeric field with integer",
			predicates: []selector.Predicate{{Field: "duration_ms", Op: selector.OpLe, Value: "42"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindNumericRange},
			checkFunc: func(t *testing.T, p CompiledPredicate) {
				if !p.NumericOK || p.NumericValue != 42.0 {
					t.Errorf("numeric parse: got NumericOK=%v, NumericValue=%v, want true, 42.0", p.NumericOK, p.NumericValue)
				}
			},
		},
		{
			name:       "OpGt on numeric field",
			predicates: []selector.Predicate{{Field: "status_code", Op: selector.OpGt, Value: "200"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindNumericRange},
		},
		{
			name:       "OpGe on numeric field",
			predicates: []selector.Predicate{{Field: "request_size", Op: selector.OpGe, Value: "1024"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindNumericRange},
		},
		{
			name:       "OpLt with invalid float value",
			predicates: []selector.Predicate{{Field: "duration_ms", Op: selector.OpLt, Value: "not-a-number"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindNumericRange},
			checkFunc: func(t *testing.T, p CompiledPredicate) {
				if p.NumericOK {
					t.Errorf("numeric parse of invalid value: got NumericOK=true, want false")
				}
			},
		},
		{
			name:       "OpLt with non-finite float value",
			predicates: []selector.Predicate{{Field: "duration_ms", Op: selector.OpLt, Value: "NaN"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindNumericRange},
			checkFunc: func(t *testing.T, p CompiledPredicate) {
				if p.NumericOK {
					t.Errorf("numeric parse of non-finite value: got NumericOK=true, want false")
				}
			},
		},
		{
			name:       "OpMatch with time bound",
			predicates: []selector.Predicate{{Field: "message", Op: selector.OpMatch, Value: "error"}},
			hasTime:    true,
			wantErr:    false,
			wantKind:   []PredicateKind{KindScanOnly},
		},
		{
			name:       "OpMatch without time bound returns unbounded_scan error",
			predicates: []selector.Predicate{{Field: "message", Op: selector.OpMatch, Value: "error"}},
			hasTime:    false,
			wantErr:    true,
		},
		{
			name: "Multiple predicates mixed ops",
			predicates: []selector.Predicate{
				{Field: "pattern_id", Op: selector.OpEq, Value: "tmpl_123"},
				{Field: "level", Op: selector.OpEq, Value: "error"},
				{Field: "duration_ms", Op: selector.OpLt, Value: "100"},
			},
			hasTime: true,
			wantErr: false,
			wantKind: []PredicateKind{
				KindPresentSet,
				KindZoneMapString,
				KindNumericRange,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Compile(tc.predicates, boundedWindow(tc.hasTime))
			if (err != nil) != tc.wantErr {
				t.Fatalf("Compile() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr {
				// Verify it's the correct error type and code
				apiErr, ok := err.(*query.APIError)
				if !ok {
					t.Fatalf("expected *query.APIError, got %T", err)
				}
				if apiErr.Code != query.ErrCodeUnboundedScan {
					t.Errorf("error code: got %s, want %s", apiErr.Code, query.ErrCodeUnboundedScan)
				}
				if apiErr.Retryable {
					t.Errorf("unbounded_scan error should not be retryable")
				}
				return
			}

			if len(got) != len(tc.wantKind) {
				t.Fatalf("Compile() returned %d predicates, want %d", len(got), len(tc.wantKind))
			}

			for i, cp := range got {
				if cp.Kind != tc.wantKind[i] {
					t.Errorf("predicate[%d].Kind = %v, want %v", i, cp.Kind, tc.wantKind[i])
				}
				if cp.Field != tc.predicates[i].Field {
					t.Errorf("predicate[%d].Field = %q, want %q", i, cp.Field, tc.predicates[i].Field)
				}
				if cp.Op != tc.predicates[i].Op {
					t.Errorf("predicate[%d].Op = %v, want %v", i, cp.Op, tc.predicates[i].Op)
				}

				if tc.checkFunc != nil {
					tc.checkFunc(t, cp)
				}
			}
		})
	}
}

// TestMayMatchSegment_PresentSet checks present-set matching.
func TestMayMatchSegment_PresentSet(t *testing.T) {
	ps := segment.NewPresentSet()
	ps.Add("tmpl_123")
	ps.Add("tmpl_456")

	footer := segment.Footer{
		Patterns: ps,
	}

	cases := []struct {
		name   string
		pred   CompiledPredicate
		wantOK bool
	}{
		{
			name: "eq on present value",
			pred: CompiledPredicate{
				Field: "pattern_id",
				Op:    selector.OpEq,
				Kind:  KindPresentSet,
				Value: "tmpl_123",
			},
			wantOK: true,
		},
		{
			name: "eq on absent value",
			pred: CompiledPredicate{
				Field: "pattern_id",
				Op:    selector.OpEq,
				Kind:  KindPresentSet,
				Value: "tmpl_999",
			},
			wantOK: false,
		},
		{
			name: "ne on present value can match (segment may have other values)",
			pred: CompiledPredicate{
				Field: "pattern_id",
				Op:    selector.OpNe,
				Kind:  KindPresentSet,
				Value: "tmpl_123",
			},
			wantOK: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MayMatchSegment(footer, tc.pred)
			if got != tc.wantOK {
				t.Errorf("MayMatchSegment() = %v, want %v", got, tc.wantOK)
			}
		})
	}
}

// TestMayMatchSegment_ZoneMapString checks zone-map string range matching.
func TestMayMatchSegment_ZoneMapString(t *testing.T) {
	footer := segment.Footer{
		ZoneMaps: segment.ZoneMaps{
			"level": {
				Type: value.TypeString,
				Min:  value.Value{String: "debug"},
				Max:  value.Value{String: "warning"},
			},
			"service": {
				Type: value.TypeString,
				Min:  value.Value{String: "api"},
				Max:  value.Value{String: "worker"},
			},
		},
	}

	cases := []struct {
		name   string
		pred   CompiledPredicate
		wantOK bool
	}{
		{
			name: "eq within range",
			pred: CompiledPredicate{
				Field: "level",
				Op:    selector.OpEq,
				Kind:  KindZoneMapString,
				Value: "error",
			},
			wantOK: true,
		},
		{
			name: "eq outside range (high)",
			pred: CompiledPredicate{
				Field: "level",
				Op:    selector.OpEq,
				Kind:  KindZoneMapString,
				Value: "zzz",
			},
			wantOK: false,
		},
		{
			name: "eq outside range (low)",
			pred: CompiledPredicate{
				Field: "level",
				Op:    selector.OpEq,
				Kind:  KindZoneMapString,
				Value: "aaa",
			},
			wantOK: false,
		},
		{
			name: `eq "" below range, which a row without the column matches`,
			pred: CompiledPredicate{
				Field: "level",
				Op:    selector.OpEq,
				Kind:  KindZoneMapString,
			},
			wantOK: true,
		},
		{
			name: "ne can match (segment may have different values)",
			pred: CompiledPredicate{
				Field: "level",
				Op:    selector.OpNe,
				Kind:  KindZoneMapString,
				Value: "error",
			},
			wantOK: true,
		},
		{
			name: "exists on field with zone map",
			pred: CompiledPredicate{
				Field: "level",
				Op:    selector.OpExists,
				Kind:  KindZoneMapString,
			},
			wantOK: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MayMatchSegment(footer, tc.pred)
			if got != tc.wantOK {
				t.Errorf("MayMatchSegment() = %v, want %v", got, tc.wantOK)
			}
		})
	}
}

func TestMayMatchSegment_NumericZoneMapComparesTypedBounds(t *testing.T) {
	footer := segment.Footer{
		ZoneMaps: segment.ZoneMaps{
			"count": {
				Type: value.TypeInt,
				Min:  value.Value{Int: 2},
				Max:  value.Value{Int: 10},
			},
		},
	}

	cases := []struct {
		name   string
		pred   CompiledPredicate
		wantOK bool
	}{
		{
			name: "greater than upper bound prunes",
			pred: CompiledPredicate{
				Field:        "count",
				Op:           selector.OpGt,
				Kind:         KindNumericRange,
				NumericOK:    true,
				NumericValue: 10,
			},
			wantOK: false,
		},
		{
			name: "less than lower bound prunes",
			pred: CompiledPredicate{
				Field:        "count",
				Op:           selector.OpLt,
				Kind:         KindNumericRange,
				NumericOK:    true,
				NumericValue: 2,
			},
			wantOK: false,
		},
		{
			name: "inside range admits",
			pred: CompiledPredicate{
				Field:        "count",
				Op:           selector.OpGe,
				Kind:         KindNumericRange,
				NumericOK:    true,
				NumericValue: 10,
			},
			wantOK: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MayMatchSegment(footer, tc.pred)
			if got != tc.wantOK {
				t.Errorf("MayMatchSegment() = %v, want %v", got, tc.wantOK)
			}
		})
	}
}

func TestMayMatchSegment_NumericZoneMapTypeMismatchAdmits(t *testing.T) {
	footer := segment.Footer{
		ZoneMaps: segment.ZoneMaps{
			"duration_ms": {
				Type: value.TypeString,
				Min:  value.Value{String: "2"},
				Max:  value.Value{String: "10"},
			},
		},
	}

	pred := CompiledPredicate{
		Field:        "duration_ms",
		Op:           selector.OpGt,
		Kind:         KindNumericRange,
		NumericOK:    true,
		NumericValue: 10,
	}

	got := MayMatchSegment(footer, pred)
	if !got {
		t.Errorf("MayMatchSegment() = false, want true for mismatched numeric zone-map type")
	}
}

// TestMayMatchSegment_NumericRange checks numeric range matching via DDSketch.
func TestMayMatchSegment_NumericRange(t *testing.T) {
	// Build a DDSketch with some numeric values
	sk := sketch.NewDDSketch(0.01)
	for _, v := range []float64{10, 20, 30, 40, 50} {
		sk.Add(v)
	}

	footer := segment.Footer{
		Distributions: map[string]*sketch.DDSketch{
			"duration_ms": sk,
		},
	}

	cases := []struct {
		name   string
		pred   CompiledPredicate
		wantOK bool
	}{
		{
			name: "lt within range",
			pred: CompiledPredicate{
				Field:        "duration_ms",
				Op:           selector.OpLt,
				Kind:         KindNumericRange,
				Value:        "35",
				NumericOK:    true,
				NumericValue: 35,
			},
			wantOK: true,
		},
		{
			name: "lt below range",
			pred: CompiledPredicate{
				Field:        "duration_ms",
				Op:           selector.OpLt,
				Kind:         KindNumericRange,
				Value:        "5",
				NumericOK:    true,
				NumericValue: 5,
			},
			wantOK: false,
		},
		{
			name: "gt within range",
			pred: CompiledPredicate{
				Field:        "duration_ms",
				Op:           selector.OpGt,
				Kind:         KindNumericRange,
				Value:        "35",
				NumericOK:    true,
				NumericValue: 35,
			},
			wantOK: true,
		},
		{
			name: "gt above range",
			pred: CompiledPredicate{
				Field:        "duration_ms",
				Op:           selector.OpGt,
				Kind:         KindNumericRange,
				Value:        "100",
				NumericOK:    true,
				NumericValue: 100,
			},
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MayMatchSegment(footer, tc.pred)
			if got != tc.wantOK {
				t.Errorf("MayMatchSegment() = %v, want %v", got, tc.wantOK)
			}
		})
	}
}

// TestMayMatchSegment_MissingStructures checks zero-false-negatives on missing footer data.
func TestMayMatchSegment_MissingStructures(t *testing.T) {
	emptyFooter := segment.Footer{}

	cases := []struct {
		name string
		pred CompiledPredicate
	}{
		{
			name: "zone-map when no zone map for field",
			pred: CompiledPredicate{
				Field: "level",
				Op:    selector.OpEq,
				Kind:  KindZoneMapString,
				Value: "error",
			},
		},
		{
			name: "numeric when no sketch for field",
			pred: CompiledPredicate{
				Field:        "duration_ms",
				Op:           selector.OpLt,
				Kind:         KindNumericRange,
				NumericOK:    true,
				NumericValue: 100,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// All checks on missing structures must return true (zero false negatives)
			got := MayMatchSegment(emptyFooter, tc.pred)
			if !got {
				t.Errorf("MayMatchSegment() on missing structure = %v, want true (zero-false-negatives invariant)", got)
			}
		})
	}
}

// TestMayMatchSegment_ZeroAlloc gates the hot path at zero heap allocations.
func TestMayMatchSegment_ZeroAlloc(t *testing.T) {
	// Build a footer with all structure types present
	ps := segment.NewPresentSet()
	ps.Add("tmpl_123")

	sk := sketch.NewDDSketch(0.01)
	for _, v := range []float64{10, 20, 30, 40, 50} {
		sk.Add(v)
	}

	footer := segment.Footer{
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

	cases := []struct {
		name string
		pred CompiledPredicate
	}{
		{
			name: "present-set kind",
			pred: CompiledPredicate{
				Field: "pattern_id",
				Op:    selector.OpEq,
				Kind:  KindPresentSet,
				Value: "tmpl_123",
			},
		},
		{
			name: "zone-map string kind",
			pred: CompiledPredicate{
				Field: "level",
				Op:    selector.OpEq,
				Kind:  KindZoneMapString,
				Value: "error",
			},
		},
		{
			name: "numeric range kind",
			pred: CompiledPredicate{
				Field:        "duration_ms",
				Op:           selector.OpLt,
				Kind:         KindNumericRange,
				NumericOK:    true,
				NumericValue: 35,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Use testing.AllocsPerRun to measure heap allocations
			// Run 200 iterations to warm up and measure steady state
			allocs := testing.AllocsPerRun(200, func() {
				_ = MayMatchSegment(footer, tc.pred)
			})
			if allocs != 0 {
				t.Errorf("MayMatchSegment(%s): %.0f allocs/op, want 0", tc.name, allocs)
			}
		})
	}
}

// TestPruningNeverRejectsWhatTheScanMatches is the oracle for the pruner: the
// scan's own matcher decides truth, and a segment the pruner rejects must hold no
// row that matcher accepts. It is the property the two stages drifted on before
// (see matchOp in internal/catalog/store.go).
func TestPruningNeverRejectsWhatTheScanMatches(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	valueSets := []struct {
		name    string
		columns []segment.Column
	}{
		{"ints", []segment.Column{intCol(42), intCol(7)}},
		{"int and string", []segment.Column{intCol(42), stringCol("abc")}},
		{"int and float", []segment.Column{intCol(42), floatCol(42.5)}},
		{"numeric strings", []segment.Column{stringCol("9"), stringCol("10")}},
		{"floats", []segment.Column{floatCol(1.5), floatCol(-2.25)}},
		{"bools", []segment.Column{boolCol(true), boolCol(false)}},
		{"words", []segment.Column{stringCol("error"), stringCol("info")}},
		{"string and float", []segment.Column{stringCol("abc"), floatCol(0.5)}},
		{"string lists", []segment.Column{stringList("error", "info"), stringList("warn")}},
		{"int lists", []segment.Column{intList(42, 7), intList(9)}},
		{"empty and one-element lists", []segment.Column{stringList(), stringList("abc")}},
		{"empty lists", []segment.Column{stringList(), stringList()}},
	}
	literals := []string{"100", "42", "9", "10", "abc", "0", "-1", "42.5", "4.25e1", "true", "", "NaN", "Inf", "-Inf", "error", "warn"}
	quantifiers := []selector.Quantifier{"", selector.QuantifierAny, selector.QuantifierAll}

	for _, set := range valueSets {
		t.Run(set.name, func(t *testing.T) {
			footer, cells := sealOneColumn(t, ts, set.columns)
			list := set.columns[0].Type.Repeated()
			for _, op := range selector.AllOps {
				for _, literal := range literals {
					for _, q := range quantifiers {
						p := selector.Predicate{Field: "col", Op: op, Value: literal, Quantifier: q}
						// The scan's matcher over the column's on-disk cells is the oracle.
						scanMatches := false
						for _, cell := range cells {
							if cellMatches(t, p.Parse(), cell, list) {
								scanMatches = true
							}
						}
						compiled, err := Compile([]selector.Predicate{p}, boundedWindow(true))
						require.NoError(t, err)
						require.Len(t, compiled, 1)
						may := MayMatchSegment(footer, compiled[0])
						if scanMatches && !may {
							t.Errorf("pruned %v %q %q: cells %q hold a row the scan matches", op, literal, q, cells)
						}
						if list && q == selector.QuantifierAll && (!may || !mayMatchRowGroup(footer.RowGroupZoneMaps[0], compiled)) {
							t.Errorf("pruned %v %q under all: all never prunes a list column", op, literal)
						}
						assertGroupPrunesByItsAlternatives(t, footer, cells, list, p)
					}
				}
			}
		})
	}
}

// assertGroupPrunesByItsAlternatives groups p with alternatives that prune differently,
// and requires the group to admit a segment or row group exactly when one alternative
// does and never to reject a cell the scan matches.
func assertGroupPrunesByItsAlternatives(t *testing.T, footer segment.Footer, cells []string, list bool, p selector.Predicate) {
	t.Helper()
	others := []selector.Predicate{
		{Field: "col", Op: selector.OpEq, Value: "never"},
		{Field: "col", Op: selector.OpExists},
		{Field: "absent", Op: selector.OpExists},
		{Field: "absent", Op: selector.OpEq, Value: "x"},
	}
	for _, other := range others {
		alts := []selector.Predicate{p, other}
		compiled, err := Compile([]selector.Predicate{{Any: alts}}, boundedWindow(true))
		require.NoError(t, err)
		alone, err := Compile(alts, boundedWindow(true))
		require.NoError(t, err)

		// Only col is ever present, so only an alternative on it can match a cell.
		parsed := selector.Predicate{Any: alts}.Parse()
		scanMatches := false
		for _, cell := range cells {
			if slices.ContainsFunc(parsed.Any, func(alt selector.ParsedPredicate) bool {
				return alt.Field == "col" && cellMatches(t, alt, cell, list)
			}) {
				scanMatches = true
			}
		}
		may := MayMatchSegment(footer, compiled[0])
		if scanMatches && !may {
			t.Errorf("pruned any(%v, %v): cells %q hold a row the scan matches", p, other, cells)
		}
		assert.Equalf(t, MayMatchSegment(footer, alone[0]) || MayMatchSegment(footer, alone[1]), may,
			"segment admits any(%v, %v) when an alternative does", p, other)
		rowGroup := footer.RowGroupZoneMaps[0]
		assert.Equalf(t, mayMatchRowGroup(rowGroup, alone[:1]) || mayMatchRowGroup(rowGroup, alone[1:]), mayMatchRowGroup(rowGroup, compiled),
			"row group admits any(%v, %v) when an alternative does", p, other)
	}
}

func TestCompileRejectsANestedGroup(t *testing.T) {
	inner := selector.Predicate{Any: []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "a"}}}
	_, err := Compile([]selector.Predicate{{Any: []selector.Predicate{inner}}}, boundedWindow(true))
	var invalid *query.InvalidRequestError
	require.ErrorAs(t, err, &invalid)
}

// cellMatches is the scan's decision for one on-disk cell, by element for a list column.
func cellMatches(t *testing.T, p selector.ParsedPredicate, cell string, list bool) bool {
	t.Helper()
	if !list {
		return p.Matches(cell, true)
	}
	var elems []string
	require.NoError(t, json.Unmarshal([]byte(cell), &elems))
	return p.MatchesList(elems)
}

// sealOneColumn seals one record per column value and returns the footer plus the
// column's decoded on-disk cells — the exact strings the scan compares.
func sealOneColumn(t *testing.T, ts time.Time, columns []segment.Column) (segment.Footer, []string) {
	t.Helper()
	records := make([]segment.Record, 0, len(columns))
	for i, col := range columns {
		records = append(records, segment.Record{
			Timestamp: ts.Add(time.Duration(i) * time.Second),
			Columns:   []segment.Column{col},
		})
	}
	obj, err := segment.Sealed{Records: records}.Encode()
	require.NoError(t, err)
	rd := segment.NewReader(storage.Fake{
		GetSuffixFn: func(_ string, n int64) ([]byte, error) { return obj[max(0, int64(len(obj))-n):], nil },
		GetRangeFn:  func(_ string, off, n int64) ([]byte, error) { return obj[off : off+n], nil },
	})
	ctx := context.Background()
	footer, _, err := rd.Footer(ctx, segment.ObjectLocation{})
	require.NoError(t, err)
	raw, _, err := rd.Columns(ctx, segment.ObjectLocation{}, footer, 0, nil, "col")
	require.NoError(t, err)
	cells := make([]string, 0, raw["col"].Len())
	for _, b := range raw["col"].Rows() {
		cells = append(cells, string(b))
	}
	return footer, cells
}

func intCol(v int64) segment.Column {
	return segment.Column{Name: "col", Type: value.TypeInt, Value: value.Value{Int: v}}
}

func floatCol(v float64) segment.Column {
	return segment.Column{Name: "col", Type: value.TypeFloat, Value: value.Value{Float: v}}
}

func stringCol(v string) segment.Column {
	return segment.Column{Name: "col", Type: value.TypeString, Value: value.Value{String: v}}
}

func boolCol(v bool) segment.Column {
	return segment.Column{Name: "col", Type: value.TypeBool, Value: value.Value{Bool: v}}
}

func stringList(vs ...string) segment.Column {
	list := make([]value.Value, len(vs))
	for i, v := range vs {
		list[i] = value.Value{String: v}
	}
	return segment.Column{Name: "col", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: list}}
}

func intList(vs ...int64) segment.Column {
	list := make([]value.Value, len(vs))
	for i, v := range vs {
		list[i] = value.Value{Int: v}
	}
	return segment.Column{Name: "col", Type: value.TypeInt | value.TypeRepeated, Value: value.Value{List: list}}
}

// TestCompileRefusesAnInvalidRegex refuses a pattern RE2 cannot compile rather than select nothing.
func TestCompileRefusesAnInvalidRegex(t *testing.T) {
	_, err := Compile([]selector.Predicate{{Field: "message", Op: selector.OpRegex, Value: "("}}, boundedWindow(true))
	var invalid *query.InvalidRequestError
	assert.ErrorAs(t, err, &invalid)
}

// TestCompileRefusesAnUnknownOperator refuses the request rather than report a missing field.
func TestCompileRefusesAnUnknownOperator(t *testing.T) {
	_, err := Compile([]selector.Predicate{{Field: "level", Op: "like", Value: "x"}}, boundedWindow(true))
	var invalid *query.InvalidRequestError
	assert.ErrorAs(t, err, &invalid)
}
