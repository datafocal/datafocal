package segment

import (
	"math"
	"unicode"
	"unicode/utf8"

	"github.com/datafocal/datafocal/internal/value"
)

type ZoneMaps map[string]ZoneMap

type ZoneMap struct {
	Type value.Type  `json:"type"`
	Min  value.Value `json:"min,omitzero"`
	Max  value.Value `json:"max,omitzero"`
	// PresentRows is how many values Min and Max bound: the rows carrying this column,
	// or a list column's elements, so only a scalar's may be read against a row count.
	PresentRows int `json:"present_rows"`
}

func (z *ZoneMaps) Update(column string, typ value.Type, v value.Value) {
	if *z == nil {
		*z = make(ZoneMaps)
	}

	current, ok := (*z)[column]
	if !ok || current.PresentRows == 0 {
		(*z)[column] = ZoneMap{Type: typ, Min: v, Max: v, PresentRows: 1}
		return
	}
	if typ != current.Type {
		// Records disagree on this column's type, so no single bound covers every
		// row. Drop the entry: readers read a missing zone map as unknown, whereas a
		// bound that silently omits rows prunes segments the scan would match.
		delete(*z, column)
		return
	}
	current.PresentRows++
	if value.Compare(typ, v, current.Min) < 0 {
		current.Min = v
	}
	if value.Compare(typ, v, current.Max) > 0 {
		current.Max = v
	}
	(*z)[column] = current
}

// merge widens z to cover o's rows as well, both of one type per column.
func (z ZoneMaps) merge(o ZoneMaps) {
	for column, m := range o {
		cur, ok := z[column]
		if !ok || cur.PresentRows == 0 {
			z[column] = m
			continue
		}
		if m.PresentRows == 0 {
			continue
		}
		if value.Compare(m.Type, m.Min, cur.Min) < 0 {
			cur.Min = m.Min
		}
		if value.Compare(m.Type, m.Max, cur.Max) > 0 {
			cur.Max = m.Max
		}
		cur.PresentRows += m.PresentRows
		z[column] = cur
	}
}

// maxBoundBytes is the most of a text bound a zone map keeps: a column of tool results or
// message bodies otherwise kept two of its values whole in every footer and descriptor.
const maxBoundBytes = 64

// bounded is z with text bounds past maxBoundBytes cut, the minimum to a prefix and the maximum
// to the least text above every one sharing its prefix, so each still bounds what it did;
// bounds that agree stay whole, as the column's sole value (SoleValue).
func (z ZoneMap) bounded() ZoneMap {
	switch z.Type.Scalar() {
	case value.TypeString:
		if z.Min.String != z.Max.String {
			z.Min.String, z.Max.String = boundText(z.Min.String, false), boundText(z.Max.String, true)
		}
	case value.TypeJSON:
		if string(z.Min.Bytes) != string(z.Max.Bytes) {
			z.Min.Bytes, z.Max.Bytes = []byte(boundText(string(z.Min.Bytes), false)), []byte(boundText(string(z.Max.Bytes), true))
		}
	}
	return z
}

// boundText is s cut to at most maxBoundBytes on a rune's start, and for an upper bound its
// last rune then incremented, which UTF-8's byte order puts after every text the cut prefixes.
func boundText(s string, upper bool) string {
	if len(s) <= maxBoundBytes {
		return s
	}
	cut := maxBoundBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	prefix := s[:cut]
	if !upper {
		return prefix
	}
	for prefix != "" {
		r, size := utf8.DecodeLastRuneInString(prefix)
		prefix = prefix[:len(prefix)-size]
		if r == utf8.RuneError || r == unicode.MaxRune {
			continue // a byte that is no rune, or the last rune, has nothing above it to take
		}
		next := r + 1
		if next >= 0xD800 && next <= 0xDFFF { // surrogates are no runes UTF-8 encodes
			next = 0xE000
		}
		return prefix + string(next)
	}
	return s
}

// declare records a column holding no value yet, so a list column whose every list is
// empty still tells a reader by its type that it is a list.
func (z ZoneMaps) declare(column string, typ value.Type) {
	if _, ok := z[column]; !ok {
		z[column] = ZoneMap{Type: typ}
	}
}

// MayContainString reports whether the column may hold v, a list column among its elements.
func (z ZoneMap) MayContainString(v string) bool {
	typ := z.Type.Scalar()
	if typ != value.TypeString && typ != value.TypeJSON {
		return true
	}
	return v >= value.Format(typ, z.Min) && v <= value.Format(typ, z.Max)
}

// SoleValue returns the value every one of segmentRows holds for this column, and
// whether the bound establishes that: the extremes must agree and the column must
// have been present on every row, because a bound over a subset says nothing about
// the rows it never saw. A list column's count is of elements, so it never qualifies.
func (z ZoneMap) SoleValue(segmentRows int) (value.Value, bool) {
	if segmentRows <= 0 || z.PresentRows != segmentRows || z.Type.Repeated() {
		return value.Value{}, false
	}
	if value.Compare(z.Type, z.Min, z.Max) != 0 {
		return value.Value{}, false
	}
	return z.Min, true
}

// StringBounds returns the column's text bounds, and whether they mean anything:
// only a string or timestamp bound orders as text, which is how the scan compares a
// column whose values are not all numbers (selector.ParsedPredicate.comparesOrdered).
// Timestamps qualify because they decode to a fixed-width layout.
func (z ZoneMap) StringBounds() (minimum, maximum string, ok bool) {
	switch z.Type {
	case value.TypeString:
		return z.Min.String, z.Max.String, true
	case value.TypeTimestamp:
		return value.Format(z.Type, z.Min), value.Format(z.Type, z.Max), true
	default:
		return "", "", false
	}
}

// NumericBounds returns the column's bounds as float64, and whether the column is
// numeric at all. These come from the stored extremes rather than a sketch's
// reconstruction of them, so they never exclude a row the column holds.
func (z ZoneMap) NumericBounds() (minimum, maximum float64, ok bool) {
	switch z.Type {
	case value.TypeInt:
		return intBound(z.Min.Int, math.Inf(-1)), intBound(z.Max.Int, math.Inf(1)), true
	case value.TypeFloat:
		return z.Min.Float, z.Max.Float, true
	default:
		return 0, 0, false
	}
}

// maxExactFloatInt is the largest magnitude float64 represents exactly. Ids and
// nanosecond epochs stored as ints reach past it, and there rounding could move a
// bound inward and prune a segment holding the boundary value.
const maxExactFloatInt = 1 << 53

func intBound(v int64, outward float64) float64 {
	if v > maxExactFloatInt || v < -maxExactFloatInt {
		return math.Nextafter(float64(v), outward)
	}
	return float64(v)
}
