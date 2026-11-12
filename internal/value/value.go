// Package value is the typed scalar system shared by segments, zone maps, and
// predicates.
package value

import (
	"encoding/json"
	"strconv"
	"time"
)

type Type uint8

const (
	TypeString Type = iota + 1
	TypeInt
	TypeFloat
	TypeBool
	TypeTimestamp
	TypeJSON

	TypeRepeated Type = 1 << 7
)

// TimeLayoutFixedNano keeps all 9 fractional digits where time.RFC3339Nano trims
// trailing zeros, so string min/max in a zone map matches chronological order.
const TimeLayoutFixedNano = "2006-01-02T15:04:05.000000000Z07:00"

func (t Type) Scalar() Type {
	return t &^ TypeRepeated
}

// Numeric reports whether the type holds a number, which is what quantile sketches
// and float bounds can summarize.
func (t Type) Numeric() bool {
	return t.Scalar() == TypeInt || t.Scalar() == TypeFloat
}

func (t Type) Repeated() bool {
	return t&TypeRepeated != 0
}

// Promote returns the one type that can hold both a and b: their shared
// type, Float where an Int meets a Float, and String otherwise — every value has
// a canonical string form, so that widening always succeeds.
func Promote(a, b Type) Type {
	if a == 0 {
		return b
	}
	if b == 0 {
		return a
	}
	return promoteScalar(a.Scalar(), b.Scalar()) | (a|b)&TypeRepeated
}

func promoteScalar(a, b Type) Type {
	if a == b {
		return a
	}
	if (a == TypeInt && b == TypeFloat) || (a == TypeFloat && b == TypeInt) {
		return TypeFloat
	}
	return TypeString
}

// Coerce handles only the conversions Promote can demand: Int to Float, anything
// to its canonical string, and a scalar to a one-element list.
func Coerce(from Type, v Value, to Type) Value {
	if to.Repeated() {
		elems := v.List
		if !from.Repeated() {
			elems = []Value{v}
		}
		list := make([]Value, len(elems))
		for i, elem := range elems {
			list[i] = Coerce(from.Scalar(), elem, to.Scalar())
		}
		return Value{List: list}
	}
	if from.Scalar() == to.Scalar() {
		return v
	}
	if from.Scalar() == TypeInt && to.Scalar() == TypeFloat {
		return Value{Float: float64(v.Int)}
	}
	return Value{String: Format(from, v)}
}

// Value keeps scalars in concrete fields so scan-time comparisons neither box nor
// allocate, and omitzero keeps the two per zone map small in the descriptors
// the catalog parses on every cache miss.
type Value struct {
	String string    `json:"string,omitzero"`
	Int    int64     `json:"int,omitzero"`
	Float  float64   `json:"float,omitzero"`
	Bool   bool      `json:"bool,omitzero"`
	Time   time.Time `json:"time,omitzero"`
	Bytes  []byte    `json:"bytes,omitzero"`
	// List holds a repeated value's elements; a record without the column is a null
	// list, distinct from a present empty one.
	List []Value `json:"list,omitzero"`
}

func JSON(v []byte) Value {
	return Value{Bytes: append([]byte(nil), v...)}
}

func Compare(typ Type, left, right Value) int {
	switch typ.Scalar() {
	case TypeInt:
		return cmpOrdered(left.Int, right.Int)
	case TypeFloat:
		return cmpOrdered(left.Float, right.Float)
	case TypeBool:
		return compareBool(left.Bool, right.Bool)
	case TypeTimestamp:
		return left.Time.Compare(right.Time)
	default:
		return cmpOrdered(Format(typ.Scalar(), left), Format(typ.Scalar(), right))
	}
}

func compareBool(left, right bool) int {
	switch {
	case left == right:
		return 0
	case !left && right:
		return -1
	default:
		return 1
	}
}

func cmpOrdered[T ~string | ~int64 | ~float64](left, right T) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func Format(typ Type, v Value) string {
	if typ.Repeated() {
		return formatList(typ.Scalar(), v.List)
	}
	switch typ.Scalar() {
	case TypeJSON:
		return string(v.Bytes)
	case TypeString:
		return v.String
	}
	return string(AppendFormat(nil, typ, v))
}

// AppendFormat appends Format's text to dst.
func AppendFormat(dst []byte, typ Type, v Value) []byte {
	if typ.Repeated() {
		return append(dst, formatList(typ.Scalar(), v.List)...)
	}
	switch typ.Scalar() {
	case TypeInt:
		return strconv.AppendInt(dst, v.Int, 10)
	case TypeFloat:
		return strconv.AppendFloat(dst, v.Float, 'g', -1, 64)
	case TypeBool:
		return strconv.AppendBool(dst, v.Bool)
	case TypeTimestamp:
		return v.Time.UTC().AppendFormat(dst, TimeLayoutFixedNano)
	case TypeJSON:
		return append(dst, v.Bytes...)
	default:
		return append(dst, v.String...)
	}
}

// formatList renders a repeated value as a JSON array of its elements' canonical
// strings.
func formatList(typ Type, list []Value) string {
	elems := make([]string, len(list))
	for i, v := range list {
		elems[i] = Format(typ, v)
	}
	return string(ListText(elems))
}

// ListText is the text of a list whose elements' canonical strings are elems.
func ListText(elems []string) []byte {
	text, _ := json.Marshal(elems) // a []string always marshals
	return text
}

func Parse(typ Type, text string) (Value, error) {
	switch typ.Scalar() {
	case TypeInt:
		v, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return Value{}, err
		}
		return Value{Int: v}, nil
	case TypeFloat:
		v, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return Value{}, err
		}
		return Value{Float: v}, nil
	case TypeBool:
		v, err := strconv.ParseBool(text)
		return Value{Bool: v}, err
	case TypeTimestamp:
		// Parse leniently but format fixed-width: callers pass request-supplied
		// timestamps here, which carry however many fractional digits they like.
		v, err := time.Parse(time.RFC3339Nano, text)
		return Value{Time: v}, err
	case TypeJSON:
		return JSON([]byte(text)), nil
	default:
		return Value{String: text}, nil
	}
}
