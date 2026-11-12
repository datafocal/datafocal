package sketch

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"slices"
)

// HLL is the github.com/axiomhq/hyperloglog dense p=14 sketch register for register, holding
// only its nonzero registers until they outgrow 16 KiB so its memory follows its values.
type HLL struct {
	// sparse is each nonzero register as index<<8 | value, sorted, while regs is nil.
	sparse []uint32
	// regs is the dense form, one byte per register.
	regs []byte
}

const (
	// hllPrecision is p, the number of hash bits that pick a register.
	hllPrecision = 14
	// hllRegisters is m, the dense representation's register count.
	hllRegisters = 1 << hllPrecision
	// hllSparseMax is the most nonzero registers the sparse form holds: the dense form's bytes.
	hllSparseMax = hllRegisters / 4
	// hllHeader is the library's dense encoding header: version 2, precision, base, a 0
	// sparse flag and a 4-byte register count; the sparse form's flag and count of entries.
	hllHeader = 8
	// hllSparseFlag marks the sparse form, which the library's own sparse flag of 1 is not.
	hllSparseFlag = 2
)

// HLLStandardError is the relative standard error of an Estimate: 1.04/√m.
//
// It is exported because a caller that *compares* two estimates has to know how
// much of the difference is the sketch rather than the data — subtracting two
// counts that each carry this error yields a figure whose own error is larger
// than either, so the comparison needs an absolute noise floor scaled by the
// cardinalities involved.
const HLLStandardError = 1.04 / 128 // √(1<<14) = 128

// NewHLL returns an empty HLL.
func NewHLL() *HLL { return &HLL{} }

// Add folds one observed value into the sketch.
func (h *HLL) Add(b []byte) {
	h.insert(register(b))
}

// MayHold reports whether b may have been added: false is certain, and true may be other
// values having raised its register as far, more likely the more values the sketch holds.
func (h *HLL) MayHold(b []byte) bool {
	i, v := register(b)
	if h.regs != nil {
		return h.regs[i] >= v
	}
	at, found := slices.BinarySearchFunc(h.sparse, i, func(e, i uint32) int { return int(e>>8) - int(i) })
	return found && uint8(h.sparse[at]) >= v
}

// register is the library's register choice for b: the hash's top p bits pick it, and the
// rest give its rank.
func register(b []byte) (uint32, uint8) {
	x := Hash64(b)
	return uint32(x >> (64 - hllPrecision)), uint8(bits.LeadingZeros64(x<<hllPrecision|1<<(hllPrecision-1))) + 1
}

// insert raises register i to at least v.
func (h *HLL) insert(i uint32, v uint8) {
	if h.regs != nil {
		h.regs[i] = max(h.regs[i], v)
		return
	}
	at, found := slices.BinarySearchFunc(h.sparse, i, func(e, i uint32) int { return int(e>>8) - int(i) })
	switch {
	case found && uint8(h.sparse[at]) < v:
		h.sparse[at] = i<<8 | uint32(v)
	case !found:
		h.sparse = slices.Insert(h.sparse, at, i<<8|uint32(v))
		if len(h.sparse) > hllSparseMax {
			h.regs, h.sparse = h.dense(), nil
		}
	}
}

// dense returns a copy of the sketch's registers, one byte each.
func (h *HLL) dense() []byte {
	if h.regs != nil {
		return slices.Clone(h.regs)
	}
	regs := make([]byte, hllRegisters)
	for _, e := range h.sparse {
		regs[e>>8] = uint8(e)
	}
	return regs
}

// Merge combines o into h (register-wise max), never erroring despite the signature.
func (h *HLL) Merge(o *HLL) error {
	if o.regs != nil && h.regs == nil {
		h.regs, h.sparse = h.dense(), nil
	}
	if h.regs == nil {
		h.mergeSparse(o.sparse)
		return nil
	}
	for _, e := range o.sparse {
		h.insert(e>>8, uint8(e))
	}
	for i, v := range o.regs {
		h.regs[i] = max(h.regs[i], v)
	}
	return nil
}

// mergeSparse merges sorted entries into h's from the back of the grown slice, so a
// register's entries run in value order and its last is its maximum.
func (h *HLL) mergeSparse(entries []uint32) {
	i := len(h.sparse) - 1
	h.sparse = append(h.sparse, entries...)
	merged := h.sparse
	for j, k := len(entries)-1, len(merged)-1; j >= 0; k-- {
		if i >= 0 && merged[i] > entries[j] {
			merged[k], i = merged[i], i-1
		} else {
			merged[k], j = entries[j], j-1
		}
	}
	h.sparse = merged[:0]
	for i, e := range merged {
		if i+1 == len(merged) || merged[i+1]>>8 != e>>8 {
			h.sparse = append(h.sparse, e)
		}
	}
	if len(h.sparse) > hllSparseMax {
		h.regs, h.sparse = h.dense(), nil
	}
}

// Estimate returns the estimated number of distinct values added.
func (h *HLL) Estimate() float64 {
	if h.regs != nil {
		return UnionEstimate(h.regs)
	}
	var sum, zeros float64
	next := 0
	for i := range hllRegisters {
		v := byte(0)
		if next < len(h.sparse) && h.sparse[next]>>8 == uint32(i) {
			v = uint8(h.sparse[next])
			next++
		}
		if v == 0 {
			zeros++
		}
		sum += inversePow2[v]
	}
	return estimate(sum, zeros)
}

// MarshalBinary writes the library's dense encoding, or while h is sparse its nonzero
// registers after a header flagging them, which UnmarshalBinary reads as well.
func (h *HLL) MarshalBinary() ([]byte, error) {
	if h.regs != nil {
		return append([]byte{2, hllPrecision, 0, 0, 0, 0, hllRegisters >> 8, 0}, h.regs...), nil
	}
	data := binary.BigEndian.AppendUint32([]byte{2, hllPrecision, 0, hllSparseFlag}, uint32(len(h.sparse)))
	for _, e := range h.sparse {
		data = binary.BigEndian.AppendUint32(data, e)
	}
	return data, nil
}

// UnmarshalBinary reads either encoding MarshalBinary writes, keeping a dense one with few
// registers set as the sparse form; it does not retain data.
func (h *HLL) UnmarshalBinary(data []byte) error {
	if len(data) >= hllHeader && data[1] == hllPrecision && data[3] == hllSparseFlag {
		return h.unmarshalSparse(data[hllHeader:], binary.BigEndian.Uint32(data[4:]))
	}
	if len(data) != hllHeader+hllRegisters || data[1] != hllPrecision || data[3] != 0 {
		return fmt.Errorf("sketch: HLL encoding of %d bytes is not a dense one of %d registers", len(data), hllRegisters)
	}
	regs := data[hllHeader:]
	nonzero := hllRegisters - bytes.Count(regs, []byte{0})
	if nonzero > hllSparseMax {
		h.regs, h.sparse = slices.Clone(regs), nil
		return nil
	}
	h.regs, h.sparse = nil, make([]uint32, 0, nonzero)
	for i, v := range regs {
		if v != 0 {
			h.sparse = append(h.sparse, uint32(i)<<8|uint32(v))
		}
	}
	return nil
}

// unmarshalSparse reads n sparse entries, rejecting any a dense sketch could not hold.
func (h *HLL) unmarshalSparse(data []byte, n uint32) error {
	if n > hllSparseMax || len(data) != 4*int(n) {
		return fmt.Errorf("sketch: sparse HLL encoding of %d bytes does not hold %d registers", len(data), n)
	}
	h.regs, h.sparse = nil, make([]uint32, n)
	for i := range h.sparse {
		e := binary.BigEndian.Uint32(data[4*i:])
		if e>>8 >= hllRegisters || uint8(e) == 0 || i > 0 && e>>8 <= h.sparse[i-1]>>8 {
			return fmt.Errorf("sketch: sparse HLL entry %d of %#x is out of order or range", i, e)
		}
		h.sparse[i] = e
	}
	return nil
}

// Size is the bytes h holds, for budgeting a cache of decoded footers.
func (h *HLL) Size() int64 { return int64(4*cap(h.sparse) + cap(h.regs)) }

// Registers is a copy of the sketch's registers, one byte each, for estimating unions of
// sketches without merging them (UnionEstimate).
func (h *HLL) Registers() ([]byte, error) { return h.dense(), nil }

// UnionEstimate is the library's Estimate of the register-wise max of regs, bit for bit,
// with 2^-v looked up where the library computes it per register.
func UnionEstimate(regs ...[]byte) float64 {
	if len(regs) == 2 {
		return pairEstimate(regs[0], regs[1])
	}
	var sum, zeros float64
	for i := range hllRegisters {
		v := byte(0)
		for _, r := range regs {
			v = max(v, r[i])
		}
		if v == 0 {
			zeros++
		}
		sum += inversePow2[v]
	}
	return estimate(sum, zeros)
}

// pairEstimate is UnionEstimate of two, the pairs a topology compares, without its loop over them.
func pairEstimate(a, b []byte) float64 {
	a, b = a[:hllRegisters], b[:hllRegisters]
	var sum, zeros float64
	for i := range hllRegisters {
		v := max(a[i], b[i])
		if v == 0 {
			zeros++
		}
		sum += inversePow2[v]
	}
	return estimate(sum, zeros)
}

// estimate is the library's Estimate from its register sum and count of zero registers.
func estimate(sum, zeros float64) float64 {
	// A variable, not a constant, so alpha rounds at each step as the library's does.
	m := float64(hllRegisters)
	alpha := 0.7213 / (1 + 1.079/m)
	return float64(uint64(alpha*m*(m-zeros)/(sum+beta14(zeros)) + 0.5))
}

// inversePow2 is 1/2^v for every register value, as the library computes it.
var inversePow2 = func() (t [256]float64) {
	for v := range t {
		t[v] = 1.0 / math.Pow(2.0, float64(v))
	}
	return t
}()

// beta14 is the library's LogLog-Beta bias correction for precision 14.
//
//nolint:staticcheck // math.Pow as the library computes it, so the estimate matches it bit for bit
func beta14(zeros float64) float64 {
	zl := math.Log(zeros + 1)
	return -0.371009760230692*zeros +
		0.00978811941207509*zl +
		0.185796293324165*math.Pow(zl, 2) +
		0.203015527328432*math.Pow(zl, 3) +
		-0.116710521803686*math.Pow(zl, 4) +
		0.0431106699492820*math.Pow(zl, 5) +
		-0.00599583540511831*math.Pow(zl, 6) +
		0.000449704299509437*math.Pow(zl, 7)
}

var (
	_ encoding.BinaryMarshaler   = (*HLL)(nil)
	_ encoding.BinaryUnmarshaler = (*HLL)(nil)
)
