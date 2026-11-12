package sketch

import (
	"errors"
	"math"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/axiomhq/hyperloglog"
	"github.com/stretchr/testify/require"
)

// All tests use a fixed RNG seed so results are deterministic and never depend
// on the wall clock.
const seed = 0xC0FFEE

// --- HLL ---------------------------------------------------------------------

func TestHLLAccuracy(t *testing.T) {
	const n = 200_000
	h := NewHLL()
	for i := range n {
		h.Add([]byte("item-" + strconv.Itoa(i)))
	}

	est := h.Estimate()
	relErr := math.Abs(est-float64(n)) / float64(n)

	// Standard error is ~1.04/sqrt(m); allow 3x as the property bound.
	se := 1.04 / math.Sqrt(float64(hllRegisters))
	bound := 3 * se
	if relErr > bound {
		t.Fatalf("HLL relative error %.4f exceeds 3*SE=%.4f (est=%.0f, n=%d)", relErr, bound, est, n)
	}
}

func TestHLLMerge(t *testing.T) {
	const n = 100_000
	a := NewHLL()
	b := NewHLL()
	// a covers [0,n); b covers [n/2, 3n/2); union distinct = 3n/2.
	for i := range n {
		a.Add([]byte("k" + strconv.Itoa(i)))
	}
	for i := n / 2; i < 3*n/2; i++ {
		b.Add([]byte("k" + strconv.Itoa(i)))
	}
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}

	union := float64(3 * n / 2)
	est := a.Estimate()
	relErr := math.Abs(est-union) / union
	bound := 3 * (1.04 / math.Sqrt(float64(hllRegisters)))
	if relErr > bound {
		t.Fatalf("merged HLL relative error %.4f exceeds %.4f (est=%.0f, union=%.0f)", relErr, bound, est, union)
	}
}

// TestHLLMayHold: a sketch holds every value added to it, in either form, and others the
// more often the more values it holds.
func TestHLLMayHold(t *testing.T) {
	for n, most := range map[int]int{100: 2, hllSparseMax + 1: hllSparseMax / 5} {
		h := NewHLL()
		for i := range n {
			h.Add([]byte("k" + strconv.Itoa(i)))
		}
		missing, others := 0, 0
		for i := range n {
			if !h.MayHold([]byte("k" + strconv.Itoa(i))) {
				missing++
			}
			if h.MayHold([]byte("other" + strconv.Itoa(i))) {
				others++
			}
		}
		if missing > 0 || others > most {
			t.Fatalf("n=%d: sketch lacks %d values added and may hold %d of %d never added", n, missing, others, n)
		}
	}
}

// TestHLLIsTheDenseLibrarySketch: however values are split across sketches and merged,
// the result reads exactly as the library's dense sketch fed the same values, on both
// sides of the sparse form's limit, and so does either encoding of it decoded.
func TestHLLIsTheDenseLibrarySketch(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	for _, n := range []int{0, 1, 3, 100, hllSparseMax - 50, hllSparseMax + 50, 6000, 20_000} {
		oracle := hyperloglog.NewNoSparse()
		parts := make([]*HLL, 1+rng.Intn(5))
		for i := range parts {
			parts[i] = NewHLL()
		}
		for range n {
			v := []byte(strconv.Itoa(rng.Intn(4 * n)))
			oracle.InsertHash(Hash64(v))
			parts[rng.Intn(len(parts))].Add(v)
		}
		for len(parts) > 1 { // merge random neighbours until one is left
			i := rng.Intn(len(parts) - 1)
			require.NoError(t, parts[i].Merge(parts[i+1]))
			parts = slices.Delete(parts, i+1, i+2)
		}
		dense, err := oracle.MarshalBinary()
		require.NoError(t, err)
		encoded, err := parts[0].MarshalBinary()
		require.NoError(t, err)
		if parts[0].regs != nil {
			require.Equal(t, dense, encoded, "n=%d", n)
		}
		fromDense, fromEncoded := NewHLL(), NewHLL()
		require.NoError(t, fromDense.UnmarshalBinary(dense))
		require.NoError(t, fromEncoded.UnmarshalBinary(encoded))
		for _, h := range []*HLL{parts[0], fromDense, fromEncoded} {
			regs, err := h.Registers()
			require.NoError(t, err)
			require.Equal(t, dense[hllHeader:], regs, "n=%d", n)
			require.Equal(t, float64(oracle.Estimate()), h.Estimate(), "n=%d", n)
		}
	}
}

// --- DDSketch ----------------------------------------------------------------

// TestUnionEstimateIsTheLibrarys: the register-wise estimate must equal the library's
// Estimate of the merged sketch exactly, from empty through saturated, since topology
// compares the two kinds of figure against each other.
func TestUnionEstimateIsTheLibrarys(t *testing.T) {
	for _, n := range []int{0, 1, 10, 1000, 30_000, 200_000, 2_000_000} {
		a, b := NewHLL(), NewHLL()
		for i := range n {
			a.Add([]byte("a" + strconv.Itoa(i)))
			if i%3 == 0 {
				b.Add([]byte("a" + strconv.Itoa(i))) // a third shared
			} else {
				b.Add([]byte("b" + strconv.Itoa(i)))
			}
		}
		ra, err := a.Registers()
		if err != nil {
			t.Fatal(err)
		}
		rb, err := b.Registers()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := UnionEstimate(ra), a.Estimate(); got != want {
			t.Errorf("one sketch of %d: UnionEstimate %v, Estimate %v", n, got, want)
		}
		union := NewHLL()
		if err := errors.Join(union.Merge(a), union.Merge(b)); err != nil {
			t.Fatal(err)
		}
		if got, want := UnionEstimate(ra, rb), union.Estimate(); got != want {
			t.Errorf("union of %d each: UnionEstimate %v, Estimate %v", n, got, want)
		}
	}
}

func TestDDSketchQuantileGuarantee(t *testing.T) {
	const (
		n     = 100_000
		alpha = 0.01
	)
	rng := rand.New(rand.NewSource(seed))
	d := NewDDSketch(alpha)
	vals := make([]float64, 0, n)
	for range n {
		v := rng.Float64()*1e6 + 1 // positive
		vals = append(vals, v)
		d.Add(v)
	}
	sort.Float64s(vals)

	for _, q := range []float64{0.0, 0.1, 0.5, 0.9, 0.99, 1.0} {
		trueVal := vals[int(q*float64(n-1))]
		est := d.Quantile(q)
		if math.Abs(est-trueVal) > alpha*trueVal {
			t.Fatalf("q=%.2f: |%.3f-%.3f|=%.3f exceeds alpha*true=%.3f", q, est, trueVal, math.Abs(est-trueVal), alpha*trueVal)
		}
	}
}

func TestDDSketchMerge(t *testing.T) {
	const (
		n     = 60_000
		alpha = 0.01
	)
	rng := rand.New(rand.NewSource(seed + 1))
	a := NewDDSketch(alpha)
	b := NewDDSketch(alpha)
	all := make([]float64, 0, 2*n)
	for range n {
		v := rng.Float64()*5e5 + 1
		a.Add(v)
		all = append(all, v)
	}
	for range n {
		v := rng.Float64()*5e5 + 1
		b.Add(v)
		all = append(all, v)
	}
	if err := a.Merge(b); err != nil {
		t.Fatal(err)
	}
	sort.Float64s(all)

	for _, q := range []float64{0.1, 0.5, 0.9, 0.99} {
		trueVal := all[int(q*float64(len(all)-1))]
		est := a.Quantile(q)
		if math.Abs(est-trueVal) > alpha*trueVal {
			t.Fatalf("merged q=%.2f: |%.3f-%.3f| exceeds alpha*true=%.3f", q, est, trueVal, alpha*trueVal)
		}
	}
}

// --- Marshal round-trips -----------------------------------------------------

func TestHLLRoundTrip(t *testing.T) {
	h := NewHLL()
	for i := range 10_000 {
		h.Add([]byte(strconv.Itoa(i)))
	}
	data, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got := NewHLL()
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	// Behavioral equivalence is the contract (DeepEqual on the wrapped library
	// state is not meaningful): the restored sketch must estimate identically.
	if h.Estimate() != got.Estimate() {
		t.Fatalf("HLL estimate differs after round-trip: %v vs %v", h.Estimate(), got.Estimate())
	}
}

func TestDDSketchRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(seed))
	d := NewDDSketch(0.01)
	for range 10_000 {
		d.Add(rng.Float64()*1e4 + 1)
	}
	data, err := d.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got := NewDDSketch(0.5) // wrong alpha on purpose; unmarshal must restore it
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}
	// Behavioral equivalence: restored quantiles must match (the embedded mapping
	// is decoded, overriding the wrong alpha above).
	for _, q := range []float64{0.1, 0.5, 0.9, 0.99} {
		if d.Quantile(q) != got.Quantile(q) {
			t.Fatalf("DDSketch quantile %.2f differs after round-trip", q)
		}
	}
}
