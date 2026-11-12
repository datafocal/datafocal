package sketch

import "testing"

// TestHotPathZeroAlloc gates the sketch hot paths at zero heap allocations per
// call. These are the exact regressions the adversarial review found: a per-call
// fnv.New64a() hasher allocation and a heap-escaping closure. If a
// change reintroduces an allocation on any of these, this test fails — the
// machine-checked half of the "efficient code" bar.
func TestHotPathZeroAlloc(t *testing.T) {
	key := []byte("trace-8f2a1c9d3e4b5a6f")

	hll := NewHLL()

	cases := []struct {
		name string
		fn   func()
	}{
		{"Hash64", func() { _ = Hash64(key) }},
		{"HLL.Add", func() { hll.Add(key) }},
	}
	for _, c := range cases {
		if got := testing.AllocsPerRun(200, c.fn); got != 0 {
			t.Errorf("%s: %.0f allocs/op, want 0", c.name, got)
		}
	}
}
