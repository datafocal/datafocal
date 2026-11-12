package sketch

import "testing"

// Micro-benchmarks for the sketch hot paths. They establish a baseline for the
// CI bench-smoke stage and are the input to a benchstat regression gate.
// Run: go test -bench=. -benchmem ./internal/sketch

func BenchmarkHash64(b *testing.B) {
	key := []byte("trace-8f2a1c9d3e4b5a6f")
	b.ReportAllocs()
	for b.Loop() {
		_ = Hash64(key)
	}
}

func BenchmarkHLLAdd(b *testing.B) {
	h := NewHLL()
	key := []byte("trace-8f2a1c9d3e4b5a6f")
	b.ReportAllocs()
	for b.Loop() {
		h.Add(key)
	}
}

func BenchmarkHLLMerge(b *testing.B) {
	a, o := NewHLL(), NewHLL()
	for i := range 1000 {
		a.Add([]byte{byte(i), byte(i >> 8)})
		o.Add([]byte{byte(i + 1), byte(i >> 8)})
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = a.Merge(o)
	}
}
