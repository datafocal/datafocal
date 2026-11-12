package pattern

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

// deriveCorpus is a record with a message to mask, and one Derive names from its columns.
var deriveCorpus = []struct {
	name    string
	columns []segment.Column
	allocs  float64 // per Derive, under CI's toolchain and -race
}{
	{"message", []segment.Column{str("service", "api"), str("level", "error"), str("message", "query took 12.5 ms on shard 7"), str("error", "timeout after 30s")}, 20},
	{"structured", []segment.Column{str("service", "api"), str("level", "info"), str("method", "GET"), str("path", "/v1/users"),
		{Name: "status", Type: value.TypeInt, Value: value.Value{Int: 200}}}, 11},
}

func BenchmarkDerive(b *testing.B) {
	for _, tc := range deriveCorpus {
		b.Run(tc.name, func(b *testing.B) {
			p := New(10_000)
			b.ReportAllocs()
			for b.Loop() {
				p.Derive(tc.columns)
			}
		})
	}
}

// TestDeriveAllocations pins the allocations of one Derive of a record the Pattern has seen.
func TestDeriveAllocations(t *testing.T) {
	for _, tc := range deriveCorpus {
		t.Run(tc.name, func(t *testing.T) {
			p := New(10_000)
			got := testing.AllocsPerRun(100, func() { p.Derive(tc.columns) })
			assert.LessOrEqual(t, got, tc.allocs)
		})
	}
}
