package logformat

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parseCorpus is a line in each format ingest reads, cri wrapping JSON as kubelet writes it.
var parseCorpus = []struct {
	name   string
	cfg    Config
	line   string
	allocs float64 // per Parse, under CI's toolchain and -race
}{
	{"json", Config{Format: "json"}, `{"ts":"2026-10-20T12:00:00.123Z","level":"error","msg":"db timeout","service":"api","latency_ms":120,"user":{"id":"u1"},"tags":["a","b"]}`, 32},
	{"logfmt", Config{Format: "logfmt"}, `time=2026-10-20T12:00:00.123Z level=info msg="request done" method=GET path=/v1/users status=200 duration_ms=12.5`, 20},
	{"klog", Config{Format: "klog"}, `W1020 12:00:00.123456    1 reflector.go:424] failed to list *v1.Pod: connection refused`, 4},
	{"plain", Config{Format: "plain"}, `2026-10-20 12:00:00.123 UTC [57] LOG:  checkpoint complete: wrote 42 buffers (0.3%)`, 2},
	{"syslog", Config{Format: "syslog"}, `<34>Oct 20 12:00:00 router1 bgpd[812]: neighbor 10.0.0.2 down`, 8},
	{"journal", Config{Format: "journal"}, `{"__CURSOR":"s=6bf3;i=1","__REALTIME_TIMESTAMP":"1793196001123456","PRIORITY":"6","_HOSTNAME":"web1","_PID":"99","_SYSTEMD_UNIT":"sshd.service","SYSLOG_IDENTIFIER":"sshd","MESSAGE":"Accepted publickey for deploy"}`, 43},
	{"cri", Config{Format: "auto", Envelope: "cri"}, `2026-10-20T12:00:00.123456789Z stdout F {"level":"info","msg":"request done","status":200,"duration_ms":12.5}`, 17},
}

func BenchmarkParse(b *testing.B) {
	for _, tc := range parseCorpus {
		b.Run(tc.name, func(b *testing.B) {
			p, err := New(tc.cfg)
			require.NoError(b, err)
			b.SetBytes(int64(len(tc.line)))
			b.ReportAllocs()
			for b.Loop() {
				p.Parse(tc.line)
			}
		})
	}
}

// TestParseAllocations pins the allocations of one Parse per format, so a change that
// adds one fails here; lower a budget when a change removes one.
func TestParseAllocations(t *testing.T) {
	for _, tc := range parseCorpus {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(tc.cfg)
			require.NoError(t, err)
			// Race builds randomly discard pooled JSON parsers, so take the steady state.
			got := testing.AllocsPerRun(1, func() { p.Parse(tc.line) })
			for range 100 {
				got = min(got, testing.AllocsPerRun(1, func() { p.Parse(tc.line) }))
			}
			assert.LessOrEqual(t, got, tc.allocs)
		})
	}
}
