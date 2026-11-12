package seedcorpus

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReplayCapture: two copies of a capture re-timed to end at end, the earlier one a
// second before the later starts, its ids its own but named alike throughout it, and the
// newest copy the captured lines but for their times.
func TestReplayCapture(t *testing.T) {
	dir, out := t.TempDir(), t.TempDir()
	files := map[string]string{
		CaptureManifest: `{"name": "test", "kind": "ai", "sources": [
			{"path": "pods/*.log", "parse": {"format": "auto", "envelope": "cri"}},
			{"path": "claude/*/*.jsonl", "parse": {"format": "json"}},
			{"path": "journal.jsonl", "parse": {"format": "journal"}}]}`,
		"pods/api-7d4b9c8f6b-x2k9p_default_api-" + strings.Repeat("ab", 32) + ".log": "" +
			"2026-11-01T10:00:00.123456789Z stdout F {\"msg\":\"start\",\"trace_id\":\"4bf92f3577b34da6a3ce929d0e0e4736\"}\n" +
			"2026-11-01T10:00:30.000000001Z stdout P half \n" +
			"2026-11-01T10:00:30.000000002Z stdout F a line\n",
		"journal.jsonl": `{"__CURSOR":"s=1","__REALTIME_TIMESTAMP":"1793527230000000","_SOURCE_REALTIME_TIMESTAMP":"1793527230000000","MESSAGE":"up"}` + "\n",
		"claude/-home-proj/1b4e28ba-2fa1-11d2-883f-0016d3cca427.jsonl": "" +
			`{"type":"user","sessionId":"1b4e28ba-2fa1-11d2-883f-0016d3cca427","timestamp":"2026-11-01T10:00:10.500Z"}` + "\n" +
			`{"type":"assistant","sessionId":"1b4e28ba-2fa1-11d2-883f-0016d3cca427","timestamp":"2026-11-01T10:01:00.000Z","text":"see 2019-01-01T00:00:00Z"}` + "\n",
	}
	for name, body := range files {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644))
	}
	end := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)

	r, err := ReplayCapture(dir, out, 2, end)
	require.NoError(t, err)
	r.Fingerprint, r.Bytes = "", 0 // content hashes and sizes, checked by the files below
	assert.Equal(t, Replay{LogsResult: LogsResult{Lines: map[string]int{"": 12}}, Records: 10, Span: 59876543211,
		Shift: end.Sub(time.Date(2026, 11, 1, 10, 1, 0, 0, time.UTC)), Files: 3, InputBytes: 570,
		Start: time.Date(2026, 11, 2, 23, 58, 0, 123456789, time.UTC), End: end}, r)

	read := func(name string) string {
		data, err := os.ReadFile(filepath.Join(out, name))
		require.NoError(t, err)
		return string(data)
	}
	// The copy before the newest starts 60 s (the span, whole seconds, and one) earlier.
	assert.Equal(t, ""+
		`{"type":"user","sessionId":"eae44c60-34e0-094c-0b46-b14ae7b6a826","timestamp":"2026-11-02T23:58:10.500Z"}`+"\n"+
		`{"type":"assistant","sessionId":"eae44c60-34e0-094c-0b46-b14ae7b6a826","timestamp":"2026-11-02T23:59:00.000Z","text":"see 2019-01-01T00:00:00Z"}`+"\n"+
		`{"type":"user","sessionId":"1b4e28ba-2fa1-11d2-883f-0016d3cca427","timestamp":"2026-11-02T23:59:10.500Z"}`+"\n"+
		`{"type":"assistant","sessionId":"1b4e28ba-2fa1-11d2-883f-0016d3cca427","timestamp":"2026-11-03T00:00:00.000Z","text":"see 2019-01-01T00:00:00Z"}`+"\n",
		read("claude/-home-proj/1b4e28ba-2fa1-11d2-883f-0016d3cca427.jsonl"))
	// journald's times are numbers, shifted as times rather than renamed as ids.
	at := func(shift time.Duration) string {
		return strconv.FormatInt(time.UnixMicro(1793527230000000).Add(shift).UnixMicro(), 10)
	}
	shift := end.Sub(time.Date(2026, 11, 1, 10, 1, 0, 0, time.UTC))
	assert.Equal(t, ""+
		`{"__CURSOR":"s=1","__REALTIME_TIMESTAMP":"`+at(shift-time.Minute)+`","_SOURCE_REALTIME_TIMESTAMP":"`+at(shift-time.Minute)+`","MESSAGE":"up"}`+"\n"+
		`{"__CURSOR":"s=1","__REALTIME_TIMESTAMP":"`+at(shift)+`","_SOURCE_REALTIME_TIMESTAMP":"`+at(shift)+`","MESSAGE":"up"}`+"\n", read("journal.jsonl"))
	pod := read("pods/api-7d4b9c8f6b-x2k9p_default_api-" + strings.Repeat("ab", 32) + ".log")
	assert.Equal(t, ""+
		"2026-11-02T23:58:00.123456789Z stdout F {\"msg\":\"start\",\"trace_id\":\""+renameIDs("4bf92f3577b34da6a3ce929d0e0e4736", 1)+"\"}\n"+
		"2026-11-02T23:58:30.000000001Z stdout P half \n"+
		"2026-11-02T23:58:30.000000002Z stdout F a line\n"+
		"2026-11-02T23:59:00.123456789Z stdout F {\"msg\":\"start\",\"trace_id\":\"4bf92f3577b34da6a3ce929d0e0e4736\"}\n"+
		"2026-11-02T23:59:30.000000001Z stdout P half \n"+
		"2026-11-02T23:59:30.000000002Z stdout F a line\n", pod)
}
