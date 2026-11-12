package logformat

import (
	"encoding/json"
	"flag"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

var updateGolden = flag.Bool("update", false, "update golden files")

func str(name, s string) segment.Column {
	return segment.Column{Name: name, Type: value.TypeString, Value: value.Value{String: s}}
}

// parseOne is the one record line parses to, which auto holds until it is flushed.
func parseOne(t *testing.T, p Parser, line string) segment.Record {
	t.Helper()
	r, ok := p.Parse(line)
	if !ok {
		r, ok = p.Flush()
	}
	require.True(t, ok)
	return r
}

func TestParse(t *testing.T) {
	for _, tc := range []struct {
		name, format, line string
		want               segment.Record
	}{
		{name: "json keeps types and flattens objects", format: "json",
			line: `{"level":"error","msg":"db timeout","latency_ms":120,"ok":false,"ratio":0.5,"user":{"id":"u1"},"tags":["a",1],"codes":[200,404],"gone":null}`,
			want: segment.Record{Columns: []segment.Column{
				{Name: "codes", Type: value.TypeInt | value.TypeRepeated, Value: value.Value{List: []value.Value{{Int: 200}, {Int: 404}}}},
				{Name: "latency_ms", Type: value.TypeInt, Value: value.Value{Int: 120}},
				str("level", "error"),
				str("message", "db timeout"),
				{Name: "ok", Type: value.TypeBool, Value: value.Value{Bool: false}},
				{Name: "ratio", Type: value.TypeFloat, Value: value.Value{Float: 0.5}},
				{Name: "tags", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: []value.Value{{String: "a"}, {String: "1"}}}},
				str("user.id", "u1"),
			}}},
		{name: "an OTLP span's time is its end and its duration its length", format: "json",
			line: `{"name":"GET /cart","startTimeUnixNano":"1792929600000000000","endTimeUnixNano":"1792929600012500000"}`,
			want: segment.Record{Timestamp: time.Unix(0, 1792929600012500000).UTC(), Columns: []segment.Column{
				str("endTimeUnixNano", "1792929600012500000"),
				str("name", "GET /cart"),
				{Name: "duration_ms", Type: value.TypeFloat, Value: value.Value{Float: 12.5}},
			}}},
		{name: "json list of objects is a list column per key path, aligned by object", format: "json",
			line: `{"steps":["setup",{"name":"build","env":{"go":"1.25"},"args":["-v"]},{"name":"test","ok":true},{}]}`,
			want: segment.Record{Columns: []segment.Column{
				{Name: "steps", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: []value.Value{{String: "setup"}}}},
				{Name: "steps.args", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: []value.Value{{String: `["-v"]`}, {}, {}}}},
				{Name: "steps.env.go", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: []value.Value{{String: "1.25"}, {}, {}}}},
				{Name: "steps.name", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: []value.Value{{String: "build"}, {String: "test"}, {}}}},
				{Name: "steps.ok", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: []value.Value{{}, {String: "true"}, {}}}},
			}}},
		{name: "json timestamp becomes the record's", format: "json", line: `{"ts":"2026-10-18T12:00:00Z","severity":"warn"}`,
			want: segment.Record{Timestamp: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC), Columns: []segment.Column{str("level", "warn")}}},
		{name: "logfmt yields its keys", format: "logfmt", line: "lvl=info msg=\"user logged in\"\tuser=u\\\"1 empty=",
			want: segment.Record{Columns: []segment.Column{str("level", "info"), str("message", "user logged in"), str("user", `u\"1`), str("empty", "")}}},
		{name: "klog header gives level and source", format: "klog", line: "E1018 12:00:00.123456   42 server.go:117] dial failed",
			want: segment.Record{Columns: []segment.Column{str("level", "error"), str("source", "server.go:117"), str("message", "dial failed")}}},
		{name: "json normalizes info", format: "json", line: `{"level":"INFO"}`, want: segment.Record{Columns: []segment.Column{str("level", "info")}}},
		{name: "json normalizes severity", format: "json", line: `{"severity":"WARNING"}`, want: segment.Record{Columns: []segment.Column{str("level", "warn")}}},
		{name: "json keeps numeric levels", format: "json", line: `{"level":30}`, want: segment.Record{Columns: []segment.Column{str("level", "30")}}},
		{name: "logfmt types numbers", format: "logfmt", line: "took=12 duration_ms=3.5 lvl=5 v=1.2.3",
			want: segment.Record{Columns: []segment.Column{{Name: "took", Type: value.TypeInt, Value: value.Value{Int: 12}},
				{Name: "duration_ms", Type: value.TypeFloat, Value: value.Value{Float: 3.5}}, str("level", "5"), str("v", "1.2.3")}}},
		{name: "logfmt normalizes aliases", format: "logfmt", line: "lvl=[ERR:]", want: segment.Record{Columns: []segment.Column{str("level", "error")}}},
		{name: "klog warning", format: "klog", line: "W1020 12:00:00.123456 1 f.go:1] x", want: segment.Record{Columns: []segment.Column{str("level", "warn"), str("source", "f.go:1"), str("message", "x")}}},
		{name: "plain is the message", format: "plain", line: `{"msg":"x"}`, want: segment.LogRecord(time.Time{}, "", `{"msg":"x"}`, nil)},
		{name: "a cAdvisor sample names its container and keeps its totals, not its per-cpu and per-device detail", format: "cadvisor",
			line: "cName=/system.slice/sshd.service host=localhost:8086 cpu_usage_total=1200 cpu_usage_per_cpu.0=600 timestamp=1793196001123456789 memory_working_set=4096 /dev/sda1.fs_usage=7 fs_summary.fs_usage=7",
			want: segment.Record{Timestamp: time.Unix(0, 1793196001123456789).UTC(), Columns: []segment.Column{
				str("container.id", "/system.slice/sshd.service"),
				{Name: "cpu_usage_total", Type: value.TypeInt, Value: value.Value{Int: 1200}},
				{Name: "memory_working_set", Type: value.TypeInt, Value: value.Value{Int: 4096}},
				{Name: "fs_summary.fs_usage", Type: value.TypeInt, Value: value.Value{Int: 7}},
			}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(Config{Format: tc.format})
			require.NoError(t, err)
			got, ok := p.Parse(tc.line)
			assert.True(t, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestLineThatDoesNotParseIsKeptWhole(t *testing.T) {
	for _, tc := range []struct{ format, envelope, line string }{
		{"json", "", `{"msg": "cut`},
		{"json", "", `{"msg":"a"} trailing`},
		{"logfmt", "", `msg="unterminated`},
		{"klog", "", "not a klog line"},
		{"plain", "cri", "not a cri line"},
	} {
		p, err := New(Config{Format: tc.format, Envelope: tc.envelope})
		require.NoError(t, err)
		got, ok := p.Parse(tc.line)
		assert.True(t, ok)
		assert.Equal(t, segment.LogRecord(time.Time{}, "", tc.line, nil), got, tc.line)
	}
}

func TestEnvelopeIsStripped(t *testing.T) {
	want := segment.Record{Timestamp: time.Date(2026, 10, 18, 12, 0, 0, 123456789, time.UTC), Columns: []segment.Column{str("level", "error"), str("message", "down")}}
	for _, tc := range []struct {
		name, envelope string
		lines          []string
	}{
		{name: "cri gives its timestamp", envelope: "cri", lines: []string{"2026-10-18T12:00:00.123456789Z stdout F level=error msg=down"}},
		{name: "cri joins partial lines", envelope: "cri", lines: []string{"2026-10-18T11:00:00Z stdout P level=error ", "2026-10-18T12:00:00.123456789Z stdout F msg=down"}},
		{name: "docker log is the body without its newline", envelope: "docker", lines: []string{`{"log":"level=error msg=down\n","stream":"stderr","time":"2026-10-18T12:00:00.123456789Z"}`}},
		{name: "docker joins a split log", envelope: "docker", lines: []string{`{"log":"level=error ","time":"2026-10-18T11:00:00Z"}`, `{"log":"msg=down\n","time":"2026-10-18T12:00:00.123456789Z"}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(Config{Format: "logfmt", Envelope: tc.envelope})
			require.NoError(t, err)
			last := len(tc.lines) - 1
			for _, line := range tc.lines[:last] {
				_, ok := p.Parse(line)
				assert.False(t, ok, "a partial line is held")
			}
			got, ok := p.Parse(tc.lines[last])
			assert.True(t, ok)
			assert.Equal(t, want, got)
		})
	}
}

func TestAutoParsesEachLine(t *testing.T) {
	p, err := New(Config{Format: "auto"})
	require.NoError(t, err)
	for _, tc := range []struct {
		line string
		want segment.Record
	}{
		{"starting up", segment.LogRecord(time.Time{}, "", "starting up", nil)},
		{"E1018 12:00:00.123456   42 server.go:117] dial failed", segment.Record{Columns: []segment.Column{str("level", "error"), str("source", "server.go:117"), str("message", "dial failed")}}},
		{`{"msg":"a"}`, segment.Record{Columns: []segment.Column{str("message", "a")}}},
		{"level=info msg=b", segment.Record{Columns: []segment.Column{str("level", "info"), str("message", "b")}}},
		{`{"msg":"d"}`, segment.Record{Columns: []segment.Column{str("message", "d")}}},
	} {
		assert.Equal(t, tc.want, parseOne(t, p, tc.line), tc.line)
	}
}

func TestUnknownFormat(t *testing.T) {
	_, err := New(Config{Format: "xml"})
	assert.Error(t, err)
	_, err = New(Config{Format: "plain", Envelope: "syslog"})
	assert.Error(t, err)
	_, err = New(Config{Format: "plain", RecordStart: "(unclosed"})
	assert.Error(t, err)
}

// A stack trace is one record headed by its first line; a line before any start line is
// its own.
func TestRecordStartAssemblesMultiLineRecords(t *testing.T) {
	p, err := New(Config{Format: "plain", Envelope: "cri", RecordStart: `^\d{4}-\d{2}-\d{2} `})
	require.NoError(t, err)
	ts := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	trace := func(message string, detail ...string) segment.Record {
		r := segment.LogRecord(ts, "", message, nil)
		if len(detail) > 0 {
			r.Columns = append(r.Columns, str("detail", strings.Join(detail, "\n")))
		}
		return r
	}
	var got []segment.Record
	for _, body := range []string{
		"F orphan line",
		"F 2026-10-18 12:00:00 ERROR request failed",
		"F java.lang.IllegalStateException: closed",
		"F \tat com.shop.Cart.add(Cart.java:42)",
		"P \tat com.shop.Api",
		"F .handle(Api.java:7)",
		"P 2026-10-18 12:00:01 ",
		"F INFO retried",
	} {
		if r, ok := p.Parse("2026-10-18T12:00:00Z stdout " + body); ok {
			got = append(got, r)
		}
	}
	assert.Equal(t, 2, p.Held(), "the open record's two lines")
	r, ok := p.Flush()
	require.True(t, ok)
	got = append(got, r)
	assert.Equal(t, []segment.Record{
		trace("orphan line"),
		trace("2026-10-18 12:00:00 ERROR request failed", "java.lang.IllegalStateException: closed", "\tat com.shop.Cart.add(Cart.java:42)", "\tat com.shop.Api.handle(Api.java:7)"),
		trace("2026-10-18 12:00:01 INFO retried"),
	}, got)
	assert.Zero(t, p.Held())
	_, ok = p.Flush()
	assert.False(t, ok, "nothing is open")
}

func TestRecordIsClosedAtItsLineLimit(t *testing.T) {
	p, err := New(Config{Format: "plain", RecordStart: "^start"})
	require.NoError(t, err)
	_, ok := p.Parse("start")
	assert.False(t, ok)
	for range maxRecordLines - 2 {
		_, ok = p.Parse("more")
		assert.False(t, ok)
	}
	r, ok := p.Parse("last")
	assert.True(t, ok)
	want := segment.LogRecord(time.Time{}, "", "start", nil)
	want.Columns = append(want.Columns, str("detail", strings.Repeat("more\n", maxRecordLines-2)+"last"))
	assert.Equal(t, want, r)
	assert.Zero(t, p.Held())
}

// An assistant message's content blocks, from a Claude Code transcript.
const transcriptLine = `{"parentUuid":"9b1c","isSidechain":false,"type":"assistant","uuid":"c2f4","timestamp":"2026-10-18T12:00:00.000Z",` +
	`"message":{"id":"msg_01","type":"message","role":"assistant","model":"claude-opus-5-5","content":[` +
	`{"type":"text","text":"Fixing the typo."},` +
	`{"type":"tool_use","id":"toolu_01","name":"Edit","input":{"file_path":"/workspace/main.go","old_string":"fmt.Prinln","new_string":"fmt.Println"}}],` +
	`"stop_reason":"tool_use","usage":{"input_tokens":12,"output_tokens":34}}}`

func TestTranscriptContentBlocksAreListColumns(t *testing.T) {
	p, err := New(Config{Format: "json"})
	require.NoError(t, err)
	r, ok := p.Parse(transcriptLine)
	require.True(t, ok)
	cols := map[string]value.Value{}
	for _, col := range r.Columns {
		cols[col.Name] = col.Value
	}
	strs := func(ss ...string) value.Value {
		var v value.Value
		for _, s := range ss {
			v.List = append(v.List, value.Value{String: s})
		}
		return v
	}
	assert.Equal(t, strs("text", "tool_use"), cols["message.content.type"])
	assert.Equal(t, strs("", "Edit"), cols["message.content.name"], "element i is block i's")
	assert.Equal(t, strs("Fixing the typo.", ""), cols["message.content.text"])
	assert.Equal(t, strs("", "/workspace/main.go"), cols["message.content.input.file_path"])
	assert.NotContains(t, cols, "message.content", "no element is a scalar")

	var text strings.Builder
	for _, col := range r.Columns {
		text.WriteString(value.Format(col.Type, col.Value) + "\n")
	}
	for _, leaf := range []string{"9b1c", "false", "assistant", "c2f4", "msg_01", "message", "claude-opus-5-5", "text", "Fixing the typo.",
		"tool_use", "toolu_01", "Edit", "/workspace/main.go", "fmt.Prinln", "fmt.Println", "12", "34"} {
		assert.Contains(t, text.String(), leaf, "every leaf value is kept")
	}

	again, _ := p.Parse(transcriptLine)
	assert.Equal(t, r, again, "the same line gives the same columns in the same order")
}

// TestJSONTreeIsEncodingJSONs holds the fastjson path to encoding/json's: a line is an
// object for one exactly when it is for the other, and decodes to the same tree.
func TestJSONTreeIsEncodingJSONs(t *testing.T) {
	lines := []string{
		`{"level":"info","msg":"ok","n":120,"f":120.5,"big":123456789012345678901234567890,"e":1e400}`,
		`{"a":1,"a":2,"b":{"a":3,"a":{"x":true}}}`,
		`{"list":[1,"two",{"k":null},[3,4],false,null],"empty":[],"obj":{}}`,
		`{"esc":"tab\tquote\"slash\/ \u00e9 \ud83d\ude00 lone\ud800 x","k\u00e9y":"v","raw":"` + "\u00e9\u2603" + `"}`,
		"{\"bad\":\"a\xffb\xc0\xafc\",\"k\xfe\":1}",
		`{"pair":"\ud83d\ude00","lone":"\ud800 x","upper":"\uDC00","literal":"\\ud800","plain":"\u00e9"}`,
		`{"html":"<a href='x'>&amp;</a>"}`,
		`  {"padded": true}  `,
		`{}`, `null`, `[]`, `[{"a":1}]`, `"str"`, `42`, `true`,
		`{"a":1}{"b":2}`, `{"a":1} x`, `{"a":01}`, `{"a":1.}`, `{"a":.5}`, `{"a":-}`, `{"a":1e}`,
		`{"a":tru}`, `{"a":"unterminated}`, `{"a":"ctl` + "\x01" + `"}`, `{"a":1,}`, `{a:1}`, `{"a" 1}`, ``, `{`,
		`{"deep":` + strings.Repeat(`[`, 50) + strings.Repeat(`]`, 50) + `}`,
	}
	for _, line := range lines {
		want, wantOK := encodingJSONTree(line)
		got, gotOK := decodeJSON(line)
		assert.Equalf(t, wantOK, gotOK, "is %q a JSON object", line)
		assert.Equalf(t, want, got, "tree of %q", line)
	}
}

func encodingJSONTree(line string) (map[string]any, bool) {
	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber()
	var fields map[string]any
	if err := dec.Decode(&fields); err != nil || fields == nil {
		return nil, false
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, false
	}
	return fields, true
}

func TestAutoAllocationParity(t *testing.T) {
	for _, tc := range []struct{ format, line string }{
		{"json", `{"level":"INFO","msg":"request","n":42}`},
		{"klog", "W1020 12:00:00.123456 1 f.go:1] x"},
		{"logfmt", "level=INFO msg=request n=42"},
		{"syslog", "Oct 20 12:00:00 web1 sshd[99]: Accepted publickey for deploy"},
		{"journal", `{"__CURSOR":"s=6bf3;i=1","__REALTIME_TIMESTAMP":"1793196001123456","PRIORITY":"6","MESSAGE":"Accepted"}`},
	} {
		t.Run(tc.format, func(t *testing.T) {
			explicit, err := New(Config{Format: tc.format})
			require.NoError(t, err)
			auto, err := New(Config{Format: "auto"})
			require.NoError(t, err)
			// Race builds randomly discard pooled JSON parsers and regexp state, so
			// compare steady-state allocations rather than random pool misses.
			want := testing.AllocsPerRun(1, func() { explicit.Parse(tc.line) })
			got := testing.AllocsPerRun(1, func() { auto.Parse(tc.line) })
			for range 100 {
				want = min(want, testing.AllocsPerRun(1, func() { explicit.Parse(tc.line) }))
				got = min(got, testing.AllocsPerRun(1, func() { auto.Parse(tc.line) }))
			}
			assert.Equal(t, want, got)
		})
	}
}

func TestAutoText(t *testing.T) {
	ts := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		line string
		want segment.Record
	}{
		{"2026-10-20T12:00:00Z\tinfo\trequest gateway=g http.status=401", segment.Record{Timestamp: ts, Columns: []segment.Column{str("level", "info"), str("message", "info request"), str("gateway", "g"), {Name: "http.status", Type: value.TypeInt, Value: value.Value{Int: 401}}}}},
		{"2026/10/20 12:00:00 INFO request method=GET request_id=r1", segment.Record{Timestamp: ts, Columns: []segment.Column{str("level", "info"), str("message", "INFO request"), str("method", "GET"), str("request_id", "r1")}}},
		{"2026-10-20 12:00:00.123 UTC [57] LOG:  database system is ready", segment.LogRecord(ts.Add(123*time.Millisecond), "info", "[57] LOG: database system is ready", nil)},
		{`ERROR:  relation "x" does not exist`, segment.LogRecord(time.Time{}, "error", `ERROR: relation "x" does not exist`, nil)},
		{"INFO: Formatting 1st pool", segment.LogRecord(time.Time{}, "info", "INFO: Formatting 1st pool", nil)},
		{"error handling done", segment.LogRecord(time.Time{}, "", "error handling done", nil)},
		{`INFO request note="two  words" time=later`, segment.Record{Columns: []segment.Column{str("level", "info"), str("message", "INFO request"), str("note", "two  words"), str("time", "later")}}},
		{"starting up", segment.LogRecord(time.Time{}, "", "starting up", nil)},
	} {
		t.Run(tc.line, func(t *testing.T) {
			p, err := New(Config{Format: "auto"})
			require.NoError(t, err)
			assert.Equal(t, tc.want, parseOne(t, p, tc.line))
		})
	}
}

func TestParseTime(t *testing.T) {
	for _, input := range []string{
		"2026-10-20T12:00:00.123Z", "2026-10-20 12:00:00.123",
		"2026/10/20 12:00:00.123 UTC", "2026/10/20 14:00:00.123+02:00",
		"2026-10-20 10:00:00.123-02:00", "2026/10/20 12:00:00.123Z",
	} {
		got, _, ok := parseTime(input)
		assert.True(t, ok, input)
		assert.True(t, time.Date(2026, 10, 20, 12, 0, 0, 123000000, time.UTC).Equal(got), input)
	}
}

// TestCommonDefaults reads the default output of Java, Python, Node and Rust loggers, with
// no record_start declared.
func TestCommonDefaults(t *testing.T) {
	epoch := time.Date(2026, 10, 28, 14, 4, 47, 336000000, time.UTC)
	detailed := func(r segment.Record, lines ...string) segment.Record {
		r.Columns = append(r.Columns, str("detail", strings.Join(lines, "\n")))
		return r
	}
	for _, tc := range []struct {
		name  string
		lines []string
		want  []segment.Record
	}{
		{name: "java frames join the exception before them",
			lines: []string{
				"2026-10-28T09:52:45.987-04:00 ERROR 1183712 --- [io-28417-exec-9] o.a.c.c.C.[.[.[/].[dispatcherServlet]    : Servlet.service() threw exception",
				"java.lang.IllegalArgumentException: Owner not found with id: 999.",
				"\tat org.springframework.samples.petclinic.owner.OwnerController.lambda$findOwner$0(OwnerController.java:68) ~[!/:4.0.0-SNAPSHOT]",
				"Caused by: java.lang.IllegalStateException: closed",
				"\t... 52 more",
			},
			want: []segment.Record{
				segment.LogRecord(time.Date(2026, 10, 28, 13, 52, 45, 987000000, time.UTC), "error",
					"ERROR 1183712 --- [io-28417-exec-9] o.a.c.c.C.[.[.[/].[dispatcherServlet] : Servlet.service() threw exception", nil),
				detailed(segment.LogRecord(time.Time{}, "", "java.lang.IllegalArgumentException: Owner not found with id: 999.", nil),
					"\tat org.springframework.samples.petclinic.owner.OwnerController.lambda$findOwner$0(OwnerController.java:68) ~[!/:4.0.0-SNAPSHOT]",
					"Caused by: java.lang.IllegalStateException: closed", "\t... 52 more"),
			}},
		{name: "python traceback joins its log line, which basicConfig glues to the logger",
			lines: []string{"ERROR:app.utils:send failed", "Traceback (most recent call last):", `  File "/app/utils.py", line 3, in send`, `    raise ValueError("boom")`, "ValueError: boom"},
			want: []segment.Record{
				detailed(segment.LogRecord(time.Time{}, "error", "ERROR:app.utils:send failed", nil),
					"Traceback (most recent call last):", `  File "/app/utils.py", line 3, in send`, `    raise ValueError("boom")`),
				segment.LogRecord(time.Time{}, "", "ValueError: boom", nil),
			}},
		{name: "env_logger's bracketed header and anyhow's causes",
			lines: []string{"[2026-10-28T12:00:02Z ERROR myapp::db] query failed", "", "Caused by:", "    connection refused"},
			want: []segment.Record{
				detailed(segment.LogRecord(time.Date(2026, 10, 28, 12, 0, 2, 0, time.UTC), "error", "ERROR myapp::db] query failed", nil),
					"", "Caused by:", "    connection refused"),
			}},
		{name: "pino's epoch milliseconds",
			lines: []string{`{"level":30,"time":1793196287336,"pid":1221120,"hostname":"fedora","msg":"Server listening"}`},
			want: []segment.Record{{Timestamp: epoch, Columns: []segment.Column{str("hostname", "fedora"), str("level", "30"), str("message", "Server listening"),
				{Name: "pid", Type: value.TypeInt, Value: value.Value{Int: 1221120}}}}}},
		{name: "zap's epoch seconds",
			lines: []string{`{"level":"info","ts":1793196287.336,"msg":"started"}`},
			want:  []segment.Record{{Timestamp: epoch.Truncate(time.Millisecond), Columns: []segment.Column{str("level", "info"), str("message", "started")}}}},
		{name: "a zoned time wins over a later zone-less one",
			lines: []string{`{"message":"sent","timestamp":"2026-10-28T13:56:07.123+00:00","ts":"2026-10-28 09:56:07,123"}`},
			want:  []segment.Record{{Timestamp: time.Date(2026, 10, 28, 13, 56, 7, 123000000, time.UTC), Columns: []segment.Column{str("message", "sent")}}}},
		{name: "ecs time and level",
			lines: []string{`{"@timestamp":"2026-10-28T13:54:02.487181209Z","log":{"level":"INFO","logger":"org.springframework.boot.StartupInfoLogger"},"message":"Starting PetClinicApplication"}`},
			want: []segment.Record{{Timestamp: time.Date(2026, 10, 28, 13, 54, 2, 487181209, time.UTC),
				Columns: []segment.Column{str("level", "info"), str("log.logger", "org.springframework.boot.StartupInfoLogger"), str("message", "Starting PetClinicApplication")}}}},
		{name: "pm2's time prefix before json",
			lines: []string{`2026-10-28T09:56:07: {"level":"info","message":"request done","request_id":"ab","duration_ms":1.5,"timestamp":"2026-10-28T13:56:07.123Z"}`},
			want: []segment.Record{{Timestamp: time.Date(2026, 10, 28, 13, 56, 7, 123000000, time.UTC), Columns: []segment.Column{
				{Name: "duration_ms", Type: value.TypeFloat, Value: value.Value{Float: 1.5}}, str("level", "info"), str("message", "request done"), str("request_id", "ab")}}}},
		{name: "winston's npm levels",
			lines: []string{`{"level":"http","message":"GET /"}`, `{"level":"verbose"}`, `{"level":"silly"}`},
			want: []segment.Record{{Columns: []segment.Column{str("level", "debug"), str("message", "GET /")}},
				{Columns: []segment.Column{str("level", "debug")}}, {Columns: []segment.Column{str("level", "debug")}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(Config{Format: "auto"})
			require.NoError(t, err)
			var got []segment.Record
			for _, line := range tc.lines {
				if r, ok := p.Parse(line); ok {
					got = append(got, r)
				}
			}
			r, ok := p.Flush()
			require.True(t, ok)
			got = append(got, r)
			for i := range got {
				got[i].Timestamp = got[i].Timestamp.UTC() // the instant, not the zone it was written in
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestSyslog(t *testing.T) {
	defer func(clock func() time.Time) { now = clock }(now)
	now = func() time.Time { return time.Date(2026, 10, 28, 12, 0, 0, 0, time.Local) }
	pid := func(n int64) segment.Column {
		return segment.Column{Name: "pid", Type: value.TypeInt, Value: value.Value{Int: n}}
	}
	for _, tc := range []struct {
		line string
		want segment.Record
	}{
		{"<34>Oct 11 22:14:15 router1 bgpd[812]: neighbor down", segment.Record{Timestamp: time.Date(2026, 10, 11, 22, 14, 15, 0, time.Local),
			Columns: []segment.Column{str("message", "neighbor down"), str("level", "fatal"), str("host", "router1"), str("service", "bgpd"), pid(812)}}},
		{`<165>1 2026-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application"] An application event`,
			segment.Record{Timestamp: time.Date(2026, 10, 11, 22, 14, 15, 3000000, time.UTC), Columns: []segment.Column{str("message", "An application event"), str("level", "info"),
				str("host", "mymachine.example.com"), str("service", "evntslog"), str("msgid", "ID47"), str("exampleSDID@32473.iut", "3"), str("exampleSDID@32473.eventSource", "Application")}}},
		{"2026-10-28T10:00:01.123456-04:00 web1 sshd[99]: Accepted publickey for deploy", segment.Record{Timestamp: time.Date(2026, 10, 28, 14, 0, 1, 123456000, time.UTC),
			Columns: []segment.Column{str("message", "Accepted publickey for deploy"), str("host", "web1"), str("service", "sshd"), pid(99)}}},
		{"Oct 28 10:00:01 web1 CRON[123]: (root) CMD (run-parts)", segment.Record{Timestamp: time.Date(2026, 10, 28, 10, 0, 1, 0, time.Local),
			Columns: []segment.Column{str("message", "(root) CMD (run-parts)"), str("host", "web1"), str("service", "CRON"), pid(123)}}},
		{`<14>Oct 28 10:00:01 web1 myapp[5]: {"level":"error","msg":"charge failed","order_id":"o1"}`, segment.Record{Timestamp: time.Date(2026, 10, 28, 10, 0, 1, 0, time.Local),
			Columns: []segment.Column{str("level", "error"), str("message", "charge failed"), str("order_id", "o1"), str("host", "web1"), str("service", "myapp"), pid(5)}}},
	} {
		p, err := New(Config{Format: "auto"})
		require.NoError(t, err)
		got := parseOne(t, p, tc.line)
		assert.True(t, tc.want.Timestamp.Equal(got.Timestamp), tc.line)
		tc.want.Timestamp = got.Timestamp
		assert.Equal(t, tc.want, got, tc.line)
	}
	now = func() time.Time { return time.Date(2027, 1, 1, 0, 0, 30, 0, time.Local) }
	p, err := New(Config{Format: "syslog"})
	require.NoError(t, err)
	r, _ := p.Parse("Dec 31 23:59:59 h app: last of the year")
	assert.Equal(t, time.Date(2026, 12, 31, 23, 59, 59, 0, time.Local), r.Timestamp)
}

// TestJournal parses entries journalctl --output=json wrote, compared with the golden records.
func TestJournal(t *testing.T) {
	data, err := os.ReadFile("testdata/journal.jsonl")
	require.NoError(t, err)
	p, err := New(Config{Format: "auto"})
	require.NoError(t, err)
	var got []byte
	for line := range strings.Lines(string(data)) {
		record, err := json.Marshal(parseOne(t, p, strings.TrimSuffix(line, "\n")))
		require.NoError(t, err)
		got = append(append(got, record...), '\n')
	}
	if *updateGolden {
		require.NoError(t, os.WriteFile("testdata/journal.golden.jsonl", got, 0o644))
	}
	want, err := os.ReadFile("testdata/journal.golden.jsonl")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got))
}

func TestParseAllSplitsRecordBatches(t *testing.T) {
	batch := `{"service":"agent","records":[{"time":"2026-10-01T00:00:00Z","body":"a"},{"time":"2026-10-01T00:00:01Z","body":"b"}]}`
	for _, tc := range []struct {
		name, envelope string
		lines          []string
		rows           int
	}{
		{name: "managed fields", lines: []string{`{"metadata":{"managedFields":[{"time":"2026-10-01T00:00:00Z","manager":"a"},{"time":"2026-10-01T00:00:01Z","manager":"b"}]},"kind":"Pod"}`}, rows: 1},
		{name: "content", lines: []string{`{"timestamp":"2026-10-01T00:00:00Z","message":{"content":[{"type":"text","text":"hello"}]}}`}, rows: 1},
		{name: "timestamped message", lines: []string{`{"timestamp":"2026-10-01T00:00:00Z","message":"one event","messages":[{"time":"2026-10-01T00:00:00Z","body":"nested"}]}`}, rows: 1},
		{name: "batch", lines: []string{batch}, rows: 2},
		{name: "batch a runtime split", envelope: "cri", lines: []string{"2026-10-01T00:00:02Z stdout P " + batch[:40], "2026-10-01T00:00:02Z stdout F " + batch[40:]}, rows: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(Config{Format: "json", Envelope: tc.envelope})
			require.NoError(t, err)
			var rows []segment.Record
			for _, line := range tc.lines {
				rows = append(rows, p.ParseAll(line)...)
			}
			assert.Len(t, rows, tc.rows)
			if tc.rows > 1 {
				for _, row := range rows {
					assert.Contains(t, row.Columns, str("service", "agent"), "a batch's own fields ride on each record")
				}
			}
		})
	}
}
