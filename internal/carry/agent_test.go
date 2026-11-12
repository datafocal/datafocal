package carry

import (
	"fmt"
	"testing"

	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discoverLine(t *testing.T, c Carrier, line string) segment.Record {
	t.Helper()
	p, err := logformat.New(logformat.Config{Format: "json"})
	require.NoError(t, err)
	r, ok := p.Parse(line)
	if !ok {
		r, ok = p.Flush()
	}
	require.True(t, ok)
	return c.Carry("session.jsonl", r)
}

func TestCanonicalAgentFields(t *testing.T) {
	for _, tc := range []struct {
		name, line string
		want       map[string]string
	}{
		{"otel inclusive cache", `{"attributes":[{"key":"gen_ai.request.model","value":{"stringValue":"gpt-5.1-codex"}},{"key":"gen_ai.usage.input_tokens","value":{"intValue":"100"}},{"key":"gen_ai.usage.cache_read.input_tokens","value":{"intValue":"20"}},{"key":"gen_ai.usage.cache_creation.input_tokens","value":{"intValue":"10"}}]}`, map[string]string{"model": "gpt-5.1-codex", "input_tokens": "70", "cache_read_tokens": "20", "cache_write_tokens": "10"}},
		{"cache total beats TTL breakdown", `{"message":{"model":"claude-sonnet-4-20250514","usage":{"input_tokens":2,"cache_read_input_tokens":1000,"cache_creation_input_tokens":654,"cache_creation":{"ephemeral_1h_input_tokens":0,"ephemeral_5m_input_tokens":654}}}}`, map[string]string{"input_tokens": "2", "cache_write_tokens": "654"}},
		{"loader", `{"message":{"content":[{"type":"tool_use","name":"Skill","input":{"skill":"review"}}]}}`, map[string]string{"tool_name": `["Skill"]`, "skill_name": "review"}},
		{"activate loader", `{"function":{"name":"activate_skill","arguments":{"name":"review"}}}`, map[string]string{"tool_name": "activate_skill", "skill_name": "review"}},
		{"encoded loader", `{"type":"function_call","name":"load_skill","arguments":"{\"name\":\"review\"}"}`, map[string]string{"skill_name": "review"}},
		{"skill read", `{"message":{"content":[{"type":"tool_use","name":"Read","input":{"file_path":"/skills/review/SKILL.md"}}]}}`, map[string]string{"skill_name": "review"}},
		{"named model wins", `{"model":"gpt-5.1-codex","source":"custom"}`, map[string]string{"model": "gpt-5.1-codex"}},
		{"listing is not skill use", `{"message":{"role":"user","content":"grep /skills/review/SKILL.md"},"path":"/skills/review/SKILL.md"}`, map[string]string{}},
		{"grep is not skill use", `{"tool":{"name":"Grep","input":{"path":"/skills/review/SKILL.md"}}}`, map[string]string{}},
		{"skill tag", `{"message":{"role":"tool","content":"<skill name=\"review\">instructions</skill>"}}`, map[string]string{"skill_name": "review"}},
		{"memory load", `{"type":"attachment","attachment":{"type":"instructions","files":[{"path":"/p/AGENTS.md","type":"Project","content":"rules"},{"path":"/h/.claude/projects/p/memory/MEMORY.md","type":"AutoMem","content":"- [a](a.md) — b界"}]}}`, map[string]string{"memory_op": "load", "memory_name": "MEMORY.md", "memory_chars": "16"}},
		{"memory write", `{"message":{"content":[{"type":"tool_use","name":"Write","input":{"file_path":"/h/.claude/projects/p/memory/a.md","content":"note"}}]}}`, map[string]string{"memory_op": "write", "memory_name": "a.md"}},
		{"memory read", `{"message":{"content":[{"type":"tool_use","name":"Read","input":{"file_path":"/h/.claude/projects/p/memory/a.md"}}]}}`, map[string]string{"memory_op": "read", "memory_name": "a.md"}},
		{"memory tool path", `{"type":"function_call","name":"view","arguments":"{\"path\":\"/memories/notes.md\"}"}`, map[string]string{"memory_op": "read", "memory_name": "notes.md"}},
		{"memory tool view", `{"message":{"content":[{"type":"tool_use","name":"memory","input":{"command":"view","path":"/memories"}}]}}`, map[string]string{"memory_op": "read", "memory_name": "memories"}},
		{"memory tool create", `{"message":{"content":[{"type":"tool_use","name":"memory","input":{"command":"create","path":"/memories/notes.md","file_text":"x"}}]}}`, map[string]string{"memory_op": "write", "memory_name": "notes.md"}},
		{"memories namespace read", `{"type":"response_item","payload":{"type":"function_call","name":"read","namespace":"memories","arguments":"{\"path\":\"MEMORY.md\"}","call_id":"c1"}}`, map[string]string{"memory_op": "read", "memory_name": "MEMORY.md"}},
		{"memories namespace note", `{"type":"response_item","payload":{"type":"function_call","name":"add_ad_hoc_note","namespace":"memories","arguments":"{\"filename\":\"build.md\",\"note\":\"x\"}","call_id":"c2"}}`, map[string]string{"memory_op": "write", "memory_name": "build.md"}},
		{"global context file write", `{"type":"gemini","toolCalls":[{"id":"t","name":"write_file","args":{"file_path":"/home/u/.gemini/GEMINI.md","content":"x"}}]}`, map[string]string{"memory_op": "write", "memory_name": "GEMINI.md"}},
		{"save memory", `{"type":"gemini","toolCalls":[{"id":"t","name":"save_memory","args":{"fact":"x","scope":"global"}}]}`, map[string]string{"memory_op": "write", "memory_name": "save_memory"}},
		{"store memory", `{"type":"tool.execution_start","data":{"toolName":"store_memory","arguments":{"subject":"testing","fact":"x"}}}`, map[string]string{"memory_op": "write", "memory_name": "testing"}},
		{"a repo's context file is not memory", `{"type":"gemini","toolCalls":[{"id":"t","name":"write_file","args":{"file_path":"/src/app/GEMINI.md","content":"x"}}]}`, map[string]string{"memory_op": ""}},
		{"a repo's memory package is not memory", `{"message":{"content":[{"type":"tool_use","name":"Read","input":{"file_path":"/src/internal/memory/cache.go"}}]}}`, map[string]string{"memory_op": ""}},
		{"project instructions are not memory", `{"type":"attachment","attachment":{"type":"instructions","files":[{"path":"/p/AGENTS.md","type":"Project","content":"rules"}]}}`, map[string]string{"memory_op": ""}},
		{"grep is not memory use", `{"tool":{"name":"Grep","input":{"path":"/h/.claude/projects/p/memory"}}}`, map[string]string{"memory_op": ""}},
		{"session identity", `{"type":"session.start","id":"event","data":{"session_id":"session"}}`, map[string]string{"run_id": "session"}},
		{"context roles", `{"messages":[{"role":"user","content":"hello界"},{"role":"assistant","content":"world!"}]}`, map[string]string{"content_user_chars": "6", "content_assistant_chars": "6", "content_chars": "12"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := discoverLine(t, New(nil), tc.line)
			got := map[string]string{}
			for key := range tc.want {
				col, _ := column(r, key)
				got[key] = value.Format(col.Type, col.Value)
			}
			assert.Equal(t, tc.want, got)
			if len(tc.want) == 0 {
				_, held := column(r, "skill_name")
				assert.False(t, held)
			}
		})
	}
}

// compactionColumns are the compaction columns of each line, in order.
func compactionColumns(t *testing.T, lines ...string) []map[string]string {
	t.Helper()
	c := New(nil)
	var got []map[string]string
	for _, line := range lines {
		r := discoverLine(t, c, line)
		cols := map[string]string{}
		for _, name := range []string{"compaction_tokens", "after_compaction"} {
			if col, held := column(r, name); held {
				cols[name] = value.Format(col.Type, col.Value)
			}
		}
		got = append(got, cols)
	}
	return got
}

func TestCompaction(t *testing.T) {
	call := func(run string, input, read int) string {
		return fmt.Sprintf(`{"session_id":%q,"message":{"usage":{"input_tokens":%d,"cache_read_input_tokens":%d}}}`, run, input, read)
	}
	none := map[string]string{}
	after := func(removed string) map[string]string {
		return map[string]string{"compaction_tokens": removed, "after_compaction": "true"}
	}
	for _, tc := range []struct {
		name  string
		lines []string
		want  []map[string]string
	}{
		{"context halving", []string{call("s", 3, 1000), call("s", 2, 1100), call("s", 2, 500)}, []map[string]string{none, none, after("600")}},
		{"a small drop is no compaction", []string{call("s", 3, 1000), call("s", 2, 900)}, []map[string]string{none, none}},
		{"runs apart", []string{call("s", 3, 1000), call("other", 1, 5)}, []map[string]string{none, none}},
		{"output receipt", []string{call("s", 3, 1000), `{"session_id":"s","outputTokens":20}`}, []map[string]string{none, none}},
		{"claude code boundary", []string{
			call("s", 2, 360386),
			`{"type":"system","subtype":"compact_boundary","session\u0049d":"s","compactMetadata":{"trigger":"auto","preTokens":363101,"postTokens":16369,"cumulativeDroppedTokens":346732}}`,
			`{"type":"assistant","session\u0049d":"s","message":{"usage":{"input_tokens":2,"cache_read_input_tokens":20258,"cache_creation_input_tokens":30056}}}`,
			call("s", 2, 50314),
		}, []map[string]string{none, {"compaction_tokens": "346732"}, {"after_compaction": "true"}, none}},
		{"boundary without its size after", []string{
			call("s", 2, 360000),
			`{"type":"system","subtype":"compact_boundary","session\u0049d":"s","compactMetadata":{"trigger":"manual","preTokens":363000}}`,
			call("s", 2, 50000),
		}, []map[string]string{none, none, after("312998")}},
		{"codex rollout", []string{
			`{"type":"session_meta","payload":{"id":"s"}}`,
			`{"type":"event_msg","payload":{"type":"token_count","info":{"input_tokens":90000,"cached_input_tokens":80000}}}`,
			`{"type":"compacted","payload":{"message":"summary"}}`,
			`{"type":"event_msg","payload":{"type":"context_compacted"}}`,
			`{"type":"event_msg","payload":{"type":"token_count","info":{"input_tokens":60000,"cached_input_tokens":1000}}}`,
		}, []map[string]string{none, none, none, none, after("30000")}},
		{"pi entry", []string{
			call("s", 1000, 150000),
			`{"type":"compaction","session_id":"s","summary":"summary","tokensBefore":152000,"usage":{"input":4000,"output":600}}`,
			call("s", 1000, 9000),
		}, []map[string]string{none, none, after("142000")}},
		{"gemini cli telemetry", []string{
			call("s", 120000, 0),
			`{"session.id":"s","event.name":"gemini_cli.chat_compression","tokens_before":121000,"tokens_after":15000}`,
			call("s", 15500, 0),
		}, []map[string]string{none, {"compaction_tokens": "106000"}, {"after_compaction": "true"}}},
		{"copilot cli", []string{
			call("s", 100, 98000),
			`{"type":"session.compaction_complete","session_id":"s","data":{"success":true,"preCompactionTokens":98100,"postCompactionTokens":12000,"compactionTokensUsed":{"inputTokens":98100,"outputTokens":900}}}`,
			call("s", 100, 12000),
		}, []map[string]string{none, {"compaction_tokens": "86100"}, {"after_compaction": "true"}}},
		{"a failed compaction is none", []string{
			call("s", 100, 98000),
			`{"type":"session.compaction_complete","session_id":"s","data":{"success":false,"error":"timeout"}}`,
			call("s", 100, 98500),
		}, []map[string]string{none, none, none}},
		{"openhands condensation", []string{
			call("s", 80000, 0),
			`{"kind":"Condensation","session_id":"s","forgotten_event_ids":["e1","e2"],"summary":"summary"}`,
			call("s", 50000, 0),
		}, []map[string]string{none, none, after("30000")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, compactionColumns(t, tc.lines...))
		})
	}
}

func TestCanonicalRunState(t *testing.T) {
	c := New(nil)
	discoverLine(t, c, `{"session_id":"s","message":{"usage":{"input_tokens":3}}}`)
	output := discoverLine(t, c, `{"session_id":"s","outputTokens":20}`)
	_, duplicated := column(output, "output_tokens")
	assert.False(t, duplicated)
	var previous string
	for _, defs := range []string{`[{"name":"read","description":"a"}]`, `[{"name":"read","description":"b"}]`} {
		other := discoverLine(t, c, `{"tools":`+defs+`}`)
		hash, _ := column(other, "tools_hash")
		assert.Len(t, hash.Value.String, 64)
		assert.NotEqual(t, previous, hash.Value.String)
		previous = hash.Value.String
	}
}

// TestRunsAreAnAgentsOnly: a file's lines become an agent's run, carried onto its later
// lines, once one holds an agent's content; a journal's login session or a build's trace id
// before then is the line's own field.
func TestRunsAreAnAgentsOnly(t *testing.T) {
	runs := func(lines ...string) []string {
		c := New(nil)
		var got []string
		for _, line := range lines {
			run, _ := column(discoverLine(t, c, line), "run_id")
			got = append(got, run.Value.String)
		}
		return got
	}
	for _, tc := range []struct {
		name  string
		lines []string
		want  []string
	}{
		{"journal", []string{`{"SESSION_ID":"c58","MESSAGE":"New session c58 of user dev."}`,
			`{"MESSAGE":"layer cached","traceID":"e998406bf57ed0f9832b3d9319db90d9"}`, `{"MESSAGE":"Started unit"}`}, []string{"", "", ""}},
		{"agent", []string{`{"sessionId":"s1","type":"user","message":{"role":"user","content":"hi"}}`,
			`{"type":"summary"}`}, []string{"s1", "s1"}},
	} {
		assert.Equal(t, tc.want, runs(tc.lines...), tc.name)
	}
}

func TestDiscoveryAllocations(t *testing.T) {
	c := New(nil)
	r := segment.Record{Columns: []segment.Column{
		{Name: "httpMethod", Type: value.TypeString, Value: value.Value{String: "GET"}},
		{Name: "requestPath", Type: value.TypeString, Value: value.Value{String: "/inbox"}},
		{Name: "statusCode", Type: value.TypeInt, Value: value.Value{Int: 200}},
	}}
	assert.Zero(t, testing.AllocsPerRun(100, func() { c.Carry("http.jsonl", r) }), "repeated ordinary fields must not allocate for classification")
	r.Columns[1].Name = "message.content"
	assert.LessOrEqual(t, testing.AllocsPerRun(100, func() { c.Carry("http.jsonl", r) }), 3.0, "AI content counting must not allocate a byte copy")
}
