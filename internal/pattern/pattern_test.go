package pattern

import (
	"math/rand"
	"testing"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
	"github.com/stretchr/testify/assert"
)

func str(name, s string) segment.Column {
	return segment.Column{Name: name, Type: value.TypeString, Value: value.Value{String: s}}
}

// Each case derives from a Pattern that has already seen 10,000 random lines, so a
// matching id shows that nothing it saw moved the id.
func TestDerive(t *testing.T) {
	tests := []struct {
		name        string
		columns     []segment.Column
		wantPattern string
		wantColumns []segment.Column
	}{
		{name: "message ignores other fields",
			columns:     []segment.Column{str("level", "info"), str("message", "user alice logged in")},
			wantPattern: "user alice logged in"},
		{name: "free text gets a masked copy",
			columns:     []segment.Column{str("message", "request failed"), str("error", "timeout after 30s"), str("host", "db-1")},
			wantPattern: "request failed", wantColumns: []segment.Column{str("error_pattern", "timeout after <*>")}},
		{name: "only the first duration is named and numbering does not shift",
			columns:     []segment.Column{str("message", "took 12.5 ms then 20 ms")},
			wantPattern: "took <*> ms then <*> ms", wantColumns: []segment.Column{
				{Name: "duration_ms", Type: value.TypeFloat, Value: value.Value{Float: 12.5}},
				{Name: "param_2", Type: value.TypeInt, Value: value.Value{Int: 20}},
			}},
		{name: "an unparseable duration keeps its name",
			columns:     []segment.Column{str("message", "took abc1 ms")},
			wantPattern: "took <*> ms", wantColumns: []segment.Column{str("duration_ms", "abc1")}},
	}
	words := []string{"user", "alice", "logged", "in", "db-1", "30s", "request", "failed"}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := New(10_000)
			r := rand.New(rand.NewSource(1))
			for range 10_000 {
				p.Derive([]segment.Column{str("message", words[r.Intn(len(words))]+" "+words[r.Intn(len(words))])})
			}
			got := p.Derive(tt.columns)
			assert.Equal(t, ID(tt.wantPattern), got.PatternID)
			assert.Equal(t, tt.wantColumns, got.Columns)
		})
	}
}

func TestStructure(t *testing.T) {
	tests := []struct {
		name      string
		columns   []segment.Column
		key, text string
	}{
		{"name order", []segment.Column{str("a.b", "enum"), str("a", "enum")}, "{a=enum a.b=enum}", "enum"},
		{"empty message", []segment.Column{str("message", ""), str("kind", "assistant")}, "{kind=assistant message}", "assistant"},
		{"non-string message", []segment.Column{{Name: "message", Type: value.TypeInt, Value: value.Value{Int: 12}}}, "{message}", "message"},
		{"identity omitted", []segment.Column{str("host", "a"), str("pod", "b"), str("namespace", "c"), str("container", "d"), str("service", "e"), str("level", "info"), str("ts", "now"), str("kind", "tool_use")}, "{kind=tool_use}", "tool_use"},
		{"variable values", []segment.Column{str("id", "abc123"), str("path", "/tmp/a"), str("time", "2026-10-24"), str("text", "free text"), str("long", "abcdefghijklmnopqrstuvwxyzabcdefg")}, "{id long path text time}", "id long path text time"},
		{"enums", []segment.Column{str("kind", "ai-title"), {Name: "ok", Type: value.TypeBool, Value: value.Value{Bool: false}}, str("tool", "Read.File")}, "{kind=ai-title ok=false tool=Read.File}", "ai-title · Read.File"},
		{"list", []segment.Column{{Name: "types", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: []value.Value{{String: "text"}, {String: "id123"}, {String: "tool_use"}}}}}, "{types=[text,tool_use]}", "text,tool_use"},
		{"non-enum list", []segment.Column{{Name: "types", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: []value.Value{{String: "/tmp/a"}}}}}, "{types}", "types"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := New(100).Derive(tt.columns)
			var copies []segment.Column
			if tt.name == "variable values" {
				copies = []segment.Column{str("text_pattern", "free text")}
			}
			assert.Equal(t, segment.ParsedPattern{Pattern: tt.text, PatternID: ID(tt.key), Columns: copies}, got)
		})
	}
}

// TestStructureName: once a service's records have shown which columns vary, a record is
// named by its kind columns and its rarest varying value, never by a constant.
func TestStructureName(t *testing.T) {
	record := func(kind, content, tool string) []segment.Column {
		cols := []segment.Column{str("service", "agent"), str("type", kind), str("message.content.type", content),
			str("entrypoint", "cli"), str("session", "s-1f2e")}
		if tool != "" {
			cols = append(cols, str("message.content.name", tool))
		}
		return cols
	}
	p := New(100)
	for range 3 {
		p.Derive(record("user", "text", ""))
		p.Derive(record("assistant", "text", ""))
		p.Derive(record("assistant", "tool_use", "Read"))
	}
	got := p.Derive(record("assistant", "tool_use", "Bash"))
	key := "{entrypoint=cli message.content.name=Bash message.content.type=tool_use session type=assistant}"
	assert.Equal(t, segment.ParsedPattern{Pattern: "assistant · tool_use Bash", PatternID: ID(key)}, got)
}

func TestGuardPerService(t *testing.T) {
	p := New(2)
	for i, name := range []string{"a", "b", "c", "d", "e"} {
		got := p.Derive([]segment.Column{str("service", "noisy"), str(name, "enum")})
		want := "{" + name + "=enum}"
		if i >= 2 {
			want = "{" + name + "}"
		}
		if i == 4 {
			want = catchAll
		}
		assert.Equal(t, ID(want), got.PatternID)
		quiet := p.Derive([]segment.Column{str("service", "quiet"), str("kind", "assistant")})
		assert.Equal(t, ID("{kind=assistant}"), quiet.PatternID)
	}
	assert.Equal(t, segment.PatternProfile{Name: "auto", Version: 1, Components: []string{"extract(message)", "structure"}, Degradation: LevelCatchAll}, p.Footer())
}

func TestStructureIdentity(t *testing.T) {
	for _, tt := range []struct {
		name        string
		left, right []segment.Column
		same        bool
	}{
		{"host", []segment.Column{str("host", "a"), str("kind", "assistant")}, []segment.Column{str("kind", "assistant"), str("host", "b")}, true},
		{"pod", []segment.Column{str("pod", "a"), str("kind", "assistant")}, []segment.Column{str("pod", "b"), str("kind", "assistant")}, true},
		{"non-enum", []segment.Column{str("id", "abc123"), str("path", "/a")}, []segment.Column{str("path", "/b"), str("id", "def456")}, true},
		{"enum", []segment.Column{str("kind", "assistant")}, []segment.Column{str("kind", "tool_use")}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := New(100)
			assert.Equal(t, tt.same, p.Derive(tt.left).PatternID == p.Derive(tt.right).PatternID)
		})
	}
}
