package carry

import (
	"maps"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

type line struct {
	path string
	cols map[string]string
}

func record(cols map[string]string) segment.Record {
	var r segment.Record
	for _, name := range slices.Sorted(maps.Keys(cols)) {
		r.Columns = append(r.Columns, segment.Column{Name: name, Type: value.TypeString, Value: value.Value{String: cols[name]}})
	}
	return r
}

func TestCarry(t *testing.T) {
	model := []Rule{{Column: "model", By: "turn_id"}}
	a := func(cols map[string]string) line { return line{"a.log", cols} }
	b := func(cols map[string]string) line { return line{"b.log", cols} }
	type m = map[string]string
	for _, tc := range []struct {
		name   string
		rules  []Rule
		column string
		lines  []line
		want   []string
	}{
		{"a turn's later lines take its model", model, "model",
			[]line{a(m{"turn_id": "a", "model": "m"}), a(m{"turn_id": "a", "tokens": "5"}), a(m{"turn_id": "b", "tokens": "3"}),
				a(m{"turn_id": "b", "model": "n"}), a(m{"turn_id": "b", "tokens": "2"})},
			[]string{"m", "m", "", "n", "n"}},
		{"a result takes the name of the call whose id it holds", []Rule{{Column: "name", By: "call", From: "id"}}, "name",
			[]line{a(m{"id": "x", "name": "Bash"}), a(m{"call": "x", "is_error": "true"}), a(m{"call": "y"})},
			[]string{"Bash", "Bash", ""}},
		{"files do not share what they remember", model, "model",
			[]line{a(m{"turn_id": "a", "model": "m"}), b(m{"turn_id": "a"}), b(m{"turn_id": "a", "model": "n"}), a(m{"turn_id": "a"})},
			[]string{"m", "", "n", "m"}},
		{"a line keeps its own value", model, "model",
			[]line{a(m{"turn_id": "a", "model": "m"}), a(m{"turn_id": "a", "model": "z"})},
			[]string{"m", "z"}},
		{"nil carries the defaults", nil, "payload.model",
			[]line{a(m{"payload.turn_id": "a", "payload.model": "m"}), a(m{"payload.turn_id": "a"})},
			[]string{"m", "m"}},
		{"a carried value passes down a chain of parents", []Rule{{Column: "prompt", By: "parent", From: "id"}}, "prompt",
			[]line{a(m{"id": "u1", "prompt": "p"}), a(m{"id": "u2", "parent": "u1"}), a(m{"id": "u3", "parent": "u2"})},
			[]string{"p", "p", "p"}},
		{"a tool result takes its call's id", nil, "message.content.id",
			[]line{a(m{"message.content.id": "x", "message.content.name": "Bash"}), a(m{"message.content.tool_use_id": "x"})},
			[]string{"x", "x"}},
		{"fresh model beats carried alias", nil, "model",
			[]line{a(m{"session_id": "s", "message.model": "claude-opus-4-1-20250805"}), a(m{"session_id": "s", "response.model": "claude-sonnet-4-20250514"})}, []string{"claude-opus-4-1-20250805", "claude-sonnet-4-20250514"}},
		{"a field about a model is no model", nil, "attachment.tools.schema.properties.model.description",
			[]line{a(m{"session_id": "s", "attachment.tools.schema.properties.model.description": "Optional model override"}), a(m{"session_id": "s", "type": "user"})},
			[]string{"Optional model override", ""}},
		{"a model's id is carried", nil, "attachment.identity.model_id",
			[]line{a(m{"session_id": "s", "attachment.identity.model_id": "claude-opus-5-5"}), a(m{"session_id": "s", "type": "user"})},
			[]string{"claude-opus-5-5", "claude-opus-5-5"}},
		{"no rules carry nothing", []Rule{}, "payload.model",
			[]line{a(m{"payload.turn_id": "a", "payload.model": "m"}), a(m{"payload.turn_id": "a"})},
			[]string{"m", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := New(tc.rules)
			var got []string
			for _, l := range tc.lines {
				col, _ := column(c.Carry(l.path, record(l.cols)), tc.column)
				got = append(got, col.Value.String)
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// Redacting a record's list in place, as ingest does, leaves the value carried onto the next.
func TestCarryHandsOutCopies(t *testing.T) {
	c := New([]Rule{{Column: "model", By: "turn_id"}})
	list := segment.Column{Name: "model", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: []value.Value{{String: "m"}}}}
	turn := segment.Column{Name: "turn_id", Type: value.TypeString, Value: value.Value{String: "a"}}
	var got []string
	for _, r := range []segment.Record{{Columns: []segment.Column{list, turn}}, {Columns: []segment.Column{turn}}, {Columns: []segment.Column{turn}}} {
		col, _ := column(c.Carry("a.log", r), "model")
		got = append(got, col.Value.List[0].String)
		col.Value.List[0] = value.Value{String: "hashed"}
	}
	assert.Equal(t, []string{"m", "m", "m"}, got)
}
