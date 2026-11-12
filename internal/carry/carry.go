// Package carry copies a column onto the later records of a file that share a key with the
// record holding it, as a usage line its turn's model.
package carry

import (
	"cmp"
	"slices"
	"strings"

	"github.com/datafocal/datafocal/internal/llm"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

// Carrier remembers each file's columns apart, and only for as long as it runs.
type Carrier interface {
	Carry(path string, r segment.Record) segment.Record // r with each rule's column it lacks
	Forget(path string)
}

// Rule copies Column onto a later record of the same file that holds By but not Column,
// from the last record holding Column whose From value equals that By value.
type Rule struct {
	Column string `json:"column"`
	// By matches From as value.Format prints them: a one-element list matches a one-element
	// list, a longer list only an identical one.
	By   string `json:"by"`
	From string `json:"from,omitempty"` // empty means By
}

// maxKeys is how many keys a file remembers per rule, the oldest dropped.
const maxKeys = 1024 //nolint:dupdefault // a file's carry memory, unrelated to segment's counted values

type carrier struct {
	rules []Rule
	names map[string]agentField
	files map[string][]memory // by path: explicit rules, or discovered links and signals
}

// memory is one rule's columns in one file, by From's formatted value.
type memory struct {
	columns map[string][]segment.Column
	keys    []string // oldest first
	// agent is whether a record of the file has held an agent's content, before which a field
	// named like a session or a trace is the file's own, not an agent's run.
	agent bool
}

// New discovers links when rules is nil and otherwise carries by the explicit rules.
func New(rules []Rule) Carrier {
	return &carrier{rules: rules, files: map[string][]memory{}, names: map[string]agentField{}}
}

// Carry remembers a column a record received as if it held it, so a value passes down a
// chain of records; it keeps and hands out copies, so redacting one record in place leaves
// the others.
func (c *carrier) Carry(path string, r segment.Record) segment.Record {
	mem, ok := c.files[path]
	if !ok {
		rules := len(c.rules)
		if c.rules == nil {
			rules = 2
		}
		mem = make([]memory, rules)
		c.files[path] = mem
	}
	if c.rules == nil {
		return c.discover(&mem[0], &mem[1], r)
	}
	for n, rule := range c.rules {
		col, ok := column(r, rule.Column)
		if by, held := column(r, rule.By); held && !ok {
			if cols, found := mem[n].columns[value.Format(by.Type, by.Value)]; found {
				col, ok = cols[0], true
				r.Columns = append(r.Columns, clone(col))
			}
		}
		if from, held := column(r, cmp.Or(rule.From, rule.By)); ok && held {
			mem[n].remember(value.Format(from.Type, from.Value), []segment.Column{col})
		}
	}
	return r
}

func (c *carrier) Forget(path string) {
	delete(c.files, path)
}

func (m *memory) remember(key string, cols []segment.Column) {
	if m.columns == nil {
		m.columns = map[string][]segment.Column{}
	}
	if _, ok := m.columns[key]; !ok {
		if len(m.keys) == maxKeys {
			delete(m.columns, m.keys[0])
			m.keys = m.keys[1:]
		}
		m.keys = append(m.keys, key)
	}
	m.columns[key] = make([]segment.Column, len(cols))
	for i, col := range cols {
		m.columns[key][i] = clone(col)
	}
}

func column(r segment.Record, name string) (segment.Column, bool) {
	i := slices.IndexFunc(r.Columns, func(c segment.Column) bool { return c.Name == name })
	if i < 0 {
		return segment.Column{}, false
	}
	return r.Columns[i], true
}

func clone(col segment.Column) segment.Column {
	col.Value.List = slices.Clone(col.Value.List)
	return col
}

func (c *carrier) discover(m, state *memory, r segment.Record) segment.Record {
	if len(m.columns) == 0 && len(state.columns) == 0 && !slices.ContainsFunc(r.Columns, func(col segment.Column) bool { return c.field(col.Name).interesting || sessionStart(col) }) {
		return r
	}
	r = llm.Normalize(r)
	original := r.Columns
	if len(m.columns) > 0 {
		for _, id := range original {
			for _, col := range m.columns[value.Format(id.Type, id.Value)] {
				if _, held := column(r, col.Name); !held {
					r.Columns = append(r.Columns, clone(col))
				}
			}
		}
	}
	r = c.agentFields(r, state)
	c.rememberContext(m, r, original)
	return r
}

func (c *carrier) rememberContext(m *memory, r segment.Record, original []segment.Column) {
	if !slices.ContainsFunc(r.Columns, func(col segment.Column) bool {
		f := c.field(col.Name)
		return f.context || f.named || strings.Contains(col.Name, ".id") || col.Name == fieldToolName
	}) {
		return
	}
	for _, id := range original {
		if !c.field(id.Name).id {
			continue
		}
		if cols := c.carriedContext(r, id.Name); len(cols) > 0 {
			m.remember(value.Format(id.Type, id.Value), cols)
		}
	}
}

func (c *carrier) carriedContext(r segment.Record, id string) []segment.Column {
	prefix, _, _ := strings.Cut(id, ".id")
	_, tool := column(r, prefix+".name")
	tool = tool || strings.Contains(strings.ToLower(id), "call")
	var cols []segment.Column
	for _, col := range r.Columns {
		f := c.field(col.Name)
		if f.context || ((col.Name == fieldToolName || col.Name == "skill_name") && tool) || (strings.HasPrefix(col.Name, prefix+".") && (f.named || col.Name == id)) {
			cols = append(cols, col)
		}
	}
	return cols
}
