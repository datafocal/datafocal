// Package pattern derives a record's pattern_id from its masked message, or
// from its column names and enum-like values when it has no non-empty string message.
package pattern

import (
	"cmp"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/value"
)

// Pattern derives pattern_ids with a cardinality guard per service.
type Pattern interface {
	// Derive's Columns are the extract field's masked parameters plus a <column>_pattern
	// masked copy of every other string column containing a space.
	Derive(columns []segment.Column) segment.ParsedPattern
	// Footer is the derivation as the sealed footer records it.
	Footer() segment.PatternProfile
}

// The degradation levels a service steps through as it breaches its budget.
const (
	LevelProfile = iota
	LevelColumnNames
	LevelCatchAll
)

const catchAll = "unpatterned"

type profile struct {
	budget   int
	services map[string]*guard
}

type guard struct {
	level   int
	seen    map[string]struct{}
	records int                     // the structured records seen
	columns map[string]*columnStats // by the name of a column holding an enum-like string
}

// columnStats is a column's distinct values, up to one past the cap, and records holding it.
type columnStats struct {
	values  map[string]struct{}
	records int
}

// maxDiscriminatorValues is the most values a column naming a record's kind takes.
const maxDiscriminatorValues = 16

// maxDiscriminators is how many kind columns a structured record's name leads with.
const maxDiscriminators = 2

// New returns automatic message or structure derivation with a budget per service.
func New(budget int) Pattern {
	return &profile{budget: budget, services: make(map[string]*guard)}
}

func ID(text string) string {
	return strconv.FormatUint(sketch.Hash64([]byte(text)), 16)
}

func (p *profile) Derive(columns []segment.Column) segment.ParsedPattern {
	var parsed segment.ParsedPattern
	var extracted string
	hasMessage := false
	for _, col := range columns {
		switch {
		case col.Name == segment.ColMessage && col.Type == value.TypeString:
			hasMessage = col.Value.String != ""
			var params []segment.Column
			extracted, params = mask(col.Value.String)
			parsed.Columns = append(parsed.Columns, params...)
		case col.Type == value.TypeString && strings.Contains(col.Value.String, " "):
			masked, _ := mask(col.Value.String)
			parsed.Columns = append(parsed.Columns, segment.Column{Name: col.Name + segment.PatternSuffix, Type: value.TypeString, Value: value.Value{String: masked}})
		}
	}
	g := p.guard(columns)
	name := extracted
	if !hasMessage {
		extracted = renderStructure(columns, true)
		name = g.name(columns, extracted)
	}
	parsed.Pattern, parsed.PatternID = p.derive(g, columns, extracted, name)
	return parsed
}

func (p *profile) guard(columns []segment.Column) *guard {
	service := ""
	for _, col := range columns {
		if col.Name == segment.ColService {
			service = value.Format(col.Type, col.Value)
		}
	}
	g := p.services[service]
	if g == nil {
		g = &guard{seen: make(map[string]struct{}), columns: make(map[string]*columnStats)}
		p.services[service] = g
	}
	return g
}

// derive keeps the id of a structured record's structure while naming it readably.
func (p *profile) derive(g *guard, columns []segment.Column, extracted, name string) (text, id string) {
	for {
		key := extracted
		text = name
		switch g.level {
		case LevelColumnNames:
			key = renderStructure(columns, false)
			text = key
		case LevelCatchAll:
			key, text = catchAll, catchAll
		}
		id = ID(key)
		if _, ok := g.seen[id]; ok {
			return text, id
		}
		if len(g.seen) < p.budget || g.level == LevelCatchAll {
			g.seen[id] = struct{}{}
			return text, id
		}
		g.level++
		clear(g.seen)
	}
}

// name leads with the values of the record's kind columns, the service's columns holding few
// enum-like values in most of its records, outermost first, then the value of the record's
// rarest other varying column, and falls back to structure's key list.
func (g *guard) name(columns []segment.Column, structure string) string {
	held, values := g.observe(columns)
	slices.SortFunc(held, func(a, b string) int {
		sa, sb := g.columns[a], g.columns[b]
		return cmp.Or(cmp.Compare(sb.records, sa.records), cmp.Compare(len(sb.values), len(sa.values)),
			cmp.Compare(strings.Count(a, "."), strings.Count(b, ".")), cmp.Compare(a, b))
	})
	var kinds []string
	for _, col := range held {
		stats := g.columns[col]
		if len(kinds) < maxDiscriminators && stats.records*2 > g.records && len(stats.values) <= maxDiscriminatorValues &&
			!slices.Contains(kinds, values[col]) {
			kinds = append(kinds, values[col])
		}
	}
	text := strings.Join(kinds, " · ")
	for _, col := range slices.Backward(held) {
		if v := values[col]; !slices.Contains(kinds, v) {
			return strings.TrimSpace(text + " " + v)
		}
	}
	return cmp.Or(text, strings.Trim(renderStructure(columns, false), "{}"), structure)
}

// observe returns the record's columns holding an enum-like value that varies or is new.
func (g *guard) observe(columns []segment.Column) (held []string, values map[string]string) {
	g.records++
	values = map[string]string{}
	for _, col := range columns {
		v := strings.Trim(enumValue(col.Type, col.Value), "[]")
		if v == "" || col.Type.Scalar() == value.TypeBool || identity(col.Name) {
			continue
		}
		stats := g.columns[col.Name]
		if stats == nil {
			stats = &columnStats{values: map[string]struct{}{}}
			g.columns[col.Name] = stats
		}
		stats.records++
		if len(stats.values) <= maxDiscriminatorValues {
			stats.values[v] = struct{}{}
		}
		if len(stats.values) > 1 || stats.records == 1 { // a constant only once it has repeated
			held = append(held, col.Name)
			values[col.Name] = v
		}
	}
	return held, values
}

func renderStructure(columns []segment.Column, enums bool) string {
	columns = slices.Clone(columns)
	slices.SortFunc(columns, func(a, b segment.Column) int { return cmp.Compare(a.Name, b.Name) })
	entries := make([]string, 0, len(columns))
	for _, col := range columns {
		if identity(col.Name) {
			continue
		}
		entry := col.Name
		if enums {
			if text := enumValue(col.Type, col.Value); text != "" {
				entry += "=" + text
			}
		}
		entries = append(entries, entry)
	}
	return "{" + strings.Join(entries, " ") + "}"
}

// identity reports whether a column says where a record came from rather than what it is.
func identity(name string) bool {
	switch name {
	case segment.ColService, segment.ColLevel, segment.ColTimestamp, segment.ColHost,
		segment.ColPod, segment.ColNamespace, segment.ColContainer:
		return true
	}
	return false
}

func enumValue(typ value.Type, v value.Value) string {
	if typ.Repeated() {
		var elems []string
		for _, elem := range v.List {
			if text := enumValue(typ.Scalar(), elem); text != "" {
				elems = append(elems, text)
			}
		}
		if len(elems) == 0 {
			return ""
		}
		return "[" + strings.Join(elems, ",") + "]"
	}
	if typ == value.TypeBool {
		return strconv.FormatBool(v.Bool)
	}
	if typ != value.TypeString || len(v.String) == 0 || len(v.String) > 32 {
		return ""
	}
	if strings.Trim(v.String, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-.") != "" {
		return ""
	}
	return v.String
}

func (p *profile) Footer() segment.PatternProfile {
	level := LevelProfile
	for _, g := range p.services {
		level = max(level, g.level)
	}
	return segment.PatternProfile{Name: "auto", Version: 1, Components: []string{"extract(message)", "structure"}, Degradation: level}
}

// wildcard is rendered into the pattern text, so changing it changes every pattern_id.
const wildcard = "<*>"

// durationUnit is the only unit recognized, since a misnamed unit is worse than param_N.
const durationUnit = "ms"

// Mask returns text with its whitespace collapsed and every token containing a
// digit replaced by the wildcard.
func Mask(text string) string {
	masked, _ := mask(text)
	return masked
}

// mask also returns a param_N column per masked token, except that the first one
// followed by "ms" is named segment.ColDurationMs without shifting the numbering.
func mask(text string) (string, []segment.Column) {
	tokens := strings.Fields(text)
	var params []segment.Column
	duration := false
	for i, token := range tokens {
		if !strings.ContainsAny(token, "0123456789") {
			continue
		}
		name := segment.ParamPrefix + strconv.Itoa(len(params)+1)
		if !duration && i+1 < len(tokens) && tokens[i+1] == durationUnit {
			name, duration = segment.ColDurationMs, true
		}
		typ, v := InferTypedValue(token)
		params = append(params, segment.Column{Name: name, Type: typ, Value: v})
		tokens[i] = wildcard
	}
	return strings.Join(tokens, " "), params
}

// InferTypedValue parses once because a failed strconv allocates a *NumError on ingest.
func InferTypedValue(token string) (value.Type, value.Value) {
	if v, err := strconv.ParseInt(token, 10, 64); err == nil {
		return value.TypeInt, value.Value{Int: v}
	}
	if strings.Contains(token, ".") {
		v, err := strconv.ParseFloat(token, 64)
		if err == nil && !math.IsInf(v, 0) && !math.IsNaN(v) {
			return value.TypeFloat, value.Value{Float: v}
		}
	}
	return value.TypeString, value.Value{String: token}
}
