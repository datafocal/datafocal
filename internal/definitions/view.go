package definitions

import (
	"cmp"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"time"

	"github.com/datafocal/datafocal/internal/selector"
)

const (
	viewAPIVersion = "datafocal/v1"
	kindView       = "View"
	panelSLO       = "slo"
	// columns is the width of a view's grid.
	columns = 12
)

// View is a saved view compiled for drawing.
type View struct {
	Name      string            `json:"name"`
	Title     string            `json:"title"`
	Time      selector.TimeSpec `json:"time"`
	Variables []Variable        `json:"variables,omitempty"`
	Panels    []Panel           `json:"panels"`
}

// Variable is a column's value the view's panels select by as $Name.
type Variable struct {
	Name     string `json:"name"`
	Column   string `json:"column"`
	Default  string `json:"default,omitempty"`
	AllowAll bool   `json:"allowAll,omitempty"`
}

// Panel is a query and how to draw it in its cell of the grid.
type Panel struct {
	Title string `json:"title"`
	Kind  string `json:"kind"`
	SLO   string `json:"slo,omitempty"`
	// Query is the panel's normalized LogQL, empty for an slo panel.
	Query string `json:"query,omitempty"`
	Grid  Grid   `json:"grid"`
}

// Grid is a panel's cell, at X, Y, W columns wide and H rows tall.
type Grid struct {
	X int `json:"x"`
	Y int `json:"y"`
	W int `json:"w"`
	H int `json:"h"`
}

// panelKinds are the query each kind but slo draws, and whether a query's stage is one.
var panelKinds = map[string]struct {
	want string
	fits func(s *selector.Stage) bool
}{
	"timeseries": {"an aggregate over a range", func(s *selector.Stage) bool {
		return s != nil && s.Kind == selector.StageAggregate && s.Bins != "" && s.Bins != "all"
	}},
	"stat": {"an ungrouped aggregate of one measure", func(s *selector.Stage) bool {
		return s != nil && s.Kind == selector.StageAggregate && s.GroupBy == "" && len(s.Measures) == 0
	}},
	"table": {"a grouped aggregate or top", func(s *selector.Stage) bool {
		return s != nil && (s.Kind == selector.StageAggregate || s.Kind == selector.StageTop) && s.GroupBy != ""
	}},
	"lines":    {"a log query", func(s *selector.Stage) bool { return s == nil }},
	"patterns": {"a log query with | patterns", func(s *selector.Stage) bool { return s != nil && s.Kind == selector.StagePatterns }},
}

var variableName = regexp.MustCompile(`^[A-Za-z_]\w*$`)

// normalizeView formats each panel's query that parses, leaving compile to reject the rest.
func normalizeView(raw map[string]any) {
	spec, _ := raw["spec"].(map[string]any)
	panels, _ := spec["panels"].([]any)
	for _, p := range panels {
		panel, _ := p.(map[string]any)
		// YAML 1.1 reads a bare y key as true.
		if grid, _ := panel["grid"].(map[string]any); grid != nil && grid["true"] != nil {
			grid["y"] = grid["true"]
			delete(grid, "true")
		}
		text, _ := panel["query"].(string)
		if q, err := parseQuery(text); text != "" && err == nil {
			panel["query"], _ = logqlLang.Format(q)
		}
	}
}

func compileView(d Document, index map[string]Document) (*View, *Error) {
	var doc struct {
		Metadata meta
		Spec     View
	}
	if err := remarshal(d.Raw, &doc); err != nil {
		return nil, invalid("spec", "%v", err)
	}
	v := &doc.Spec
	if _, err := (selector.Selector{Time: v.Time}).Resolve(time.Now()); err != nil {
		return nil, invalid("spec.time", "%v", err)
	}
	v.Name, v.Title = d.Name, cmp.Or(doc.Metadata.DisplayName, d.Name)
	// Each declared variable takes a value with no $, so a use substitution leaves is misplaced.
	values := map[string]string{}
	for i, x := range v.Variables {
		field := fmt.Sprintf("spec.variables[%d]", i)
		switch _, dup := values[x.Name]; {
		case !variableName.MatchString(x.Name):
			return nil, invalid(field+".name", "%q is not a name such as service", x.Name)
		case dup:
			return nil, invalid(field+".name", "%q is declared twice", x.Name)
		case x.Column == "":
			return nil, invalid(field+".column", "is empty")
		}
		values[x.Name] = "x"
	}
	for i := range v.Panels {
		if err := v.Panels[i].compile(fmt.Sprintf("spec.panels[%d]", i), values, index); err != nil {
			return nil, err
		}
	}
	return v, nil
}

// compile checks p's grid and query, and titles it when it has no title.
func (p *Panel) compile(field string, values map[string]string, index map[string]Document) *Error {
	switch g := p.Grid; {
	case g.W <= 0 || g.H <= 0:
		return invalid(field+".grid", "needs a positive w and h")
	case g.X < 0 || g.Y < 0 || g.X+g.W > columns:
		return invalid(field+".grid", "falls outside the %d columns", columns)
	case p.Kind == panelSLO:
		return p.compileSLO(field, index)
	}
	return p.compileQuery(field, values)
}

// compileSLO checks an slo panel names an SLO and no query, titling it by the SLO.
func (p *Panel) compileSLO(field string, index map[string]Document) *Error {
	switch {
	case p.Query != "":
		return invalid(field+".query", "an slo panel takes no query")
	case p.SLO == "":
		return invalid(field+".slo", "names no SLO")
	}
	slo, err := resolve[struct{ Metadata meta }](index, kindSLO, p.SLO, field+".slo")
	p.Title = cmp.Or(p.Title, slo.Metadata.DisplayName, p.SLO)
	return err
}

// compileQuery checks a panel's query parses, fits its kind and uses only declared
// variables, each as the whole value of an = or != predicate.
func (p *Panel) compileQuery(field string, values map[string]string) *Error {
	kind, ok := panelKinds[p.Kind]
	if !ok {
		return invalid(field+".kind", "%q is not one of slo or %v", p.Kind, slices.Sorted(maps.Keys(panelKinds)))
	}
	field += ".query"
	q, err := parseQuery(p.Query)
	if err != nil {
		return invalid(field, "%v", err)
	}
	if !kind.fits(q.Stage) {
		return invalid(field, "%q is not %s, which a %s panel draws", p.Query, kind.want, p.Kind)
	}
	if q.Where, err = selector.Substitute(q.Where, values); err != nil {
		return invalid(field, "%q uses an undeclared variable: %v", p.Query, err)
	}
	if text, _ := logqlLang.Format(q); variable.MatchString(text) {
		return invalid(field, "%q uses a $variable other than as the whole value of an = or != predicate", p.Query)
	}
	p.Title = cmp.Or(p.Title, p.Query)
	return nil
}
