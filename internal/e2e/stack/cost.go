package stack

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// fields is every numeric field of a report but its run header, by its dotted path; a list
// of named objects is keyed by their names, and any other list is left out.
func fields(r map[string]any) map[string]float64 {
	out := map[string]float64{}
	var walk func(path string, v any)
	walk = func(path string, v any) {
		switch v := v.(type) {
		case float64:
			out[path] = v
		case map[string]any:
			for k, e := range v {
				walk(strings.TrimPrefix(path+"."+k, "."), e)
			}
		case []any:
			for _, e := range v {
				m, _ := e.(map[string]any)
				if name, ok := m["name"].(string); ok {
					walk(path+"."+name, e)
				}
			}
		}
	}
	r = maps.Clone(r)
	delete(r, "run")
	walk("", r)
	return out
}

func num(v float64) string {
	return strconv.FormatFloat(v, 'g', 6, 64)
}

// everyField is every numeric field of a report, sorted, under the conditions its run
// header states, so a later report compares with it on any of them.
func everyField(r map[string]any) string {
	run, _ := json.Marshal(r["run"])
	var out strings.Builder
	fmt.Fprintf(&out, "Conditions: `%s`\n\n| field | DataFocal |\n|---|---:|\n", run)
	f := fields(r)
	for _, k := range slices.Sorted(maps.Keys(f)) {
		fmt.Fprintf(&out, "| %s | %s |\n", k, num(f[k]))
	}
	return out.String()
}

// compare is every numeric field two reports both hold, before and after, and the change,
// under the conditions each states.
func compare(before, after map[string]any) string {
	var out strings.Builder
	for _, r := range []struct {
		name string
		v    map[string]any
	}{{"Before", before}, {"After", after}} {
		run, _ := json.Marshal(r.v["run"])
		fmt.Fprintf(&out, "%s: `%s`\n\n", r.name, run)
	}
	out.WriteString("| field | before | after | change |\n|---|---:|---:|---:|\n")
	b, a := fields(before), fields(after)
	for _, k := range slices.Sorted(maps.Keys(a)) {
		was, ok := b[k]
		if !ok {
			continue
		}
		change := "–"
		if was != 0 {
			change = fmt.Sprintf("%+.1f%%", (a[k]-was)/was*100)
		}
		fmt.Fprintf(&out, "| %s | %s | %s | %s |\n", k, num(was), num(a[k]), change)
	}
	return out.String()
}
