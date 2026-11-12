// Package llm exposes shared model and usage columns discovered from log fields.
package llm

import (
	"math"
	"regexp"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

// RoleAssistant is the role used by model responses.
const RoleAssistant = "assistant"

var names = strings.NewReplacer("_", "", ".", "", "-", "")
var cost = regexp.MustCompile(`(response|total)cost$|usagecost$|costusd$|costtotal$`)
var kinds = []string{"input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "reasoning_tokens"}
var tokenNames = []*regexp.Regexp{
	regexp.MustCompile(`input|prompt`), regexp.MustCompile(`output|completion|candidates`),
	regexp.MustCompile(`cache`), regexp.MustCompile(`cachecreation|cachewrite`), regexp.MustCompile(`reason|thought`),
}

type usageField struct {
	model, cost, exclusive bool
	token                  int
	quota                  quotaKind
}

var fields, _ = lru.New[string, usageField](4096)

var modelLength = sync.OnceValue(func() int {
	longest := 0
	for model := range defaultTables()["price_in"] {
		longest = max(longest, len(model))
	}
	return longest
})

func classify(name string) usageField {
	if f, ok := fields.Get(name); ok {
		return f
	}
	key := names.Replace(strings.ToLower(name))
	f := usageField{model: strings.HasSuffix(key, "model") || strings.HasSuffix(key, "modelid"), cost: cost.MatchString(key), token: tokenKind(key), exclusive: exclusiveInput(name, key), quota: quotaKindOf(name, key)}
	fields.Add(name, f)
	return f
}

// Normalize adds canonical counts, with cache excluded from input, and a shared model.
func Normalize(r segment.Record) segment.Record {
	model, fallback := "", ""
	prices, longest := defaultTables()["price_in"], modelLength()
	for _, col := range r.Columns {
		if col.Type != value.TypeString {
			continue
		}
		f := classify(col.Name)
		// A bare word the price table lists, such as auto or custom, is likelier a setting.
		if len(col.Value.String) <= longest && prices[col.Value.String] != "" && strings.ContainsAny(col.Value.String, "-/.:0123456789") {
			model = col.Value.String
		}
		if f.model {
			fallback = col.Value.String
		}
	}
	if fallback != "" {
		model = fallback
	}
	if model != "" {
		r = Set(r, segment.Column{Name: "model", Type: value.TypeString, Value: value.Value{String: model}})
	}
	return quotaColumns(usageColumns(r))
}

func usageColumns(r segment.Record) segment.Record {
	var counts [5]float64
	var held [5]bool
	var chosen [5]string
	exclusive := false
	for _, col := range r.Columns {
		n, ok := usageNumber(col)
		if !ok {
			continue
		}
		f := classify(col.Name)
		if f.cost {
			r = Set(r, segment.Column{Name: "logged_cost_usd", Type: value.TypeFloat, Value: value.Value{Float: n}})
		}
		k := f.token
		if k < 0 || (held[k] && len(col.Name) >= len(chosen[k])) {
			continue
		}
		counts[k], held[k], chosen[k] = n, true, col.Name
		exclusive = exclusive || f.exclusive
	}
	return tokenColumns(r, counts, held, exclusive)
}

func usageNumber(col segment.Column) (float64, bool) {
	if !col.Type.Numeric() || col.Type.Repeated() {
		return 0, false
	}
	n := value.Coerce(col.Type, col.Value, value.TypeFloat).Float
	return n, math.Abs(n) <= math.MaxFloat64
}

func exclusiveInput(name, key string) bool {
	name = strings.ToLower(name)
	return name == "cache_read_tokens" || name == "cache_write_tokens" || strings.Contains(name, "cache_read_input_tokens") || strings.Contains(name, "cache_creation_input_tokens") || strings.HasSuffix(key, "cacheread") || strings.HasSuffix(key, "cachewrite")
}

func tokenColumns(r segment.Record, counts [5]float64, held [5]bool, exclusive bool) segment.Record {
	// Output-only message receipts can mirror usage already logged with input.
	if !held[0] && !held[2] && !held[3] {
		return r
	}
	if !exclusive {
		counts[0] = math.Max(0, counts[0]-counts[2]-counts[3])
	}
	for i, name := range kinds {
		if held[i] || (i == 0 && (held[2] || held[3])) {
			r = Set(r, segment.Column{Name: name, Type: value.TypeFloat, Value: value.Value{Float: counts[i]}})
		}
	}
	return r
}

func tokenKind(key string) int {
	if strings.Contains(key, "totaltokenusage") || strings.Contains(key, "lasttokenusage") || strings.Contains(key, "cost") || strings.Contains(key, "ephemeral") {
		return -1
	}
	if !strings.Contains(key, "token") && !strings.Contains(key, "usage") {
		return -1
	}
	for i := len(tokenNames) - 1; i >= 0; i-- {
		if tokenNames[i].MatchString(key) {
			return i
		}
	}
	return -1
}

// Set replaces a canonical column while leaving the other source columns intact.
func Set(r segment.Record, col segment.Column) segment.Record {
	for i := range r.Columns {
		if r.Columns[i].Name == col.Name {
			r.Columns[i] = col
			return r
		}
	}
	r.Columns = append(r.Columns, col)
	return r
}
