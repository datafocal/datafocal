package llm

import (
	_ "embed"
	"maps"
	"strings"
	"sync"
)

// prices is the default lookup tables: model prices in USD per million tokens, copied from
// models.dev by scripts/model-prices.sh. It is a TSV of one model per line, whose header
// names the table each column after the model fills; an empty cell is a model with no
// such price.
//
//go:embed prices.tsv
var prices string

var defaultTables = sync.OnceValue(func() map[string]map[string]string {
	lines := strings.Split(strings.TrimSuffix(prices, "\n"), "\n")
	header := strings.Split(lines[0], "\t")
	tables := make(map[string]map[string]string, len(header)-1)
	for _, name := range header[1:] {
		tables[name] = map[string]string{}
	}
	for _, line := range lines[1:] {
		cells := strings.Split(line, "\t")
		for i, v := range cells[1:] {
			if v != "" {
				tables[header[i+1]][cells[0]] = v
			}
		}
	}
	return tables
})

// Tables returns the embedded model price tables for query-time pricing.
func Tables() map[string]map[string]string { return maps.Clone(defaultTables()) }
