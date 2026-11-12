// Package producers holds what the collector knows of each log producer, one JSON file per
// producer, so supporting a new one is a file here and no Go.
package producers

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
)

//go:embed *.json
var embedded embed.FS

var dirs = []fs.FS{embedded}

// Add adds dir's producer files after the embedded ones, as a test adds its own before
// any package reads a section.
func Add(dir fs.FS) {
	dirs = append(dirs, dir)
}

// Section is the named section of every producer file, in file name order; each package
// decodes its own, and a file that does not decode panics, as the files are embedded.
func Section[T any](name string) []T {
	var all []T
	for _, dir := range dirs {
		all = append(all, section[T](dir, name)...)
	}
	return all
}

func section[T any](dir fs.FS, name string) []T {
	var all []T
	paths, _ := fs.Glob(dir, "*.json") // the pattern is valid
	for _, path := range paths {
		data, _ := fs.ReadFile(dir, path) // it was just listed
		var sections map[string]json.RawMessage
		if err := json.Unmarshal(data, &sections); err != nil {
			panic(fmt.Sprintf("producers: %s: %v", path, err))
		}
		raw, ok := sections[name]
		if !ok {
			continue
		}
		var items []T
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields() // a misspelt key would otherwise be ignored
		if err := dec.Decode(&items); err != nil {
			panic(fmt.Sprintf("producers: %s: %s: %v", path, name, err))
		}
		all = append(all, items...)
	}
	return all
}
