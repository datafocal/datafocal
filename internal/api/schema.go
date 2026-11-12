package api

import (
	"encoding/json"
	"fmt"
	"maps"
	"reflect"

	"github.com/google/jsonschema-go/jsonschema"

	"github.com/datafocal/datafocal/internal/selector"
)

// inferSchema builds the published JSON Schema for a tool request type straight
// from its Go struct, so /v1/describe and the MCP InputSchema can never drift
// from the type the handler unmarshals into. Property names, nesting, and
// required-ness come from the struct's json tags; field descriptions come from
// `jsonschema:"..."` tags; enum-valued named types (selector.Op) are supplied from
// their Go source-of-truth via TypeSchemas. Any residual per-tool enums are set
// by the optional tweaks, which read from the same Go lists validation uses.
//
// A generation or marshal failure is a programming error in the struct tags or
// helper, caught at package-init time (and therefore in every test run), so it
// panics rather than returning an error no caller could handle.
func inferSchema[T any](tweaks ...func(*jsonschema.Schema)) json.RawMessage {
	s, err := jsonschema.For[T](&jsonschema.ForOptions{TypeSchemas: typeSchemas})
	if err != nil {
		panic(fmt.Sprintf("api: infer schema for %T: %v", *new(T), err))
	}
	for _, tweak := range tweaks {
		tweak(s)
	}
	b, err := json.Marshal(s)
	if err != nil {
		panic(fmt.Sprintf("api: marshal schema for %T: %v", *new(T), err))
	}
	return b
}

// typeSchemas supplies the named types inference cannot derive: the enums, raw JSON, and the
// predicate, whose alternatives recurse.
var typeSchemas = func() map[reflect.Type]*jsonschema.Schema {
	schemas := map[reflect.Type]*jsonschema.Schema{
		reflect.TypeFor[selector.Op]():         {Type: "string", Enum: enumOf(selector.AllOps)},
		reflect.TypeFor[selector.Quantifier](): {Type: "string", Enum: enumOf(selector.AllQuantifiers)},
		// Any JSON value, where inference would read the bytes as an array of integers.
		reflect.TypeFor[json.RawMessage](): {},
	}
	schemas[reflect.TypeFor[selector.Predicate]()] = predicateSchema(maps.Clone(schemas))
	return schemas
}()

// predicateSchema publishes a predicate's alternatives as predicates without
// alternatives of their own, since groups nest one level only.
func predicateSchema(schemas map[reflect.Type]*jsonschema.Schema) *jsonschema.Schema {
	schemas[reflect.TypeFor[[]selector.Predicate]()] = &jsonschema.Schema{Type: "array"}
	alternative, err := jsonschema.For[selector.Predicate](&jsonschema.ForOptions{TypeSchemas: schemas})
	if err != nil {
		panic(fmt.Sprintf("api: infer schema for selector.Predicate: %v", err))
	}
	predicate := alternative.CloneSchemas()
	delete(alternative.Properties, "any")
	predicate.Properties["any"] = &jsonschema.Schema{Type: "array", Items: alternative}
	return predicate
}

// setEnum returns a tweak that pins a top-level property's enum to a Go-sourced
// list, keeping the published schema in lockstep with the values validation
// accepts. It panics if the property is absent — that means the struct and the
// tweak have drifted, which is exactly the failure this whole file exists to
// prevent, so it should fail loudly at init.
func setEnum[T ~string](property string, vals []T) func(*jsonschema.Schema) {
	return func(s *jsonschema.Schema) {
		p := s.Properties[property]
		if p == nil {
			panic(fmt.Sprintf("api: setEnum: no property %q on schema", property))
		}
		p.Enum = enumOf(vals)
	}
}

// enumOf widens a slice of a string-kinded type to the []any the jsonschema
// Enum field takes.
func enumOf[T ~string](vals []T) []any {
	out := make([]any, len(vals))
	for i, v := range vals {
		out[i] = string(v)
	}
	return out
}
