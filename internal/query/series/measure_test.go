package series

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/value"
)

// TestParseMeasure tests the ParseMeasure function with valid and invalid inputs.
func TestParseMeasure(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantKind     Kind
		wantField    string
		wantLength   string
		wantQuantile float64
		wantErr      bool
	}{
		// Valid single-word measures
		{name: "count", input: "count", wantKind: KindCount, wantField: "", wantQuantile: 0},
		{name: "rate", input: "rate", wantKind: KindRate, wantField: "", wantQuantile: 0},
		{name: "ratio", input: "ratio", wantKind: KindRatio, wantField: "", wantQuantile: 0},
		// The field is optional and picks what is divided: rows without it, sums over
		// the named column with it.
		{name: "ratio_with_field", input: "ratio:bytes", wantKind: KindRatio, wantField: "bytes", wantQuantile: 0},
		{name: "p50", input: "p50", wantKind: KindQuantile, wantField: "", wantQuantile: 0.50},
		{name: "p95", input: "p95", wantKind: KindQuantile, wantField: "", wantQuantile: 0.95},
		{name: "p99", input: "p99", wantKind: KindQuantile, wantField: "", wantQuantile: 0.99},

		// Valid field-qualified measures
		{name: "distinct:trace_id", input: "distinct:trace_id", wantKind: KindDistinct, wantField: "trace_id", wantQuantile: 0},
		{name: "distinct:user_id", input: "distinct:user_id", wantKind: KindDistinct, wantField: "user_id", wantQuantile: 0},
		{name: "sum:duration", input: "sum:duration", wantKind: KindSum, wantField: "duration", wantQuantile: 0},
		{name: "sum:bytes", input: "sum:bytes", wantKind: KindSum, wantField: "bytes", wantQuantile: 0},
		{name: "p95:duration", input: "p95:duration", wantKind: KindQuantile, wantField: "duration", wantQuantile: 0.95},
		{name: "span:turn", input: "span:turn", wantKind: KindSpan, wantField: "turn"},
		{name: "p50:latency", input: "p50:latency", wantKind: KindQuantile, wantField: "latency", wantQuantile: 0.50},

		// Lengths of a column in place of its value
		{name: "sum:len", input: "sum:len(message.content.text)", wantKind: KindSum, wantField: "message.content.text", wantLength: "len"},
		{name: "p95:bytes", input: "p95:bytes(body)", wantKind: KindQuantile, wantField: "body", wantLength: "bytes", wantQuantile: 0.95},
		{name: "max:len", input: "max:len(message)", wantKind: KindMax, wantField: "message", wantLength: "len"},
		{name: "unclosed_len_is_a_column", input: "sum:len(x", wantKind: KindSum, wantField: "len(x"},

		// Arbitrary percentiles from the open p<NN> family
		{name: "p90", input: "p90", wantKind: KindQuantile, wantField: "", wantQuantile: 0.90},
		{name: "p75", input: "p75", wantKind: KindQuantile, wantField: "", wantQuantile: 0.75},
		{name: "p999", input: "p999", wantKind: KindQuantile, wantField: "", wantQuantile: 0.999},
		{name: "p9999", input: "p9999", wantKind: KindQuantile, wantField: "", wantQuantile: 0.9999},
		{name: "p5", input: "p5", wantKind: KindQuantile, wantField: "", wantQuantile: 0.5},
		{name: "p999:duration", input: "p999:duration", wantKind: KindQuantile, wantField: "duration", wantQuantile: 0.999},

		// Invalid inputs
		{name: "unknown", input: "nosuchmeasure", wantErr: true},
		{name: "distinct_no_field", input: "distinct:", wantErr: true},
		{name: "span_no_field", input: "span", wantErr: true},
		{name: "sum_no_field", input: "sum:", wantErr: true},
		{name: "p95_empty_field", input: "p95:", wantErr: true},
		{name: "empty_string", input: "", wantErr: true},
		{name: "malformed", input: "measure:field:extra", wantErr: true},
		{name: "quantile_zero", input: "p0", wantErr: true},
		{name: "quantile_nondigit", input: "p95x", wantErr: true},
		{name: "bare_p", input: "p", wantErr: true},
		{name: "count_with_field", input: "count:foo", wantErr: true},
		{name: "sum_bare", input: "sum", wantErr: true},
		{name: "distinct_len", input: "distinct:len(message)", wantErr: true},
		{name: "first_bytes", input: "first:bytes(body)", wantErr: true},
		{name: "ratio_len", input: "ratio:len(message)", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := ParseMeasure(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseMeasure(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if err != nil {
				return // no need to check other fields
			}
			assert.Equal(t, Measure{Kind: tt.wantKind, Field: tt.wantField, Length: tt.wantLength, Quantile: tt.wantQuantile}, m)
		})
	}
}

// TestNeedsGroupScan asserts the decision comes from the data rather than from the
// dimension's name: a segment whose rows all share one value for it can be keyed on
// that value, and one segment that mixes values forces the scan for every segment.
func TestNeedsGroupScan(t *testing.T) {
	// A stream label still keys the segment when every row agrees on it.
	label := segment.Descriptor{
		Stream: segment.Stream{Service: "checkout", Labels: map[string]string{"instance": "checkout-0"}},
		Rows:   2,
		ZoneMaps: segment.ZoneMaps{"instance": {
			Type: value.TypeString, Min: value.Value{String: "checkout-0"},
			Max: value.Value{String: "checkout-0"}, PresentRows: 2,
		}},
	}
	// An agent-shaped segment: the rows carry the services they name.
	agent := segment.Descriptor{
		Stream: segment.Stream{Service: "node-agent"},
		Rows:   2,
		ZoneMaps: segment.ZoneMaps{"service": {
			Type: value.TypeString, Min: value.Value{String: "checkout"},
			Max: value.Value{String: "nginx"}, PresentRows: 2,
		}},
	}
	// One agent-tailed service per segment: the rows carry it, and they agree.
	single := segment.Descriptor{
		Stream: segment.Stream{Service: "node-agent"},
		Rows:   2,
		ZoneMaps: segment.ZoneMaps{"service": {
			Type: value.TypeString, Min: value.Value{String: "nginx"},
			Max: value.Value{String: "nginx"}, PresentRows: 2,
		}},
	}
	// A row-level column whose zone map collapses to one value still keys the segment, the
	// fast path for typed ordinary columns.
	typedSingle := segment.Descriptor{
		Stream: segment.Stream{Service: "node-agent"},
		Rows:   2,
		ZoneMaps: segment.ZoneMaps{
			"duration_ms": {Type: value.TypeInt, Min: value.Value{Int: 42}, Max: value.Value{Int: 42}, PresentRows: 2},
		},
	}
	// The same single value, but on only one of the two rows: the other row holds no
	// duration at all, so no per-segment key exists and the scan has to decide.
	typedSparse := segment.Descriptor{
		Stream: segment.Stream{Service: "node-agent"},
		Rows:   2,
		ZoneMaps: segment.ZoneMaps{
			"duration_ms": {Type: value.TypeInt, Min: value.Value{Int: 42}, Max: value.Value{Int: 42}, PresentRows: 1},
		},
	}

	tests := []struct {
		name     string
		descs    []segment.Descriptor
		groupBy  string
		wantScan bool
	}{
		{"stream label", []segment.Descriptor{label}, "instance", false},
		{"rows agree on the service", []segment.Descriptor{single}, "service", false},
		{"rows mix services", []segment.Descriptor{agent}, "service", true},
		{"one mixed segment forces the scan", []segment.Descriptor{single, agent}, "service", true},
		{"typed row column with one value", []segment.Descriptor{typedSingle}, "duration_ms", false},
		{"typed row column absent from a row", []segment.Descriptor{typedSparse}, "duration_ms", true},
		{"row-level dimension", []segment.Descriptor{label}, "pattern_id", true},
		{"dimension nothing carries", []segment.Descriptor{label}, "unknown_field", true},
		{"empty groups by pattern_id", []segment.Descriptor{label}, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantScan, NeedsGroupScan(tt.descs, tt.groupBy))
		})
	}
}

func TestSoleDistribution(t *testing.T) {
	ambiguous := query.NewAPIError(query.ErrCodeInvalidRequest, "ambiguous quantile target: multiple numeric columns").
		WithSuggestedFields([]string{"duration", "latency"})
	tests := []struct {
		name       string
		footer     segment.Footer
		wantColumn string
		wantOK     bool
		wantErr    error
	}{
		{name: "none", footer: segment.Footer{}},
		{name: "one", footer: segment.Footer{Distributions: map[string]*sketch.DDSketch{"duration": {}}}, wantColumn: "duration", wantOK: true},
		{name: "several", footer: segment.Footer{Distributions: map[string]*sketch.DDSketch{"latency": {}, "duration": {}}}, wantErr: ambiguous},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			column, ok, err := soleDistribution(tc.footer)
			assert.Equal(t, tc.wantColumn, column)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantErr, err)
		})
	}
}
