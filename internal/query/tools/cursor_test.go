package tools

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
)

// TestEncodePatternsCursor requires every cursor to encode deterministically, URL-safe and non-empty, and decode back to itself.
func TestEncodePatternsCursor(t *testing.T) {
	tests := []struct {
		name   string
		cursor patternsCursor
	}{
		{"zero_value", patternsCursor{}},
		{"offset_zero", patternsCursor{Snapshot: "20260101T000000Z-abc", Sort: "count", Offset: 0}},
		{"offset_large", patternsCursor{Snapshot: "20260101T000000Z-abc", Sort: "signal", Offset: 1_000_000}},
		{"all_sort_modes", patternsCursor{Snapshot: "snap", Sort: "delta", Offset: 42}},
		{"rate_sort", patternsCursor{Snapshot: "s", Sort: "rate", Offset: 1}},
		{"url_safe", patternsCursor{Snapshot: "20260101T000000Z-abc", Sort: "signal", Offset: 999}},
		{"deterministic", patternsCursor{Snapshot: "snap", Sort: "count", Offset: 7}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			encoded := encodePatternsCursor(tc.cursor)
			assert.Equal(t, encoded, encodePatternsCursor(tc.cursor), "non-deterministic encoding")
			assert.NotEmpty(t, encoded)
			assert.False(t, strings.ContainsAny(encoded, "+/="), "encoded cursor contains URL-unsafe characters: %q", encoded)
			decoded, err := decodePatternsCursor(encoded)
			require.NoError(t, err)
			assert.Equal(t, tc.cursor, decoded)
		})
	}
}

// TestDecodePatternsCursor decodes the empty string to the zero cursor and rejects malformed input as an invalid request.
func TestDecodePatternsCursor(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantInvalid bool
	}{
		{name: "empty_string", input: ""},
		{name: "bad_base64", input: "not-valid-base64!!!", wantInvalid: true},
		{name: "valid_base64_not_json", input: "bm90LWpzb24", wantInvalid: true},
		{name: "negative_offset", input: encodePatternsCursor(patternsCursor{Offset: -1}), wantInvalid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decoded, err := decodePatternsCursor(tc.input)
			if tc.wantInvalid {
				var inv *query.InvalidRequestError
				assert.ErrorAs(t, err, &inv)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, patternsCursor{}, decoded)
		})
	}
}
