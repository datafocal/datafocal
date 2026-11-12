package catalog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestVersionTokens: a version over the v1 sequence alone is its count, as versions were; one
// over writers indexes the writers its table holds and names the rest, and reads back to the
// same counts; a newer version holds more, and a garbled one is not valid.
func TestVersionTokens(t *testing.T) {
	assert.Equal(t, Version("42"), versionOf(nil, map[string]int{"": 42}))
	assert.Equal(t, Version("0"), versionOf(nil, map[string]int{}))

	counts := map[string]int{"": 3, "node-a": 5, "node-c": 300, "node-z": 2, "late": 1}
	v := versionOf([]string{"node-a", "node-b", "node-c"}, counts)
	named, indexed, err := v.parse()
	require.NoError(t, err)
	assert.Equal(t, pin{"": 3, "late": 1, "node-z": 2}, named)
	assert.Equal(t, []int{5, 0, 300}, indexed)
	assert.Equal(t, 311, v.total())

	more := versionOf([]string{"node-a", "node-b", "node-c"}, map[string]int{"": 3, "node-a": 6, "node-c": 300, "node-z": 2, "late": 1})
	assert.True(t, v.Before(more))
	assert.True(t, Version("12").Before(v))
	for _, bad := range []Version{"", "x", "-1", "7-%%", "7-AQ", v + "_"} {
		assert.False(t, bad.Valid(), "%q", bad)
	}
}
