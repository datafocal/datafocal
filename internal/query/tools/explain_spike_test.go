package tools

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestShiftsLeaveOutWhatIsNoNews: a field restating the slice and a rise in absence are
// dropped, absence the rest never shows is kept, and fields splitting rows alike are one entry.
func TestShiftsLeaveOutWhatIsNoNews(t *testing.T) {
	_, ok := shiftsOf("container", map[string]int64{"postgres": 10}, map[string]int64{"postgres": 10, "api": 30})
	assert.False(t, ok, "restates the slice")

	path, ok := shiftsOf("path", map[string]int64{"": 10}, map[string]int64{"": 20, "/a": 20})
	assert.True(t, ok)
	assert.Empty(t, path.shifts, "absence the rest also shows")

	trace, ok := shiftsOf("trace_id", map[string]int64{"": 5, "t1": 5}, map[string]int64{"": 5, "t1": 15, "t2": 20})
	assert.True(t, ok)
	assert.Equal(t, []Shift{
		{Field: "trace_id", Value: "", SliceShare: 0.5, BaselineShare: 0, SliceCount: 5},
		{Field: "trace_id", Value: "t1", SliceShare: 0.5, BaselineShare: 1.0 / 3, SliceCount: 5, BaselineCount: 10},
	}, trace.shifts)

	slice, scope := map[string]int64{"a": 8, "b": 2}, map[string]int64{"a": 10, "b": 30}
	service, _ := shiftsOf("service", slice, scope)
	pod, _ := shiftsOf("pod", map[string]int64{"a-0": 8, "b-0": 2}, map[string]int64{"a-0": 10, "b-0": 30})
	got := collapseSame([]fieldShift{pod, service})
	assert.Equal(t, []Shift{{Field: "pod", Value: "a-0", SliceShare: 0.8, BaselineShare: 1.0 / 15, SliceCount: 8, BaselineCount: 2, Also: []string{"service=a"}}}, got[0].shifts)
	assert.Len(t, got, 1)
}

// TestShiftsKeepOnlyWhatReadsAtAGlance: a field only the slice holds and a field of many
// numbers are dropped, a few numbers still read as codes, and a value on under minShare of
// the slice's lines, or rising by under minRise, is left out.
func TestShiftsKeepOnlyWhatReadsAtAGlance(t *testing.T) {
	_, ok := shiftsOf("bytes_read", map[string]int64{"0": 70, "512": 30}, map[string]int64{"0": 70, "512": 30, "": 400})
	assert.False(t, ok, "no row outside the slice holds the field")

	numbers := map[string]int64{"": 100}
	for i := range maxCodes + 1 {
		numbers[strconv.Itoa(i)] = 10
	}
	_, ok = shiftsOf("segments", map[string]int64{"1": 10, "2": 10}, numbers)
	assert.False(t, ok, "a measurement, not codes")

	status, ok := shiftsOf("status", map[string]int64{"500": 60, "200": 40}, map[string]int64{"500": 62, "200": 500})
	assert.True(t, ok, "a few numbers are codes")
	assert.Equal(t, []string{"500"}, values(status.shifts), "200 falls")

	slice := map[string]int64{"a": 60, "b": 36, "c": 4}
	scope := map[string]int64{"a": 70, "b": 336, "c": 5}
	region, ok := shiftsOf("region", slice, scope)
	assert.True(t, ok)
	assert.Equal(t, []string{"a"}, values(region.shifts), "b falls, and c is 4% of the slice")
}

func values(shifts []Shift) []string {
	vs := make([]string, len(shifts))
	for i, s := range shifts {
		vs[i] = s.Value
	}
	return vs
}
