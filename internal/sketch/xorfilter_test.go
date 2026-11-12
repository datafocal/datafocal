package sketch

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/FastFilter/xorfilter"
)

// TestXorFilterNoFalseNegatives is the core correctness guarantee: every key in
// the set must probe positive, over many keys and several set sizes.
func TestXorFilterNoFalseNegatives(t *testing.T) {
	for _, size := range []int{0, 1, 2, 3, 10, 100, 1000, 50000} {
		keys := make([][]byte, size)
		for i := range keys {
			keys[i] = fmt.Appendf(nil, "key-%d-%x", i, i*2654435761)
		}
		f, err := NewXorFilter(keys)
		if err != nil {
			t.Fatalf("size %d: construct: %v", size, err)
		}
		for i, k := range keys {
			if !f.MayContain(string(k)) {
				t.Fatalf("size %d: false negative for key %d (%q)", size, i, k)
			}
		}
	}
}

func TestXorFilterFalsePositiveRate(t *testing.T) {
	const size = 100000
	keys := make([][]byte, size)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "member-%d", i)
	}
	f, err := NewXorFilter(keys)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	const trials = 100000
	fp := 0
	for i := range trials {
		if f.MayContain(fmt.Sprintf("absent-%d", i)) {
			fp++
		}
	}
	rate := float64(fp) / float64(trials)
	// xor8 targets ~1/256 ≈ 0.39%; allow generous slack against flakiness.
	if rate > 0.02 {
		t.Fatalf("false-positive rate = %.4f, want < 0.02", rate)
	}
}

// TestXorFilterMarshalRoundTrip: a filter decodes to one admitting its keys, whether this
// version encoded it or it is an xor8 filter of a segment sealed before binary fuse; a
// malformed one is an error.
func TestXorFilterMarshalRoundTrip(t *testing.T) {
	keys := make([][]byte, 5000)
	hashes := make([]uint64, len(keys))
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "k%d", i)
		hashes[i] = Hash64(keys[i])
	}
	f, err := NewXorFilter(keys)
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	current, err := f.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	xor8, err := xorfilter.Populate(hashes)
	if err != nil {
		t.Fatalf("construct xor8: %v", err)
	}
	v1 := binary.BigEndian.AppendUint64([]byte{1}, xor8.Seed)
	v1 = binary.BigEndian.AppendUint32(v1, xor8.BlockLength)
	v1 = binary.BigEndian.AppendUint32(v1, uint32(len(xor8.Fingerprints)))
	v1 = append(v1, xor8.Fingerprints...)
	miscounted := slices.Clone(current)
	binary.BigEndian.PutUint32(miscounted[13:], binary.BigEndian.Uint32(current[13:])+1)

	for _, tc := range []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{name: "binary fuse", data: current},
		{name: "xor8 of an older segment", data: v1},
		{name: "empty input", data: nil, wantErr: true},
		{name: "unknown version", data: append([]byte{3}, current[1:]...), wantErr: true},
		{name: "truncated", data: current[:len(current)-1], wantErr: true},
		{name: "segments not its fingerprints", data: miscounted, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var g XorFilter
			err := g.UnmarshalBinary(tc.data)
			if tc.wantErr {
				if err == nil {
					t.Fatal("want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			for _, k := range keys {
				if !g.MayContain(string(k)) {
					t.Fatalf("false negative after round-trip for %q", k)
				}
			}
			again, err := g.MarshalBinary()
			if err != nil || !bytes.Equal(again, tc.data) {
				t.Fatalf("re-marshal differs (err %v)", err)
			}
		})
	}
}
