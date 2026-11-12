package sketch

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/FastFilter/xorfilter"
)

// XorFilter is the sealed-segment membership filter for high-cardinality columns
// such as trace_id and user_id: a thin wrapper over
// github.com/FastFilter/xorfilter. At seal time the full key set is known, so a
// static filter (~1.13 bytes/key, ~0.4% false-positive rate, and — critically —
// zero false negatives) replaces the mutable live filter.
//
// It builds a binary fuse filter, whose construction holds ~24 bytes a key where
// xor8's holds ~55, as a merged hour's tens of millions of span ids would not fit
// a compactor's memory as xor8; it still reads the xor8 filters segments sealed
// before it.
//
// Keys are hashed with the engine's shared Hash64; a miss is definitive, a hit
// may be a rare false positive.
type XorFilter struct {
	f    *xorfilter.Xor8
	fuse *xorfilter.BinaryFuse8
}

// NewXorFilter builds a filter over the complete key set of a sealed column.
// Duplicate keys are tolerated (deduplicated by hash).
func NewXorFilter(keys [][]byte) (*XorFilter, error) {
	hashes := make([]uint64, len(keys))
	for i, k := range keys {
		hashes[i] = Hash64(k)
	}
	return NewXorFilterOfHashes(hashes)
}

// NewXorFilterOfHashes builds a filter over keys given by their Hash64, so a caller
// gathering many keys holds eight bytes a key rather than the key. Duplicates are
// tolerated, and the order of hashes does not change the filter. It sorts hashes.
func NewXorFilterOfHashes(hashes []uint64) (*XorFilter, error) {
	slices.Sort(hashes)
	hashes = slices.Compact(hashes)
	// An empty key set is a valid filter that admits nothing — the library
	// cannot build one, so represent it as a nil inner (MayContain ⇒ false).
	if len(hashes) == 0 {
		return &XorFilter{}, nil
	}
	f, err := xorfilter.PopulateBinaryFuse8(hashes)
	if err != nil {
		return nil, fmt.Errorf("sketch: build xor filter: %w", err)
	}
	return &XorFilter{fuse: f}, nil
}

// MayContain reports whether value might be in the set. False is definitive (no
// false negatives); true may be a ~0.4% false positive. An empty filter always
// returns false.
func (x *XorFilter) MayContain(value string) bool {
	switch {
	case x.fuse != nil:
		return x.fuse.Contains(Hash64([]byte(value)))
	case x.f != nil:
		return x.f.Contains(Hash64([]byte(value)))
	}
	return false
}

// MarshalBinary encodes the filter into the footer representation: version 2, a binary
// fuse filter, as version(1) + seed(8) + segmentLength(4) + segmentCount(4) + fpLen(4) +
// fingerprints; version 1, an xor8 filter read from an older segment, as version(1) +
// seed(8) + blockLength(4) + fpLen(4) + fingerprints. An empty filter is a version 2
// filter with no fingerprints.
func (x *XorFilter) MarshalBinary() ([]byte, error) {
	if x.f != nil {
		buf := make([]byte, 0, 17+len(x.f.Fingerprints))
		buf = append(buf, 1)
		buf = binary.BigEndian.AppendUint64(buf, x.f.Seed)
		buf = binary.BigEndian.AppendUint32(buf, x.f.BlockLength)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(x.f.Fingerprints)))
		return append(buf, x.f.Fingerprints...), nil
	}
	var fuse xorfilter.BinaryFuse8
	if x.fuse != nil {
		fuse = *x.fuse
	}
	buf := make([]byte, 0, 21+len(fuse.Fingerprints))
	buf = append(buf, 2)
	buf = binary.BigEndian.AppendUint64(buf, fuse.Seed)
	buf = binary.BigEndian.AppendUint32(buf, fuse.SegmentLength)
	buf = binary.BigEndian.AppendUint32(buf, fuse.SegmentCount)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(fuse.Fingerprints)))
	return append(buf, fuse.Fingerprints...), nil
}

// UnmarshalBinary decodes a filter produced by MarshalBinary, of either version.
func (x *XorFilter) UnmarshalBinary(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("sketch: xor filter empty")
	}
	header := map[byte]int{1: 17, 2: 21}[data[0]]
	if header == 0 {
		return fmt.Errorf("sketch: unsupported xor filter version %d", data[0])
	}
	if len(data) < header {
		return fmt.Errorf("sketch: xor filter header truncated (%d bytes)", len(data))
	}
	fpLen := binary.BigEndian.Uint32(data[header-4:])
	if len(data)-header != int(fpLen) {
		return fmt.Errorf("sketch: xor filter body wants %d bytes, got %d", fpLen, len(data)-header)
	}
	*x = XorFilter{}
	if fpLen == 0 {
		return nil // empty filter
	}
	fingerprints := slices.Clone(data[header:])
	seed := binary.BigEndian.Uint64(data[1:])
	if data[0] == 1 {
		x.f = &xorfilter.Xor8{Seed: seed, BlockLength: binary.BigEndian.Uint32(data[9:]), Fingerprints: fingerprints}
		return nil
	}
	length, count := binary.BigEndian.Uint32(data[9:]), binary.BigEndian.Uint32(data[13:])
	// A probe reads within count+2 segments of length, a power of two.
	if length == 0 || length&(length-1) != 0 || uint64(count+2)*uint64(length) != uint64(fpLen) {
		return fmt.Errorf("sketch: xor filter of %d segments of %d holds %d fingerprints", count, length, fpLen)
	}
	x.fuse = &xorfilter.BinaryFuse8{Seed: seed, SegmentLength: length, SegmentLengthMask: length - 1,
		SegmentCount: count, SegmentCountLength: count * length, Fingerprints: fingerprints}
	return nil
}

var (
	_ encoding.BinaryMarshaler   = (*XorFilter)(nil)
	_ encoding.BinaryUnmarshaler = (*XorFilter)(nil)
)
