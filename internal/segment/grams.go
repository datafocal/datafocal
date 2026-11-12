package segment

import (
	"bytes"
	"context"
	"fmt"

	"github.com/datafocal/datafocal/internal/sketch"
)

// GramsSection cannot collide with a column name.
const GramsSection = "\x00grams"

// Grams holds each row group's message membership filter, nil for an unfiltered group.
type Grams [][]byte

// Admits rejects only a definitive miss; old segments and short needles pass through.
func (g Grams) Admits(group int, needle string) bool {
	if group >= len(g) || g[group] == nil || len(needle) < 3 {
		return true
	}
	filter := g[group]
	for _, b := range []byte(needle) {
		if filter[(b%16)/8]&(1<<(b%8)) == 0 {
			return false
		}
	}
	for i := 0; i+3 <= len(needle); i++ {
		bit := sketch.Hash64([]byte(needle[i:i+3])) % uint64(8*(len(filter)-2))
		if filter[2+bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// Keep one bit set across collections; sync.Pool drops it under sustained GC pressure.
var gramBits = make(chan *[1 << 18]uint64, 1)

func takeGramBits() *[1 << 18]uint64 {
	select {
	case bits := <-gramBits:
		return bits
	default:
		return new([1 << 18]uint64)
	}
}

func buildGrams(messages []string) []byte {
	bits := takeGramBits()
	positions := make([]uint32, 0, 4096)
	defer func() {
		for _, p := range positions {
			bits[p/64] &^= uint64(1) << (p % 64)
		}
		select {
		case gramBits <- bits:
		default:
		}
	}()
	for _, message := range messages {
		for i := 0; i+3 <= len(message); i++ {
			p := uint32(message[i])<<16 | uint32(message[i+1])<<8 | uint32(message[i+2])
			mask := uint64(1) << (p % 64)
			if bits[p/64]&mask != 0 {
				continue
			}
			bits[p/64] |= mask
			positions = append(positions, p)
			if len(positions) > 65536 {
				return nil
			}
		}
	}
	// One hash and 1.5 bits per key trade individual hits for a compact substring filter.
	filter := make([]byte, 2+max(1, (3*len(positions)+15)/16))
	for _, p := range positions {
		key := [3]byte{byte(p >> 16), byte(p >> 8), byte(p)}
		mask := uint16(1)<<(key[0]%16) | uint16(1)<<(key[1]%16) | uint16(1)<<(key[2]%16)
		filter[0], filter[1] = filter[0]|byte(mask), filter[1]|byte(mask>>8)
		bit := sketch.Hash64(key[:]) % uint64(8*(len(filter)-2))
		filter[2+bit/8] |= 1 << (bit % 8)
	}
	return filter
}

// Each group is a two-byte hashed byte-presence mask followed by its trigram Bloom bitmap.
func encodeGrams(grams Grams) []byte {
	var buf bytes.Buffer
	buf.WriteByte(1)
	putU32(&buf, uint32(len(grams)))
	for _, filter := range grams {
		putBytes(&buf, filter)
	}
	return buf.Bytes()
}

func (r storeReader) Grams(ctx context.Context, loc ObjectLocation, footer Footer) (Grams, Cost, error) {
	br, ok := footer.filters[GramsSection]
	if !ok {
		return nil, Cost{}, nil
	}
	raw, err := r.store.GetRange(ctx, loc.Key, br.Offset, br.Length)
	cost := Cost{GETs: 1, Bytes: br.Length}
	if err != nil {
		return nil, cost, fmt.Errorf("segment: read grams: %w", err)
	}
	c := cursor{b: raw}
	if version := c.byte(); version != 1 {
		return nil, cost, &VersionError{Object: "grams", Version: version}
	}
	grams := make(Grams, c.count())
	if len(grams) != footer.RowGroups {
		return nil, cost, fmt.Errorf("segment: grams group count mismatch")
	}
	for i := range grams {
		data := c.bytes()
		if len(data) == 0 {
			continue
		}
		if len(data) < 3 {
			return nil, cost, fmt.Errorf("segment: truncated gram bitmap")
		}
		grams[i] = data
	}
	if c.err == nil && c.off != len(raw) {
		c.err = fmt.Errorf("segment: trailing grams bytes")
	}
	return grams, cost, c.err
}
