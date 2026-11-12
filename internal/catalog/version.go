package catalog

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// pin is what a version holds: each writer's first commits, as many as its count, and for writer
// "" the v1 sequence's first descriptors, which every replica that has listed that far holds.
type pin map[string]int

func (p pin) holds(o origin) bool { return o.Seq < p[o.Writer] }

// exceeds reports whether p holds more of some writer's commits than q.
func (p pin) exceeds(q pin) bool {
	for w, n := range p {
		if n > q[w] {
			return true
		}
	}
	return false
}

// versionOf is the v1 count, as versions were, then with writers a base64 of uvarints: the table's
// length and counts in its order, then each other writer's name length, name and count.
func versionOf(table []string, counts map[string]int) Version {
	named := maps.Clone(counts)
	delete(named, "")
	b := binary.AppendUvarint(nil, uint64(len(table)))
	for _, w := range table {
		b = binary.AppendUvarint(b, uint64(counts[w]))
		delete(named, w)
	}
	for _, w := range slices.Sorted(maps.Keys(named)) {
		b = binary.AppendUvarint(append(binary.AppendUvarint(b, uint64(len(w))), w...), uint64(counts[w]))
	}
	if len(b) == 1 {
		return Version(strconv.Itoa(counts[""]))
	}
	return Version(strconv.Itoa(counts[""]) + "-" + base64.RawURLEncoding.EncodeToString(b))
}

// parse reads v back: the counts by name, and those of the writers a table indexes in its order.
func (v Version) parse() (pin, []int, error) {
	head, body, _ := strings.Cut(string(v), "-")
	v1, err := strconv.Atoi(head)
	b, bodyErr := base64.RawURLEncoding.DecodeString(body)
	p, r, indexed := pin{"": v1}, bytes.NewReader(b), []int(nil)
	next := func() int {
		x, e := binary.ReadUvarint(r)
		err = cmp.Or(err, e)
		return int(x)
	}
	n := 0
	if len(b) > 0 {
		n = next()
	}
	for err == nil && len(indexed) < n {
		indexed = append(indexed, next())
	}
	for err == nil && r.Len() > 0 {
		name := make([]byte, min(next(), r.Len()+1))
		_, e := io.ReadFull(r, name)
		err = cmp.Or(err, e)
		p[string(name)] = next()
	}
	if err != nil || bodyErr != nil || v1 < 0 {
		return nil, nil, fmt.Errorf("catalog: version %q is malformed", v)
	}
	return p, indexed, nil
}

// total is the commits a version holds, by which a newer version of one catalog holds more.
func (v Version) total() int {
	p, indexed, err := v.parse()
	if err != nil {
		return -1
	}
	return sum(maps.Values(p)) + sum(slices.Values(indexed))
}

func sum(counts iter.Seq[int]) int {
	n := 0
	for c := range counts {
		n += c
	}
	return n
}
