package segment

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/value"
)

// groupCells is the rows times the columns they hold a Writer holds before it encodes them:
// a held cell and the text indexing keeps of it cost tens of bytes, an absent one nothing.
const groupCells = 1 << 20

// Writer encodes a segment a row group at a time, holding one group's rows rather than the
// segment's, so its memory does not grow with the rows written.
type Writer interface {
	// Append adds a row as Builder.Append does.
	Append(record Record, parsed ParsedPattern) error
	// Close returns the segment and its descriptor, profiled as profile.
	Close(profile PatternProfile) ([]byte, Descriptor, error)
}

// streamWriter sorts each group's rows by service and folds the group's indexes into the
// footer: sketches merged, value counts summed, and filters built from the keys kept.
type streamWriter struct {
	b *Builder
	// cells is the rows times the columns they hold kept before they are encoded, groupCells but in tests.
	cells  int
	cols   []column
	types  map[string]value.Type
	body   []byte
	footer Footer
	// keys is the hashes of each string column's values, nil once one holds a space and so
	// cannot be filtered, and rows the rows holding one: a merge's hour of request ids would
	// hold gigabytes as strings.
	keys map[string]*hashSet
	rows map[string]int
	// uncounted and undistributed are the columns some group did not count or whose
	// distribution it dropped, which the whole segment then does not count or keep either.
	uncounted, undistributed map[string]bool
}

// NewWriter returns a Writer of rows whose columns have the given types.
func NewWriter(cfg BuilderConfig, types map[string]value.Type) Writer {
	w := &streamWriter{b: NewBuilder(cfg), cells: groupCells, types: types, keys: map[string]*hashSet{}, rows: map[string]int{},
		uncounted: map[string]bool{}, undistributed: map[string]bool{}}
	for _, name := range slices.Sorted(maps.Keys(types)) {
		w.cols = append(w.cols, column{name: name, typ: types[name]})
	}
	return w
}

func (w *streamWriter) Append(record Record, parsed ParsedPattern) error {
	if err := w.b.Append(record, parsed); err != nil {
		return err
	}
	if len(w.b.rows.ts)*(len(w.b.rows.cols)+1) < w.cells {
		return nil
	}
	return w.flush()
}

func (w *streamWriter) Close(profile PatternProfile) ([]byte, Descriptor, error) {
	if err := w.flush(); err != nil {
		return nil, Descriptor{}, err
	}
	f := &w.footer
	for service, v := range f.services {
		for name, counts := range v.valueCounts {
			if len(counts) > maxCountedValues || w.uncounted[name] {
				delete(v.valueCounts, name)
				delete(f.valueCounts, name)
			}
		}
		maps.DeleteFunc(v.Distributions, func(name string, _ *sketch.DDSketch) bool { return w.undistributed[name] })
		v.valuePairs = countedPairs(v.valuePairs, v.valueCounts)
		f.services[service] = v
	}
	maps.DeleteFunc(f.valueCounts, func(name string, _ map[string]uint64) bool { return w.uncounted[name] })
	maps.DeleteFunc(f.Distributions, func(name string, _ *sketch.DDSketch) bool { return w.undistributed[name] })
	filters := map[string][]byte{}
	for _, name := range slices.Sorted(maps.Keys(w.keys)) {
		section, err := keysFilter(name, w.keys[name], w.rows[name])
		if err != nil {
			return nil, Descriptor{}, err
		}
		if section != nil {
			filters[name] = section
		}
		delete(w.keys, name) // so the next column's filter is built without this one's hashes
	}
	f.PatternProfile = profile
	body, err := sealBody(w.body, *f, filters)
	return body, w.b.descriptor(f.ZoneMaps, slices.Sorted(maps.Keys(f.services))), err
}

// flush encodes the rows held as row groups and folds their indexes into the footer.
func (w *streamWriter) flush() error {
	t := w.b.rows
	w.b.rows = table{}
	t.finish(w.types, true)
	cols, zm := slices.Clone(w.cols), t.zoneMaps(0, len(t.ts))
	texts := textsOf(cols, &t)
	group, _, err := buildFooter(cols, &t, texts, zm, w.b.patterns)
	if err != nil {
		return err
	}
	w.note(cols, texts, group)
	if err := w.fold(group); err != nil {
		return err
	}
	w.body = appendRowGroups(w.body, &w.footer, cols, &t, zm, targetRowGroupBytes)
	return nil
}

// note records the columns a group left uncounted or undistributed and keeps its keys.
func (w *streamWriter) note(cols []column, texts []columnTexts, group Footer) {
	for i, c := range cols {
		if _, ok := group.valueCounts[c.name]; !ok && c.typ == value.TypeString {
			w.uncounted[c.name] = true
		}
		if group.Distributions[c.name] == nil && c.typ.Numeric() && group.ZoneMaps[c.name].PresentRows > 0 {
			w.undistributed[c.name] = true
		}
		if keys, ok := w.keys[c.name]; c.typ == value.TypeString && (!ok || keys != nil) {
			var rows int
			w.keys[c.name], rows = keysOf(keys, texts[i].texts)
			w.rows[c.name] += rows
		}
	}
}

// keysOf adds the hashes of vals' non-empty values to keys, nil if one holds a space, and is
// the rows holding one.
func keysOf(keys *hashSet, vals []string) (*hashSet, int) {
	if keys == nil {
		keys = &hashSet{}
	}
	rows := 0
	for _, v := range vals {
		if strings.Contains(v, " ") {
			return nil, 0
		}
		if v != "" {
			keys.add(sketch.Hash64([]byte(v)))
			rows++
		}
	}
	return keys, rows
}

// hashSet is distinct hashes held as a slice deduplicated each time it fills, then grown by
// its distinct count, so it holds a hash in 8 to 16 bytes where a map takes 35: a merged
// hour of spans has tens of millions of ids.
type hashSet []uint64

func (s *hashSet) add(h uint64) {
	if len(*s) == cap(*s) && len(*s) > 0 {
		distinct := s.distinct()
		*s = slices.Grow(distinct, len(distinct))
	}
	*s = append(*s, h)
}

// distinct deduplicates s and returns its hashes, sorted.
func (s *hashSet) distinct() []uint64 {
	slices.Sort(*s)
	*s = slices.Compact(*s)
	return *s
}

// fold adds a group's footer to the footer so far.
func (w *streamWriter) fold(group Footer) error {
	f := &w.footer
	if f.Patterns == nil {
		*f = group
		f.ZoneMaps = maps.Clone(group.ZoneMaps) // the group's own, which merge must not widen
		return nil
	}
	f.Rows += group.Rows
	f.ZoneMaps.merge(group.ZoneMaps)
	maps.Copy(f.Patterns, group.Patterns)
	maps.Copy(f.patternTexts, group.patternTexts)
	for key, n := range group.ServicePatternCounts {
		f.ServicePatternCounts[key] += n
	}
	for service, v := range group.services {
		cur, ok := f.services[service]
		if !ok {
			f.services[service] = v
			continue
		}
		cur.Rows += v.Rows
		if err := foldIndexes(&cur, v); err != nil {
			return err
		}
		if err := foldFieldStats(cur.fields, v.fields); err != nil {
			return err
		}
		if cur.valuePairs == nil && len(v.valuePairs) > 0 {
			cur.valuePairs = map[ValuePair]uint64{}
		}
		for p, n := range v.valuePairs {
			cur.valuePairs[p] += n
		}
		f.services[service] = cur
	}
	return foldIndexes(f, group)
}

// foldIndexes merges g's distinct counts and distributions into f's and adds its value counts.
func foldIndexes(f *Footer, g Footer) error {
	for name, values := range g.DistinctCounts {
		if f.DistinctCounts[name] == nil {
			f.DistinctCounts[name] = values
		} else if err := f.DistinctCounts[name].Merge(values); err != nil {
			return fmt.Errorf("segment: merge distinct counts of %s: %w", name, err)
		}
	}
	for name, dist := range g.Distributions {
		if f.Distributions[name] == nil {
			f.Distributions[name] = dist
		} else if err := f.Distributions[name].Merge(dist); err != nil {
			return fmt.Errorf("segment: merge distribution of %s: %w", name, err)
		}
	}
	for name, counts := range g.valueCounts {
		if f.valueCounts[name] == nil {
			f.valueCounts[name] = counts
			continue
		}
		for v, n := range counts {
			f.valueCounts[name][v] += n
		}
	}
	return nil
}

// foldFieldStats adds g's field stats, one row group's of a service, into f's.
func foldFieldStats(f, g map[string]FieldStats) error {
	for name, s := range g {
		cur, ok := f[name]
		if !ok {
			f[name] = s
			continue
		}
		if err := cur.Distinct.Merge(s.Distinct); err != nil {
			return fmt.Errorf("segment: merge field stats of %s: %w", name, err)
		}
		cur.Values += s.Values
		cur.Text = cur.Text || s.Text
		f[name] = cur
	}
	return nil
}

// countedPairs is the pairs of a whole segment's groups whose columns it still counts, less
// a column pair past maxCountedValues pairs, as sealing its rows at once leaves out.
func countedPairs(pairs map[ValuePair]uint64, counted map[string]map[string]uint64) map[ValuePair]uint64 {
	perColumns := map[[2]string]int{}
	for p := range pairs {
		perColumns[p.Columns]++
	}
	maps.DeleteFunc(pairs, func(p ValuePair, _ uint64) bool {
		return counted[p.Columns[0]] == nil || counted[p.Columns[1]] == nil || perColumns[p.Columns] > maxCountedValues
	})
	if len(pairs) == 0 {
		return nil
	}
	return pairs
}
