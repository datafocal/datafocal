package series

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
)

// binKey keys a row by its group_by value and its bin, and notes in multiplied when a
// row went under more than one key. position, when set, is the position the row being
// keyed bins by in place of its time.
type binKey struct {
	column     string
	bins       Bins
	multiplied *bool
	position   *int64
}

func newBinKey(req Request) binKey {
	k := binKey{column: groupByColumn(req.GroupBy), bins: req.Bins, multiplied: new(bool)}
	if req.OrdinalOf != "" {
		k.position = new(int64)
	}
	return k
}

// each passes add the row's bin under each of its keys, one per distinct element of a
// list column.
func (k binKey) each(r scan.Row, add func(key string, bin int64)) {
	bin := k.bin(r)
	if _, list := r.List(k.column); !list {
		// A scalar's one key read as it is, since a key slice a row each measure was a scan's
		// allocations.
		text, _ := r.Text(k.column)
		add(text, bin)
		return
	}
	keys := r.Keys(k.column)
	if len(keys) > 1 {
		*k.multiplied = true
	}
	for _, key := range keys {
		add(key, bin)
	}
}

// bin is the bin the row falls in, by its position when the key bins by one.
func (k binKey) bin(r scan.Row) int64 {
	if k.position != nil {
		return k.bins.BinStart(time.UnixMilli(*k.position))
	}
	return k.bins.BinStart(r.Time)
}

// eachValue passes add each value of field, or its length, with the key and bin it goes
// under: a list element paired by index with a sibling group_by list's element.
func (k binKey) eachValue(r scan.Row, field, length string, add func(key string, bin int64, text string)) {
	values, list := r.Values(field)
	if len(values) == 0 {
		return
	}
	if keys, paired := k.pairedKeys(r, field, list, len(values)); paired {
		bin := k.bin(r)
		for i, v := range values {
			if v != "" {
				add(keys[i], bin, measured(v, length))
			}
		}
		return
	}
	k.each(r, func(key string, bin int64) {
		for _, v := range values {
			if v != "" || !list {
				add(key, bin, measured(v, length))
			}
		}
	})
}

// pairedKeys is the row's group_by list when field is a sibling list of n elements.
func (k binKey) pairedKeys(r scan.Row, field string, list bool, n int) ([]string, bool) {
	keys, paired := r.List(k.column)
	return keys, list && paired && len(keys) == n && siblings(field, k.column)
}

// siblings reports whether columns a and b are keys of one object path.
func siblings(a, b string) bool {
	i, j := strings.LastIndexByte(a, '.'), strings.LastIndexByte(b, '.')
	return i > 0 && i == j && a[:i] == b[:j]
}

// measured is text, or its length in characters or bytes when length names one.
func measured(text, length string) string {
	switch length {
	case "len":
		return strconv.Itoa(utf8.RuneCountInString(text))
	case "bytes":
		return strconv.Itoa(len(text))
	}
	return text
}

// groupByColumn is the column a group_by reads, pattern_id when none is named.
func groupByColumn(groupBy string) string {
	if groupBy == "" {
		return segment.ColPatternID
	}
	return groupBy
}

// binTotals totals a row count, or a sum over field when one is named, per (key, bin),
// and for a ratio the rows numerator matches into matched.
type binTotals struct {
	binKey
	field     string
	length    string
	rate      bool
	numerator []selector.ParsedPredicate
	all       Values
	matched   Values
}

func (m binTotals) observe(r scan.Row) error {
	matched := m.matched != nil && r.Matches(m.numerator)
	add := func(key string, bin int64, v float64) {
		addTo(m.all, key, bin, v)
		if matched {
			addTo(m.matched, key, bin, v)
		}
	}
	if m.field == "" {
		m.each(r, func(key string, bin int64) { add(key, bin, 1) })
		return nil
	}
	m.eachValue(r, m.field, m.length, func(key string, bin int64, text string) {
		if v, finite := parseFinite(text); finite {
			add(key, bin, v)
		}
	})
	return nil
}

func (m binTotals) result() Result {
	if m.rate {
		divideByWidth(m.all, m.bins.Width)
	}
	return Result{Values: m.all}
}

func (m binTotals) countSegment(d segment.Descriptor, key string, bin int64) bool {
	if m.field != "" {
		return false
	}
	matched := true
	for _, p := range m.numerator {
		v, ok := plan.SegmentValue(d, p.Field)
		if !ok {
			return false
		}
		matched = matched && p.Matches(v, true)
	}
	addTo(m.all, key, bin, float64(d.Rows))
	if m.matched != nil && matched {
		addTo(m.matched, key, bin, float64(d.Rows))
	}
	return true
}

// binDistinct folds each row's raw value into its bin's own sketch.
type binDistinct struct {
	binKey
	field    string
	sketches map[string]map[int64]*sketch.HLL
}

func (m binDistinct) observe(r scan.Row) error {
	raw, ok := r.Value(m.field)
	if !ok {
		return nil
	}
	m.each(r, func(key string, bin int64) {
		binSketch(m.sketches, key, bin, sketch.NewHLL).Add(raw)
	})
	return nil
}

func (m binDistinct) result() Result {
	return Result{Values: reduceBins(m.sketches, (*sketch.HLL).Estimate), Approx: true}
}

// binQuantiles gathers each bin's values to sort for an exact quantile.
type binQuantiles struct {
	binKey
	field     string
	length    string
	quantile  float64
	values    map[string]map[int64][]float64
	collected int64
}

// observe reads a scalar field under a scalar key straight off the row, unformatted.
func (m *binQuantiles) observe(r scan.Row) error {
	_, fieldList := r.List(m.field)
	if _, keyList := r.List(m.column); m.length == "" && !fieldList && !keyList {
		v, finite := r.Number(m.field)
		key, _ := r.Text(m.column)
		return m.collect(key, m.bin(r), v, finite)
	}
	var err error
	m.eachValue(r, m.field, m.length, func(key string, bin int64, text string) {
		if err == nil {
			v, finite := parseFinite(text)
			err = m.collect(key, bin, v, finite)
		}
	})
	return err
}

// collect appends a finite v under key and bin, refusing past maxCollectedValues.
func (m *binQuantiles) collect(key string, bin int64, v float64, finite bool) error {
	if !finite {
		return nil
	}
	if m.collected++; m.collected > maxCollectedValues {
		return query.NewAPIError(query.ErrCodeScanTooLarge,
			fmt.Sprintf("aggregate: an exact quantile over more than %d values does not fit in memory",
				maxCollectedValues)).
			WithHint("narrow the time window or the selector, or ask for the sketch quantile by dropping the row-level predicate and group_by")
	}
	appendTo(m.values, key, bin, v)
	return nil
}

// QuantileOf is the exact nearest-rank quantile, rank ceil(q*n) of the sorted values,
// and 0 for none.
func QuantileOf(vals []float64, q float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sort.Float64s(vals)
	rank := int(math.Ceil(q * float64(len(vals))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(vals) {
		rank = len(vals)
	}
	return vals[rank-1]
}

// parseFinite reads a numeric column value, rejecting anything that is not a
// finite number so those rows drop out of the sum.
func parseFinite(text string) (float64, bool) {
	f, err := strconv.ParseFloat(text, 64)
	if err != nil || !isFinite(f) {
		return 0, false
	}
	return f, true
}

// addTo increments the (key, bin) value by v, allocating maps as needed.
func addTo(values Values, key string, bin int64, v float64) {
	if values[key] == nil {
		values[key] = map[int64]float64{}
	}
	values[key][bin] += v
}

// appendTo appends v to the (key, bin) samples, allocating maps as needed.
func appendTo(samples map[string]map[int64][]float64, key string, bin int64, v float64) {
	if samples[key] == nil {
		samples[key] = map[int64][]float64{}
	}
	samples[key][bin] = append(samples[key][bin], v)
}

// sample is one row's value of a sequence measure's column, a number where the column is
// numeric.
type sample struct {
	at      int64
	text    string
	num     float64
	numeric bool
}

func (m *binQuantiles) result() Result {
	return Result{Values: reduceBins(m.values, func(v []float64) float64 { return QuantileOf(v, m.quantile) })}
}

// binSequences gathers each bin's values to reduce in time order, a folded kind's only its answer.
type binSequences struct {
	binKey
	kind      Kind
	field     string
	length    string
	samples   map[string]map[int64][]sample
	collected int64
}

func (m *binSequences) observe(r scan.Row) error {
	// A column the segment keeps no zone map of is a derived one, whose values are numbers.
	zm, stored := r.Segment.ZoneMaps[m.field]
	numeric := m.length != "" || zm.Type.Numeric() || !stored
	if _, keyList := r.List(m.column); m.kind.folded() && m.length == "" && !keyList &&
		(m.field == segment.ColTimestamp || numeric && !zm.Type.Repeated()) {
		return m.observeNumber(r)
	}
	var err error
	m.eachValue(r, m.field, m.length, func(key string, bin int64, text string) {
		if err != nil || text == "" {
			return
		}
		if s, ok := m.sampleOf(r, text, numeric); ok {
			err = m.keep(key, bin, s)
		}
	})
	return err
}

// observeNumber reads a scalar number or time under a scalar key straight off the row,
// unformatted, as binQuantiles does, since a folded kind keeps no text of a number.
func (m *binSequences) observeNumber(r scan.Row) error {
	s := sample{at: r.Time.UnixNano(), num: float64(r.Time.UnixMilli()), numeric: true}
	if m.field != segment.ColTimestamp {
		var ok bool
		if s.num, ok = r.Number(m.field); !ok {
			return nil
		}
	}
	key, _ := r.Text(m.column)
	return m.keep(key, m.bin(r), s)
}

// keep adds s to the samples of key's bin, folding it into those kept where the kind folds.
func (m *binSequences) keep(key string, bin int64, s sample) error {
	kept, held := m.samples[key][bin]
	if held && m.kind.folded() {
		m.samples[key][bin] = fold(m.kind, kept, s)
		return nil
	}
	s.text = strings.Clone(s.text)
	if m.collected++; m.collected > maxCollectedValues {
		return query.NewAPIError(query.ErrCodeScanTooLarge,
			fmt.Sprintf("aggregate: a sequence measure over more than %d values does not fit in memory", maxCollectedValues)).
			WithHint("narrow the time window or the selector")
	}
	if m.samples[key] == nil {
		m.samples[key] = map[int64][]sample{}
	}
	m.samples[key][bin] = append(m.samples[key][bin], s)
	return nil
}

// sampleOf is r's value text as a sample, numeric if the field is, false if a numeric field's
// text is no finite number.
func (m *binSequences) sampleOf(r scan.Row, text string, numeric bool) (sample, bool) {
	s := sample{at: r.Time.UnixNano(), text: text}
	if m.field == segment.ColTimestamp {
		s.num, s.numeric = float64(r.Time.UnixMilli()), true
	} else if numeric {
		if s.num, s.numeric = parseFinite(text); !s.numeric {
			return s, false
		}
	}
	return s, true
}

func (m *binSequences) result() Result {
	res := Result{Values: Values{}, Labels: Labels{}}
	for key, bins := range m.samples {
		res.Labels[key] = map[int64]string{}
		for bin, samples := range bins {
			v, label := reduceSequence(m.kind, samples)
			addTo(res.Values, key, bin, v)
			res.Labels[key][bin] = label
		}
	}
	return res
}

// folded reports whether k keeps only the samples it reduces to, so it holds groups, not rows.
func (k Kind) folded() bool {
	return k == KindFirst || k == KindLast || k == KindMin || k == KindMax || k == KindSpan
}

// fold is kept, the samples kind keeps for a bin, with s observed as reduceSequence would.
func fold(kind Kind, kept []sample, s sample) []sample {
	if kind == KindSpan {
		return foldSpan(kept, s)
	}
	if wins(kind, inTimeOrder(s, kept[0]), compareSamples(s, kept[0])) {
		kept[0] = owned(s)
	}
	return kept
}

// wins reports whether a sample ordered order by time and c by value replaces the kept one.
func wins(kind Kind, order, c int) bool {
	switch kind {
	case KindFirst:
		return order < 0
	case KindLast:
		return order > 0
	case KindMin:
		return c < 0 || c == 0 && order < 0
	}
	return c > 0 || c == 0 && order < 0
}

// foldSpan is a span's earliest and latest samples with s observed.
func foldSpan(kept []sample, s sample) []sample {
	if len(kept) == 1 {
		kept = append(kept, kept[0])
	}
	if inTimeOrder(s, kept[0]) < 0 {
		kept[0] = owned(s)
	}
	if inTimeOrder(s, kept[1]) > 0 {
		kept[1] = owned(s)
	}
	return kept
}

// owned is s holding its own copy of its text.
func owned(s sample) sample {
	s.text = strings.Clone(s.text)
	return s
}

// inTimeOrder orders samples as reduceSequence reads them: by time, ties by value.
func inTimeOrder(a, b sample) int {
	return cmp.Or(cmp.Compare(a.at, b.at), compareSamples(a, b))
}

// reduceSequence is kind over samples in time order, ties broken by value: a number, or
// a string column's value as the label.
func reduceSequence(kind Kind, samples []sample) (float64, string) {
	slices.SortFunc(samples, inTimeOrder)
	switch kind {
	case KindFirst:
		return samples[0].point()
	case KindLast:
		return samples[len(samples)-1].point()
	case KindMin:
		return slices.MinFunc(samples, compareSamples).point()
	case KindMax:
		return slices.MaxFunc(samples, compareSamples).point()
	case KindChanges:
		runs := slices.CompactFunc(slices.Clone(samples), func(a, b sample) bool { return a.text == b.text })
		return float64(len(runs) - 1), ""
	case KindSpan:
		return float64(samples[len(samples)-1].at-samples[0].at) / 1e6, ""
	}
	return longestRun(samples)
}

// longestRun is the length and value of the earliest longest run of one value.
func longestRun(samples []sample) (float64, string) {
	best, value, run := 0, "", 0
	for i, s := range samples {
		if run++; i > 0 && s.text != samples[i-1].text {
			run = 1
		}
		if run > best {
			best, value = run, s.text
		}
	}
	return float64(best), value
}

func compareSamples(a, b sample) int {
	if a.numeric && b.numeric {
		return cmp.Compare(a.num, b.num)
	}
	return strings.Compare(a.text, b.text)
}

func (s sample) point() (float64, string) {
	if s.numeric {
		return s.num, ""
	}
	return 0, s.text
}

// ordinal holds a measure's rows back until the scan is done, then folds each entity's
// in time order, each binned by how many of the entity's rows up to it where matches and
// left out while none has.
type ordinal struct {
	measure
	of       string
	where    []selector.ParsedPredicate
	position *int64
	rows     []scan.Row
}

// ordered is m binned by position when key is, else m.
func ordered(req Request, key binKey, m measure) measure {
	if key.position == nil {
		return m
	}
	return &ordinal{measure: m, of: req.OrdinalOf, where: selector.ParsePredicates(req.OrdinalWhere), position: key.position}
}

func (o *ordinal) observe(r scan.Row) error {
	if _, ok := r.Text(o.of); ok {
		o.rows = append(o.rows, r)
	}
	return nil
}

// replay folds the held rows into the measure, a tie in time kept in scan order.
func (o *ordinal) replay() error {
	entity := func(r scan.Row) string {
		v, _ := r.Text(o.of)
		return v
	}
	slices.SortStableFunc(o.rows, func(a, b scan.Row) int {
		return cmp.Or(strings.Compare(entity(a), entity(b)), a.Time.Compare(b.Time))
	})
	for i, r := range o.rows {
		if i == 0 || entity(r) != entity(o.rows[i-1]) {
			*o.position = 0
		}
		if r.Matches(o.where) {
			*o.position++
		}
		if *o.position == 0 {
			continue
		}
		if err := o.measure.observe(r); err != nil {
			return err
		}
	}
	return nil
}
