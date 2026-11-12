// Package compact merges each complete stream-hour's segments into one that supersedes
// them, so a long window reads a segment an hour rather than one per flush, and each settled
// window of an hour not yet complete into one the hour's merge later supersedes.
package compact

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/internal/value"
)

// Compactor merges segments.
type Compactor interface {
	// Run merges every complete stream-hour, and every complete window of an hour that is not,
	// with more than one live descriptor, once each of them was created before settled, past any
	// whose merge fails, reporting those, then folds the catalog into a snapshot.
	Run(ctx context.Context, settled time.Time) error
}

type compactor struct {
	cat         catalog.Catalog
	snapshotter catalog.Snapshotter
	rd          segment.Reader
	store       storage.Store
	cfg         Config
}

// Config is whose segments a pass merges, how far back it looks, and what each rollup holds.
type Config struct {
	Tenant string
	// Lookback is how far before settled a pass looks for hours to merge; 0 looks at every hour.
	Lookback time.Duration
	// Window is the span of an incomplete hour a pass merges once it is complete, dividing an
	// hour; 0 merges only whole hours.
	Window time.Duration
	// Summary is the querier's summary config, whose bin width and sketch columns a rollup is built for.
	Summary summary.Config
}

// New returns a Compactor of tenant's segments looking back over every hour, with rollups for
// the default summary config.
func New(cat catalog.Catalog, snapshotter catalog.Snapshotter, rd segment.Reader, store storage.Store, tenant string) Compactor {
	return NewWithConfig(cat, snapshotter, rd, store, Config{Tenant: tenant})
}

// NewWithConfig returns a Compactor reading segments through rd, writing the merged ones and
// their rollups to store and snapshotting cat through snapshotter.
func NewWithConfig(cat catalog.Catalog, snapshotter catalog.Snapshotter, rd segment.Reader, store storage.Store, cfg Config) Compactor {
	return compactor{cat: cat, snapshotter: snapshotter, rd: rd, store: store, cfg: cfg}
}

// span is one stream's descriptors starting in one hour, or in one window of an hour not
// yet complete.
type span struct {
	stream string
	start  int64
	width  time.Duration
}

func (c compactor) Run(ctx context.Context, settled time.Time) error {
	v, err := c.cat.Version(ctx)
	if err != nil {
		return fmt.Errorf("compact: catalog version: %w", err)
	}
	var sel selector.Resolved
	if c.cfg.Lookback > 0 { // from an hour's start, so no hour is merged in part
		sel.Window.From = settled.Add(-c.cfg.Lookback).Truncate(time.Hour)
	}
	descs, err := c.cat.Prune(ctx, v, sel)
	if err != nil {
		return fmt.Errorf("compact: prune: %w", err)
	}
	groups, newest := c.spans(descs)
	// Newest first, so a run killed on a merge, as an OOM kill is, costs only that span.
	keys := slices.SortedFunc(maps.Keys(groups), func(a, b span) int {
		return cmp.Or(cmp.Compare(b.start, a.start), cmp.Compare(a.stream, b.stream))
	})
	var failed []error
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failed, err)...)
		}
		group, start := groups[key], time.Unix(key.start, 0).UTC()
		if !mergeable(group, start.Add(key.width), newest[key.stream], settled) {
			continue
		}
		tier := segment.TierHour
		if key.width < time.Hour {
			tier = segment.TierWindow
		}
		if err := c.mergeDecodable(ctx, group, tier); err != nil {
			slog.Default().Error("merge failed", "service", group[0].Stream.Service, "stream", key.stream, "start", start, "tier", tier, "inputs", len(group), "error", err)
			failed = append(failed, fmt.Errorf("compact: %s %s %s: %w", group[0].Stream.Service, tier, start.Format(time.RFC3339), err))
		}
	}
	if err := c.snapshotter.Snapshot(ctx); err != nil {
		failed = append(failed, fmt.Errorf("compact: snapshot: %w", err))
	}
	return errors.Join(failed...)
}

// spans groups descs by stream and hour, or by window of an hour no later segment of its stream
// completes yet, and is each stream's newest segment start.
func (c compactor) spans(descs []segment.Descriptor) (map[span][]segment.Descriptor, map[string]time.Time) {
	newest := map[string]time.Time{}
	for _, d := range descs {
		if stream := d.Stream.Fingerprint(); d.TimeRange.Min.After(newest[stream]) {
			newest[stream] = d.TimeRange.Min
		}
	}
	groups := map[span][]segment.Descriptor{}
	for _, d := range descs {
		key := span{stream: d.Stream.Fingerprint(), width: time.Hour}
		if hour := d.TimeRange.Min.Truncate(time.Hour); c.cfg.Window > 0 && newest[key.stream].Before(hour.Add(time.Hour)) {
			key.width = c.cfg.Window
		}
		key.start = d.TimeRange.Min.Truncate(key.width).Unix()
		groups[key] = append(groups[key], d)
	}
	return groups, newest
}

// mergeable reports whether a span's segments are several, complete because a later one
// of the stream starts past the span, and each created before settled, by when a summary
// refreshing well inside that margin has counted them.
func mergeable(group []segment.Descriptor, end, newest, settled time.Time) bool {
	return len(group) > 1 && !newest.Before(end) &&
		!slices.ContainsFunc(group, func(d segment.Descriptor) bool { return !d.CreatedAt.Before(settled) })
}

// maxInputGroupCells is the largest row group, in rows times columns, a merge decodes. A
// segment written before row groups were cut by cells can hold one that decodes to more
// memory than the compactor has, as a 210,000-row group did on the factory.
var maxInputGroupCells = 2 * segment.MaxGroupCells

// mergeDecodable merges group less each segment with a row group past maxInputGroupCells,
// which stays as it is, still served, while the rest of its hour merges without it.
func (c compactor) mergeDecodable(ctx context.Context, group []segment.Descriptor, tier segment.Tier) error {
	for len(group) > 1 {
		tooLarge, err := c.merge(ctx, group, tier)
		if err != nil || tooLarge < 0 {
			return err
		}
		group = slices.Delete(group, tooLarge, tooLarge+1)
	}
	return nil
}

// merge writes inputs' rows, read a row group at a time and oldest input first so lines of
// one time keep their order, as one segment taking the newest input's seal time and pattern
// profile, so the same inputs give the same key, then commits it superseding them. It holds
// only the inputs read ahead, not the hour's. It stops at an input with a row group too large
// to decode and returns that input's index in inputs, which it sorts, or -1 once committed.
func (c compactor) merge(ctx context.Context, inputs []segment.Descriptor, tier segment.Tier) (int, error) {
	slices.SortFunc(inputs, func(a, b segment.Descriptor) int {
		return cmp.Or(a.TimeRange.Min.Compare(b.TimeRange.Min), cmp.Compare(a.ID, b.ID))
	})
	newest := slices.MaxFunc(inputs, func(a, b segment.Descriptor) int { return a.CreatedAt.Compare(b.CreatedAt) })
	// Before the merge, so an OOM kill names the hour.
	rows := 0
	for _, d := range inputs {
		rows += d.Rows
	}
	slog.Default().Info("merging segments", "service", newest.Stream.Service, "tier", tier, "start", inputs[0].TimeRange.Min, "inputs", len(inputs), "rows", rows)
	w := segment.NewWriter(segment.BuilderConfig{Tenant: c.cfg.Tenant, Stream: newest.Stream, CreatedAt: newest.CreatedAt}, columnTypes(inputs))
	ids := make([]string, len(inputs))
	filtered := map[string]bool{}
	var profile segment.PatternProfile
	opened, stop := ahead(ctx, inputs, c.open)
	defer stop()
	for i, d := range inputs {
		ids[i] = d.ID
		in, err := opened(i)
		if err != nil {
			return -1, err
		}
		if cells := in.footer.LargestGroupCells(); cells > maxInputGroupCells {
			slog.Default().Warn("leaving a segment unmerged: a row group too large to decode",
				"segment", d.ID, "service", d.Stream.Service, "cells", cells)
			return i, nil
		}
		if d.ID == newest.ID {
			profile = in.footer.PatternProfile
		}
		markFiltered(filtered, *in.footer)
		if err := c.appendInput(ctx, in.rd, w, d, *in.footer); err != nil {
			return -1, err
		}
	}

	body, merged, err := w.Close(profile)
	if err != nil {
		return -1, fmt.Errorf("compact: encode: %w", err)
	}
	key := segment.BuildSegmentKey(c.cfg.Tenant, tier, merged)
	if err := c.store.Put(ctx, key, body); err != nil {
		return -1, fmt.Errorf("compact: put %s: %w", key, err)
	}
	slices.Sort(ids)
	merged.Object, merged.Supersedes = segment.ObjectLocation{Key: key}, ids
	if merged.Rollup, merged.RollupFilters, err = c.putRollup(ctx, merged, body, filtered); err != nil {
		return -1, fmt.Errorf("compact: rollup %s: %w", merged.ID, err)
	}
	if err := c.cat.Append(ctx, merged); err != nil {
		return -1, fmt.Errorf("compact: commit %s: %w", merged.ID, err)
	}
	return -1, nil
}

// appendInput writes d's rows to w a row group at a time, so the merge holds one group's
// rows rather than the input's.
func (c compactor) appendInput(ctx context.Context, rd segment.Reader, w segment.Writer, d segment.Descriptor, footer segment.Footer) error {
	for g := range footer.RowGroups {
		records, _, err := rd.RowGroupRecords(ctx, d.Object, footer, g)
		if err != nil {
			return fmt.Errorf("compact: read %s: %w", d.ID, err)
		}
		for _, r := range records {
			id := r.Text(segment.ColPatternID)
			text, _ := footer.PatternText(id)
			if err := w.Append(r, segment.ParsedPattern{PatternID: id, Pattern: text}); err != nil {
				return fmt.Errorf("compact: append %s: %w", d.ID, err)
			}
		}
	}
	return nil
}

// readAhead is how many inputs a merge opens at once: each waits a round trip to the store,
// and a window of a large fleet has thousands of small inputs.
const readAhead = 16

// input is a segment a merge reads, through rd, with its footer while the merge holds it.
type input struct {
	segment.Descriptor
	rd     segment.Reader
	footer *segment.Footer
}

// open reads a raw input, small by the collector's cut, whole in one GET, so its footer and row
// groups come from memory; a merged input, which can be large, is read by range.
func (c compactor) open(ctx context.Context, d segment.Descriptor) (input, error) {
	in := input{Descriptor: d, rd: c.rd}
	if len(d.Supersedes) == 0 {
		body, err := c.store.GetSuffix(ctx, d.Object.Key, math.MaxInt64)
		if err != nil {
			return in, fmt.Errorf("compact: read %s: %w", d.ID, err)
		}
		in.rd = bodyReader(body)
	}
	footer, _, err := in.rd.Footer(ctx, d.Object)
	if err != nil {
		return in, fmt.Errorf("compact: footer %s: %w", d.ID, err)
	}
	in.footer = &footer
	return in, nil
}

// ahead fetches inputs in order, at most readAhead not yet taken, and returns each by its
// index; stop abandons the rest and waits for those in flight.
func ahead[T any](ctx context.Context, inputs []segment.Descriptor, fetch func(context.Context, segment.Descriptor) (T, error)) (next func(int) (T, error), stop func()) {
	type result struct {
		v   T
		err error
	}
	ctx, cancel := context.WithCancel(ctx)
	slots := make(chan struct{}, readAhead)
	results := make([]chan result, len(inputs))
	for i := range results {
		results[i] = make(chan result, 1)
	}
	var fetches sync.WaitGroup
	fetches.Go(func() {
		for i, d := range inputs {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			fetches.Go(func() {
				v, err := fetch(ctx, d)
				results[i] <- result{v, err}
			})
		}
	})
	return func(i int) (T, error) {
		r := <-results[i]
		<-slots
		return r.v, r.err
	}, func() { cancel(); fetches.Wait() }
}

// putRollup writes the rollup and the filters of merged, whose object is body and whose inputs
// filtered the columns filtered marks, under keys that depend only on the inputs as the
// segment's does, so a pass after a crash before the commit rewrites them.
func (c compactor) putRollup(ctx context.Context, merged segment.Descriptor, body []byte, filtered map[string]bool) (rollup, filters segment.ObjectLocation, err error) {
	footer, _, err := c.rd.Footer(ctx, merged.Object)
	if err != nil {
		return rollup, filters, err
	}
	base := strings.TrimSuffix(merged.Object.Key, ".seg")
	rollup, filters = segment.ObjectLocation{Key: base + ".rollup"}, segment.ObjectLocation{Key: base + ".filters"}
	if err := c.putFilters(ctx, filters.Key, footer, body, filtered); err != nil {
		return rollup, filters, err
	}
	r, err := summary.BuildRollup(ctx, c.rd, merged, footer, c.cfg.Summary)
	if err != nil {
		return rollup, filters, err
	}
	encoded, err := r.Encode()
	if err != nil {
		return rollup, filters, err
	}
	return rollup, filters, c.store.Put(ctx, rollup.Key, encoded)
}

// putFilters writes at key the membership filters of body, whose footer is footer, with a filter
// of the values of each string column body does not filter that is named an id or an input
// filtered, as filtered marks, so a needle for an id the hour does not hold is ruled out without
// its footer.
func (c compactor) putFilters(ctx context.Context, key string, footer segment.Footer, body []byte, filtered map[string]bool) error {
	filters, err := footer.Filters(body)
	if err != nil {
		return err
	}
	rd := bodyReader(body)
	for _, column := range slices.Sorted(maps.Keys(footer.ZoneMaps)) {
		id := footer.ZoneMaps[column].Type == value.TypeString && segment.IsID(column) || filtered[column]
		if !id || filters[column] != nil {
			continue
		}
		if filters[column], err = columnFilter(ctx, rd, footer, column); err != nil {
			return err
		}
	}
	encoded, err := segment.EncodeFilters(filters)
	if err != nil {
		return err
	}
	return c.store.Put(ctx, key, encoded)
}

// markFiltered marks in filtered each column footer filters.
func markFiltered(filtered map[string]bool, footer segment.Footer) {
	for column := range footer.ZoneMaps {
		filtered[column] = filtered[column] || footer.HasFilter(column)
	}
}

// columnFilter is a membership filter of column's values in the segment rd reads, whose
// footer is footer.
func columnFilter(ctx context.Context, rd segment.Reader, footer segment.Footer, column string) (*sketch.XorFilter, error) {
	keys := map[uint64]struct{}{} // hashes, as a merged hour's values are too many to hold
	for g := range footer.RowGroups {
		cols, _, err := rd.Columns(ctx, segment.ObjectLocation{}, footer, g, nil, column)
		if err != nil {
			return nil, err
		}
		for i := range cols[column].Len() {
			if v := cols[column].Text(i); len(v) > 0 {
				keys[sketch.Hash64(v)] = struct{}{}
			}
		}
	}
	return sketch.NewXorFilterOfHashes(slices.Collect(maps.Keys(keys)))
}

// bodyReader reads a segment from body, held in memory.
func bodyReader(body []byte) segment.Reader {
	return segment.NewReader(storage.Fake{
		GetSuffixFn: func(_ string, n int64) ([]byte, error) { return body[max(0, int64(len(body))-n):], nil },
		GetRangeFn:  func(_ string, off, n int64) ([]byte, error) { return body[off : off+n], nil },
	})
}

// columnTypes is each column's type in inputs, promoted where two disagree as sealing
// their rows together would.
func columnTypes(inputs []segment.Descriptor) map[string]value.Type {
	types := map[string]value.Type{}
	for _, d := range inputs {
		for name, zm := range d.ZoneMaps {
			types[name] = value.Promote(types[name], zm.Type)
		}
	}
	return types
}
