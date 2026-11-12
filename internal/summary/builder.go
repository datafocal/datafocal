package summary

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/metrics"
	"slices"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// Builder populates a Store from a catalog snapshot plus footers. It replays
// descriptors in ascending time order (load-bearing for eviction), skips
// already-added descriptors on refresh, and drops descriptors the store would
// immediately evict, so footers already past the retention floor are never read.
type Builder interface {
	Build(ctx context.Context) error
	Refresh(ctx context.Context)
	// Store is the store this builder writes.
	Store() Store
}

type builder struct {
	cat   catalog.Catalog
	rd    segment.Reader
	cfg   Config
	store *store
	// absorbFrom is the load-window floor, fixed on the first build; see absorbFloor.
	absorbFrom int64
	absorbSet  bool
}

// NewBuilder returns a Builder of a fresh store configured by cfg, reading descriptors
// from cat and their footers from rd.
func NewBuilder(cat catalog.Catalog, rd segment.Reader, cfg Config) Builder {
	cfg = cfg.withDefaults()
	return &builder{cat: cat, rd: rd, cfg: cfg, store: newStore(cfg)}
}

func (b *builder) Store() Store { return b.store }

// Refresh builds now and then every Config.RefreshInterval until ctx is done. It
// blocks, so the caller owns the goroutine and can see when it stops.
//
// Building before the first tick is the point: the first build reads one footer per
// retained segment, which over a retention window is thousands of round trips even
// footerReads at a time, so a caller runs this in the background rather than holding a
// listener closed for it — and waiting an interval first would leave every tool on the scan
// path for that long after startup for nothing.
//
// A failed build is logged and the loop continues, which is why this returns
// nothing. Until the store reaches the catalog's epoch it matches no read snapshot
// and every tool answers from the scan path anyway (Store.CatalogVersion), so a build that
// cannot finish costs latency, not availability. Ending the loop would turn a
// transient object-store failure into a process that never warms again.
func (b *builder) Refresh(ctx context.Context) {
	ticker := time.NewTicker(b.cfg.RefreshInterval)
	defer ticker.Stop()
	for {
		if err := b.Build(ctx); err != nil {
			slog.Default().Error("build summary", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Build merges each new descriptor's footer, and a failed build can leave some applied
// without advancing the version (audit findings F01/F04).
func (b *builder) Build(ctx context.Context) error {
	decoded := 0
	start := time.Now()
	version, err := b.cat.Version(ctx)
	if err != nil {
		return fmt.Errorf("builder catalog version: %w", err)
	}
	descs, err := b.cat.Prune(ctx, version, b.selector())
	if err != nil {
		return fmt.Errorf("builder prune: %w", err)
	}

	if len(descs) == 0 {
		b.store.MarkSynced(version)
		return nil
	}

	floor := b.absorbFloor(descs)
	descs = retainedDescriptors(descs, b.cfg, floor)

	descs = slices.DeleteFunc(descs, func(d segment.Descriptor) bool { return b.store.HasAdded(d.ID) })
	var errs []error
	reads := b.readFooters(ctx, descs)
	for i, d := range descs {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("builder sync: %w", err)
		}
		read := b.decodeRollup(ctx, d, <-reads(i))
		if read.decoded {
			decoded++
		}
		if read.err != nil {
			errs = append(errs, fmt.Errorf("read segment %s: %w", d.ID, read.err))
			continue
		}
		if err := b.store.Add(d, read.footer, read.rollup); err != nil {
			errs = append(errs, fmt.Errorf("seal %s: %w", d.ID, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("builder sync: %w", errors.Join(errs...))
	}

	// Coverage before the epoch, so no read can find the store warm at an epoch and
	// then be told it covers nothing.
	b.store.SetCompleteFrom(time.UnixMilli(floor).UTC())
	b.store.MarkSynced(version)
	if len(descs) > 0 {
		slog.Default().Info("built summary", "segments", len(descs), "catalog_version", version, "duration_ms", float64(time.Since(start).Microseconds())/1000,
			"footers_decoded", decoded, "heap_bytes", heapInUse(), "value_series_evicted", b.store.valueSeriesEvicted())
	}
	return nil
}

// footerReads is how many footers a build reads at once, since a read waits on the object
// store rather than the CPU; a build of thousands of segments was thousands of serial
// round trips. Of them, rollupReads may be rollups, whose bodies are megabytes.
const (
	footerReads = 32
	rollupReads = 4
)

type footerRead struct {
	footer  segment.Footer
	decoded bool // a footer a cache held costs no GET
	rollup  Rollup
	// body is a rollup's, which decodeRollup decodes as the build absorbs it, since decoded a
	// rollup is many times its body and a read-ahead of them was a build's peak heap.
	body []byte
	err  error
}

// readFooters reads descs' footers, and a multi-bin one's counts or a merged one's rollup, up to
// footerReads ahead of the one reads(i) hands the caller, so the store absorbs them in replay
// order and holds at most footerReads.
func (b *builder) readFooters(ctx context.Context, descs []segment.Descriptor) (reads func(i int) <-chan footerRead) {
	pending := make([]chan footerRead, len(descs))
	started := 0
	return func(i int) <-chan footerRead {
		for ; started < len(descs) && started <= i+footerReads && (descs[started].Rollup.Key == "" || started <= i+rollupReads); started++ {
			done := make(chan footerRead, 1)
			pending[started] = done
			desc := descs[started]
			go func() { done <- b.read(ctx, desc) }()
		}
		read := pending[i]
		pending[i] = nil // taken once, and released for the collector
		return read
	}
}

// read reads desc's rollup's body, when it has one, or else its footer and a multi-bin one's
// counts.
func (b *builder) read(ctx context.Context, desc segment.Descriptor) footerRead {
	if desc.Rollup.Key != "" {
		body, _, err := b.rd.Rollup(ctx, desc.Rollup)
		return footerRead{body: body, err: err}
	}
	return b.readFooter(ctx, desc)
}

// decodeRollup is read with desc's rollup decoded, or desc's footer read in its place when
// the rollup counts by bins other than the summary's.
func (b *builder) decodeRollup(ctx context.Context, desc segment.Descriptor, read footerRead) footerRead {
	if desc.Rollup.Key == "" || read.err != nil {
		return read
	}
	r, err := DecodeRollup(read.body)
	if err != nil || r.BinWidth == b.cfg.BinWidth {
		return footerRead{footer: r.Footer, rollup: r, err: err}
	}
	return b.readFooter(ctx, desc)
}

func (b *builder) readFooter(ctx context.Context, desc segment.Descriptor) footerRead {
	footer, cost, err := b.rd.Footer(ctx, desc.Object)
	read := footerRead{footer: footer, decoded: cost.GETs > 0, err: err}
	if err == nil && spansBins(desc, b.cfg.BinWidth) {
		read.rollup, read.err = countRows(ctx, b.rd, desc, footer, b.cfg.BinWidth, nil)
	}
	return read
}

// countValues counts row i's value of each of columns in cols under key's pattern and bin, ""
// for a row without one.
func countValues(values ValueCounts, cols map[string]segment.ColumnChunk, columns []string, i int, key ValueKey) {
	for _, column := range columns {
		key.Column, key.Value = column, ""
		if c := cols[column]; i < c.Len() {
			key.Value = string(c.Text(i))
		}
		values[key]++
	}
}

// absorbFloor is the oldest bin this build absorbs descriptors from: the larger of
// the retention floor and the load-window floor, the latter fixed on the first build.
//
// The load window has to be sticky. If it only applied to the first build, the second one
// thirty seconds later would read every footer the first one skipped, and the knob
// would buy nothing beyond a slightly earlier first answer. Fixed instead, it is
// the point this process started from: the retention floor rises past it as the
// clock advances, and the span the store holds grows from LoadWindow to Retention.
func (b *builder) absorbFloor(descs []segment.Descriptor) int64 {
	var newest int64
	for _, d := range descs {
		if m := d.TimeRange.Max.UnixMilli(); m > newest {
			newest = m
		}
	}
	newestBin := segment.FloorBin(newest, b.cfg.BinWidth.Milliseconds())
	if !b.absorbSet {
		b.absorbFrom, b.absorbSet = newestBin-b.cfg.LoadWindow.Milliseconds(), true
	}
	return max(newestBin-b.cfg.Retention.Milliseconds(), b.absorbFrom)
}

// selector asks the catalog for the tenant's descriptors from the store's current
// retention start onward, so a refresh does not materialize and sort a
// descriptor for every segment the tenant has ever written on every tick.
//
// The retention start is a safe bound, and deliberately a looser one than
// retainedDescriptors applies: it trails the newest bin the store has actually
// absorbed, where the retention floor tracks the newest descriptor the catalog
// offers. So the catalog may return descriptors the floor then drops — cheap — but
// never drops one the series would have kept, which would be unrecoverable. A
// store that has absorbed nothing has no retention start to bound by, so its first build
// reads the manifest unbounded.
func (b *builder) selector() selector.Resolved {
	var sel selector.Resolved
	if floor := b.store.bins.RetentionStart(); floor > 0 {
		sel.Window.From = time.UnixMilli(floor).UTC()
	}
	return sel
}

// retainedDescriptors returns descs sorted ascending by (TimeRange.Min, ID) with
// those landing below floor dropped, floor being a bin start from absorbFloor. A
// descriptor is read here if and only if the store would keep what it contributes.
func retainedDescriptors(descs []segment.Descriptor, cfg Config, floor int64) []segment.Descriptor {
	// Clone: descs belongs to the caller's catalog read, and sorting it in place
	// would reorder a slice the catalog may hand out again.
	descs = slices.Clone(descs)
	slices.SortFunc(descs, func(a, b segment.Descriptor) int {
		if c := a.TimeRange.Min.Compare(b.TimeRange.Min); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})

	width := cfg.BinWidth.Milliseconds()
	return slices.DeleteFunc(descs, func(d segment.Descriptor) bool {
		return segment.FloorBin(d.TimeRange.Max.UnixMilli(), width) < floor
	})
}

// heapInUse is the bytes of heap objects, live or not yet swept.
func heapInUse() uint64 {
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}
