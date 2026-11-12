package tools_test

import (
	"context"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// Shared fixtures for the tools tests. Every test here builds its corpus through
// a real collector rather than hand-assembling footers, so the sketches, zone
// maps, and patterns under test are byte-identical to what the write path
// emits. writeSegment is the single place that scaffolding lives; the per-file
// helpers above each test only decide which rows go in.

// testTenant is the tenant for every test corpus in this package. The value is
// arbitrary — what matters is that writers and readers agree on one.
const testTenant = "acme"

// fakeClock pins the collector's Clock seam to an explicit instant so segment
// boundaries and descriptor TimeRanges are deterministic across runs.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

// newCorpus returns an empty corpus directory and its write seams.
func newCorpus(t *testing.T) (string, storage.Store, catalog.Catalog) {
	t.Helper()
	dir := t.TempDir()
	return dir, storage.NewFS(dir), catalog.New(storage.NewFS(dir), testTenant)
}

// readers returns the read seams over a corpus directory: the FS catalog reader
// and the footer-caching segment reader, the same pair cmd/querier wires up.
func readers(dir string) (catalog.Catalog, segment.Reader) {
	return catalog.New(storage.NewFS(dir), testTenant), segment.NewFooterCache(storage.NewFS(dir))
}

// uncachedReaders is readers without the footer cache, for a test that reads Cost.GETs
// as evidence of which path ran. GETs counts object-store requests, so a cached footer
// costs nothing and a second call over the same segments reports zero however much work
// it did — true, and useless as a routing signal. Without the cache the two coincide.
func uncachedReaders(dir string) (catalog.Catalog, segment.Reader) {
	return catalog.New(storage.NewFS(dir), testTenant), segment.NewReader(storage.NewFS(dir))
}

// toolsOver is the tool surface reading through cat and rd, with rs as its summary.
func toolsOver(cat catalog.Catalog, rd segment.Reader, rs summary.Store) tools.Tools {
	planner, scanner := plan.New(plan.Deps{Catalog: cat, Reader: rd, Summary: rs}), scan.New(rd, plan.Derived{})
	return tools.New(tools.Deps{Catalog: cat, Series: series.New(planner, scanner), Planner: planner, Scanner: scanner, Summary: rs})
}

// writeSegment drives events through a real collector and flushes exactly one
// sealed segment into store+cat.
//
// The clock is advanced to each event's own timestamp, and every implicit flush
// trigger is raised out of reach, so the whole slice lands in one segment however
// long a span it covers — tests that need several segments call this repeatedly.
func writeSegment(t *testing.T, store storage.Store, cat catalog.Catalog, service string, events []segment.Record) {
	t.Helper()
	writeLabeledSegment(t, store, cat, service, nil, events)
}

// writeRecords seals an explicit set of records into one segment. It is the
// test-only escape hatch for row-shaped fixtures that the log-event collector
// cannot synthesize by itself.
func writeRecords(t *testing.T, store storage.Store, cat catalog.Catalog, service string, records []segment.Record) {
	t.Helper()
	if len(records) == 0 {
		t.Fatal("writeRecords: at least one record required")
	}

	sealed := segment.Sealed{
		Records:    records,
		Descriptor: segmentDescriptor(t, service, records),
	}
	body, err := sealed.Encode()
	if err != nil {
		t.Fatalf("encode sealed records: %v", err)
	}
	sealed.Descriptor.EstimatedSize = int64(len(body))

	ctx := context.Background()
	key := segment.BuildSegmentKey(testTenant, segment.TierRaw, sealed.Descriptor)
	if err := store.Put(ctx, key, body); err != nil {
		t.Fatalf("put segment: %v", err)
	}

	desc := sealed.Descriptor
	desc.Object = segment.ObjectLocation{Key: key}
	if err := cat.Append(ctx, desc); err != nil {
		t.Fatalf("append catalog descriptor: %v", err)
	}
}

// writeLabeledSegment is writeSegment with stream labels, for the tests that turn
// on what a descriptor says about itself rather than what its rows contain.
func writeLabeledSegment(t *testing.T, store storage.Store, cat catalog.Catalog,
	service string, labels map[string]string, events []segment.Record) {
	t.Helper()
	if len(events) == 0 {
		t.Fatal("writeSegment: at least one event required")
	}
	cfg := collector.DefaultConfig()
	cfg.Tenant = testTenant
	cfg.Stream = collector.StreamConfig{Service: service, Labels: labels}
	cfg.FlushInterval = time.Hour
	cfg.FlushBytes = 1 << 40
	cfg.MaxRows = 1 << 30

	clock := &fakeClock{now: events[0].Timestamp}
	c, err := collector.New(cfg, collector.Dependencies{Store: store, CatalogWriter: cat, Clock: clock})
	if err != nil {
		t.Fatalf("new collector: %v", err)
	}
	ctx := context.Background()
	for _, ev := range events {
		clock.now = ev.Timestamp
		if _, err := c.Collect(ctx, ev); err != nil {
			t.Fatalf("collect: %v", err)
		}
	}
	if _, err := c.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// messageEvents builds one info event per message a second apart, each message shape one pattern.
func messageEvents(start time.Time, messages ...string) []segment.Record {
	events := make([]segment.Record, len(messages))
	for i, msg := range messages {
		events[i] = segment.LogRecord(start.Add(time.Duration(i)*time.Second), "info", msg, nil)
	}
	return events
}

func segmentDescriptor(t *testing.T, service string, records []segment.Record) segment.Descriptor {
	t.Helper()

	tr := segment.TimeRange{}
	for _, record := range records {
		tr.Add(record.Timestamp)
	}

	ids := patternIDs(records)
	createdAt := records[0].Timestamp
	// Every service its rows name, as a collector's or compactor's descriptor says.
	var services []string
	for _, record := range records {
		if v, ok := recordString(record, segment.ColService); ok && !slices.Contains(services, v) {
			services = append(services, v)
		}
	}
	slices.Sort(services)
	return segment.Descriptor{
		Services:      services,
		ID:            segment.NewID(testTenant, segment.Stream{Service: service}, tr, len(records), createdAt),
		Stream:        segment.Stream{Service: service},
		TimeRange:     tr,
		Rows:          len(records),
		PatternIDs:    ids,
		ZoneMaps:      segment.BuildZoneMaps(records),
		CreatedAt:     createdAt,
		EstimatedSize: 0,
	}
}

func patternIDs(records []segment.Record) []string {
	seen := map[string]struct{}{}
	for _, record := range records {
		if value, ok := recordString(record, segment.ColPatternID); ok && value != "" {
			seen[value] = struct{}{}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func recordString(record segment.Record, name string) (string, bool) {
	for _, col := range record.Columns {
		if col.Name == name {
			return col.Value.String, true
		}
	}
	return "", false
}

// window is the absolute time window covering [from, to] as the tools take it.
func window(from, to time.Time) selector.TimeSpec {
	return selector.TimeSpec{From: from.Format(time.RFC3339), To: to.Format(time.RFC3339)}
}

// present is a series array's entries in the bins that have one.
func present(values []*float64) []float64 {
	var out []float64
	for _, v := range values {
		if v != nil {
			out = append(out, *v)
		}
	}
	return out
}

// num is v as an entry of a series array.
func num(v float64) *float64 { return &v }
