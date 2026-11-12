//go:build stack

package stack

import (
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/compact"
	"github.com/datafocal/datafocal/internal/e2e"
	"github.com/datafocal/datafocal/internal/identity"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

var (
	fleetNodes    = flag.Int("stack.fleet", 0, "simulated collectors TestFleet runs in one process; 0 skips it")
	fleetRate     = flag.Int("stack.rate", 5, "lines a second each simulated collector writes")
	fleetInterval = flag.Duration("stack.interval", collector.DefaultConfig().FlushInterval, "simulated time between a simulated collector's cuts, whole seconds dividing an hour")
	fleetHours    = flag.Int("stack.hours", 1, "simulated hours TestFleet writes")
)

// fleetReport is the costs that grow with a fleet's writers and segments rather than with
// parsing: ingest, catalog commits, the querier's catalog load and summary build, and
// queries.
type fleetReport struct {
	Run             runHeader     `json:"run"`
	Nodes           int           `json:"nodes"`
	RatePerNode     int           `json:"lines_per_sec_per_node"`
	Interval        time.Duration `json:"interval_ns"`
	Simulated       time.Duration `json:"simulated_ns"`
	Ingest          ingestReport  `json:"ingest"`
	Storage         fleetStorage  `json:"storage"`
	USDPerGBMonth   float64       `json:"usd_per_gb_month"`
	Segments        int           `json:"segments"`
	Commits         int           `json:"commits"`
	CommitConflicts int           `json:"commit_conflicts"` // PutIfAbsent answered ErrExists
	CommitLatencyMS struct {
		P50 float64 `json:"p50"`
		P99 float64 `json:"p99"`
	} `json:"commit_latency_ms"`
	// RawSegmentBytes is every segment the collectors sealed, LiveSegments those the catalog
	// serves once compaction has superseded what it merged.
	RawSegmentBytes    int64         `json:"raw_segment_bytes"`
	LiveSegments       int           `json:"live_segments"`
	Compaction         compactReport `json:"compaction"`
	CatalogObjects     int           `json:"catalog_objects"`
	CatalogBytes       int64         `json:"catalog_bytes"`
	QuerierCatalogLoad timedGETs     `json:"querier_catalog_load"`
	SummaryBuild       timedGETs     `json:"summary_build"`
	QuerierPeakHeapMB  float64       `json:"querier_peak_heap_mb"`
	Queries            []queryReport `json:"queries"`
}

// fleetStorage is the bytes the catalog serves and the corpus's compression in them.
type fleetStorage struct {
	Bytes       int64   `json:"bytes"`
	Compression float64 `json:"compression"`
}

type timedGETs struct {
	MS     float64 `json:"ms"`
	GETs   int     `json:"gets"`
	Bytes  int64   `json:"bytes"`
	HeapMB float64 `json:"heap_mb"` // live after it, less before
}

// compactReport is the compactor's passes on its deployed cadence over simulated time.
type compactReport struct {
	Passes         int            `json:"passes"`
	WallSeconds    float64        `json:"wall_seconds"`
	MaxPassSeconds float64        `json:"max_pass_seconds"`
	CPUSeconds     float64        `json:"cpu_seconds"`
	Merged         int            `json:"merged_segments"`
	Inputs         int            `json:"input_segments"`
	Rows           int            `json:"rows"`
	RowsPerSec     float64        `json:"rows_per_sec"`
	Requests       map[string]int `json:"requests"`
}

// The compactor's deployed interval, settle, lookback and window, cmd/compactor's defaults.
const (
	compactEvery    = time.Minute
	compactSettle   = 2 * time.Minute
	compactLookback = 24 * time.Hour
	compactWindow   = 5 * time.Minute
)

// TestFleet runs -stack.fleet collectors in this process, each writing -stack.rate lines a
// second of simulated time into one bucket, then the querier in-process over what they wrote.
func TestFleet(t *testing.T) {
	if *fleetNodes == 0 {
		t.Skip("set -stack.fleet to run the fleet benchmark")
	}
	if *limitCPUs > 0 {
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(*limitCPUs))
	}
	if *limitMemMB > 0 {
		defer debug.SetMemoryLimit(debug.SetMemoryLimit(int64(*limitMemMB) << 20))
	}
	require.True(t, *fleetInterval >= time.Second && *fleetInterval%time.Second == 0 && time.Hour%*fleetInterval == 0,
		"-stack.interval is whole seconds dividing an hour")
	start, _ := seedcorpus.IncidentWindow()
	f := fleet{nodes: *fleetNodes, rate: *fleetRate, hours: *fleetHours, interval: *fleetInterval, start: start,
		hour: corpusHour(t, *fleetRate*3600)}
	s := newStack(t, f.interval)
	lines, written := f.groundTruth()
	report := fleetReport{Run: runOf(t, corpusHeader{Name: fmt.Sprintf("%s at %d lines/s per node, cut every %s, compacted and queried as deployed", seedcorpus.ScenarioAll, f.rate, f.interval),
		Copies: f.hours, Nodes: f.nodes, RawBytes: written.Bytes, Lines: totalLines(written)}, minioImage),
		Nodes: f.nodes, RatePerNode: f.rate, Interval: f.interval, Simulated: time.Duration(f.hours) * time.Hour}
	s.fleetWrite(t, f, written, &report)
	s.fleetQuery(t, e2e.Corpus{Start: f.start, Copies: f.hours, TraceID: lines.firstValue("gateway", "trace_id"),
		SymptomPattern: lines.patternID("slow query ")}, &report)
	writeReport(t, report)
}

// fleet is the simulated writers' input: one hour of the seeded corpus's lines, which every
// writer replays every simulated hour under its own host and ID prefix.
type fleet struct {
	nodes, rate, hours int
	interval           time.Duration
	start              time.Time
	hour               []fleetLine
}

// fleetLine is one corpus line: the file it was read from, its fields, and its record.
type fleetLine struct {
	file   string
	row    map[string]string
	record segment.Record
}

// corpusHour is n of the seeded corpus's lines spread evenly over it in time order, each
// parsed once.
func corpusHour(t *testing.T, n int) []fleetLine {
	t.Helper()
	dir := t.TempDir()
	_, err := seedcorpus.WriteLogs(dir, seedcorpus.ScenarioAll, 1, 0)
	require.NoError(t, err)
	paths, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	var all []fleetLine
	for _, path := range paths {
		parser, err := logformat.New(logformat.Config{Format: "json"})
		require.NoError(t, err)
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		for line := range strings.Lines(string(data)) {
			l := fleetLine{file: "/logs/" + filepath.Base(path)}
			require.NoError(t, json.Unmarshal([]byte(line), &l.row))
			var ok bool
			l.record, ok = parser.Parse(strings.TrimSuffix(line, "\n"))
			require.True(t, ok, "a seeded line is one JSON record")
			all = append(all, l)
		}
	}
	slices.SortStableFunc(all, func(a, b fleetLine) int { return a.record.Timestamp.Compare(b.record.Timestamp) })
	hour := make([]fleetLine, n)
	for j := range hour {
		hour[j] = all[j*len(all)/n]
	}
	return hour
}

func nodeName(w int) string { return fmt.Sprintf("node-%04d", w) }

// at is a writer's line j's event time.
func (f fleet) at(j int) time.Time {
	return f.start.Add(time.Duration(j) * time.Second / time.Duration(f.rate))
}

// span is the lines of interval k, none past the last.
func (f fleet) span(k int) (lo, hi int) {
	n, total := f.rate*int(f.interval/time.Second), f.rate*3600*f.hours
	return min(k*n, total), min((k+1)*n, total)
}

// collected is l as writer w collects it at at: its trace and request IDs prefixed with w's name.
func (l fleetLine) collected(w int, at time.Time) segment.Record {
	r := segment.Record{Timestamp: at, Columns: slices.Clone(l.record.Columns)}
	for i, c := range r.Columns {
		if c.Name == segment.ColTraceID || c.Name == segment.ColRequestID {
			r.Columns[i].Value.String = nodeName(w) + "-" + c.Value.String
		}
	}
	return identity.New(nodeName(w)).Resolve(l.file, r)
}

// sent is l as writer w's agent sends it at at, a line's JSON fields.
func (l fleetLine) sent(w int, at time.Time) map[string]string {
	row := maps.Clone(l.row)
	for _, k := range []string{segment.ColTraceID, segment.ColRequestID} {
		if v, ok := row[k]; ok {
			row[k] = nodeName(w) + "-" + v
		}
	}
	row["ts"] = at.Format(time.RFC3339Nano)
	return row
}

// groundTruth is writer 0's first hour, which holds every trace_id it ever writes, and
// every writer's lines per service and bytes, a line's JSON as its agent sends it.
func (f fleet) groundTruth() (logLines, seedcorpus.LogsResult) {
	lines := logLines{patterns: map[string]string{}}
	written := seedcorpus.LogsResult{Lines: map[string]int{}}
	for j, l := range f.hour {
		row := l.sent(0, f.at(j))
		lines.add(row)
		written.Lines[l.row["service"]] += f.hours * f.nodes
		text, _ := json.Marshal(row) // a map of strings always encodes
		written.Bytes += int64(len(text)+1) * int64(f.hours*f.nodes)
	}
	return lines, written
}

// run runs step(w, k) for every interval k in turn on writer w's own goroutine, every
// writer at once and none waiting on another, having set clocks[w] to w's cut of interval k.
func (f fleet) run(clocks []*fleetClock, step func(w, k int) error) error {
	errs := make([]error, f.nodes)
	var wg sync.WaitGroup
	for w := range f.nodes {
		wg.Go(func() {
			for k := range f.hours*int(time.Hour/f.interval) + 1 {
				clocks[w].set(f.cut(w, k))
				if errs[w] = step(w, k); errs[w] != nil {
					return
				}
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// cut is when writer w's timer cuts in interval k: at second w mod the interval's, as a
// fleet's timers spread over it, and w nanoseconds on, since a segment's id is unique by its
// seal time and writers replaying the same lines would otherwise seal identical ones at once.
func (f fleet) cut(w, k int) time.Time {
	return f.start.Add(time.Duration(k)*f.interval + time.Duration(w%int(f.interval/time.Second))*time.Second + time.Duration(w))
}

// fleetClock is the simulated time one writer's collector reads, which its background seal
// reads while the writer moves it on.
type fleetClock struct{ nanos atomic.Int64 }

func (c *fleetClock) Now() time.Time  { return time.Unix(0, c.nanos.Load()).UTC() }
func (c *fleetClock) set(t time.Time) { c.nanos.Store(t.UnixNano()) }

// fleetWrite runs a collector per writer on the stack's bucket. At its cut of each interval
// a writer flushes the batch it filled the interval before, sealing it in the background and
// waiting for it to land, then fills the next. The compactor runs beside them on its
// deployed cadence of the slowest writer's time.
func (s *stack) fleetWrite(t *testing.T, f fleet, written seedcorpus.LogsResult, report *fleetReport) {
	t.Helper()
	ctx := t.Context()
	counts := &fleetCounts{n: map[string]int{}}
	store := counts.counted(ctx, s.store)
	cfg := collector.DefaultConfig()
	cfg.Tenant, cfg.FlushInterval, cfg.SealInBackground = tenant, f.interval, true
	collectors, clocks := make([]collector.Collector, f.nodes), make([]*fleetClock, f.nodes)
	for w := range collectors {
		clocks[w] = &fleetClock{}
		clocks[w].set(f.cut(w, 0))
		c, err := collector.New(cfg, collector.Dependencies{Store: store, CatalogWriter: timedCatalog{catalog.NewWithConfig(simClock{store, clocks[w].Now}, tenant, catalog.Config{Writer: nodeName(w)}), counts},
			Clock: clocks[w], Logger: slog.New(slog.DiscardHandler)})
		require.NoError(t, err)
		collectors[w] = c
	}
	compactor := s.fleetCompactor(ctx, &report.Compaction)
	slowest := func() time.Time {
		return slices.MinFunc(clocks, func(a, b *fleetClock) int { return a.Now().Compare(b.Now()) }).Now()
	}
	done, compacted := make(chan struct{}), make(chan error, 1)
	go func() {
		for next := f.start.Add(compactEvery); ; <-time.After(50 * time.Millisecond) {
			select {
			case <-done:
				compacted <- nil
				return
			default:
			}
			if now := slowest(); !now.Before(next) {
				if err := compactor(now.Add(-compactSettle)); err != nil {
					compacted <- err
					return
				}
				next = now.Truncate(compactEvery).Add(compactEvery)
			}
		}
	}()
	cpu, _ := processUsage()
	start := time.Now()
	require.NoError(t, f.run(clocks, func(w, k int) error {
		if _, err := collectors[w].Flush(ctx); err != nil {
			return err
		}
		lo, hi := f.span(k)
		for j := lo; j < hi; j++ {
			if _, err := collectors[w].Collect(ctx, f.hour[j%len(f.hour)].collected(w, f.at(j))); err != nil {
				return err
			}
		}
		return nil
	}))
	wall := time.Since(start).Seconds()
	close(done)
	require.NoError(t, <-compacted)
	cpuEnd, maxRSS := processUsage()
	cpu = cpuEnd - cpu - report.Compaction.CPUSeconds
	t.Logf("fleet: %d writers wrote %d segments in %.0fs beside %d compaction passes", f.nodes, counts.n["put/ok"], wall, report.Compaction.Passes)
	// A settle past the last line, as the compactor's next pass would see it.
	require.NoError(t, compactor(slowest()))
	t.Logf("fleet: %d compaction passes took %.0fs", report.Compaction.Passes, report.Compaction.WallSeconds)
	lines, mb := totalLines(written), float64(written.Bytes)/1e6
	report.Ingest = ingestReport{Lines: lines, RawBytes: written.Bytes, WallSeconds: wall, CPUSeconds: cpu,
		LinesPerSec: float64(lines) / wall, MBPerSec: mb / wall, MBPerCPUSec: mb / cpu, CPUPerGB: cpu / (mb / 1e3), MaxRSSMB: maxRSS, Requests: counts.n}
	rows, live, err := s.catalogRows(ctx)
	require.NoError(t, err)
	require.Equal(t, lines, rows, "the catalog holds every line the writers collected, once")
	report.LiveSegments = live
	report.Segments, report.CommitConflicts = counts.n["put/ok"], counts.n["put-if-absent/exists"]
	commits := counts.commitMS
	report.Commits = len(commits)
	slices.Sort(commits)
	report.CommitLatencyMS.P50, report.CommitLatencyMS.P99 = commits[len(commits)/2], commits[min(len(commits)-1, len(commits)*99/100)]
	cat := catalog.New(s.store, tenant)
	v, err := cat.Version(ctx)
	require.NoError(t, err)
	descs, err := cat.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	served := map[string]bool{}
	for _, d := range descs {
		served[d.Object.Key] = true
		if len(d.Supersedes) > 0 {
			report.Compaction.Merged++
			report.Compaction.Inputs += len(d.Supersedes)
			report.Compaction.Rows += d.Rows
		}
	}
	report.Compaction.RowsPerSec = float64(report.Compaction.Rows) / report.Compaction.WallSeconds
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: tenant + "/", Recursive: true}) {
		require.NoError(t, obj.Err)
		if strings.HasPrefix(obj.Key, tenant+"/catalog/") {
			report.CatalogObjects++
			report.CatalogBytes += obj.Size
			continue
		}
		if strings.HasPrefix(obj.Key, tenant+"/"+string(segment.TierRaw)+"/") {
			report.RawSegmentBytes += obj.Size
		}
		if served[strings.TrimSuffix(obj.Key, ".rollup")+".seg"] || served[obj.Key] {
			report.Storage.Bytes += obj.Size
		}
	}
	report.Storage.Compression = float64(written.Bytes) / float64(report.Storage.Bytes)
	stored := float64(report.Storage.Bytes+report.CatalogBytes) / float64(written.Bytes)
	report.USDPerGBMonth = usdPerGBMonth(report.Ingest.CPUPerGB, stored, prices.ObjectGBMonth)
}

// fleetCompactor is a compaction pass as the deployed compactor runs it, timed into r with
// its CPU and requests, which then are not the collectors'. Its CPU is its own thread's,
// leaving out what the runtime's background collection spends on its garbage.
func (s *stack) fleetCompactor(ctx context.Context, r *compactReport) func(settled time.Time) error {
	counts := &fleetCounts{n: map[string]int{}}
	r.Requests = counts.n
	store := counts.counted(ctx, s.store)
	var now time.Time
	store = simClock{store, func() time.Time { return now }}
	cat := catalog.NewWithConfig(store, tenant, catalog.Config{Grace: catalog.DefaultGrace, Now: func() time.Time { return now }, Writer: "compactor"})
	c := compact.NewWithConfig(cat, catalog.NewSnapshotter(cat, catalog.SnapshotConfig{}), segment.NewReader(store), store,
		compact.Config{Tenant: tenant, Lookback: compactLookback, Window: compactWindow})
	return func(settled time.Time) error {
		now = settled.Add(compactSettle)
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		cpu := threadCPU()
		start := time.Now()
		err := c.Run(ctx, settled)
		wall := time.Since(start).Seconds()
		end := threadCPU()
		r.Passes++
		r.WallSeconds += wall
		r.MaxPassSeconds = max(r.MaxPassSeconds, wall)
		r.CPUSeconds += end - cpu
		return err
	}
}

// threadCPU is the calling OS thread's CPU so far, which is the compactor's while it runs
// locked to one beside the writers.
func threadCPU() float64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(1, &ru) // RUSAGE_THREAD, which package syscall does not name
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()).Seconds()
}

// processUsage is this process's CPU so far and its peak memory: the fleet's collectors
// run in it.
func processUsage() (cpuSeconds, maxRSSMB float64) {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru) // RUSAGE_SELF cannot fail
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()).Seconds(), float64(ru.Maxrss) / 1024
}

// fleetQuery opens the querier in-process over the bucket, with a fresh footer cache, and
// times its first catalog load, its summary build, and each archetype cold and warm.
func (s *stack) fleetQuery(t *testing.T, corpus e2e.Corpus, report *fleetReport) {
	t.Helper()
	ctx := t.Context()
	counts := &fleetCounts{n: map[string]int{}}
	store := counts.counted(ctx, s.store)
	end := corpus.Start.Add(time.Duration(corpus.Copies)*time.Hour + 2*(*fleetInterval))
	store = simClock{store, func() time.Time { return end }}
	cat := catalog.NewWithConfig(store, tenant, catalog.Config{Grace: catalog.DefaultGrace, MaxAge: 5 * time.Second})
	timed := func(name string, fn func() error) timedGETs {
		before := liveHeap()
		gets, bytes, start := counts.gets(), counts.bytes, time.Now()
		require.NoError(t, fn())
		r := timedGETs{MS: ms(time.Since(start)), GETs: counts.gets() - gets, Bytes: counts.bytes - bytes, HeapMB: (liveHeap() - before) / (1 << 20)}
		t.Logf("fleet: %s took %.0fms and %d GETs", name, r.MS, r.GETs)
		return r
	}
	report.QuerierCatalogLoad = timed("catalog load", func() error { _, err := cat.Version(ctx); return err })
	derived, err := plan.NewDerived(querier.DefaultDerived())
	require.NoError(t, err)
	app := querier.Open(querier.Options{Catalog: cat, Store: store, Summary: &summary.Config{}, Derived: derived,
		FooterReads: min(runtime.GOMAXPROCS(0), 16), MaxInFlight: deployedMaxInFlight, RequestMemBudget: deployedMemBudget})
	report.SummaryBuild = timed("summary build", func() error { return app.Refresh(ctx) })
	// The querier's own /debug/vars, here the test process's memory with the querier in it.
	mux := http.NewServeMux()
	mux.Handle("/", app.Handler())
	mux.Handle("GET /debug/vars", expvar.Handler())
	server := httptest.NewServer(mux)
	defer server.Close()
	sampled, stop := make(chan float64), make(chan struct{})
	go func() {
		peak := 0.0
		var m runtime.MemStats
		for tick := time.Tick(50 * time.Millisecond); ; <-tick {
			runtime.ReadMemStats(&m)
			peak = max(peak, float64(m.HeapInuse)/(1<<20))
			select {
			case <-stop:
				sampled <- peak
				return
			default:
			}
		}
	}()
	for _, a := range e2e.Archetypes(corpus) {
		start := time.Now()
		report.Queries = append(report.Queries, timeFlow(t, server.URL, archetypeFlow(a)))
		t.Logf("fleet: %s took %.0fs", a.Name, time.Since(start).Seconds())
	}
	close(stop)
	report.QuerierPeakHeapMB = <-sampled
}

// The deployed querier's in-flight cap and the request budget it derives from its 3Gi limit,
// as cmd/querier's memBudget does.
const (
	deployedMaxInFlight = 8
	deployedMemBudget   = (3<<30/100*85 - segment.FooterCacheBytes - 3<<30/100*85/4) / deployedMaxInFlight
)

// liveHeap is the heap in bytes once a collection has freed what is garbage.
func liveHeap() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc)
}

// fleetCounts is a store's requests by operation and outcome (ok, exists for a PutIfAbsent
// another writer won, or failed), and every catalog commit's latency in milliseconds.
type fleetCounts struct {
	mu       sync.Mutex
	n        map[string]int
	bytes    int64 // read by GETs
	commitMS []float64
}

func (c *fleetCounts) add(op string, err error) error {
	outcome := "ok"
	if errors.Is(err, storage.ErrExists) {
		outcome = "exists"
	} else if err != nil {
		outcome = "failed"
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n[op+"/"+outcome]++
	return err
}

func (c *fleetCounts) got(body []byte, err error) ([]byte, error) {
	c.mu.Lock()
	c.bytes += int64(len(body))
	c.mu.Unlock()
	return body, c.add("get", err)
}

func (c *fleetCounts) gets() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n["get/ok"] + c.n["get/failed"]
}

// counted is store counting its requests into c, which MinIO's bucket metrics lag.
func (c *fleetCounts) counted(ctx context.Context, store storage.Store) storage.Store {
	return storage.Fake{
		GetRangeFn:    func(key string, off, n int64) ([]byte, error) { return c.got(store.GetRange(ctx, key, off, n)) },
		GetSuffixFn:   func(key string, n int64) ([]byte, error) { return c.got(store.GetSuffix(ctx, key, n)) },
		PutFn:         func(key string, body []byte) error { return c.add("put", store.Put(ctx, key, body)) },
		PutIfAbsentFn: func(key string, body []byte) error { return c.add("put-if-absent", store.PutIfAbsent(ctx, key, body)) },
		ListFn: func(prefix, after string) ([]string, error) {
			keys, err := store.List(ctx, prefix, after)
			return keys, c.add("list", err)
		},
	}
}

// timedCatalog records how long each Append, a collector's catalog commit, takes.
type timedCatalog struct {
	catalog.Catalog
	counts *fleetCounts
}

func (c timedCatalog) Append(ctx context.Context, ds ...segment.Descriptor) error {
	start := time.Now()
	err := c.Catalog.Append(ctx, ds...)
	c.counts.mu.Lock()
	defer c.counts.mu.Unlock()
	c.counts.commitMS = append(c.counts.commitMS, ms(time.Since(start)))
	return err
}

// simClock is a store telling the fleet's simulated time, as an object store tells its own
// time, so whatever the catalog dates by the store's clock follows the time simulated.
type simClock struct {
	storage.Store
	now func() time.Time
}

func (c simClock) Now() time.Time { return c.now() }
