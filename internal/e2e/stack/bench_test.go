//go:build stack

package stack

import (
	"bufio"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

var (
	benchCopies = flag.Int("stack.copies", 0, "hour-apart copies of the seeded corpus TestBenchmark ingests; 0 skips it")
	benchOut    = flag.String("stack.out", "", "file TestBenchmark writes its JSON report to")
	benchReps   = flag.Int("stack.reps", 10, "warm repetitions of each benchmark query")
	benchFlush  = flag.Duration("stack.flush", 2*time.Second, "collector flush interval during the benchmark")
	benchNodes  = flag.Int("stack.nodes", 1, "collector processes writing the same time window into one bucket, each following its own copy of the logs")
	benchPhases = flag.String("stack.phases", "ingest,compact,query", "comma-separated phases to run: ingest, compact, query; a query phase without its ingest reads what an earlier run left")
	benchBucket = flag.String("stack.bucket", "", "bucket to ingest into, or to query what an earlier run ingested; empty is a fresh one")
	benchCache  = flag.String("stack.logcache", "", "directory keeping generated logs for a later run of the same copies, nodes and hour")
	benchTenant = flag.String("stack.tenant", "", "bucket/tenant whose storage and catalog load to measure, reading only, instead of running the benchmark")
)

// prices are the unit prices usd_per_gb_month is computed at, AWS us-east-1 list prices of
// October 2026, so a price change is an edit here and a re-run.
var prices = struct {
	VCPUHour      float64 `json:"usd_per_vcpu_hour"`       // c7i on demand
	ObjectGBMonth float64 `json:"usd_per_object_gb_month"` // S3 Standard
}{VCPUHour: 0.0446, ObjectGBMonth: 0.023}

// usdPerGBMonth is a GB of logs ingested and kept a month: the CPU ingesting it took, and
// what it stores as, at storePrice per GB-month. Requests and querying are not in it.
func usdPerGBMonth(cpuSecondsPerGB, storedPerRawByte, storePrice float64) float64 {
	return cpuSecondsPerGB/3600*prices.VCPUHour + storedPerRawByte*storePrice
}

// The benchmark's phases, as -stack.phases names them.
const (
	phaseIngest  = "ingest"
	phaseCompact = "compact"
	phaseQuery   = "query"
)

// manifestKey is where an ingest phase records the corpus it ingested, under a prefix no
// segment or catalog key takes.
const manifestKey = "bench/manifest.json"

// benchManifest is the corpus an ingest phase left in its bucket, which a later run's
// query phases read so they query what is there rather than what the flags say.
type benchManifest struct {
	Copies int                   `json:"copies"`
	Nodes  int                   `json:"nodes"`
	Shift  time.Duration         `json:"shift"`
	Logs   seedcorpus.LogsResult `json:"logs"`
	// Capture is the captured corpus replayed, nil for the seeded one, and Start and End the
	// time its copies span.
	Capture *seedcorpus.Capture `json:"capture,omitempty"`
	Replay  *seedcorpus.Replay  `json:"replay,omitempty"`
	Start   time.Time           `json:"start"`
	End     time.Time           `json:"end"`
	// Session is the longest agent session a captured corpus holds, "" for none.
	Session string `json:"session,omitempty"`
}

// benchReport is one benchmark run, the measured side of the scorecard.
// Every quantity is a number named with its unit, so the scorecard prints any of them.
type benchReport struct {
	Run           runHeader         `json:"run"`
	Ingest        *ingestReport     `json:"ingest,omitempty"`
	Storage       *storageReport    `json:"storage,omitempty"`
	USDPerGBMonth float64           `json:"usd_per_gb_month,omitempty"`
	Compaction    *compactionReport `json:"compaction,omitempty"`
	Querier       *querierReport    `json:"querier,omitempty"`
	Queries       []queryReport     `json:"queries,omitempty"`
	// Phases is each phase's wall time in seconds, a query's with its querier's start and
	// first summary build, so a slow run shows which part to work on.
	Phases map[string]float64 `json:"phase_seconds"`
}

// runHeader is what a report measured, and at what commit; two reports compare only when
// their corpus fields match.
type runHeader struct {
	SHA      string       `json:"sha"`
	Dirty    bool         `json:"dirty"`
	Corpus   corpusHeader `json:"corpus"`
	Store    string       `json:"store"`
	Date     string       `json:"date"`
	Prices   any          `json:"prices"`
	CPUs     int          `json:"cpus,omitempty"`
	MemoryMB int          `json:"memory_mb,omitempty"`
}

type corpusHeader struct {
	Name     string `json:"name"`
	Copies   int    `json:"copies"`
	Nodes    int    `json:"nodes"`
	RawBytes int64  `json:"raw_bytes"`
	Lines    int    `json:"lines"`
	// A captured corpus's fingerprint, one copy's span and its files' bytes.
	Fingerprint string  `json:"fingerprint,omitempty"`
	SpanHours   float64 `json:"span_hours,omitempty"`
	InputBytes  int64   `json:"input_bytes,omitempty"`
}

type ingestReport struct {
	Lines        int     `json:"lines"`
	RawBytes     int64   `json:"raw_bytes"`
	WallSeconds  float64 `json:"wall_seconds"`
	CPUSeconds   float64 `json:"cpu_seconds"`
	LinesPerSec  float64 `json:"lines_per_sec"`
	MBPerSec     float64 `json:"mb_per_sec"`
	MBPerCPUSec  float64 `json:"mb_per_cpu_sec"`
	CPUPerGB     float64 `json:"cpu_seconds_per_gb"`
	MaxRSSMB     float64 `json:"max_rss_mb"`
	RSSSamplesMB []int   `json:"rss_samples_mb"`
	// Requests is the S3 requests ingest made by API, and Rejected those MinIO answered
	// with a 4xx: a create-only catalog commit another collector won comes back 412.
	Requests map[string]int `json:"requests"`
	Rejected map[string]int `json:"rejected"`
	// ObjectsPerHour is the objects ingest wrote per hour of logs it ingested.
	ObjectsPerHour float64 `json:"objects_written_per_hour"`
}

type storageReport struct {
	Segments        int     `json:"segments"`
	SegmentBytes    int64   `json:"segment_bytes"`
	FooterBytes     int64   `json:"footer_bytes"`
	CatalogObjects  int     `json:"catalog_objects"`
	CatalogBytes    int64   `json:"catalog_bytes"`
	OtherObjects    int     `json:"other_objects"`
	OtherBytes      int64   `json:"other_bytes"`
	Compression     float64 `json:"compression"`
	FooterOverhead  float64 `json:"footer_overhead"`
	RowsPerSegment  float64 `json:"rows_per_segment"`
	MeanSegmentSize int64   `json:"mean_segment_bytes"`
	// CatalogLoad is the GETs, LISTs and bytes a querier's first catalog load reads once the
	// catalog is snapshotted: the snapshot's pointer and body, then the commits after it.
	CatalogLoadGETs     int     `json:"catalog_load_gets"`
	CatalogLoadLISTs    int     `json:"catalog_load_lists"`
	CatalogLoadBytes    int64   `json:"catalog_load_bytes"`
	CatalogLoadPerBytes float64 `json:"catalog_load_bytes_per_segment_byte"`
}

type queryReport struct {
	Name      string    `json:"name"`
	Refused   string    `json:"refused,omitempty"` // the refusal's body, in place of any timing
	Cold      callStats `json:"cold"`
	WarmP50MS float64   `json:"warm_p50_ms"`
	WarmP95MS float64   `json:"warm_p95_ms"`
	WarmMaxMS float64   `json:"warm_max_ms"`
	Warm      callStats `json:"warm"`
}

// callStats is one call's costs, or a flow's calls' summed.
type callStats struct {
	MS        float64 `json:"ms"`
	GETs      int     `json:"gets"`
	BytesRead int64   `json:"bytes_read"`
	Coverage  float64 `json:"coverage"`
	Tokens    int     `json:"tokens"`
	Segments  int     `json:"segments"`
	// Calls is a flow's calls and Refused those refused; ResponseBytes is what they answered
	// and AllocBytes what the querier allocated answering.
	Calls         int    `json:"calls,omitempty"`
	Refused       int    `json:"refused,omitempty"`
	ResponseBytes int64  `json:"response_bytes,omitempty"`
	AllocBytes    uint64 `json:"alloc_bytes,omitempty"`
}

// TestBenchmark ingests -stack.copies copies of the seeded corpus through the collector
// process and reports throughput, footprint, and query latency cold and warm.
func TestBenchmark(t *testing.T) {
	if *benchTenant != "" {
		bucket, name, ok := strings.Cut(*benchTenant, "/")
		require.True(t, ok, "-stack.tenant is bucket/tenant")
		endpoint := cmp.Or(os.Getenv("DATAFOCAL_S3_ENDPOINT"), minioEndpoint)
		storage := tenantStack(t, endpoint, bucket).measureStorage(t, name, 0)
		writeReport(t, benchReport{Run: runOf(t, corpusHeader{Name: *benchTenant}, endpoint), Storage: &storage})
		return
	}
	if *benchCopies == 0 && *benchBucket == "" && *benchCorpusDir == "" {
		t.Skip("set -stack.copies or -stack.corpus, or -stack.bucket to query an earlier run's, to run the benchmark")
	}
	phases := map[string]bool{}
	for _, p := range strings.Split(*benchPhases, ",") {
		phases[strings.TrimSpace(p)] = true
	}
	s := newStack(t, *benchFlush)
	if *benchBucket != "" {
		s.useBucket(t, *benchBucket, phases[phaseIngest])
	}
	report, began := benchReport{Phases: map[string]float64{}}, time.Now()
	timed := func(name string, fn func()) {
		start := time.Now()
		fn()
		report.Phases[name] += time.Since(start).Seconds()
	}

	var m benchManifest
	var nodes []node
	if phases[phaseIngest] {
		m, nodes = s.benchCorpus(t, timed)
	} else {
		m = s.readManifest(t)
	}
	report.Run = runOf(t, corpusOf(m), minioImage)
	var lines logLines
	var w benchWindow
	if m.Capture == nil {
		// Every copy holds the same lines, so one copy is enough to pick query inputs from.
		sample := t.TempDir()
		_, err := seedcorpus.WriteLogs(sample, seedcorpus.ScenarioAll, 1, m.Shift)
		require.NoError(t, err)
		lines = readLines(t, sample)
		start, _ := seedcorpus.IncidentWindow()
		w = benchWindow{start: start.Add(m.Shift), copies: m.Copies}
		m.Start, m.End = w.start, w.end()
	}

	s.ingestAndCompact(t, phases, m, nodes, &report, timed)
	if phases[phaseIngest] {
		timed("storage", func() {
			storage := s.measureStorage(t, tenant, m.Logs.Bytes)
			report.Storage = &storage
		})
		stored := float64(report.Storage.SegmentBytes+report.Storage.CatalogBytes) / float64(m.Logs.Bytes)
		report.USDPerGBMonth = usdPerGBMonth(report.Ingest.CPUPerGB, stored, prices.ObjectGBMonth)
	}
	if phases[phaseQuery] {
		var flows []flow
		timed("querier at rest", func() {
			querier := s.measureQuerier(t, func(base string) {
				timed("discover", func() { flows = benchFlows(t, base, m, lines, w) })
			})
			report.Querier = &querier
		})
		for _, f := range flows {
			timed("query: "+f.name, func() { report.Queries = append(report.Queries, s.measureFlow(t, f, report.Querier, timed)) })
		}
	}
	report.Phases["total"] = time.Since(began).Seconds()
	writeReport(t, report)
}

// ingestAndCompact runs the ingest and compaction phases, or restores a captured corpus's
// bucket and their reports from -stack.ingestcache when the code they ran is unchanged.
func (s *stack) ingestAndCompact(t *testing.T, phases map[string]bool, m benchManifest, nodes []node, report *benchReport, timed func(string, func())) {
	t.Helper()
	key := ""
	if m.Capture != nil && *benchIngestCache != "" && phases[phaseIngest] && phases[phaseCompact] {
		key = ingestKey(t, m)
		restored := false
		timed("restore ingest", func() { restored = s.restoreIngest(t, key, report) })
		if restored {
			return
		}
	}
	if phases[phaseIngest] {
		timed("ingest", func() {
			ingest := s.measureIngest(t, nodes, totalLines(m.Logs), m.Logs.Bytes, m.End.Sub(m.Start).Hours())
			report.Ingest = &ingest
		})
		s.writeManifest(t, m)
	}
	if phases[phaseCompact] {
		timed("compact", func() {
			compaction := s.compactAll(t)
			report.Compaction = &compaction
		})
	}
	if key != "" {
		timed("save ingest", func() { s.saveIngest(t, key, *report) })
	}
}

// tenantStack reads bucket at endpoint with the environment's AWS credentials, or the
// stack's MinIO's when it sets none.
func tenantStack(t *testing.T, endpoint, bucket string) *stack {
	t.Helper()
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", accessKey)
		t.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)
	}
	client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewEnvAWS()})
	require.NoError(t, err)
	store, err := storage.NewS3(storage.S3Config{Endpoint: endpoint, Bucket: bucket})
	require.NoError(t, err)
	return &stack{bucket: bucket, client: client, store: store}
}

// runOf is the run header of a report on corpus in store, at the checked-out commit.
func runOf(t *testing.T, corpus corpusHeader, store string) runHeader {
	t.Helper()
	git := func(args ...string) string {
		cmd := exec.Command("git", args...)
		cmd.Dir = moduleRoot()
		out, err := cmd.Output()
		require.NoError(t, err)
		return strings.TrimSpace(string(out))
	}
	dirty := git("status", "--porcelain") != ""
	return runHeader{SHA: git("rev-parse", "HEAD"), Dirty: dirty, Corpus: corpus,
		Store: store, Date: time.Now().UTC().Format(time.DateOnly), Prices: prices, CPUs: *limitCPUs, MemoryMB: *limitMemMB}
}

func writeReport(t *testing.T, report any) {
	t.Helper()
	data, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)
	t.Logf("report:\n%s", data)
	if *benchOut != "" {
		require.NoError(t, os.WriteFile(*benchOut, data, 0o644))
	}
}

// benchCorpus writes, or takes from -stack.logcache, the logs a fresh corpus ingests:
// the copies end at the last whole hour, and each node has its own log directory.
func (s *stack) benchCorpus(t *testing.T, timed func(string, func())) (benchManifest, []node) {
	t.Helper()
	if *benchCorpusDir != "" {
		return s.replayCorpus(t, timed)
	}
	copies, nodeCount := *benchCopies, max(*benchNodes, 1)
	require.Positive(t, copies, "writing a corpus needs -stack.copies")
	start, _ := seedcorpus.IncidentWindow()
	end := time.Now().UTC().Truncate(time.Hour)
	m := benchManifest{Copies: copies, Nodes: nodeCount, Shift: end.Add(-time.Duration(copies) * time.Hour).Sub(start)}
	cached := ""
	if *benchCache != "" {
		cached = filepath.Join(*benchCache, fmt.Sprintf("%dc-%dn-%s", copies, nodeCount, end.Format("20060102T15")))
		s.logs = cached
	}
	nodes := []node{s.defaultNode()}
	if nodeCount > 1 {
		nodes = nil
		for i := range nodeCount {
			nodes = append(nodes, s.newNode(t, fmt.Sprintf("node-%03d", i)))
		}
	}
	written := filepath.Join(s.logs, "written.json")
	if data, err := os.ReadFile(written); cached != "" && err == nil {
		require.NoError(t, json.Unmarshal(data, &m.Logs))
		return m, nodes
	}
	timed("write logs", func() { m.Logs = writeNodeLogs(t, nodes, seedcorpus.ScenarioAll, copies, m.Shift) })
	if cached != "" {
		data, err := json.Marshal(m.Logs)
		require.NoError(t, err)
		// Last, so an interrupted write leaves no record of a complete one.
		require.NoError(t, os.WriteFile(written, data, 0o644))
	}
	return m, nodes
}

func (s *stack) writeManifest(t *testing.T, m benchManifest) {
	t.Helper()
	data, err := json.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, s.store.Put(t.Context(), manifestKey, data))
}

func (s *stack) readManifest(t *testing.T) benchManifest {
	t.Helper()
	data, err := s.store.GetSuffix(t.Context(), manifestKey, math.MaxInt64)
	require.NoErrorf(t, err, "bucket %s holds no corpus an ingest phase left", s.bucket)
	var m benchManifest
	require.NoError(t, json.Unmarshal(data, &m))
	return m
}

// measureIngest runs a collector per node at once and waits for all of their lines; the
// CPU is every collector's, the peak memory the largest one's, and the samples the first's.
func (s *stack) measureIngest(t *testing.T, nodes []node, lines int, rawBytes int64, hours float64) ingestReport {
	t.Helper()
	requests, rejected := s.bucketRequests(t)
	start := time.Now()
	collectors := make([]*process, len(nodes))
	for i, n := range nodes {
		collectors[i] = s.startNode(t, n)
	}
	samples := make(chan []int, 1)
	ctx, stop := context.WithCancel(t.Context())
	go func() { samples <- sampleRSS(ctx, collectors[0].cmd.Process.Pid) }()
	// Five minutes for each 60 MB, a seeded copy's size, collected.
	s.waitRows(t, lines, time.Duration(1+rawBytes/(60<<20))*5*time.Minute, collectors...)
	wall := time.Since(start)
	stop()

	var cpu, maxRSS float64
	for _, c := range collectors {
		state := c.stop(t)
		cpu += (state.UserTime() + state.SystemTime()).Seconds()
		maxRSS = max(maxRSS, float64(state.SysUsage().(*syscall.Rusage).Maxrss)/1024)
	}
	mb := float64(rawBytes) / 1e6
	r := ingestReport{
		Lines: lines, RawBytes: rawBytes,
		WallSeconds: wall.Seconds(), CPUSeconds: cpu,
		LinesPerSec: float64(lines) / wall.Seconds(), MBPerSec: mb / wall.Seconds(), MBPerCPUSec: mb / cpu, CPUPerGB: cpu / (mb / 1e3),
		MaxRSSMB: maxRSS, RSSSamplesMB: <-samples,
	}
	r.Requests, r.Rejected = s.settledRequests(t)
	for api, n := range requests {
		r.Requests[api] -= n
	}
	for api, n := range rejected {
		r.Rejected[api] -= n
	}
	r.ObjectsPerHour = float64(r.Requests["putobject"]-r.Rejected["putobject"]) / hours
	return r
}

// settledRequests is bucketRequests once two reads a second apart agree, since MinIO
// refreshes its bucket metrics behind the requests they count.
func (s *stack) settledRequests(t *testing.T) (requests, rejected map[string]int) {
	t.Helper()
	requests, rejected = s.bucketRequests(t)
	for deadline := time.Now().Add(time.Minute); time.Now().Before(deadline); {
		<-time.After(time.Second)
		nextRequests, nextRejected := s.bucketRequests(t)
		if maps.Equal(requests, nextRequests) && maps.Equal(rejected, nextRejected) && requests["putobject"] > 0 {
			return requests, rejected
		}
		requests, rejected = nextRequests, nextRejected
	}
	return requests, rejected
}

// bucketRequests is the stack bucket's S3 requests so far by API, and those answered
// with a 4xx, from MinIO's bucket metrics.
func (s *stack) bucketRequests(t *testing.T) (requests, rejected map[string]int) {
	t.Helper()
	resp, err := http.Get("http://" + minioEndpoint + "/minio/v2/metrics/bucket")
	require.NoError(t, err)
	defer resp.Body.Close()
	requests, rejected = map[string]int{}, map[string]int{}
	bucket := fmt.Sprintf("bucket=%q", s.bucket)
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(nil, 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		into := requests
		switch {
		case strings.HasPrefix(line, "minio_bucket_requests_total{"):
		case strings.HasPrefix(line, "minio_bucket_requests_4xx_errors_total{"):
			into = rejected
		default:
			continue
		}
		if !strings.Contains(line, bucket) {
			continue
		}
		_, rest, _ := strings.Cut(line, `api="`)
		api, _, _ := strings.Cut(rest, `"`)
		v, err := strconv.ParseFloat(line[strings.LastIndexByte(line, ' ')+1:], 64)
		require.NoError(t, err)
		into[api] += int(v)
	}
	require.NoError(t, scanner.Err())
	return requests, rejected
}

// sampleRSS reads pid's resident set every second until ctx ends, to show whether
// memory stays flat under sustained ingest rather than only its peak.
func sampleRSS(ctx context.Context, pid int) []int {
	var out []int
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return out
		case <-tick.C:
		}
		f, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
		if err != nil {
			return out
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			if kb, ok := strings.CutPrefix(scanner.Text(), "VmRSS:"); ok {
				n, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimSpace(kb), " kB")) // a bad sample reads as zero
				out = append(out, n/1024)
			}
		}
		f.Close()
	}
}

// measureStorage sizes tenant's objects and counts what its catalog's first load reads. Given rawBytes,
// the benchmark's own corpus, it first snapshots the catalog, as the compactor would.
func (s *stack) measureStorage(t *testing.T, tenant string, rawBytes int64) storageReport {
	t.Helper()
	ctx := t.Context()
	sizes := map[string]int64{}
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: tenant + "/", Recursive: true}) {
		require.NoError(t, obj.Err)
		sizes[obj.Key] = obj.Size
	}
	cat := catalog.New(s.store, tenant)
	v, err := cat.Version(ctx)
	require.NoError(t, err)
	descs, err := cat.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)

	var r storageReport
	rows := 0
	for _, d := range descs {
		size, ok := sizes[d.Object.Key]
		require.Truef(t, ok, "catalog names segment %s the bucket does not hold", d.Object.Key)
		delete(sizes, d.Object.Key)
		r.Segments++
		r.SegmentBytes += size
		rows += d.Rows
		// The segment trailer is footerLen(8) then magic(8), per internal/segment/codec.go.
		trailer, err := s.store.GetSuffix(ctx, d.Object.Key, 16)
		require.NoError(t, err)
		r.FooterBytes += int64(binary.BigEndian.Uint64(trailer[:8])) + 16
	}
	for key, size := range sizes {
		if strings.HasPrefix(key, tenant+"/catalog/") {
			r.CatalogObjects++
			r.CatalogBytes += size
		} else {
			r.OtherObjects++
			r.OtherBytes += size
		}
	}
	if rawBytes > 0 {
		r.Compression = float64(rawBytes) / float64(r.SegmentBytes)
		require.NoError(t, catalog.NewSnapshotter(cat, catalog.SnapshotConfig{MinCommits: 1}).Snapshot(ctx))
	}
	r.FooterOverhead = float64(r.FooterBytes) / float64(r.SegmentBytes)
	r.RowsPerSegment = float64(rows) / float64(r.Segments)
	r.MeanSegmentSize = r.SegmentBytes / int64(r.Segments)
	s.catalogLoad(t, tenant, &r)
	r.CatalogLoadPerBytes = float64(r.CatalogLoadBytes) / float64(r.SegmentBytes)
	return r
}

// catalogLoad counts the requests and bytes a cold catalog load reads, as a querier's first does.
func (s *stack) catalogLoad(t *testing.T, tenant string, r *storageReport) {
	t.Helper()
	ctx := t.Context()
	counted := storage.Fake{
		ListFn: func(prefix, after string) ([]string, error) {
			r.CatalogLoadLISTs++
			return s.store.List(ctx, prefix, after)
		},
		GetSuffixFn: func(key string, n int64) ([]byte, error) {
			r.CatalogLoadGETs++
			body, err := s.store.GetSuffix(ctx, key, n)
			r.CatalogLoadBytes += int64(len(body))
			return body, err
		},
	}
	_, err := catalog.New(counted, tenant).Version(ctx)
	require.NoError(t, err)
}

// benchWindow is where the benchmark's copies lie: copies hours from start, the first
// copy's incident window starting at start.
type benchWindow struct {
	start  time.Time
	copies int
}

func (w benchWindow) end() time.Time { return w.start.Add(time.Duration(w.copies) * time.Hour) }

// waitSummary waits until the snapshot's summary version is its catalog version
// (e<n>.s<n>), which is when the summary may serve.
func (s *stack) waitSummary(t *testing.T, base string) {
	t.Helper()
	req := tools.AggregateRequest{Selector: selector.Selector{Time: selector.TimeSpec{From: "now-1m", To: "now"}}, Measure: "count", GroupBy: "service", Bins: "all"}
	require.Eventually(t, func() bool {
		env, _ := call[tools.AggregateResponse](t, base, "aggregate", req)
		catalogPart, summaryPart, _ := strings.Cut(strings.Split(env.Meta.Snapshot, ".d")[0], ".s")
		return strings.TrimPrefix(catalogPart, "e") == summaryPart
	}, 5*time.Minute, 100*time.Millisecond, "summary never caught up with the catalog")
}

func statsOf(meta query.Meta, elapsed time.Duration) callStats {
	return callStats{MS: ms(elapsed), GETs: meta.Cost.GETs, BytesRead: meta.Cost.BytesRead, Coverage: meta.Coverage, Tokens: meta.Tokens, Segments: meta.Cost.Segments}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
