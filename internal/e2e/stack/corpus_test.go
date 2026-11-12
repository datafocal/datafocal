//go:build stack

package stack

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/metrics"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/compact"
	"github.com/datafocal/datafocal/internal/e2e"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
)

// replayCorpus writes -stack.copies copies (the capture's own count unless set) of the captured corpus to the
// stack's log directory, ending at the current minute as a live system's logs do, and has
// the collector read it as the capture's sources declare.
func (s *stack) replayCorpus(t *testing.T, timed func(string, func())) (benchManifest, []node) {
	t.Helper()
	require.LessOrEqual(t, *benchNodes, 1, "a captured corpus replays on one node")
	capture, err := seedcorpus.ReadCapture(*benchCorpusDir)
	require.NoError(t, err)
	copies := max(cmp.Or(*benchCopies, capture.Copies), 1)
	replay := s.cachedReplay(t, capture, copies, timed)
	s.sources = capture.Sources
	return benchManifest{Copies: copies, Nodes: 1, Capture: &capture, Replay: &replay, Start: replay.Start, End: replay.End,
		Session: longestSession(t, s.logs), Logs: seedcorpus.LogsResult{Lines: map[string]int{"": replay.Records}, Bytes: replay.Bytes}}, []node{s.defaultNode()}
}

// cachedReplay replays copies of the capture into the stack's logs, or with -stack.logcache
// reuses the one an earlier run wrote there, ending when it did, so runs it compares read the
// same bytes and only the first pays for the replay.
func (s *stack) cachedReplay(t *testing.T, capture seedcorpus.Capture, copies int, timed func(string, func())) seedcorpus.Replay {
	t.Helper()
	var replay seedcorpus.Replay
	written := ""
	if *benchCache != "" {
		s.logs = filepath.Join(*benchCache, fmt.Sprintf("%s-%dc", capture.Name, copies))
		written = filepath.Join(s.logs, "replay.json")
		if data, err := os.ReadFile(written); err == nil {
			require.NoError(t, json.Unmarshal(data, &replay))
			return replay
		}
		require.NoError(t, os.RemoveAll(s.logs))
	}
	var err error
	timed("write logs", func() {
		replay, err = seedcorpus.ReplayCapture(*benchCorpusDir, s.logs, copies, time.Now().UTC().Truncate(time.Minute))
	})
	require.NoError(t, err)
	if written != "" {
		data, err := json.Marshal(replay)
		require.NoError(t, err)
		// Last, so an interrupted replay leaves no record of a complete one.
		require.NoError(t, os.WriteFile(written, data, 0o644))
	}
	return replay
}

// sessionName is a transcript's name, its session's UUID.
var sessionName = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// longestSession is the session of the transcript under dir with the most lines, an agent's
// transcript being named for its session, "" when dir holds none.
func longestSession(t *testing.T, dir string) string {
	t.Helper()
	best, most := "", 0
	err := filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
		session := strings.TrimSuffix(filepath.Base(path), ".jsonl")
		if err != nil || e.IsDir() || filepath.Ext(path) != ".jsonl" || strings.Contains(path, "subagents") || !sessionName.MatchString(session) {
			return err
		}
		data, err := os.ReadFile(path)
		if n := bytes.Count(data, []byte("\n")); err == nil && n > most {
			best, most = session, n
		}
		return err
	})
	require.NoError(t, err)
	return best
}

// corpusOf is the run header's account of the corpus m holds.
func corpusOf(m benchManifest) corpusHeader {
	c := corpusHeader{Name: string(seedcorpus.ScenarioAll), Copies: m.Copies, Nodes: m.Nodes, RawBytes: m.Logs.Bytes, Lines: totalLines(m.Logs)}
	if m.Capture != nil {
		c.Name, c.Fingerprint, c.SpanHours, c.InputBytes = m.Capture.Name, m.Replay.Fingerprint, m.Replay.Span.Hours(), m.Replay.InputBytes
	}
	return c
}

// benchFlows is what the benchmark asks of m: the seeded corpus's archetypes and the views
// any cluster's logs open, or what a captured corpus's kind is read by, found on the querier at base.
func benchFlows(t *testing.T, base string, m benchManifest, lines logLines, w benchWindow) []flow {
	t.Helper()
	if m.Capture != nil {
		return discover(t, base, m.Capture.Kind, m.Session, m.Start, m.End)
	}
	corpus := e2e.Corpus{Start: w.start, Copies: w.copies, TraceID: lines.firstValue("gateway", "trace_id"),
		SymptomPattern: lines.patternID("slow query ")}
	var flows []flow
	for _, a := range e2e.Archetypes(corpus) {
		flows = append(flows, archetypeFlow(a))
	}
	return append(flows, viewFlow(t, "graph view: open, last hour", "view-graph", m.End.Add(-time.Hour), m.End, "", ""),
		viewFlow(t, "logs view: open, last hour", "view-logs", m.End.Add(-time.Hour), m.End, "", ""))
}

// compactionReport is one compactor pass merging every hour and window the ingest left, as
// the deployed compactor would have as they completed.
type compactionReport struct {
	WallSeconds    float64 `json:"wall_seconds"`
	CPUSeconds     float64 `json:"cpu_seconds"`
	PeakHeapMB     float64 `json:"peak_heap_mb"`
	SegmentsBefore int     `json:"segments_before"`
	SegmentsAfter  int     `json:"segments_after"`
}

// compactAll runs that pass in this process, whose CPU and heap the benchmark otherwise
// leaves idle while it runs, so they are the compactor's.
func (s *stack) compactAll(t *testing.T) compactionReport {
	t.Helper()
	ctx := t.Context()
	cat := catalog.NewWithConfig(s.store, tenant, catalog.Config{Grace: catalog.DefaultGrace, Writer: "compactor"})
	c := compact.NewWithConfig(cat, catalog.NewSnapshotter(cat, catalog.SnapshotConfig{}), segment.NewReader(s.store), s.store,
		compact.Config{Tenant: tenant, Window: compactWindow})
	var r compactionReport
	var err error
	_, r.SegmentsBefore, err = s.catalogRows(ctx)
	require.NoError(t, err)
	// The compactor's pod's cores, as the querier and collector are held to theirs.
	if *limitCPUs > 0 {
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(*limitCPUs))
	}
	base, cpu, start := liveHeapBytes(), processCPU(), time.Now()
	peak := peakLiveDuring(func() { require.NoError(t, c.Run(ctx, time.Now())) })
	r.WallSeconds, r.CPUSeconds = time.Since(start).Seconds(), processCPU()-cpu
	r.PeakHeapMB = float64(peak-min(peak, base)) / (1 << 20)
	_, r.SegmentsAfter, err = s.catalogRows(ctx)
	require.NoError(t, err)
	return r
}

func processCPU() float64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano()).Seconds()
}

func liveHeapBytes() uint64 {
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}

// peakLiveDuring is the most live heap a collection found during fn, sampled every 10 ms.
func peakLiveDuring(fn func()) uint64 {
	done, peak := make(chan struct{}), make(chan uint64)
	go func() {
		most := liveHeapBytes()
		for tick := time.NewTicker(10 * time.Millisecond); ; {
			select {
			case <-done:
				tick.Stop()
				peak <- most
				return
			case <-tick.C:
				most = max(most, liveHeapBytes())
			}
		}
	}()
	fn()
	close(done)
	return <-peak
}

var benchIngestCache = flag.String("stack.ingestcache", "", "a directory keeping a captured corpus's ingested and compacted bucket, keyed by the code that wrote it, so a run that changed neither restores it rather than ingesting again")

// ingestKey names what a captured corpus's bucket holds once ingested and compacted: the
// replay it read, the limits and flush it ran under, and the source of every package the
// collector and compactor are built from.
func ingestKey(t *testing.T, m benchManifest) string {
	t.Helper()
	list := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.Dir}}{{end}}", "./cmd/collector", "./internal/compact")
	list.Dir = moduleRoot()
	out, err := list.Output()
	require.NoError(t, err)
	h := sha256.New()
	fmt.Fprintf(h, "%s %d %s %s %d %d %s\n", m.Replay.Fingerprint, m.Copies, m.Replay.Start, m.Replay.End, *limitCPUs, *limitMemMB, *benchFlush)
	files := []string{filepath.Join(moduleRoot(), "go.sum")}
	for dir := range strings.FieldsSeq(string(out)) {
		matches, err := filepath.Glob(filepath.Join(dir, "*.go"))
		require.NoError(t, err)
		for _, f := range matches {
			if !strings.HasSuffix(f, "_test.go") {
				files = append(files, f)
			}
		}
	}
	for _, f := range files {
		data, err := os.ReadFile(f)
		require.NoError(t, err)
		fmt.Fprintf(h, "%s %d\n", f, len(data))
		h.Write(data)
	}
	return fmt.Sprintf("%s-%x", m.Capture.Name, h.Sum(nil)[:8])
}

// cachedIngest is what a cached ingest keeps beside its bucket's objects: the phases' reports.
type cachedIngest struct {
	Ingest     *ingestReport     `json:"ingest"`
	Compaction *compactionReport `json:"compaction"`
}

// restoreIngest puts key's cached objects in the stack's bucket and their reports in r,
// false when nothing is cached under key.
func (s *stack) restoreIngest(t *testing.T, key string, r *benchReport) bool {
	t.Helper()
	dir := filepath.Join(*benchIngestCache, key)
	data, err := os.ReadFile(filepath.Join(dir, "reports.json"))
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	require.NoError(t, err)
	var c cachedIngest
	require.NoError(t, json.Unmarshal(data, &c))
	objects := filepath.Join(dir, "objects")
	err = filepath.WalkDir(objects, func(path string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		name, _ := filepath.Rel(objects, path)
		_, err = s.client.FPutObject(t.Context(), s.bucket, filepath.ToSlash(name), path, minio.PutObjectOptions{})
		return err
	})
	require.NoError(t, err)
	r.Ingest, r.Compaction = c.Ingest, c.Compaction
	return true
}

// saveIngest keeps the stack's bucket and r's ingest and compaction under key, the reports
// last and the directory renamed into place, so a partial save is never restored.
func (s *stack) saveIngest(t *testing.T, key string, r benchReport) {
	t.Helper()
	require.NoError(t, os.MkdirAll(*benchIngestCache, 0o755))
	tmp, err := os.MkdirTemp(*benchIngestCache, key+".tmp-")
	require.NoError(t, err)
	for obj := range s.client.ListObjects(t.Context(), s.bucket, minio.ListObjectsOptions{Recursive: true}) {
		require.NoError(t, obj.Err)
		require.NoError(t, s.client.FGetObject(t.Context(), s.bucket, obj.Key, filepath.Join(tmp, "objects", filepath.FromSlash(obj.Key)), minio.GetObjectOptions{}))
	}
	data, err := json.Marshal(cachedIngest{Ingest: r.Ingest, Compaction: r.Compaction})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "reports.json"), data, 0o644))
	dir := filepath.Join(*benchIngestCache, key)
	require.NoError(t, os.RemoveAll(dir))
	require.NoError(t, os.Rename(tmp, dir))
}
