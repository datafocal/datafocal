//go:build stack

package stack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

const (
	tenant    = seedcorpus.Tenant
	accessKey = "datafocal"
	secretKey = "datafocal-dev-secret"
	// minioImage is Chainguard's build of MinIO, since MinIO no longer publishes
	// pullable images to quay.io or Docker Hub.
	minioImage = "cgr.dev/chainguard/minio:latest"
)

// Shared across the package: one MinIO container and one build of each binary. Every
// test gets its own bucket, so tests never see each other's segments or catalog.
var (
	minioEndpoint string
	collectorBin  string
	querierBin    string
	// dockerNetwork is where containers reach MinIO, as minioAlias, while the binaries
	// on the host reach it at minioEndpoint.
	dockerNetwork string
)

const minioAlias = "minio"

// stackPersist keeps the docker network and MinIO under fixed names when the run ends,
// and reuses them when a run starts and finds them, so a later run skips their start
// and can query a bucket this one left.
// The limits hold the processes the benchmark measures to the given cores and memory, so its
// timings are per node of that size. MinIO, standing in for the object store, is not limited.
var (
	limitCPUs  = flag.Int("stack.cpus", 0, "cores each measured system may use; 0 is the whole host")
	limitMemMB = flag.Int("stack.memory_mb", 0, "MiB of memory each measured system may use; 0 is no limit")
)

// limitEnv holds a DataFocal process to the limits through its runtime; GOMEMLIMIT is a soft
// limit.
func limitEnv() []string {
	var env []string
	if *limitCPUs > 0 {
		env = append(env, fmt.Sprintf("GOMAXPROCS=%d", *limitCPUs))
	}
	if *limitMemMB > 0 {
		// The limit a pod's querier reads its request budget from, as the downward API gives it.
		env = append(env, fmt.Sprintf("GOMEMLIMIT=%dMiB", *limitMemMB), fmt.Sprintf("DATAFOCAL_MEMORY_LIMIT=%d", *limitMemMB<<20))
	}
	return env
}

var stackPersist = flag.Bool("stack.persist", false, "keep MinIO running after the run under fixed names, and reuse them when they are")

// persistName prefixes what -stack.persist keeps.
const persistName = "datafocal-stack"

// TestMain returns rather than exiting, which hands m.Run's result to the test binary;
// a stack that cannot start is a setup failure, so it panics.
func TestMain(m *testing.M) {
	flag.Parse() // -stack.persist decides how the stack starts, before m.Run would parse
	if err := runWithStack(m); err != nil {
		panic(fmt.Sprintf("stack: %v", err))
	}
}

// runWithStack builds the binaries and starts MinIO, runs the tests, and removes the
// container whatever they did.
func runWithStack(m *testing.M) error {
	bin, err := os.MkdirTemp("", "datafocal-stack-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(bin) }()
	collectorBin, querierBin = filepath.Join(bin, "collector"), filepath.Join(bin, "querier")
	for out, pkg := range map[string]string{collectorBin: "./cmd/collector", querierBin: "./cmd/querier"} {
		build := exec.Command("go", "build", "-o", out, pkg)
		build.Dir = moduleRoot()
		if msg, err := build.CombinedOutput(); err != nil {
			return fmt.Errorf("build %s: %w: %s", pkg, err, msg)
		}
	}

	if *stackPersist {
		dockerNetwork = persistName
		if err := exec.Command("docker", "network", "inspect", dockerNetwork).Run(); err != nil {
			if out, err := exec.Command("docker", "network", "create", dockerNetwork).CombinedOutput(); err != nil {
				return fmt.Errorf("docker network create: %w: %s", err, out)
			}
		}
		if _, err := startMinIO(persistName + "-minio"); err != nil {
			return err
		}
		m.Run()
		return nil
	}
	removeOrphans()
	dockerNetwork = fmt.Sprintf("datafocal-stack-%d", os.Getpid())
	if out, err := exec.Command("docker", "network", "create", dockerNetwork).CombinedOutput(); err != nil {
		return fmt.Errorf("docker network create: %w: %s", err, out)
	}
	defer exec.Command("docker", "network", "rm", dockerNetwork).Run() //nolint:errcheck // best-effort teardown
	container, err := startMinIO("")
	if container != "" {
		defer exec.Command("docker", "rm", "-f", container).Run() //nolint:errcheck // best-effort teardown
	}
	if err != nil {
		return err
	}
	m.Run()
	return nil
}

func moduleRoot() string {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}").Output()
	if err != nil {
		panic(fmt.Sprintf("find module root: %v", err))
	}
	return strings.TrimSpace(string(out))
}

// removeOrphans removes what a run killed before its teardown left: each datafocal-stack-<pid>
// network whose process is gone, and the containers on it.
func removeOrphans() {
	out, _ := exec.Command("docker", "network", "ls", "--format", "{{.Name}}").Output()
	for name := range strings.FieldsSeq(string(out)) {
		pid, err := strconv.Atoi(strings.TrimPrefix(name, "datafocal-stack-"))
		if err != nil || !strings.HasPrefix(name, "datafocal-stack-") || syscall.Kill(pid, 0) == nil {
			continue
		}
		ids, _ := exec.Command("docker", "ps", "-aq", "--filter", "network="+name).Output()
		if containers := strings.Fields(string(ids)); len(containers) > 0 {
			_ = exec.Command("docker", append([]string{"rm", "-f"}, containers...)...).Run()
		}
		_ = exec.Command("docker", "network", "rm", name).Run()
	}
}

// startMinIO runs MinIO on a loopback port docker picks and waits for it to be ready; a
// named one already running is reused.
func startMinIO(name string) (string, error) {
	if name != "" && running(name) {
		return name, waitMinIO(name)
	}
	args := []string{"run", "-d", "--rm"}
	if name != "" {
		args = append(args, "--name", name)
	}
	out, err := exec.Command("docker", append(args, "-p", "127.0.0.1::9000",
		"--network", dockerNetwork, "--network-alias", minioAlias,
		"-e", "MINIO_ROOT_USER="+accessKey, "-e", "MINIO_ROOT_PASSWORD="+secretKey,
		// Public metrics, so the benchmark can count each bucket's requests by API.
		"-e", "MINIO_PROMETHEUS_AUTH_TYPE=public", "--cpus=2",
		minioImage, "server", "/data")...).Output()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return "", fmt.Errorf("docker run minio: %w: %s", err, exit.Stderr)
	}
	if err != nil {
		return "", fmt.Errorf("docker run minio: %w", err)
	}
	container := strings.TrimSpace(string(out))
	return container, waitMinIO(container)
}

// running reports whether a container of that name is running.
func running(name string) bool {
	out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", name).Output()
	return err == nil && strings.TrimSpace(string(out)) == "true"
}

// waitMinIO finds the loopback port container's MinIO listens on and waits for it.
func waitMinIO(container string) error {
	port, err := exec.Command("docker", "port", container, "9000/tcp").Output()
	if err != nil {
		return fmt.Errorf("docker port: %w", err)
	}
	minioEndpoint = strings.TrimSpace(strings.Split(string(port), "\n")[0])
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + minioEndpoint + "/minio/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		<-time.After(200 * time.Millisecond)
	}
	return fmt.Errorf("minio at %s not ready after a minute", minioEndpoint)
}

// stack is one test's bucket, log directory and collector state directory.
type stack struct {
	bucket   string
	logs     string
	state    string
	scratch  string
	client   *minio.Client
	store    storage.Store
	flush    time.Duration
	children []*process
	// sources are a captured corpus's, under each node's logs; nil reads the seeded JSON lines.
	sources []seedcorpus.CaptureSource
}

var bucketChars = regexp.MustCompile(`[^a-z0-9-]+`)

func newStack(t *testing.T, flush time.Duration) *stack {
	t.Helper()
	client, err := minio.New(minioEndpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, "")})
	require.NoError(t, err)
	bucket := strings.Trim(bucketChars.ReplaceAllString(strings.ToLower(t.Name()), "-"), "-")
	bucket = bucket[:min(len(bucket), 50)] + fmt.Sprintf("-%d", time.Now().UnixNano()%1e9)
	require.NoError(t, client.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{}))

	t.Setenv("AWS_ACCESS_KEY_ID", accessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)
	store, err := storage.NewS3(storage.S3Config{Endpoint: minioEndpoint, Bucket: bucket})
	require.NoError(t, err)

	dir := t.TempDir()
	s := &stack{bucket: bucket, logs: filepath.Join(dir, "logs"), state: filepath.Join(dir, "state"),
		scratch: dir, client: client, store: store, flush: flush}
	require.NoError(t, os.MkdirAll(s.logs, 0o755))
	require.NoError(t, os.MkdirAll(s.state, 0o755))
	t.Cleanup(func() {
		for _, p := range s.children {
			p.kill()
		}
		if t.Failed() {
			for _, p := range s.children {
				t.Logf("--- %s log tail ---\n%s", p.name, p.tail(40))
			}
		}
	})
	return s
}

// useBucket points the stack at the named bucket, creating it when missing; emptied when
// the run will ingest into it, so its catalog holds only this run's segments.
func (s *stack) useBucket(t *testing.T, name string, empty bool) {
	t.Helper()
	ctx := t.Context()
	exists, err := s.client.BucketExists(ctx, name)
	require.NoError(t, err)
	if !exists {
		require.NoError(t, s.client.MakeBucket(ctx, name, minio.MakeBucketOptions{}))
	}
	if empty {
		s.emptyBucket(t, name)
	}
	store, err := storage.NewS3(storage.S3Config{Endpoint: minioEndpoint, Bucket: name})
	require.NoError(t, err)
	s.bucket, s.store = name, store
}

func (s *stack) emptyBucket(t *testing.T, name string) {
	t.Helper()
	ctx := t.Context()
	objects := make(chan minio.ObjectInfo)
	go func() {
		defer close(objects)
		for obj := range s.client.ListObjects(ctx, name, minio.ListObjectsOptions{Recursive: true}) {
			objects <- obj
		}
	}()
	for err := range s.client.RemoveObjects(ctx, name, objects, minio.RemoveObjectsOptions{}) {
		require.NoError(t, err.Err)
	}
}

// env is the deployed configuration (deploy/k8s/base) pointed at this stack's bucket.
func (s *stack) env() []string {
	return append(append(os.Environ(), limitEnv()...),
		"DATAFOCAL_STORE=s3",
		"DATAFOCAL_S3_ENDPOINT="+minioEndpoint,
		"DATAFOCAL_S3_BUCKET="+s.bucket,
		"DATAFOCAL_TENANT="+tenant,
		"DATAFOCAL_FLUSH_INTERVAL="+s.flush.String(),
		// As deployed (deploy/k8s/base, deploy/local): a backfill of days is cut by the hour.
		"DATAFOCAL_FLUSH_ALIGNMENT=1h",
	)
}

// node is one collector's view of the stack: the log files it follows, where it keeps
// its offsets, and the host it names records with, the machine's when empty.
type node struct {
	name, logs, state, host string
}

// defaultNode is the stack's own log and state directories, for a single collector.
func (s *stack) defaultNode() node {
	return node{name: "collector", logs: s.logs, state: s.state}
}

// newNode is a collector named name with its own directories under the stack's logs.
func (s *stack) newNode(t *testing.T, name string) node {
	t.Helper()
	n := node{name: name, logs: filepath.Join(s.logs, name), state: filepath.Join(s.scratch, name+"-state"), host: name}
	require.NoError(t, os.MkdirAll(n.logs, 0o755))
	require.NoError(t, os.MkdirAll(n.state, 0o755))
	return n
}

// startCollector runs the collector binary following every JSON-lines file under logs.
func (s *stack) startCollector(t *testing.T) *process {
	t.Helper()
	return s.startNode(t, s.defaultNode())
}

func (s *stack) startNode(t *testing.T, n node) *process {
	t.Helper()
	sources := []map[string]any{{"path": filepath.Join(n.logs, "*.log"), "parse": map[string]string{"format": "json"}}}
	if s.sources != nil {
		sources = nil
		for _, src := range s.sources {
			sources = append(sources, map[string]any{"path": filepath.Join(n.logs, src.Path), "parse": src.Parse})
		}
	}
	cfg := map[string]any{"ingest": map[string]any{"state_dir": n.state, "host": n.host, "sources": sources}}
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	path := filepath.Join(s.scratch, n.name+".json")
	require.NoError(t, os.WriteFile(path, data, 0o644))
	return s.start(t, n.name, collectorBin, []string{"DATAFOCAL_CONFIG_FILE=" + path})
}

// startQuerier runs the querier binary and waits for its health check.
func (s *stack) startQuerier(t *testing.T) (*process, string) {
	t.Helper()
	addr := freeAddr(t)
	p := s.start(t, "querier", querierBin, nil, "--addr", addr, "--root", s.scratch)
	base := "http://" + addr
	require.Eventually(t, func() bool {
		resp, err := http.Get(base + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 30*time.Second, 50*time.Millisecond, "querier never became healthy")
	return p, base
}

func (s *stack) start(t *testing.T, name, bin string, env []string, args ...string) *process {
	t.Helper()
	log, err := os.Create(filepath.Join(s.scratch, fmt.Sprintf("%s-%d.log", name, len(s.children))))
	require.NoError(t, err)
	cmd := exec.Command(bin, args...)
	cmd.Env = append(s.env(), env...)
	cmd.Stdout, cmd.Stderr = log, log
	require.NoError(t, cmd.Start())
	p := &process{name: name, cmd: cmd, log: log.Name(), done: make(chan struct{})}
	go func() {
		_ = cmd.Wait() // the exit status is read from ProcessState
		log.Close()
		close(p.done)
	}()
	s.children = append(s.children, p)
	return p
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// process is a running child binary.
type process struct {
	name string
	cmd  *exec.Cmd
	log  string
	done chan struct{}
}

// stop sends SIGTERM and waits for a clean exit, which is when the collector flushes.
func (p *process) stop(t *testing.T) *os.ProcessState {
	t.Helper()
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	select {
	case <-p.done:
	case <-time.After(time.Minute):
		t.Fatalf("%s did not exit within a minute of SIGTERM", p.name)
	}
	require.Truef(t, p.cmd.ProcessState.Success(), "%s exit: %s\n%s", p.name, p.cmd.ProcessState, p.tail(40))
	return p.cmd.ProcessState
}

// kill is SIGKILL: no flush, no saved offsets, the crash a restart must recover from.
func (p *process) kill() {
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Kill() // it may have exited since the check
	<-p.done
}

func (p *process) running() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

func (p *process) tail(n int) string {
	data, err := os.ReadFile(p.log)
	if err != nil {
		return err.Error()
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	return strings.Join(lines[max(0, len(lines)-n):], "\n")
}

// catalogRows is the rows every descriptor in the bucket's catalog holds, read back
// through the catalog the querier reads.
func (s *stack) catalogRows(ctx context.Context) (rows, segments int, err error) {
	cat := catalog.New(s.store, tenant)
	v, err := cat.Version(ctx)
	if err != nil {
		return 0, 0, err
	}
	descs, err := cat.Prune(ctx, v, selector.Resolved{})
	if err != nil {
		return 0, 0, err
	}
	for _, d := range descs {
		rows += d.Rows
	}
	return rows, len(descs), nil
}

// waitRows waits until the catalog holds want rows, failing at once on more, since
// rows past want are lines collected twice. A captured corpus's want is its parser's count
// over whole files, which a collector closing a held record when it goes idle can pass by a
// few, so its rows need only come within a thousandth of want and stop growing.
func (s *stack) waitRows(t *testing.T, want int, timeout time.Duration, collectors ...*process) {
	t.Helper()
	slack := 0
	if s.sources != nil {
		slack = want / 1000
	}
	deadline, last, still := time.Now().Add(timeout), -1, time.Now()
	for {
		rows, _, err := s.catalogRows(context.Background())
		require.NoError(t, err)
		require.LessOrEqualf(t, rows, want+slack, "catalog holds more rows than lines were written: lines collected twice")
		if rows != last {
			last, still = rows, time.Now()
		}
		if rows == want || slack > 0 && rows >= want-slack && time.Since(still) > 3*s.flush {
			return
		}
		for _, c := range collectors {
			require.Truef(t, c.running(), "%s exited at %d of %d rows:\n%s", c.name, rows, want, c.tail(40))
		}
		require.Truef(t, time.Now().Before(deadline), "catalog reached %d of %d rows in %s", rows, want, timeout)
		<-time.After(100 * time.Millisecond)
	}
}

// writeLogs writes the seeded scenario into the stack's log directory, shift later.
func (s *stack) writeLogs(t *testing.T, scenario seedcorpus.Scenario, copies int, shift time.Duration) seedcorpus.LogsResult {
	t.Helper()
	return writeNodeLogs(t, []node{s.defaultNode()}, scenario, copies, shift)
}

// writeNodeLogs writes the scenario into every node's log directory, over the same time
// window, and totals what it wrote.
func writeNodeLogs(t *testing.T, nodes []node, scenario seedcorpus.Scenario, copies int, shift time.Duration) seedcorpus.LogsResult {
	t.Helper()
	total := seedcorpus.LogsResult{Lines: map[string]int{}}
	for _, n := range nodes {
		result, err := seedcorpus.WriteLogs(n.logs, scenario, copies, shift)
		require.NoError(t, err)
		for svc, lines := range result.Lines {
			total.Lines[svc] += lines
		}
		total.Bytes += result.Bytes
	}
	return total
}

func totalLines(r seedcorpus.LogsResult) int {
	n := 0
	for _, lines := range r.Lines {
		n += lines
	}
	return n
}

// call POSTs req to the querier's /v1/<tool> and decodes the envelope, failing on a refusal.
func call[Resp any](t *testing.T, base, tool string, req any) (query.Envelope[Resp], time.Duration) {
	t.Helper()
	env, elapsed, raw := tryCall[Resp](t, base, tool, req)
	require.NotEmptyf(t, env.Meta.Snapshot, "/v1/%s answered without a snapshot, so it refused: %s", tool, raw)
	return env, elapsed
}

// tryCall is call returning a refusal's body rather than failing: a refusal comes back 200
// with an error body, which the decode then reports as a zero Meta.
func tryCall[Resp any](t *testing.T, base, tool string, req any) (query.Envelope[Resp], time.Duration, []byte) {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoError(t, err)
	start := time.Now()
	resp, err := http.Post(base+"/v1/"+tool, "application/json", bytes.NewReader(body))
	require.NoErrorf(t, err, "POST /v1/%s", tool)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "POST /v1/%s: %s", tool, raw)
	var env query.Envelope[Resp]
	require.NoErrorf(t, json.Unmarshal(raw, &env), "decode /v1/%s: %s", tool, raw)
	return env, elapsed, raw
}

// callMCP makes the same call over the querier's /mcp endpoint.
func callMCP[Resp any](t *testing.T, base, tool string, req any) query.Envelope[Resp] {
	t.Helper()
	ctx := context.Background()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "stack"}, nil).Connect(ctx,
		&mcp.StreamableClientTransport{Endpoint: base + "/mcp", DisableStandaloneSSE: true}, nil)
	require.NoError(t, err, "connect MCP")
	defer func() { _ = session.Close() }()
	body, err := json.Marshal(req)
	require.NoError(t, err)
	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: json.RawMessage(body)})
	require.NoErrorf(t, err, "MCP %s", tool)
	var env query.Envelope[Resp]
	require.NoErrorf(t, json.Unmarshal([]byte(res.Content[0].(*mcp.TextContent).Text), &env), "decode MCP %s", tool)
	return env
}
