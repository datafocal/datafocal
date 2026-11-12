package seedcorpus

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/ingest"
	"github.com/datafocal/datafocal/internal/llm"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/storage"
)

// ScenarioFactory is one agent-factory node's files, read through ingest as a collector on
// the node reads them.
const ScenarioFactory Scenario = "factory"

// FactoryTruth is what the factory generator wrote, counted from its own lines.
type FactoryTruth struct {
	Sessions map[string]SessionTruth // by the transcript's session
	Tasks    map[string]TaskTruth
}

// TaskTruth lists the services that logged a task.
type TaskTruth struct {
	Services []string
}

// SessionTruth is one transcript's model, tokens, spend and peak context, each message's
// usage counted once however many lines repeat it; the characters of its blocks' own text
// by block type and of its tool results by tool name, counted as the engine's len counts;
// its failed tool results by tool name; each turn's ms from its first line to its last; and
// each tool call's ms from the call to its result.
type SessionTruth struct {
	Model                                string
	Input, CacheRead, CacheWrite, Output int64
	CostUSD                              float64 // at the querier's default prices
	PeakContext                          int64   // the most input, cache read and cache write of a message
	BlockChars                           map[string]int64
	ResultChars                          map[string]int64
	ToolErrors                           map[string]int64
	TurnMs                               []int64            // per prompt id
	ToolMs                               map[string][]int64 // by tool name, per call
}

const (
	factoryHost = "factory-node-1"
	factoryDays = 27
	claudeRoot  = "/run/containerd/io.containerd.runtime.v2.task/k8s.io/"
)

//go:embed transcript_keys.json
var transcriptKeysJSON []byte

// transcriptKeys are Claude Code's names for a line's session, request and prompt ids.
var transcriptKeys = func() (k struct{ Session, Request, Prompt string }) {
	_ = json.Unmarshal(transcriptKeysJSON, &k)
	return k
}()

// SessionColumn is the column a transcript line names its session in.
var SessionColumn = transcriptKeys.Session

// PromptColumn is the column a transcript line names the prompt that started its turn in.
var PromptColumn = transcriptKeys.Prompt

var factoryEnd = time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC)

// FactoryWindow is the time range ScenarioFactory's lines span: quiet containers the whole
// of it, the agents' sessions its last day.
func FactoryWindow() (start, end time.Time) {
	return factoryEnd.AddDate(0, 0, -factoryDays), factoryEnd
}

// IncidentHour is the hour of ScenarioFactory's heartbeats and health probes, an error burst at its middle.
func IncidentHour() (start, end time.Time) {
	start = factoryEnd.Add(-10*time.Hour - 30*time.Minute)
	return start, start.Add(time.Hour)
}

// factorySources are a Kubernetes node's container logs and the agents' transcripts inside
// their containers, under root rather than /.
func factorySources(root string) []ingest.Source {
	cri := logformat.Config{Format: "auto", Envelope: "cri"}
	jsonl := logformat.Config{Format: "json"}
	sources := []ingest.Source{
		{Path: "/var/log/containers/*.log", Parse: cri},
		{Path: claudeRoot + "*/rootfs/home/agent/.claude/projects/*/*.jsonl", Parse: jsonl},
		{Path: claudeRoot + "*/rootfs/home/agent/.claude/projects/*/*/subagents/*.jsonl", Parse: jsonl},
		{Path: claudeRoot + "*/rootfs/home/agent/.codex/sessions/*/*/*/*.jsonl", Parse: jsonl},
	}
	for i := range sources {
		sources[i].Path = filepath.Join(root, strings.TrimPrefix(sources[i].Path, "/"))
	}
	return sources
}

// buildFactory writes a node's files under outDir/node and drains them through ingest into
// the store at outDir, with the collector the factory's deployment runs.
func buildFactory(ctx context.Context, outDir string) (BuildResult, error) {
	start := time.Now()
	root := filepath.Join(outDir, "node")
	truth, err := writeFactory(root)
	if err != nil {
		return BuildResult{}, err
	}
	store := storage.NewFS(outDir)
	cfg := collector.DefaultConfig()
	cfg.Tenant = Tenant
	cfg.FlushInterval = 10 * time.Second
	cfg.FlushAlignment = time.Hour
	from, _ := FactoryWindow()
	c := &recordTime{clock: &seedClock{now: from}}
	if c.Collector, err = collector.New(cfg, collector.Dependencies{Store: store, CatalogWriter: catalog.New(store, Tenant), Clock: c.clock}); err != nil {
		return BuildResult{}, err
	}
	in, err := ingest.New(ingest.Config{Sources: factorySources(root), StateDir: filepath.Join(outDir, "state"), Host: factoryHost, FlushInterval: 10 * time.Second}, c)
	if err != nil {
		return BuildResult{}, err
	}
	if err := in.Drain(ctx); err != nil {
		return BuildResult{}, err
	}
	if err := verifyCatalog(ctx, store, len(c.descriptors)); err != nil {
		return BuildResult{}, err
	}
	return BuildResult{Descriptors: c.descriptors, Patterns: map[string]string{}, Elapsed: time.Since(start), Truth: truth}, nil
}

// recordTime is a collector whose clock reads the time of the record it collects, so seals
// and segment ids follow log time, and which keeps every descriptor it made durable.
type recordTime struct {
	collector.Collector
	clock       *seedClock
	descriptors []segment.Descriptor
}

func (c *recordTime) Collect(ctx context.Context, r segment.Record) (collector.CollectResult, error) {
	c.clock.set(r.Timestamp)
	result, err := c.Collector.Collect(ctx, r)
	c.descriptors = append(c.descriptors, result.Descriptors...)
	return result, err
}

func (c *recordTime) Flush(ctx context.Context) ([]segment.Descriptor, error) {
	descs, err := c.Collector.Flush(ctx)
	c.descriptors = append(c.descriptors, descs...)
	return descs, err
}

// factoryNode is the files a node's generator writes, each a path under the node's root and
// its lines in time order.
type factoryNode struct {
	r *rand.Rand
	// prose draws the text of blocks and tool results apart from r, so the ids r draws do
	// not move with it.
	prose *rand.Rand
	files map[string][]string
	truth *FactoryTruth
}

func writeFactory(root string) (*FactoryTruth, error) {
	n := &factoryNode{r: rand.New(rand.NewSource(20260728)), prose: rand.New(rand.NewSource(1)), files: map[string][]string{}, truth: &FactoryTruth{Sessions: map[string]SessionTruth{}, Tasks: map[string]TaskTruth{}}}
	n.quiet()
	n.sessions()
	n.incident()
	for _, path := range slices.Sorted(maps.Keys(n.files)) {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(full, []byte(strings.Join(n.files[path], "")), 0o644); err != nil {
			return nil, err
		}
	}
	return n.truth, nil
}

func (n *factoryNode) hex(chars int) string {
	return hex(n.r, chars)
}

func hex(r *rand.Rand, chars int) string {
	b := make([]byte, chars)
	for i := range b {
		b[i] = "0123456789abcdef"[r.Intn(16)]
	}
	return string(b)
}

func (n *factoryNode) uuid() string {
	h := n.hex(32)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// containerLog is the kubelet's file for one container, named as identity reads it.
func (n *factoryNode) containerLog(pod, ns, container string) string {
	return fmt.Sprintf("var/log/containers/%s_%s_%s-%s.log", pod, ns, container, n.hex(64))
}

// log appends body to a container log, CRI-wrapped as the runtime writes it.
func (n *factoryNode) log(path string, at time.Time, body string) {
	n.files[path] = append(n.files[path], at.Format(time.RFC3339Nano)+" stdout F "+body+"\n")
}

// quiet writes the containers that log every few hours across the window, all at the top
// of the hour, so a collector cutting every 10s of log time cuts once per such hour.
func (n *factoryNode) quiet() {
	postgres := n.containerLog("postgres-0", "factory", "postgres")
	etcd := n.containerLog("etcd-"+factoryHost, "kube-system", "etcd")
	scheduler := n.containerLog("kube-scheduler-"+factoryHost, "kube-system", "kube-scheduler")
	from, to := FactoryWindow()
	for at := from; at.Before(to); at = at.Add(3 * time.Hour) {
		pg := at.Add(time.Duration(n.r.Intn(1000)) * time.Millisecond)
		n.log(postgres, pg, fmt.Sprintf("%s [%d] LOG:  checkpoint starting: time", pg.Format("2006-01-02 15:04:05.000 MST"), 60+n.r.Intn(40)))
		if n.r.Intn(4) == 0 {
			n.log(postgres, pg.Add(time.Second), fmt.Sprintf(`%s [%d] ERROR:  duplicate key value violates unique constraint "tasks_pkey"`,
				pg.Add(time.Second).Format("2006-01-02 15:04:05.000 MST"), 60+n.r.Intn(40)))
		}
		if at.Hour()%6 == 0 {
			ts := at.Add(2 * time.Second)
			n.log(etcd, ts, fmt.Sprintf(`{"level":"warn","ts":"%s","caller":"etcdserver/util.go:170","msg":"apply request took too long","took":"%dms","expected-duration":"100ms"}`,
				ts.Format(time.RFC3339Nano), 100+n.r.Intn(300)))
		}
		if at.Hour()%12 == 0 {
			ts := at.Add(3 * time.Second)
			n.log(scheduler, ts, ts.Format("W0102 15:04:05.000000")+"       1 reflector.go:561] failed to list *v1.Pod: Unauthorized")
		}
	}
}

// incident writes IncidentHour: kindnet's heartbeat and the querier answering its health
// probe every 10s, and two minutes of the orchestrator's failed dispatches at its middle.
func (n *factoryNode) incident() {
	kindnet := n.containerLog("kindnet-x7k2p", "kube-system", "kindnet-cni")
	querier := n.containerLog("querier-0", "datafocal", "querier")
	orchestrator := n.containerLog("orchestrator-6b7c9d8f5-k2x7p", "factory", "orchestrator")
	from, to := IncidentHour()
	for at := from; at.Before(to); at = at.Add(10 * time.Second) {
		n.log(kindnet, at, at.Format("I0102 15:04:05.000000")+"       1 main.go:297] Handling node with IPs: map[172.18.0.2:{}]")
		probe := at.Add(time.Duration(n.r.Intn(1000)) * time.Millisecond)
		n.log(querier, probe, fmt.Sprintf("time=%s level=INFO msg=request method=GET path=/healthz status=200 duration_ms=%d request_id=%s",
			probe.Format(time.RFC3339Nano), n.r.Intn(3), n.hex(16)))
	}
	burst := from.Add(30 * time.Minute)
	for at := burst; at.Before(burst.Add(2 * time.Minute)); at = at.Add(time.Second / 3) {
		n.log(orchestrator, at, fmt.Sprintf(`time=%s level=ERROR msg="dispatch failed" err="dial tcp 10.96.0.1:443: connect: connection refused" attempt=%d`,
			at.Format(time.RFC3339Nano), 1+n.r.Intn(5)))
	}
}

// The transcript's keys for a message's content, a text block, a thinking block, a line's or
// block's type and a tool input's file, and the type and role of a model call's lines.
const (
	keyContent  = "content"
	keyText     = "text"
	keyThinking = "thinking"
	keyType     = "type"
	keyFilePath = "file_path"
	typeUser    = "user"
)

var (
	factoryModels = []string{"claude-opus-4-5", "claude-sonnet-4-5"}
	factoryTools  = []string{"Bash", "Edit", "Grep", "Read", "Write"}
	// factoryInputs is the key each tool's input names its argument by.
	factoryInputs = map[string]string{"Bash": "command", "Edit": keyFilePath, "Grep": "pattern", "Read": keyFilePath, "Write": keyFilePath}
)

// sessions writes one agent session an hour across the window's last day: the
// orchestrator's lines for its task, its sandbox's, and its Claude Code transcript.
func (n *factoryNode) sessions() {
	orchestrator := n.containerLog("orchestrator-6b7c9d8f5-k2x7p", "factory", "orchestrator")
	_, end := FactoryWindow()
	for h := range 24 {
		at := end.Add(time.Duration(h-24)*time.Hour + time.Duration(n.r.Intn(1200))*time.Second)
		task, session, pod := "t-"+n.hex(12), n.uuid(), "sandbox-s-"+n.hex(32)
		n.truth.Tasks[task] = TaskTruth{Services: []string{"orchestrator", "sandbox-s"}}
		slog := func(at time.Time, level, msg, attrs string) {
			n.log(orchestrator, at, fmt.Sprintf("time=%s level=%s msg=%q task=%s session=%s%s", at.Format(time.RFC3339Nano), level, msg, task, session, attrs))
		}
		slog(at, "INFO", "task claimed", " pod="+pod)
		sandbox := n.containerLog(pod, "factory-sandboxes", "sandbox")
		n.log(sandbox, at.Add(time.Second), fmt.Sprintf("time=%s level=info msg=\"workspace ready\" repo=acme/billing-api task=%s", at.Add(time.Second).Format(time.RFC3339Nano), task))
		n.log(sandbox, at.Add(2*time.Second), fmt.Sprintf("time=%s level=info msg=\"agent starting\" harness=claude-code task=%s", at.Add(2*time.Second).Format(time.RFC3339Nano), task))
		done, failed := n.transcript(at.Add(3*time.Second), session, func(at time.Time) {
			n.log(sandbox, at, at.Format(time.RFC3339Nano)+"\tinfo\trequest http.method=POST http.path=/v1/messages http.status=200 endpoint=api.anthropic.com:443")
		})
		if failed {
			slog(done, "WARN", "task failed", " reason=tests")
		} else {
			slog(done, "INFO", "task finished", "")
		}
	}
}

// transcript writes one session's Claude Code transcript from at, calling request at each
// model call, and returns when it ended and whether its last tool call failed.
func (n *factoryNode) transcript(at time.Time, session string, request func(time.Time)) (time.Time, bool) {
	path := claudeRoot[1:] + n.hex(64) + "/rootfs/home/agent/.claude/projects/-workspace/" + session + ".jsonl"
	model, version := factoryModels[n.r.Intn(len(factoryModels))], []string{"2.1.3", "2.1.4"}[n.r.Intn(2)]
	parent, req, started := any(nil), "req_"+n.hex(24), at
	// The prompt id is drawn from a seed of its own, so the seed's other draws stay as they were.
	prompt := (&factoryNode{r: rand.New(rand.NewSource(at.UnixNano()))}).uuid()
	line := func(line map[string]any, dated bool) {
		uuid := n.uuid()
		line["uuid"], line["parentUuid"], line["version"] = uuid, parent, version
		if line[keyType] == typeUser { // Claude Code writes it on the prompt and its tool results
			line[transcriptKeys.Prompt] = prompt
		}
		line[transcriptKeys.Session] = session
		if line[keyType] == llm.RoleAssistant { // only a model call's lines name its request
			line[transcriptKeys.Request] = req
		}
		if dated {
			line["timestamp"] = at.Format("2006-01-02T15:04:05.000Z")
		}
		b, _ := json.Marshal(line) // maps of strings, numbers and bools always marshal
		n.files[path] = append(n.files[path], string(b)+"\n")
		parent = uuid
	}
	truth := SessionTruth{Model: model, BlockChars: map[string]int64{}, ResultChars: map[string]int64{}, ToolErrors: map[string]int64{}, ToolMs: map[string][]int64{}}
	user := func(content map[string]any) map[string]any {
		truth.BlockChars[content[keyType].(string)] += blockChars(content)
		return map[string]any{keyType: typeUser, segment.ColMessage: map[string]any{"role": typeUser, keyContent: []any{content}}}
	}
	line(user(map[string]any{keyType: keyText, keyText: "Implement the task in TASK.md."}), true)
	line(map[string]any{keyType: "ai-title", "aiTitle": "Implement the task"}, false)
	cached, failed := int64(8000+n.r.Intn(4000)), false
	for turn, turns := 0, 6+n.r.Intn(8); turn < turns; turn++ {
		at = n.after(at, time.Millisecond, 2000, 8000)
		request(at)
		input, write, output := int64(1+n.r.Intn(40)), int64(n.r.Intn(3000)), int64(50+n.r.Intn(1500))
		truth.Input, truth.CacheRead, truth.CacheWrite, truth.Output = truth.Input+input, truth.CacheRead+cached, truth.CacheWrite+write, truth.Output+output
		truth.CostUSD += (float64(input)*price(model, "price_in") + float64(cached)*price(model, "price_cache_read") +
			float64(write)*price(model, "price_cache_write") + float64(output)*price(model, "price_out")) / 1e6
		truth.PeakContext = max(truth.PeakContext, input+cached+write)
		usage := map[string]any{"input_tokens": input, "cache_read_input_tokens": cached, "cache_creation_input_tokens": write, "output_tokens": output}
		cached += write + output
		last, toolUse := turn == turns-1, "toolu_"+n.hex(24)
		blocks, msg := n.blocks(last, toolUse), "msg_"+n.hex(24)
		var called time.Time
		for _, block := range blocks {
			called = at
			truth.BlockChars[block[keyType].(string)] += blockChars(block)
			line(map[string]any{keyType: llm.RoleAssistant, segment.ColMessage: map[string]any{"role": llm.RoleAssistant, keyType: segment.ColMessage, "model": model, "id": msg, keyContent: []any{block}, "usage": usage}}, true)
			at = at.Add(300 * time.Millisecond)
		}
		if last {
			truth.TurnMs = append(truth.TurnMs, called.Sub(started).Milliseconds())
			break
		}
		at = n.after(at, time.Second, 1, 20)
		failed = n.r.Intn(8) == 0
		tool, result := blocks[len(blocks)-1]["name"].(string), strings.Repeat(hex(n.prose, 16)+"\n", 1+n.prose.Intn(60))
		truth.ToolMs[tool] = append(truth.ToolMs[tool], at.Sub(called).Milliseconds())
		if failed {
			truth.ToolErrors[tool]++
		}
		truth.ResultChars[tool] += int64(utf8.RuneCountInString(result))
		line(user(map[string]any{keyType: "tool_result", "tool_use_id": toolUse, "is_error": failed, keyContent: result}), true)
		if turn%2 == 1 {
			line(map[string]any{keyType: "last-prompt", "lastPrompt": "Implement the task in TASK.md."}, false)
		}
		req = "req_" + n.hex(24)
	}
	n.truth.Sessions[session] = truth
	return at.Add(time.Second), failed
}

// after is at plus least to least+spread-1 units, drawn from the seed.
func (n *factoryNode) after(at time.Time, unit time.Duration, least, spread int) time.Time {
	return at.Add(time.Duration(least+n.r.Intn(spread)) * unit)
}

// blocks is one model call's content: perhaps thinking, a text block on the last call or
// most others, and a tool call on every call but the last.
func (n *factoryNode) blocks(last bool, toolUse string) []map[string]any {
	var blocks []map[string]any
	if n.r.Intn(2) == 0 {
		blocks = append(blocks, map[string]any{keyType: keyThinking, keyThinking: "Read " + hex(n.prose, 8) + " first."})
	}
	if last || n.r.Intn(3) > 0 {
		blocks = append(blocks, map[string]any{keyType: keyText, keyText: "Done with " + hex(n.prose, 8) + "."})
	}
	if !last {
		tool := factoryTools[n.r.Intn(len(factoryTools))]
		input := map[string]string{factoryInputs[tool]: hex(n.prose, 8+n.prose.Intn(40))}
		blocks = append(blocks, map[string]any{keyType: "tool_use", "id": toolUse, "name": tool, "input": input})
	}
	return blocks
}

// blockChars is the characters of a block's own text: its text, thinking or content, or
// each value of its input.
func blockChars(block map[string]any) int64 {
	n := 0
	for _, key := range []string{keyText, keyThinking, keyContent} {
		text, _ := block[key].(string)
		n += utf8.RuneCountInString(text)
	}
	input, _ := block["input"].(map[string]string)
	for _, v := range input {
		n += utf8.RuneCountInString(v)
	}
	return int64(n)
}

// price is model's price in USD per million tokens from the querier's default table.
func price(model, table string) float64 {
	_, tables := querier.DefaultDerived()
	p, _ := strconv.ParseFloat(tables[table][model], 64) // the factory's models are all priced
	return p
}
