package seedcorpus

import (
	"cmp"
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/storage"
)

func buildFactoryCorpus(t *testing.T) (string, BuildResult) {
	t.Helper()
	dir := t.TempDir()
	result, err := Build(context.Background(), dir, ScenarioFactory)
	require.NoError(t, err)
	return dir, result
}

func readTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	files := map[string]string{}
	require.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		files[strings.TrimPrefix(path, dir)] = string(data)
		return err
	}))
	return files
}

func TestFactoryBuildIsDeterministic(t *testing.T) {
	dir, first := buildFactoryCorpus(t)
	other, second := buildFactoryCorpus(t)
	assert.Equal(t, first.Truth, second.Truth)
	assert.Len(t, second.Descriptors, len(first.Descriptors))
	assert.Equal(t, readTree(t, dir), readTree(t, other))
}

// factoryLines is every generated file's lines, by its path under the node's root.
func factoryLines(t *testing.T, dir string) map[string][]string {
	lines := map[string][]string{}
	for path, body := range readTree(t, filepath.Join(dir, "node")) {
		lines[strings.TrimPrefix(path, "/")] = strings.Split(strings.TrimSuffix(body, "\n"), "\n")
	}
	return lines
}

// transcriptLine is the part of a Claude Code transcript line the tests read.
type transcriptLine struct {
	Session, Request string
	Type             string `json:"type"`
	UUID             string `json:"uuid"`
	Version          string `json:"version"`
	Timestamp        string `json:"timestamp"`
	Message          *struct {
		Role    string `json:"role"`
		Model   string `json:"model"`
		ID      string `json:"id"`
		Content []struct {
			Type      string `json:"type"`
			ID        string `json:"id"`
			Name      string `json:"name"`
			ToolUseID string `json:"tool_use_id"`
			IsError   *bool  `json:"is_error"`
		} `json:"content"`
		Usage *struct {
			Input      int64 `json:"input_tokens"`
			CacheRead  int64 `json:"cache_read_input_tokens"`
			CacheWrite int64 `json:"cache_creation_input_tokens"`
			Output     int64 `json:"output_tokens"`
		} `json:"usage"`
	} `json:"message"`
}

func readTranscript(t *testing.T, lines []string) []transcriptLine {
	t.Helper()
	out := make([]transcriptLine, len(lines))
	for i, l := range lines {
		require.NoError(t, json.Unmarshal([]byte(l), &out[i]))
		var ids map[string]any
		require.NoError(t, json.Unmarshal([]byte(l), &ids))
		out[i].Session, _ = ids[transcriptKeys.Session].(string)
		out[i].Request, _ = ids[transcriptKeys.Request].(string)
		require.Contains(t, l, `"parentUuid":`)
	}
	return out
}

var (
	criLine     = regexp.MustCompile(`^\S+ stdout F (.*)$`)
	transcripts = regexp.MustCompile(`^run/containerd/io\.containerd\.runtime\.v2\.task/k8s\.io/[0-9a-f]{64}/rootfs/home/agent/\.claude/projects/-workspace/[0-9a-f-]{36}\.jsonl$`)
	containers  = regexp.MustCompile(`^var/log/containers/([^_]+)_([^_]+)_(.+)-[0-9a-f]{64}\.log$`)
	slogLine    = regexp.MustCompile(`^time=\S+ level=(INFO|WARN) msg=".*" task=t-[0-9a-f]+ session=([0-9a-f-]{36})`)
	sandboxPod  = regexp.MustCompile(`^sandbox-s-[0-9a-f]{32}$`)
	klogLine    = regexp.MustCompile(`^W\d{4} `)
)

// TestFactoryFilesHaveEachShape reads the generated files back and finds each shape the
// factory's real node writes.
func TestFactoryFilesHaveEachShape(t *testing.T) {
	found := map[string]bool{}
	saw := func(shape string, ok bool) { found[shape] = found[shape] || ok }
	sessions, orchestrated := map[string]bool{}, map[string]bool{}
	total, undated := 0, 0
	dir, _ := buildFactoryCorpus(t)
	for path, lines := range factoryLines(t, dir) {
		if transcripts.MatchString(path) {
			for _, l := range readTranscript(t, lines) {
				total++
				require.True(t, l.UUID != "" && l.Session != "" && l.Version != "" && (l.Request != "") == (l.Type == "assistant"),
					"every line carries its ids, and only a model call's its request")
				sessions[l.Session] = true
				if l.Timestamp == "" {
					undated++
					assert.Contains(t, []string{"ai-title", "last-prompt"}, l.Type)
				}
				if l.Message == nil {
					continue
				}
				require.Len(t, l.Message.Content, 1, "a message is one line per block")
				block := l.Message.Content[0]
				if l.Message.Role == "assistant" {
					require.NotNil(t, l.Message.Usage, "each of a message's lines repeats its usage")
					saw("assistant message", l.Message.Model != "" && l.Message.ID != "")
				}
				saw("tool_use", block.Type == "tool_use" && block.Name != "")
				saw("tool_result", block.Type == "tool_result" && block.ToolUseID != "" && block.IsError != nil && block.Name == "")
			}
			continue
		}
		m := containers.FindStringSubmatch(path)
		require.NotNil(t, m, path)
		for _, l := range lines {
			cri := criLine.FindStringSubmatch(l)
			require.NotNil(t, cri, "a container line is CRI-wrapped: %s", l)
			body := cri[1]
			if s := slogLine.FindStringSubmatch(body); m[3] == "orchestrator" && s != nil {
				orchestrated[s[2]] = true
			}
			saw("egress proxy", sandboxPod.MatchString(m[1]) && m[2] == "factory-sandboxes" &&
				strings.Contains(body, "\tinfo\trequest ") && strings.HasSuffix(body, "http.status=200 endpoint=api.anthropic.com:443"))
			saw("sandbox logfmt", sandboxPod.MatchString(m[1]) && strings.HasPrefix(body, "time="))
			saw("postgres", strings.Contains(body, " LOG:  ") || strings.Contains(body, " ERROR:  "))
			saw("etcd", m[1] == "etcd-"+factoryHost && strings.Contains(body, `"level":"warn"`))
			saw("klog", m[1] == "kube-scheduler-"+factoryHost && klogLine.MatchString(body))
		}
	}
	for _, shape := range []string{"assistant message", "tool_use", "tool_result", "egress proxy", "sandbox logfmt", "postgres", "etcd", "klog"} {
		assert.True(t, found[shape], shape)
	}
	assert.InDelta(t, 1.0/8, float64(undated)/float64(total), 0.04, "about one line in eight is undated")
	assert.NotEmpty(t, orchestrated, "the orchestrator logs its tasks' sessions")
	for session := range orchestrated {
		assert.Contains(t, sessions, session, "an orchestrator line's session is a transcript's")
	}
}

// A session's tokens summed by hand from its file, once per message id, its failed tool
// results counted by the name of the call each answers, its turn's time from its first
// dated line to its last and each call's to its result, are its truth.
func TestFactoryTruthIsTheTranscriptsTokens(t *testing.T) {
	dir, result := buildFactoryCorpus(t)
	var lines []string
	for path, l := range factoryLines(t, dir) {
		if transcripts.MatchString(path) {
			lines = l
			break
		}
	}
	require.NotEmpty(t, lines, "no transcript was written")
	got := SessionTruth{ToolErrors: map[string]int64{}, ToolMs: map[string][]int64{}}
	seen, tools, called := map[string]bool{}, map[string]string{}, map[string]time.Time{}
	var first, last time.Time
	transcript := readTranscript(t, lines)
	for _, l := range transcript {
		at, err := time.Parse(time.RFC3339Nano, l.Timestamp)
		if err == nil {
			first, last = cmp.Or(first, at), at
		}
		if l.Message != nil && l.Message.Content[0].Type == "tool_use" {
			tools[l.Message.Content[0].ID], called[l.Message.Content[0].ID] = l.Message.Content[0].Name, at
		}
		if l.Message != nil && l.Message.Content[0].ToolUseID != "" {
			id := l.Message.Content[0].ToolUseID
			got.ToolMs[tools[id]] = append(got.ToolMs[tools[id]], at.Sub(called[id]).Milliseconds())
		}
		if l.Message != nil && l.Message.Content[0].IsError != nil && *l.Message.Content[0].IsError {
			got.ToolErrors[tools[l.Message.Content[0].ToolUseID]]++
		}
		if l.Message == nil || l.Message.Usage == nil || seen[l.Message.ID] {
			continue
		}
		seen[l.Message.ID] = true
		u := l.Message.Usage
		got = SessionTruth{Model: l.Message.Model, Input: got.Input + u.Input, CacheRead: got.CacheRead + u.CacheRead,
			CacheWrite: got.CacheWrite + u.CacheWrite, Output: got.Output + u.Output, ToolErrors: got.ToolErrors, ToolMs: got.ToolMs}
	}
	got.TurnMs = []int64{last.Sub(first).Milliseconds()}
	// Spend, peak context and characters are checked against the engine's answers (internal/e2e).
	want := result.Truth.Sessions[transcript[0].Session]
	want.CostUSD, want.PeakContext, want.BlockChars, want.ResultChars = 0, 0, nil, nil
	assert.Equal(t, want, got)
}

// Every stored record names its workload and node, and every tool result its call's tool.
func TestFactoryRecordsNameWorkloadsAndNode(t *testing.T) {
	dir, result := buildFactoryCorpus(t)
	rd := segment.NewReader(storage.NewFS(dir))
	seen := map[string]bool{}
	for _, d := range result.Descriptors {
		records, _, _, err := rd.Records(context.Background(), d.Object)
		require.NoError(t, err)
		for _, r := range records {
			cols := map[string]string{}
			for _, c := range r.Columns {
				cols[c.Name] = c.Value.String
			}
			want := "workspace"
			if pod := cols[segment.ColPod]; pod != "" {
				switch {
				case sandboxPod.MatchString(pod):
					want = "sandbox-s"
				case strings.HasSuffix(pod, "-"+factoryHost):
					want = strings.TrimSuffix(pod, "-"+factoryHost)
				case pod == "postgres-0" || pod == "querier-0":
					want = strings.TrimSuffix(pod, "-0")
				case pod == "kindnet-x7k2p":
					want = "kindnet"
				default:
					want = "orchestrator"
				}
			}
			assert.Equal(t, map[string]string{"service": want, "host": factoryHost},
				map[string]string{"service": cols[segment.ColService], "host": cols[segment.ColHost]})
			seen[want] = true
			if r.Text("message.content.type") == `["tool_result"]` {
				assert.NotEmpty(t, r.Text("message.content.name"), "a tool result carries its call's name")
			}
		}
	}
	assert.Equal(t, map[string]bool{"workspace": true, "sandbox-s": true, "etcd": true, "kube-scheduler": true, "postgres": true, "orchestrator": true,
		"kindnet": true, "querier": true}, seen)
}
