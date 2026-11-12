package ingest

import (
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/identity"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/producers"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/tail"
	"github.com/datafocal/datafocal/internal/value"
)

var errDown = errors.New("down")

func init() {
	producers.Add(os.DirFS("testdata/producers"))
}

// savedOffsets returns a Store that records each offsets save into saved, failing with putErr.
func savedOffsets(t *testing.T, saved *[]map[string]tail.Position, putErr error) storage.Store {
	return storage.Fake{PutFn: func(key string, body []byte) error {
		var offsets map[string]tail.Position
		require.NoError(t, json.Unmarshal(body, &offsets))
		*saved = append(*saved, offsets)
		return putErr
	}}
}

// Each case runs one step of an ingest that has collected a.log up to offset 4, unsaved unless saved is set.
func TestOffsetsAreSavedOnlyAfterAFlush(t *testing.T) {
	sealed := collector.CollectResult{Descriptors: []segment.Descriptor{{Rows: 1}}, Cut: 1, Durable: 1}
	sealedWithLine := collector.CollectResult{Descriptors: []segment.Descriptor{{Rows: 1}}, Cut: 1, Durable: 1, RecordSealed: true}
	cutWithLine := collector.CollectResult{Cut: 1, RecordSealed: true}
	landed := collector.CollectResult{Descriptors: []segment.Descriptor{{Rows: 1}}, Durable: 1}
	collectB := func(in *ingest) error {
		return in.collect(context.Background(), sourceLine{path: "b.log", line: tail.Line{Text: "b1", Position: tail.Position{Offset: 9}}})
	}
	flush := func(in *ingest) error { return in.flush(context.Background()) }
	intervalCut := func(in *ingest) error { return in.cut(context.Background()) }
	before, after := map[string]tail.Position{"a.log": {Offset: 4}}, map[string]tail.Position{"a.log": {Offset: 4}, "b.log": {Offset: 9}}
	for _, tc := range []struct {
		name                         string
		step                         func(*ingest) error
		result, cutResult            collector.CollectResult
		collectErr, flushErr, putErr error
		saved, held                  bool
		// records is the records b.log's line holds, one when 0, as a batch of spans holds many.
		records int
		// cuts are batches cut earlier and not yet durable, as when sealing in the background.
		cuts        []cut
		wantSaved   []map[string]tail.Position
		wantOffsets map[string]tail.Position
	}{
		{name: "a buffered line saves nothing", step: collectB, wantOffsets: after},
		{name: "a line of several records moves past it with its last", step: collectB, records: 3, wantOffsets: after},
		{name: "seals of a line's records before its last save the offsets from before it", step: collectB, records: 3, result: sealedWithLine,
			wantSaved: []map[string]tail.Position{before, before, after}, wantOffsets: after},
		{name: "a seal saves the offsets from before its line", step: collectB, result: sealed, wantSaved: []map[string]tail.Position{before}, wantOffsets: after},
		{name: "a seal holding its line saves the offsets past it", step: collectB, result: sealedWithLine, wantSaved: []map[string]tail.Position{after}, wantOffsets: after},
		{name: "a batch cut to seal in the background saves nothing until it is durable", step: collectB, result: cutWithLine, wantOffsets: after},
		{name: "a batch found durable saves the offsets as of its cut", step: collectB, result: landed,
			cuts: []cut{{offsets: map[string]tail.Position{"a.log": {Offset: 2}}, unsaved: true}}, wantSaved: []map[string]tail.Position{{"a.log": {Offset: 2}}}, wantOffsets: after},
		{name: "an interval cut sealed inline saves the offsets", step: intervalCut, cutResult: sealed, wantSaved: []map[string]tail.Position{before}, wantOffsets: before},
		{name: "an interval cut sealing in the background saves nothing yet", step: intervalCut, cutResult: collector.CollectResult{Cut: 1}, wantOffsets: before},
		{name: "a flush saves past every cut, since it waits for them", step: flush,
			cuts: []cut{{offsets: map[string]tail.Position{"a.log": {Offset: 2}}, unsaved: true}}, wantSaved: []map[string]tail.Position{before}, wantOffsets: before},
		{name: "a failed collect records nothing", step: collectB, collectErr: errDown, wantOffsets: before},
		{name: "a held line records nothing", step: collectB, held: true, wantOffsets: before},
		{name: "a removed file's offset is forgotten", wantOffsets: map[string]tail.Position{}, step: func(in *ingest) error {
			return in.collect(context.Background(), sourceLine{path: "a.log", removed: true})
		}},
		{name: "a failed save records nothing", step: collectB, result: sealed, putErr: errDown, wantSaved: []map[string]tail.Position{before}, wantOffsets: before},
		{name: "a flush saves the offsets", step: flush, wantSaved: []map[string]tail.Position{before}, wantOffsets: before},
		{name: "a failed flush saves nothing", step: flush, flushErr: errDown, wantOffsets: before},
		{name: "a flush with nothing new saves nothing", step: flush, saved: true, wantOffsets: before},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var saved []map[string]tail.Position
			in := &ingest{
				collector: collector.Fake{
					CollectFn: func(segment.Record) (collector.CollectResult, error) { return tc.result, tc.collectErr },
					CutFn:     func() (collector.CollectResult, error) { return tc.cutResult, nil },
					FlushFn:   func() ([]segment.Descriptor, error) { return nil, tc.flushErr },
				},
				resolver: identity.New("node-1"),
				parsers: map[string]logformat.Parser{"a.log": logformat.Fake{},
					"b.log": logformat.Fake{ParseAllFn: func(string) []segment.Record {
						if tc.held {
							return nil
						}
						return make([]segment.Record, max(tc.records, 1))
					}}},
				state:   savedOffsets(t, &saved, tc.putErr),
				offsets: maps.Clone(before),
				pending: map[string][]tail.Position{},
				dated:   map[string]time.Time{},
				cuts:    tc.cuts,
				unsaved: !tc.saved,
			}
			assert.ErrorIs(t, tc.step(in), cmp.Or(tc.collectErr, tc.flushErr, tc.putErr))
			assert.Equal(t, tc.wantSaved, saved)
			assert.Equal(t, tc.wantOffsets, in.offsets)
		})
	}
}

// Each seal saves the offset past the last complete record, so a restart rereads the
// open record's lines, and a flush collects the open record.
func TestOffsetsStopBeforeAnOpenRecord(t *testing.T) {
	p, err := logformat.New(logformat.Config{Format: "plain", Envelope: "cri", RecordStart: "^start"})
	require.NoError(t, err)
	var saved []map[string]tail.Position
	var collected []string
	in := &ingest{
		collector: collector.Fake{CollectFn: func(r segment.Record) (collector.CollectResult, error) {
			collected = append(collected, r.Columns[0].Value.String)
			return collector.CollectResult{Descriptors: []segment.Descriptor{{Rows: 1}}, Cut: 1, Durable: 1}, nil
		}},
		resolver: identity.New("node-1"),
		parsers:  map[string]logformat.Parser{"a.log": p},
		state:    savedOffsets(t, &saved, nil),
		offsets:  map[string]tail.Position{},
		pending:  map[string][]tail.Position{},
		dated:    map[string]time.Time{},
	}
	for n, body := range []string{"F start 1", "F tail", "F start 2", "P start", "F  3", "F tail"} {
		line := tail.Line{Text: "2026-10-18T12:00:00Z stdout " + body, Position: tail.Position{Offset: int64(n + 1)}}
		require.NoError(t, in.collect(context.Background(), sourceLine{path: "a.log", line: line}))
	}
	require.NoError(t, in.flush(context.Background()))
	assert.Equal(t, []string{"start 1", "start 2", "start 3"}, collected)
	assert.Equal(t, []map[string]tail.Position{{"a.log": {Offset: 2}}, {"a.log": {Offset: 3}}, {"a.log": {Offset: 6}}}, saved)
}

// An undated record takes the time of the record before it in its own file, and one with
// none before it is left for the collector to stamp with the read time.
func TestUndatedRecordsTakeTheirFilesLastTime(t *testing.T) {
	t1, t2, t3 := time.Unix(100, 0), time.Unix(200, 0), time.Unix(300, 0)
	parser := logformat.Fake{ParseAllFn: func(text string) []segment.Record {
		at, _ := strconv.ParseInt(text, 10, 64)
		if at == 0 {
			return []segment.Record{{}}
		}
		return []segment.Record{{Timestamp: time.Unix(at, 0)}}
	}}
	var stamped []time.Time
	in := &ingest{
		collector: collector.Fake{CollectFn: func(r segment.Record) (collector.CollectResult, error) {
			stamped = append(stamped, r.Timestamp)
			return collector.CollectResult{}, nil
		}},
		resolver: identity.New("node-1"),
		parsers:  map[string]logformat.Parser{"a.log": parser, "b.log": parser},
		offsets:  map[string]tail.Position{},
		pending:  map[string][]tail.Position{},
		dated:    map[string]time.Time{},
	}
	for n, l := range []struct{ path, text string }{{"a.log", "100"}, {"b.log", "-"}, {"a.log", "-"}, {"b.log", "300"},
		{"a.log", "200"}, {"a.log", "-"}, {"b.log", "-"}} {
		line := tail.Line{Text: l.text, Position: tail.Position{Offset: int64(n + 1)}}
		require.NoError(t, in.collect(context.Background(), sourceLine{path: l.path, line: line}))
	}
	assert.Equal(t, []time.Time{t1, {}, t1, t3, t2, t2, t3}, stamped)
}

// On a Kubernetes node with no sources configured, a container started after Run is
// followed, each in the format its first line declares, and named by its workload.
func TestRunDiscoversContainerLogs(t *testing.T) {
	root := t.TempDir()
	t.Setenv("PATH", t.TempDir()) // no journal
	require.NoError(t, os.Mkdir(filepath.Join(root, "containers"), 0o755))
	collected := make(chan segment.Record, 2)
	stop := start(t, Config{Sources: discover(root), StateDir: root, Host: "node-1", FlushInterval: 10 * time.Millisecond},
		collector.Fake{CollectFn: func(r segment.Record) (collector.CollectResult, error) {
			collected <- r
			return collector.CollectResult{}, nil
		}})
	id := strings.Repeat("0a", 32)
	for name, body := range map[string]string{
		"checkout-7d9f8b6c4-x2x9z_shop_app-": `{"msg":"paid","amount":12}`,
		"ingress-x2x9z_kube-system_nginx-":   "E1018 12:00:00.123456   42 server.go:117] dial failed",
	} {
		line := "2026-10-18T12:00:00Z stdout F " + body + "\n"
		require.NoError(t, os.WriteFile(filepath.Join(root, "containers", name+id+".log"), []byte(line), 0o644))
	}
	got := map[string]map[string]any{}
	for range 2 {
		select {
		case r := <-collected:
			cols := map[string]any{}
			for _, c := range r.Columns {
				cols[c.Name] = c.Value.String
				if c.Type == value.TypeInt {
					cols[c.Name] = c.Value.Int
				}
			}
			got[cols["service"].(string)] = cols
		case <-time.After(5 * time.Second):
			require.Fail(t, "a new container's log was not followed")
		}
	}
	require.NoError(t, stop())
	assert.Equal(t, map[string]any{"message": "paid", "amount": int64(12), "service": "checkout", "pod": "checkout-7d9f8b6c4-x2x9z",
		"namespace": "shop", "container": "app", "host": "node-1"}, got["checkout"])
	assert.Equal(t, map[string]any{"message": "dial failed", "level": "error", "source": "server.go:117", "service": "ingress", "pod": "ingress-x2x9z",
		"namespace": "kube-system", "container": "nginx", "host": "node-1"}, got["ingress"])
}

func TestRunReadsTheJournalInsteadOfTheSyslogFiles(t *testing.T) {
	root, bin := t.TempDir(), t.TempDir()
	t.Setenv("PATH", bin)
	for _, name := range []string{"auth.log", "app.log", "syslog"} {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), nil, 0o644))
	}
	var withoutJournal []string
	for _, src := range discover(root) {
		paths, err := src.paths()
		require.NoError(t, err)
		withoutJournal = append(withoutJournal, paths...)
	}
	assert.Subset(t, withoutJournal, []string{filepath.Join(root, "auth.log"), filepath.Join(root, "app.log"), filepath.Join(root, "syslog")})
	fixture, err := filepath.Abs("../logformat/testdata/journal.jsonl")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(bin, "journalctl"), []byte("#!/bin/sh\n/usr/bin/head -1 "+fixture+"\nexec /bin/sleep 60\n"), 0o755))
	sources := discover(root)
	paths, err := sources[1].paths()
	require.NoError(t, err)
	assert.Equal(t, []string{"journalctl"}, sources[0].Command[:1])
	assert.Equal(t, []string{filepath.Join(root, "app.log")}, paths)
	collected := make(chan segment.Record, 1)
	stop := start(t, Config{Sources: sources[:1], StateDir: root, Host: "node-1", FlushInterval: time.Hour},
		collector.Fake{CollectFn: func(r segment.Record) (collector.CollectResult, error) {
			collected <- r
			return collector.CollectResult{}, nil
		}})
	var r segment.Record
	select {
	case r = <-collected:
	case <-time.After(5 * time.Second):
		require.Fail(t, "no journal entry was collected")
	}
	require.NoError(t, stop())
	cols := map[string]string{}
	for _, c := range r.Columns {
		cols[c.Name] = c.Value.String
	}
	assert.Equal(t, []string{"kernel", "host1", "info", "kernel"}, []string{cols["service"], cols["host"], cols["level"], cols["_TRANSPORT"]})
}

// A journal source resumes after the last entry collected, whether Run was stopped or its
// journalctl killed, so each run reads every entry new to it exactly once.
func TestJournalResumesAfterAStopAndAKill(t *testing.T) {
	root, bin := t.TempDir(), t.TempDir()
	t.Setenv("PATH", bin)
	fixture, err := filepath.Abs("../logformat/testdata/journal.jsonl")
	require.NoError(t, err)
	data, err := os.ReadFile(fixture)
	require.NoError(t, err)
	var messages []string
	for line := range strings.Lines(string(data)) {
		var entry struct{ MESSAGE string }
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		messages = append(messages, entry.MESSAGE)
	}
	// It prints the first $(cat available) entries after the cursor it is given, then waits.
	script := `#!/bin/sh
echo $$ > ROOT/pid
for arg; do case $arg in --after-cursor=*) after=${arg#--after-cursor=};; esac; done
/usr/bin/head -n "$(/bin/cat ROOT/available)" FIXTURE | /usr/bin/awk -v after="$after" 'after == "" || found { print } index($0, "\"" after "\"") { found = 1 }'
exec /bin/sleep 60
`
	require.NoError(t, os.WriteFile(filepath.Join(bin, "journalctl"), []byte(strings.NewReplacer("ROOT", root, "FIXTURE", fixture).Replace(script)), 0o755))
	// run makes the first available entries the journal's, and collects them until it has
	// want, then stops, cleanly or by killing journalctl.
	run := func(available int, kill bool, want []string) {
		require.NoError(t, os.WriteFile(filepath.Join(root, "available"), []byte(strconv.Itoa(available)), 0o644))
		collected := make(chan string, len(messages))
		in, err := New(Config{Sources: discover(root)[:1], StateDir: root, FlushInterval: time.Hour},
			collector.Fake{CollectFn: func(r segment.Record) (collector.CollectResult, error) {
				i := slices.IndexFunc(r.Columns, func(c segment.Column) bool { return c.Name == "message" })
				collected <- r.Columns[i].Value.String
				return collector.CollectResult{}, nil
			}})
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error)
		go func() { done <- in.Run(ctx) }()
		var got []string
		for len(got) < len(want) {
			select {
			case m := <-collected:
				got = append(got, m)
			case <-time.After(5 * time.Second):
				require.Fail(t, "a journal entry was not collected", "got %q", got)
			}
		}
		if kill {
			pid, err := os.ReadFile(filepath.Join(root, "pid"))
			require.NoError(t, err)
			n, err := strconv.Atoi(strings.TrimSpace(string(pid)))
			require.NoError(t, errors.Join(err, syscall.Kill(n, syscall.SIGKILL)))
			assert.ErrorContains(t, <-done, "journalctl exited")
		} else {
			cancel()
			require.NoError(t, <-done)
		}
		assert.Equal(t, want, got)
		assert.Empty(t, collected)
	}
	run(2, false, messages[:2])
	run(4, true, messages[2:4])
	run(5, false, messages[4:])
}

// Under auto, discover reads auth.log's lines as syslog.
func TestDiscoverReadsAuthLogAsSyslog(t *testing.T) {
	root := t.TempDir()
	data, err := os.ReadFile("testdata/auth.log")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "auth.log"), data, 0o644))
	assert.Equal(t, []string{
		"Oct 28 10:00:01|web1|sshd|8812||Accepted publickey for deploy from 10.0.0.7 port 51122 ssh2: ED25519 SHA256:p0kVQ0Zl9sWq8oG8tI3nVb7m4m2yQjzYcZ1sXw3hJgE",
		"Oct 28 10:00:01|web1|sshd|8812||pam_unix(sshd:session): session opened for user deploy(uid=1000) by (uid=0)",
		"Oct 28 10:00:01|web1|systemd-logind|612||New session 41 of user deploy.",
		"Oct 28 10:00:07|web1|sudo|||  deploy : TTY=pts/0 ; PWD=/home/deploy ; USER=root ; COMMAND=/usr/bin/systemctl restart nginx",
		"Oct 28 10:00:09|web1|sshd|8840||Invalid user admin from 203.0.113.9 port 40022",
	}, discovered(t, root))
}

// testdata/producers/acme.json alone adds a producer: discover takes its source, and ingest
// reads the source's lines in its format.
func TestOneFileAddsAProducer(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "acme"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "acme", "current"),
		[]byte("2026-10-28T10:00:01Z|W|billing|card declined\n2026-10-28T10:00:02Z|E|billing|ledger unreachable\n"), 0o644))
	assert.Equal(t, []string{"Oct 28 10:00:01|node-1|billing||warn|card declined", "Oct 28 10:00:02|node-1|billing||error|ledger unreachable"},
		discovered(t, root))
}

// discovered drains what discover finds under root with no journal, each record as
// time|host|service|pid|level|message.
func discovered(t *testing.T, root string) []string {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
	var got []string
	in, err := New(Config{Sources: discover(root), StateDir: root, Host: "node-1", FlushInterval: time.Hour},
		collector.Fake{CollectFn: func(r segment.Record) (collector.CollectResult, error) {
			cols := map[string]string{}
			for _, c := range r.Columns {
				cols[c.Name] = value.Format(c.Type, c.Value)
			}
			got = append(got, strings.Join([]string{r.Timestamp.Format(time.Stamp), cols["host"], cols["service"], cols["pid"], cols["level"], cols["message"]}, "|"))
			return collector.CollectResult{}, nil
		}})
	require.NoError(t, err)
	require.NoError(t, in.Drain(context.Background()))
	return got
}

func TestGlobMatchesAnyDepth(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a.log", "nginx/access.log", "nginx/old/error.log", "nginx/notes.txt"} {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, name), nil, 0o644))
	}
	paths, err := glob(filepath.Join(root, "**", "*.log"))
	require.NoError(t, err)
	assert.Equal(t, []string{filepath.Join(root, "a.log"), filepath.Join(root, "nginx/access.log"), filepath.Join(root, "nginx/old/error.log")}, paths)
}

// Run resumes a.log at its saved offset, is woken by a later append, and saves where every file got to at shutdown.
func TestRunFlushesAndSavesOffsetsAtShutdown(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{StateDir: dir, FlushInterval: time.Hour}
	for _, name := range []string{"a", "b", "c"} {
		path := filepath.Join(dir, name+".log")
		require.NoError(t, os.WriteFile(path, []byte(name+"1\n"+name+"2\n"), 0o644))
		cfg.Sources = append(cfg.Sources, Source{Path: path})
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, offsetsKey), []byte(`{"`+cfg.Sources[0].Path+`":{"offset":3}}`), 0o644))
	collected := make(chan string, 7)
	flushes := 0
	stop := start(t, cfg, collector.Fake{
		CollectFn: func(r segment.Record) (collector.CollectResult, error) {
			collected <- r.Columns[0].Value.String
			return collector.CollectResult{}, nil
		},
		FlushFn: func() ([]segment.Descriptor, error) { flushes++; return nil, nil },
	})
	var got []string
	for range 5 {
		got = append(got, <-collected)
	}
	a, err := os.OpenFile(cfg.Sources[0].Path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = a.WriteString("a3\n")
	require.NoError(t, errors.Join(err, a.Close()))
	got = append(got, <-collected)
	require.NoError(t, stop())

	assert.ElementsMatch(t, []string{"a2", "b1", "b2", "c1", "c2", "a3"}, got)
	assert.Empty(t, collected)
	assert.Equal(t, 1, flushes)
	offsets, err := loadOffsets(cfg.StateDir)
	require.NoError(t, err)
	assert.Len(t, offsets, 3)
	for i, want := range []int64{9, 6, 6} {
		assert.Equal(t, want, offsets[cfg.Sources[i].Path].Offset)
		assert.NotZero(t, offsets[cfg.Sources[i].Path].File, "the file read is saved with the offset")
	}
}

func TestRunCutsEachInterval(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	require.NoError(t, os.WriteFile(path, nil, 0o644))
	var once sync.Once
	cut := make(chan struct{})
	stop := start(t, Config{Sources: []Source{{Path: path}}, StateDir: filepath.Dir(path), FlushInterval: 10 * time.Millisecond}, collector.Fake{
		CutFn: func() (collector.CollectResult, error) {
			once.Do(func() { close(cut) })
			return collector.CollectResult{}, nil
		},
	})
	select {
	case <-cut:
	case <-time.After(5 * time.Second):
		assert.Fail(t, "no cut within the interval")
	}
	require.NoError(t, stop())
}

// start runs an Ingest until the returned stop cancels it, which returns Run's error.
func start(t *testing.T, cfg Config, c collector.Collector) (stop func() error) {
	in, err := New(cfg, c)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- in.Run(ctx) }()
	return func() error { cancel(); return <-done }
}

// collectRedacted collects each JSON line from a source redacting names under key.
func collectRedacted(t *testing.T, key string, names []string, lines ...string) []segment.Record {
	p, err := logformat.New(logformat.Config{Format: "json"})
	require.NoError(t, err)
	var collected []segment.Record
	in := &ingest{
		cfg: Config{RedactKey: key},
		collector: collector.Fake{CollectFn: func(r segment.Record) (collector.CollectResult, error) {
			collected = append(collected, r)
			return collector.CollectResult{}, nil
		}},
		resolver: identity.New("node-1"),
		parsers:  map[string]logformat.Parser{"a.log": p},
		redacted: map[string][]string{"a.log": names},
		offsets:  map[string]tail.Position{},
		pending:  map[string][]tail.Position{},
	}
	for n, line := range lines {
		require.NoError(t, in.collect(context.Background(), sourceLine{path: "a.log", line: tail.Line{Text: line, Position: tail.Position{Offset: int64(n + 1)}}}))
	}
	return collected
}

// A named column and every column flattened from it hold the hex HMAC of each value; the
// rest are stored as written.
func TestRedactHashesNamedColumnsBeforeCollection(t *testing.T) {
	lines := []string{
		`{"message":{"content":"my card is 4242","parts":[{"text":"hi"}]},"user":"ann","tokens":12}`,
		`{"message":{"content":"my card is 4242"},"user":"bob","tokens":12}`,
	}
	mac := func(key, raw string) value.Value {
		h := hmac.New(sha256.New, []byte(key))
		h.Write([]byte(raw))
		return value.Value{String: hex.EncodeToString(h.Sum(nil))}
	}
	hashed := map[string]func(key string) segment.Column{
		"message.content": func(key string) segment.Column {
			return segment.Column{Name: "message.content", Type: value.TypeString, Value: mac(key, "my card is 4242")}
		},
		"message.parts.text": func(key string) segment.Column {
			return segment.Column{Name: "message.parts.text", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: []value.Value{mac(key, "hi")}}}
		},
		"tokens": func(key string) segment.Column {
			return segment.Column{Name: "tokens", Type: value.TypeString, Value: mac(key, "12")}
		},
	}
	for _, key := range []string{"k1", "k2"} {
		want := collectRedacted(t, key, nil, lines...)
		for _, r := range want {
			for c, col := range r.Columns {
				if h, ok := hashed[col.Name]; ok {
					r.Columns[c] = h(key)
				}
			}
		}
		assert.Equal(t, want, collectRedacted(t, key, []string{"message", "tokens"}, lines...))
	}
	redact := []string{"message", "tokens"}
	assert.NotEqual(t, collectRedacted(t, "k1", redact, lines...), collectRedacted(t, "k2", redact, lines...))
}

func TestNewRefusesRedactWithoutAKey(t *testing.T) {
	_, err := New(Config{Sources: []Source{{Path: "a.log", Redact: []string{"message"}}}, StateDir: t.TempDir(), FlushInterval: time.Second}, collector.Fake{})
	assert.ErrorContains(t, err, "redact_key")
}

// Drain hands a fake collector the records of two files whose lines interleave in time in
// time order, an undated line at the time of the line before it, then flushes once and
// saves no offsets.
func TestDrainCollectsInRecordTimeOrder(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"a.log": `{"ts":"2026-07-01T00:00:01Z","msg":"a1"}` + "\n" + `{"msg":"a2"}` + "\n" + `{"ts":"2026-07-01T00:00:04Z","msg":"a3"}` + "\n",
		"b.log": `{"ts":"2026-07-01T00:00:00Z","msg":"b1"}` + "\n" + `{"ts":"2026-07-01T00:00:02Z","msg":"b2"}` + "\n" + `{"ts":"2026-07-01T00:00:04Z","msg":"b3"}`,
	} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644))
	}
	type line struct {
		msg string
		at  int
	}
	var got []line
	flushes := 0
	in, err := New(Config{StateDir: dir, FlushInterval: time.Hour, Sources: []Source{{Path: filepath.Join(dir, "*.log"), Parse: logformat.Config{Format: "json"}}}},
		collector.Fake{
			CollectFn: func(r segment.Record) (collector.CollectResult, error) {
				msg := slices.IndexFunc(r.Columns, func(c segment.Column) bool { return c.Name == segment.ColMessage })
				got = append(got, line{r.Columns[msg].Value.String, r.Timestamp.Second()})
				return collector.CollectResult{}, nil
			},
			FlushFn: func() ([]segment.Descriptor, error) { flushes++; return nil, nil },
		})
	require.NoError(t, err)
	require.NoError(t, in.Drain(context.Background()))
	assert.Equal(t, []line{{"b1", 0}, {"a1", 1}, {"a2", 1}, {"b2", 2}, {"a3", 4}, {"b3", 4}}, got)
	assert.Equal(t, 1, flushes)
	assert.NoFileExists(t, filepath.Join(dir, offsetsKey))
}

// Each file's lines parse on their own worker, which this parser proves by returning a line's
// record only once both files are parsing, while records are collected in the order read.
func TestCollectParsesFilesAtOnce(t *testing.T) {
	var parsing sync.WaitGroup
	parsing.Add(2)
	both := make(chan struct{})
	go func() { parsing.Wait(); close(both) }()
	parser := logformat.Fake{ParseAllFn: func(line string) []segment.Record {
		parsing.Done()
		select {
		case <-both:
			return []segment.Record{segment.LogRecord(time.Unix(1, 0), "", line, map[string]string{"request.model": line})}
		case <-time.After(5 * time.Second):
			return nil
		}
	}}
	var collected []string
	in := &ingest{
		collector: collector.Fake{CollectFn: func(r segment.Record) (collector.CollectResult, error) {
			slices.SortFunc(r.Columns, func(a, b segment.Column) int { return strings.Compare(a.Name, b.Name) })
			collected = append(collected, r.Text("model"))
			return collector.CollectResult{}, nil
		}},
		resolver: identity.New(""), parsers: map[string]logformat.Parser{"a.log": parser, "b.log": parser},
		offsets: map[string]tail.Position{}, pending: map[string][]tail.Position{}, dated: map[string]time.Time{},
	}
	require.NoError(t, in.collect(context.Background(), sourceLine{path: "b.log", line: tail.Line{Text: "b1"}}, sourceLine{path: "a.log", line: tail.Line{Text: "a1"}}))
	assert.Equal(t, []string{"b1", "a1"}, collected)
}

func TestCarryStateKeepsTheMostRecentlyWrittenFiles(t *testing.T) {
	in := &ingest{cfg: Config{CarriedFiles: 2}}
	a, b := in.carryFile("a.log"), in.carryFile("b.log")
	assert.Same(t, a, in.carryFile("a.log"))
	in.carryFile("c.log")
	assert.Same(t, a, in.carryFile("a.log"))
	assert.NotSame(t, b, in.carryFile("b.log"))
}
