// Package ingest follows the log files each source's glob matches, a command's output and
// a cluster's Events and container states into the collector, naming each record's
// producer, and saves how far it got in each after every flush so a restart resumes there.
package ingest

import (
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"k8s.io/client-go/kubernetes"

	"github.com/datafocal/datafocal/internal/carry"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/identity"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/producers"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/tail"
	"github.com/datafocal/datafocal/internal/value"
)

// offsetsKey is the object under StateDir holding each source's saved offset.
const offsetsKey = "offsets.json"

// Ingest runs the write path of one collector process.
type Ingest interface {
	Run(ctx context.Context) error   // until ctx ends, then flushes
	Drain(ctx context.Context) error // every matched file from its start to EOF, in record-time order, then flushes
}

type Config struct {
	Sources  []Source `json:"sources"`   // discovered under /var/log when empty
	StateDir string   `json:"state_dir"` // where offsets are saved
	Host     string   `json:"host"`      // each record's host; default the hostname
	// RedactKey is the HMAC key for every source's Redact columns, so a stored hash can't be
	// matched by hashing guesses without it.
	RedactKey string `json:"redact_key"`
	// FlushInterval is how often buffered lines are flushed when no more arrive, and
	// sources are globbed for new files; the collector's own age check only runs when a
	// line does.
	FlushInterval time.Duration `json:"-"`
	Carry         []carry.Rule  `json:"carry"` // nil discovers links
	// Workers is how many files' lines are parsed at once, GOMAXPROCS when zero.
	Workers int `json:"workers"`
	// CarriedFiles is how many files keep carry state, the least recently written dropped,
	// so a session file idle past that many others loses its carried context; 1024 when zero.
	CarriedFiles int `json:"carried_files"`
	// Kube is the client a kube source watches, in-cluster config's when nil.
	Kube kubernetes.Interface `json:"-"`
}

// batchLines bounds the lines Run takes from its followers at once to parse in parallel.
const batchLines = 1024

type Source struct {
	// Kind is "file" (default), "command" or "kube", the cluster's Events and container states.
	Kind string `json:"kind"`
	// Path is a file glob, where /**/ matches any depth of directories, relative to the log
	// root in a producer file.
	Path    string   `json:"path"`
	Command []string `json:"command"` // a command's argv, whose output is one JSON object per line
	Resume  string   `json:"resume"`  // the argument resuming a command, {cursor} its saved cursor
	Cursor  string   `json:"cursor"`  // the field of a command's line that is its cursor
	Detect  string   `json:"detect"`  // a program on PATH, without which discover leaves the source out
	// Replaces are the paths discover leaves to this source, relative to the log root unless
	// absolute, so what the source already holds is not read twice.
	Replaces []string         `json:"replaces"`
	Parse    logformat.Config `json:"parse"` // the format defaults to "plain"
	// Redact names columns hashed before collection; a name also covers every column
	// flattened from it, as message covers message.content.
	Redact []string `json:"redact"`
	skip   []string // paths the glob matches that another source replaces
}

const kindCommand = "command"

func (s Source) parse() logformat.Config {
	if s.Kind == kindKube {
		return logformat.Config{Format: "json"} // the lines kube writes
	}
	s.Parse.Format = cmp.Or(s.Parse.Format, "plain")
	return s.Parse
}

// paths names what src reads now: the files its glob matches, or the command, whose saved
// positions they key.
func (s Source) paths() ([]string, error) {
	switch s.Kind {
	case kindCommand:
		return s.Command[:1], nil
	case kindKube:
		return []string{kindKube}, nil
	}
	paths, err := glob(s.Path)
	return slices.DeleteFunc(paths, func(p string) bool { return slices.Contains(s.skip, p) }), err
}

type ingest struct {
	cfg       Config
	collector collector.Collector
	resolver  identity.Resolver
	parsers   map[string]logformat.Parser // by path, every file followed
	redacted  map[string][]string         // by path, its source's Redact
	state     storage.Store
	// offsets holds, per file path, the position just past the last line collected.
	offsets map[string]tail.Position
	// pending holds, per file path, the position just past each line read since its offset.
	pending map[string][]tail.Position
	// dated holds, per file path, the last timestamp a record carried, to date the undated
	// after it; unsaved, so after a restart they take the read time until a dated one.
	dated map[string]time.Time
	// carrier is unsaved, so after a restart the records before the next source record go
	// without what it carries.
	carrier *lru.Cache[string, carry.Carrier]
	// cuts holds the offsets as of each batch the collector cut and has not yet reported
	// durable, oldest first; a batch's are saved once it is.
	cuts    []cut
	unsaved bool // offsets changed since they were last saved
}

// cut is the offsets as of one cut batch, and whether they had changed since the last
// save, without which saving them would write what is already stored.
type cut struct {
	offsets map[string]tail.Position
	unsaved bool
}

type sourceLine struct {
	path    string
	line    tail.Line
	removed bool // path's file is deleted and every line in it sent
}

// parsed is the record a line closed, if any, and the lines its parser held after it.
type parsed struct {
	extra  []segment.Record
	record segment.Record
	ok     bool
	held   int
}

// New loads each source's saved offset; Run opens the sources there.
func New(cfg Config, c collector.Collector) (Ingest, error) {
	if len(cfg.Sources) == 0 {
		cfg.Sources = discover(identity.LogRoot)
	}
	if cfg.StateDir == "" {
		return nil, errors.New("ingest: state dir is required")
	}
	if cfg.FlushInterval <= 0 {
		return nil, errors.New("ingest: flush interval must be positive")
	}
	if cfg.CarriedFiles < 0 {
		return nil, errors.New("ingest: carried files must not be negative")
	}
	for _, src := range cfg.Sources {
		if !slices.Contains([]string{"", "file", kindCommand, kindKube}, src.Kind) || src.Kind == kindCommand && len(src.Command) == 0 {
			return nil, fmt.Errorf("ingest: source kind %q needs a known kind, and a command its argv", src.Kind)
		}
		if _, err := logformat.New(src.parse()); err != nil {
			return nil, fmt.Errorf("ingest: source %s: %w", src.Path, err)
		}
		if len(src.Redact) > 0 && cfg.RedactKey == "" {
			return nil, fmt.Errorf("ingest: source %s: redact needs redact_key", src.Path)
		}
	}
	offsets, err := loadOffsets(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	return &ingest{cfg: cfg, collector: c, resolver: identity.New(cfg.Host), parsers: map[string]logformat.Parser{},
		redacted: map[string][]string{}, dated: map[string]time.Time{}, state: storage.NewFS(cfg.StateDir), offsets: offsets, pending: map[string][]tail.Position{}}, nil
}

// discover takes each producer source whose program is on PATH, but on a Kubernetes node
// only commands and every container's log, leaving out what a taken source replaces.
func discover(root string) []Source {
	sources, replaced := detectedProducers(root)
	containers := filepath.Join(root, "containers")
	if _, err := os.Stat(containers); err == nil {
		sources = slices.DeleteFunc(sources, func(src Source) bool { return src.Kind != kindCommand })
		sources = append(sources, Source{Path: filepath.Join(containers, "*.log"), Parse: logformat.Config{Format: "auto", Envelope: "cri"}})
	}
	sources = slices.DeleteFunc(sources, func(src Source) bool { return slices.Contains(replaced, src.Path) })
	for i := range sources {
		sources[i].skip = replaced
		slog.Default().Info("discovered source", "kind", cmp.Or(sources[i].Kind, "file"), "path", sources[i].Path, "command", sources[i].Command, "replaced", replaced)
	}
	return sources
}

// detectedProducers is each producer source whose program is on PATH, and the paths that
// they replace, with every path under root where relative.
func detectedProducers(root string) (sources []Source, replaced []string) {
	under := func(path string) string {
		if path == "" || filepath.IsAbs(path) {
			return path
		}
		return filepath.Join(root, path)
	}
	for _, src := range producers.Section[Source]("sources") {
		if _, err := exec.LookPath(src.Detect); src.Detect != "" && err != nil {
			continue
		}
		src.Path = under(src.Path)
		sources = append(sources, src)
		for _, path := range src.Replaces {
			replaced = append(replaced, under(path))
		}
	}
	return sources, replaced
}

func loadOffsets(dir string) (map[string]tail.Position, error) {
	offsets := map[string]tail.Position{}
	data, err := os.ReadFile(filepath.Join(dir, offsetsKey))
	if errors.Is(err, fs.ErrNotExist) {
		return offsets, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ingest: read offsets: %w", err)
	}
	if err := json.Unmarshal(data, &offsets); err != nil {
		return nil, fmt.Errorf("ingest: parse offsets: %w", err)
	}
	return offsets, nil
}

// Run collects lines on one goroutine, so the offsets it saves after a flush never run
// ahead of what that flush made durable.
func (i *ingest) Run(ctx context.Context) (err error) {
	watcher, err := tail.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, watcher.Close()) }()
	followCtx, stop := context.WithCancel(ctx)
	lines := make(chan sourceLine, batchLines)
	errs := make(chan error)
	var following sync.WaitGroup
	defer following.Wait()
	defer stop()
	open := func() error {
		return i.open(followCtx, watcher, func(path string, f tail.Follower) {
			following.Go(func() { follow(followCtx, path, f, lines, errs) })
		})
	}
	if err := open(); err != nil {
		return err
	}

	ticker := time.NewTicker(i.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
			defer cancel()
			return i.flush(flushCtx)
		case err := <-errs:
			return errors.Join(err, i.flush(ctx))
		case <-ticker.C:
			if err := i.cut(ctx); err != nil {
				return err
			}
			if err := open(); err != nil {
				return err
			}
		case sl := <-lines:
			if err := i.collect(ctx, take(sl, lines)...); err != nil {
				return err
			}
		}
	}
}

// drained is one file Drain reads, and the next record parsed from it while ok.
type drained struct {
	path   string
	lines  []string // those not yet parsed, each with its newline
	parser logformat.Parser
	next   segment.Record
	ok     bool
	queued []segment.Record
	dated  time.Time // the last timestamp a record in the file carried
}

// Drain hands the collector the earliest of each file's next record, ties to the lower path,
// so a node's history reaches it in the order it happened; it reads and saves no offsets.
func (i *ingest) Drain(ctx context.Context) error {
	var sources []Source
	for _, src := range i.cfg.Sources {
		if src.Kind == kindCommand || src.Kind == kindKube {
			slog.Default().Info("drain skips a source that is not files", "kind", src.Kind)
			continue
		}
		sources = append(sources, src)
	}
	files, err := i.drainFiles(sources)
	if err != nil {
		return err
	}
	for f := earliest(files); f != nil; f = earliest(files) {
		if _, err := i.collector.Collect(ctx, i.resolver.Resolve(f.path, i.redact(f.path, i.carryFile(f.path).Carry(f.path, f.next)))); err != nil {
			return fmt.Errorf("ingest: collect %s: %w", f.path, err)
		}
		f.advance()
	}
	if _, err := i.collector.Flush(ctx); err != nil {
		return fmt.Errorf("ingest: flush: %w", err)
	}
	return nil
}

// drainFiles reads each file the sources match, once however many match it, by path.
func (i *ingest) drainFiles(sources []Source) ([]*drained, error) {
	var files []*drained
	for _, src := range sources {
		paths, err := src.paths()
		if err != nil {
			return nil, fmt.Errorf("ingest: source %s: %w", src.Path, err)
		}
		for _, path := range paths {
			if slices.ContainsFunc(files, func(f *drained) bool { return f.path == path }) {
				continue
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("ingest: %w", err)
			}
			p, err := logformat.New(src.parse()) // New checked it
			if err != nil {
				return nil, err
			}
			i.redacted[path] = src.Redact
			f := &drained{path: path, lines: slices.Collect(strings.Lines(string(data))), parser: p}
			f.advance()
			files = append(files, f)
		}
	}
	slices.SortFunc(files, func(a, b *drained) int { return strings.Compare(a.path, b.path) })
	return files, nil
}

// earliest is the file whose next record is the earliest, the first of them on a tie.
func earliest(files []*drained) *drained {
	var first *drained
	for _, f := range files {
		if f.ok && (first == nil || f.next.Timestamp.Before(first.next.Timestamp)) {
			first = f
		}
	}
	return first
}

// advance parses f's lines up to its next record, or its end; an undated record takes the
// time of the one before it, as collectRecord dates it.
func (f *drained) advance() {
	for len(f.queued) == 0 && len(f.lines) > 0 {
		r := parseLine(f.parser, strings.TrimSuffix(f.lines[0], "\n"))
		f.queued = append(f.queued, r.extra...)
		if r.ok {
			f.queued = append(f.queued, r.record)
		}
		f.lines = f.lines[1:]
	}
	if len(f.queued) > 0 {
		f.next, f.ok = f.queued[0], true
		f.queued = f.queued[1:]
	} else {
		f.next, f.ok = f.parser.Flush()
	}

	if f.next.Timestamp.IsZero() {
		f.next.Timestamp = f.dated
	} else {
		f.dated = f.next.Timestamp
	}
}

// open passes start each file a source matches, or its command, that is not yet followed,
// from its saved position.
func (i *ingest) open(ctx context.Context, watcher tail.Watcher, start func(path string, f tail.Follower)) error {
	for _, src := range i.cfg.Sources {
		paths, err := src.paths()
		if err != nil {
			return fmt.Errorf("ingest: source %s: %w", src.Path, err)
		}
		for _, path := range paths {
			if _, ok := i.parsers[path]; ok {
				continue
			}
			if err := i.openFile(ctx, watcher, src, path, start); err != nil {
				return err
			}
		}
	}
	return nil
}

// openFile skips a file removed since the glob.
func (i *ingest) openFile(ctx context.Context, watcher tail.Watcher, src Source, path string, start func(path string, f tail.Follower)) error {
	var f tail.Follower
	var err error
	switch src.Kind {
	case kindCommand:
		f, err = tail.OpenCommand(ctx, src.Command, src.Resume, src.Cursor, i.offsets[path])
	case kindKube:
		f, err = openKube(ctx, i.cfg.Kube, i.offsets[path])
	default:
		f, err = watcher.Open(path, i.offsets[path])
	}
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	p, err := logformat.New(src.parse()) // New checked it
	if err != nil {
		return err
	}
	i.parsers[path] = p
	i.redacted[path] = src.Redact
	slog.Default().Info("following file", "path", path, "offset", i.offsets[path].Offset)
	start(path, f)
	return nil
}

// glob is filepath.Glob, except that a pattern's /**/ matches any depth of directories,
// and the rest of the pattern a file's name; an unreadable directory is skipped.
func glob(pattern string) ([]string, error) {
	root, name, ok := strings.Cut(pattern, string(filepath.Separator)+"**"+string(filepath.Separator))
	if !ok {
		return filepath.Glob(pattern)
	}
	if _, err := filepath.Match(name, ""); err != nil {
		return nil, err
	}
	var paths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if ok, _ := filepath.Match(name, d.Name()); ok { // name was checked above
			paths = append(paths, path)
		}
		return nil
	})
	return paths, err
}

func follow(ctx context.Context, path string, f tail.Follower, lines chan<- sourceLine, errs chan<- error) {
	for {
		line, err := f.Next(ctx)
		if errors.Is(err, tail.ErrRemoved) {
			select {
			case lines <- sourceLine{path: path, removed: true}:
			case <-ctx.Done():
			}
			return
		}
		if err != nil {
			select {
			case errs <- err:
			case <-ctx.Done():
			}
			return
		}
		select {
		case lines <- sourceLine{path: path, line: line}:
		case <-ctx.Done():
			return
		}
	}
}

// take is first and the lines already waiting after it, up to batchLines.
func take(first sourceLine, lines <-chan sourceLine) []sourceLine {
	batch := []sourceLine{first}
	for len(batch) < batchLines {
		select {
		case sl := <-lines:
			batch = append(batch, sl)
		default:
			return batch
		}
	}
	return batch
}

// collect hands each complete record to collectRecord, which saves offsets at a seal, in
// the order the lines arrived, so parsing them a file per worker changes no record or offset.
func (i *ingest) collect(ctx context.Context, batch ...sourceLine) error {
	var parsing sync.WaitGroup
	defer parsing.Wait()
	records := i.parse(batch, &parsing)
	for _, sl := range batch {
		if sl.removed {
			if err := i.remove(ctx, sl.path); err != nil {
				return err
			}
			continue
		}
		i.pending[sl.path] = append(i.pending[sl.path], sl.line.Position)
		r := <-records[sl.path]
		if !r.ok { // held, so it records no offset and a restart rereads it
			continue
		}
		for _, extra := range r.extra {
			if err := i.collectRecord(ctx, sl.path, extra, r.held+1); err != nil {
				return err
			}
		}
		if err := i.collectRecord(ctx, sl.path, r.record, r.held); err != nil {
			return err
		}
	}
	return nil
}

// parse parses and carries each file's lines in order, the files on up to Workers goroutines
// that parsing joins, and sends each file's records on its channel as they are parsed.
func (i *ingest) parse(batch []sourceLine, parsing *sync.WaitGroup) map[string]chan parsed {
	byPath := map[string][]string{}
	for _, sl := range batch {
		if !sl.removed {
			byPath[sl.path] = append(byPath[sl.path], sl.line.Text)
		}
	}
	out := make(map[string]chan parsed, len(byPath))
	workers := make(chan struct{}, cmp.Or(i.cfg.Workers, runtime.GOMAXPROCS(0)))
	for path, lines := range byPath {
		p, records := i.parsers[path], make(chan parsed, len(lines))
		c := i.carryFile(path)
		out[path] = records
		parse := func() {
			for _, line := range lines {
				records <- parseCarried(p, c, path, line)
			}
		}
		if len(batch) == 1 { // a line at a time, as a quiet file sends them, costs no goroutine
			parse()
			continue
		}
		parsing.Go(func() {
			workers <- struct{}{}
			defer func() { <-workers }()
			parse()
		})
	}
	return out
}

// Each file's worker owns its carry state; only the collector manages this cache.
func (i *ingest) carryFile(path string) carry.Carrier {
	if i.carrier == nil {
		i.carrier, _ = lru.New[string, carry.Carrier](cmp.Or(i.cfg.CarriedFiles, 1024))
	}
	if c, ok := i.carrier.Get(path); ok {
		return c
	}
	c := carry.New(i.cfg.Carry)
	i.carrier.Add(path, c)
	return c
}

// remove forgets path, so a file recreated there is followed from its start.
func (i *ingest) remove(ctx context.Context, path string) error {
	slog.Default().Info("file removed", "path", path)
	if err := i.flushParser(ctx, path); err != nil {
		return err
	}
	delete(i.parsers, path)
	delete(i.redacted, path)
	delete(i.dated, path)
	if i.carrier != nil {
		i.carrier.Remove(path)
	}
	delete(i.offsets, path)
	delete(i.pending, path)
	// A removed file cannot be reread, so no saved offset should outlive it.
	for _, c := range i.cuts {
		delete(c.offsets, path)
	}
	i.unsaved = true
	return nil
}

// collectRecord moves path's offset past the lines its parser held when it parsed the
// record, so a restart rereads the lines of a record still open. Each batch cut takes the
// offsets as they stand, with this record's lines only when the batch holds the record, and
// they are saved once the batch is durable, so a restart neither skips nor rereads a line.
func (i *ingest) collectRecord(ctx context.Context, path string, record segment.Record, held int) error {
	if record.Timestamp.IsZero() {
		record.Timestamp = i.dated[path]
	} else {
		i.dated[path] = record.Timestamp
	}
	result, err := i.collector.Collect(ctx, i.resolver.Resolve(path, i.redact(path, record)))
	if err != nil {
		return fmt.Errorf("ingest: collect %s: %w", path, err)
	}
	cutBefore := result.Cut
	if result.RecordSealed {
		cutBefore--
	}
	for range cutBefore {
		i.cuts = append(i.cuts, cut{offsets: maps.Clone(i.offsets), unsaved: i.unsaved})
	}
	if !result.RecordSealed {
		if err := i.saveDurable(ctx, result.Durable); err != nil {
			return err
		}
	}
	// A line's records before its last hold it as well as the lines held, so the first of a
	// file's lines past every line held may complete none.
	if pending := i.pending[path]; len(pending) > held {
		done := len(pending) - held
		i.offsets[path] = pending[done-1]
		i.pending[path] = pending[done:]
		i.unsaved = true
	}
	if !result.RecordSealed {
		return nil
	}
	i.cuts = append(i.cuts, cut{offsets: maps.Clone(i.offsets), unsaved: true})
	return i.saveDurable(ctx, result.Durable)
}

// saveDurable saves the offsets of the latest of the n oldest cuts, which the collector
// reports durable in the order it cut them.
func (i *ingest) saveDurable(ctx context.Context, n int) error {
	if n == 0 {
		return nil
	}
	latest := i.cuts[n-1]
	i.cuts = i.cuts[n:]
	if !latest.unsaved {
		return nil
	}
	if err := i.put(ctx, latest.offsets); err != nil {
		return err
	}
	i.unsaved = !maps.Equal(latest.offsets, i.offsets)
	return nil
}

// redact replaces the values of each column path's source names with their hex HMAC, so
// equal values still group; a list's elements are hashed one by one.
func (i *ingest) redact(path string, record segment.Record) segment.Record {
	names := i.redacted[path]
	for c, col := range record.Columns {
		if !slices.ContainsFunc(names, func(n string) bool { return col.Name == n || strings.HasPrefix(col.Name, n+".") }) {
			continue
		}
		if col.Type.Repeated() {
			for e, v := range col.Value.List {
				col.Value.List[e] = i.hash(col.Type.Scalar(), v)
			}
		} else {
			col.Value = i.hash(col.Type, col.Value)
		}
		record.Columns[c] = segment.Column{Name: col.Name, Type: value.TypeString | col.Type&value.TypeRepeated, Value: col.Value}
	}
	return record
}

func (i *ingest) hash(typ value.Type, v value.Value) value.Value {
	mac := hmac.New(sha256.New, []byte(i.cfg.RedactKey))
	mac.Write([]byte(value.Format(typ, v)))
	return value.Value{String: hex.EncodeToString(mac.Sum(nil))}
}

func (i *ingest) flushParser(ctx context.Context, path string) error {
	record, ok := i.parsers[path].Flush()
	if !ok {
		return nil
	}
	return i.collectRecord(ctx, path, i.carryFile(path).Carry(path, record), i.parsers[path].Held())
}

// cut closes every open record, then cuts the collector's batch without waiting for it to
// land, keeping the offsets as of the cut to save once it is durable, as a size cut's are.
func (i *ingest) cut(ctx context.Context) error {
	for _, path := range slices.Sorted(maps.Keys(i.parsers)) {
		if err := i.flushParser(ctx, path); err != nil {
			return err
		}
	}
	result, err := i.collector.Cut(ctx)
	if err != nil {
		return fmt.Errorf("ingest: cut: %w", err)
	}
	if result.Cut > 0 {
		i.cuts = append(i.cuts, cut{offsets: maps.Clone(i.offsets), unsaved: i.unsaved})
	}
	return i.saveDurable(ctx, result.Durable)
}

// flush closes every open record first, so a flush never leaves a line read but uncollected.
func (i *ingest) flush(ctx context.Context) error {
	for _, path := range slices.Sorted(maps.Keys(i.parsers)) {
		if err := i.flushParser(ctx, path); err != nil {
			return err
		}
	}
	if _, err := i.collector.Flush(ctx); err != nil {
		return fmt.Errorf("ingest: flush: %w", err)
	}
	i.cuts = nil // Flush returns once every batch is durable
	return i.saveOffsets(ctx)
}

func (i *ingest) saveOffsets(ctx context.Context) error {
	if !i.unsaved {
		return nil
	}
	if err := i.put(ctx, i.offsets); err != nil {
		return err
	}
	i.unsaved = false
	return nil
}

func (i *ingest) put(ctx context.Context, offsets map[string]tail.Position) error {
	data, err := json.Marshal(offsets)
	if err != nil {
		return fmt.Errorf("ingest: marshal offsets: %w", err)
	}
	if err := i.state.Put(ctx, offsetsKey, data); err != nil {
		return fmt.Errorf("ingest: save offsets: %w", err)
	}
	return nil
}

func parseCarried(p logformat.Parser, c carry.Carrier, path, line string) parsed {
	r := parseLine(p, line)
	if !r.ok {
		return r
	}
	for n := range len(r.extra) + 1 {
		row := &r.record
		if n < len(r.extra) {
			row = &r.extra[n]
		}
		*row = c.Carry(path, *row)
	}
	return r
}

func parseLine(p logformat.Parser, line string) parsed {
	rows := p.ParseAll(line)
	if len(rows) == 0 {
		return parsed{held: p.Held()}
	}
	return parsed{record: rows[len(rows)-1], ok: true, extra: rows[:len(rows)-1], held: p.Held()}
}
