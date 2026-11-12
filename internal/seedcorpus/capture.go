package seedcorpus

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/logformat"
)

// CaptureManifest is the file in a captured corpus's directory naming its sources.
const CaptureManifest = "manifest.json"

// Capture is a corpus captured from a running system, its files kept as the system wrote
// them under a directory with a CaptureManifest declaring how a collector reads them.
type Capture struct {
	// Name names the capture in a report, such as ai-host or sprawl-access.
	Name string `json:"name"`
	// Kind is the queries a benchmark asks of it: ai, for agent sessions, or mesh.
	Kind    string          `json:"kind"`
	Sources []CaptureSource `json:"sources"`
	// Copies is how many copies a benchmark replays unless told, enough hours for what grows
	// with the data, as a summary does, from a capture of minutes.
	Copies int `json:"copies,omitempty"`
}

// CaptureSource is files of a capture a collector reads alike: a glob under the capture's
// directory, a replay's too, and their format, as a collector's file source declares them.
type CaptureSource struct {
	Path  string           `json:"path"`
	Parse logformat.Config `json:"parse"`
}

// ReadCapture reads the capture in dir.
func ReadCapture(dir string) (Capture, error) {
	data, err := os.ReadFile(filepath.Join(dir, CaptureManifest))
	if err != nil {
		return Capture{}, fmt.Errorf("read capture manifest: %w", err)
	}
	var c Capture
	if err := json.Unmarshal(data, &c); err != nil {
		return Capture{}, fmt.Errorf("decode capture manifest: %w", err)
	}
	if len(c.Sources) == 0 {
		return Capture{}, fmt.Errorf("capture %s declares no sources", dir)
	}
	return c, nil
}

// Replay is what ReplayCapture wrote: the files' lines and bytes, the records a collector
// reads from them, and the capture it replayed.
type Replay struct {
	LogsResult
	Records int `json:"records"`
	// Span is one copy's time, first record to last; Shift moves the newest copy's last
	// record to the replay's end.
	Span  time.Duration `json:"span"`
	Shift time.Duration `json:"shift"`
	// Start and End are the oldest copy's first record and the newest's last.
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	// Fingerprint names the captured files' content, so two reports compare only when it
	// matches; Files and InputBytes are one copy's.
	Fingerprint string `json:"fingerprint"`
	Files       int    `json:"files"`
	InputBytes  int64  `json:"input_bytes"`
}

// ReplayCapture writes copies of the capture in dir to out, at the same paths, re-timed so
// the newest copy's last record falls at end and each earlier copy ends where the next
// begins. A copy but the newest names its own ids, every UUID and hex trace or span id
// replaced alike throughout it, so copies are other sessions and traces, not repeats.
func ReplayCapture(dir, out string, copies int, end time.Time) (Replay, error) {
	c, err := ReadCapture(dir)
	if err != nil {
		return Replay{}, err
	}
	files, err := captureFiles(dir, c)
	if err != nil {
		return Replay{}, err
	}
	first, last, err := recordSpan(dir, files)
	if err != nil {
		return Replay{}, err
	}
	r := Replay{LogsResult: LogsResult{Lines: map[string]int{}}, Span: last.Sub(first), Shift: end.Sub(last), Files: len(files), End: end}
	// A copy starts a second after the one before ends, so no two copies hold one instant.
	step := r.Span.Truncate(time.Second) + time.Second
	r.Start = first.Add(r.Shift - time.Duration(copies-1)*step)
	sum := sha256.New()
	for _, f := range files {
		in := filepath.Join(dir, f.path)
		info, err := os.Stat(in)
		if err != nil {
			return Replay{}, fmt.Errorf("stat %s: %w", f.path, err)
		}
		r.InputBytes += info.Size()
		written, err := replayFile(in, filepath.Join(out, f.path), retiming{copies: copies, shift: r.Shift, step: step,
			from: first.Add(-time.Hour), to: last.Add(time.Hour)})
		if err != nil {
			return Replay{}, fmt.Errorf("replay %s: %w", f.path, err)
		}
		r.Bytes += written.bytes
		r.Lines[""] += written.lines
		fmt.Fprintf(sum, "%s %s\n", f.path, written.digest)
	}
	r.Fingerprint = fmt.Sprintf("%x", sum.Sum(nil))[:16]
	if r.Records, err = countRecords(out, files); err != nil {
		return Replay{}, err
	}
	return r, nil
}

// captureFile is one file a capture holds, relative to its directory, and its format.
type captureFile struct {
	path  string
	parse logformat.Config
}

func captureFiles(dir string, c Capture) ([]captureFile, error) {
	var files []captureFile
	for _, src := range c.Sources {
		matches, err := filepath.Glob(filepath.Join(dir, src.Path))
		if err != nil {
			return nil, fmt.Errorf("source %s: %w", src.Path, err)
		}
		for _, m := range matches {
			rel, err := filepath.Rel(dir, m)
			if err != nil {
				return nil, err
			}
			files = append(files, captureFile{path: rel, parse: src.Parse})
		}
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("capture %s holds no file its sources match", dir)
	}
	slices.SortFunc(files, func(a, b captureFile) int { return strings.Compare(a.path, b.path) })
	return files, nil
}

// recordSpan is the first and last time a record of files is at, as a collector reads them.
func recordSpan(dir string, files []captureFile) (first, last time.Time, err error) {
	err = eachRecordTime(dir, files, func(t time.Time) {
		if t.IsZero() { // a record naming no time is collected at its own, which says nothing
			return
		}
		if first.IsZero() || t.Before(first) {
			first = t
		}
		last = maxTime(last, t)
	})
	if err == nil && first.IsZero() {
		err = fmt.Errorf("capture %s holds no timed record", dir)
	}
	return first, last, err
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

// countRecords is the records a collector reads from files under dir.
func countRecords(dir string, files []captureFile) (int, error) {
	n := 0
	err := eachRecordTime(dir, files, func(time.Time) { n++ })
	return n, err
}

// eachRecordTime calls fn with the time of each record files under dir hold, a record
// without one at the zero time, parsed as their sources declare.
func eachRecordTime(dir string, files []captureFile, fn func(time.Time)) error {
	for _, f := range files {
		p, err := logformat.New(f.parse)
		if err != nil {
			return fmt.Errorf("source %s: %w", f.path, err)
		}
		err = eachLine(filepath.Join(dir, f.path), func(line string) {
			for _, rec := range p.ParseAll(line) {
				fn(rec.Timestamp)
			}
		})
		if err != nil {
			return err
		}
		if rec, ok := p.Flush(); ok {
			fn(rec.Timestamp)
		}
	}
	return nil
}

func eachLine(path string, fn func(string)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20) // an OTLP batch or a tool result runs to megabytes
	for sc.Scan() {
		fn(sc.Text())
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	return nil
}

// replayed is what replayFile wrote and the digest of the file it read.
type replayed struct {
	lines  int
	bytes  int64
	digest string
}

// retiming is how a replay moves a capture: copies of it, the newest last and shift later,
// each earlier one step before the next, moving every time between from and to it names.
type retiming struct {
	copies      int
	shift, step time.Duration
	from, to    time.Time
}

// replayFile writes in to out as rt moves it.
func replayFile(in, out string, rt retiming) (replayed, error) {
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		return replayed{}, err
	}
	w, err := os.Create(out)
	if err != nil {
		return replayed{}, err
	}
	defer w.Close()
	bw := bufio.NewWriterSize(w, 1<<20)
	var r replayed
	sum := sha256.New()
	for k := rt.copies - 1; k >= 0; k-- {
		retime := retimer{by: rt.shift - time.Duration(k)*rt.step, from: rt.from, to: rt.to}
		err := eachLine(in, func(line string) {
			if k == 0 {
				_, _ = io.WriteString(sum, line+"\n")
			}
			if k > 0 {
				line = renameIDs(line, k)
			}
			line = retime.line(line)
			n, _ := bw.WriteString(line + "\n")
			r.lines++
			r.bytes += int64(n)
		})
		if err != nil {
			return replayed{}, err
		}
	}
	r.digest = fmt.Sprintf("%x", sum.Sum(nil))
	if err := bw.Flush(); err != nil {
		return replayed{}, err
	}
	return r, w.Close()
}

// The times a capture's lines name: RFC 3339 anywhere, as a container runtime's envelope,
// a transcript and a proxy's access log write them, and the epoch counts journald and an
// OTLP export write under their keys.
var (
	rfc3339Time = regexp.MustCompile(`\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,9})?(?:Z|[+-]\d\d:\d\d)`)
	epochTime   = regexp.MustCompile(`("(?:__REALTIME_TIMESTAMP|_SOURCE_REALTIME_TIMESTAMP|\w*[tT]imeUnixNano)"\s*:\s*"?)(\d{16,19})`)
)

// retimer moves each time from..to a line names by.
type retimer struct {
	by       time.Duration
	from, to time.Time
}

func (rt retimer) line(line string) string {
	line = rfc3339Time.ReplaceAllStringFunc(line, func(s string) string {
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil || t.Before(rt.from) || t.After(rt.to) {
			return s
		}
		return formatLike(s, t, t.Add(rt.by))
	})
	return epochTime.ReplaceAllStringFunc(line, func(s string) string {
		m := epochTime.FindStringSubmatch(s)
		n, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil {
			return s
		}
		unit := time.Microsecond // journald's
		if len(m[2]) == 19 {
			unit = time.Nanosecond // OTLP's
		}
		t := time.Unix(0, n*int64(unit))
		if t.Before(rt.from) || t.After(rt.to) {
			return s
		}
		return m[1] + strconv.FormatInt(t.Add(rt.by).UnixNano()/int64(unit), 10)
	})
}

// formatLike formats t as s, which parsed as at, is written: its fraction's digits and zone.
func formatLike(s string, at, t time.Time) string {
	layout := "2006-01-02T15:04:05"
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		end := dot + 1
		for end < len(s) && s[end] >= '0' && s[end] <= '9' {
			end++
		}
		layout += "." + strings.Repeat("0", end-dot-1)
	}
	if strings.HasSuffix(s, "Z") {
		return t.UTC().Format(layout + "Z")
	}
	_, offset := at.Zone()
	return t.In(time.FixedZone("", offset)).Format(layout + "-07:00")
}

// idToken is a UUID, or a 32- or 16-digit hex trace or span id, standing alone.
var idToken = regexp.MustCompile(`\b(?:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|[0-9a-fA-F]{32}|[0-9a-fA-F]{16})\b`)

// renameIDs replaces each id line names by copy k's, the same id always the same one; a token
// of digits alone is a number, as journald's microsecond times are, not a hex id.
func renameIDs(line string, k int) string {
	return idToken.ReplaceAllStringFunc(line, func(id string) string {
		if strings.Trim(id, "0123456789") == "" {
			return id
		}
		sum := sha256.Sum256([]byte(strconv.Itoa(k) + "/" + strings.ToLower(id)))
		digits := fmt.Sprintf("%x", sum[:])
		out := []byte(id)
		j := 0
		for i, b := range out {
			if b == '-' {
				continue
			}
			c := digits[j]
			if b >= 'A' && b <= 'F' && c >= 'a' {
				c -= 'a' - 'A'
			}
			out[i] = c
			j++
		}
		return string(out)
	})
}
