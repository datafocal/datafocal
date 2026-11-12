// Package logformat turns raw log lines into typed records, one declared body format per
// source.
package logformat

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/valyala/fastjson"

	"github.com/datafocal/datafocal/internal/pattern"
	"github.com/datafocal/datafocal/internal/producers"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

// Parser turns lines into records; a line that does not parse is kept whole as its message.
type Parser interface {
	Parse(line string) (segment.Record, bool) // false while every line read is held
	// ParseAll is every record line closes: a record per element of a batch its joined
	// body holds, as an OTLP export writes, or Parse's.
	ParseAll(line string) []segment.Record
	Flush() (segment.Record, bool) // the open record, if any
	Held() int                     // lines read that no returned record holds
}

// Config is a source's declared format: its envelope, if a container runtime wraps each
// line, the body format inside it, and the pattern of a line that starts a record.
type Config struct {
	Format      string `json:"format"`       // "auto", "plain" or a bodyFormats name
	Envelope    string `json:"envelope"`     // "" | "cri" | "docker"
	RecordStart string `json:"record_start"` // a regexp, empty means one line per record, but auto joins a stack trace's lines
}

// colDetail holds the lines of a multi-line record after its first, so the first alone
// gives the record its pattern.
const colDetail = "detail"

// maxRecordLines closes a record, so a stream with no start lines cannot grow one without limit.
const maxRecordLines = 1000

const formatAuto = "auto"

// Format is how a line becomes a record: a regexp's named groups, or a JSON object's or
// logfmt line's keys, are its columns, and Fields gives some of them a canonical meaning.
type Format struct {
	Name    string              `json:"name"`
	Match   string              `json:"match,omitempty"`   // a regexp whose named groups are the columns
	Syntax  string              `json:"syntax,omitempty"`  // "json" or "logfmt", for a format with no match
	Require string              `json:"require,omitempty"` // a substring the line must hold, checked first
	Fields  map[string][]string `json:"fields,omitempty"`  // a canonical name's source columns, the first present winning
	Levels  map[string]string   `json:"levels,omitempty"`  // a level column's own words, or a severity's number
	Drop    []string            `json:"drop,omitempty"`    // prefixes of the columns left out
	Layout  string              `json:"layout,omitempty"`  // a time layout read in the local zone, given a year when it has none
	Bytes   bool                `json:"bytes,omitempty"`   // a JSON list of bytes is the string they encode
	Params  string              `json:"params,omitempty"`  // a column of [id name="value"...] elements, each param a column id.name
	match   *regexp.Regexp
	sources map[string]bool // the column names Fields lists, set as formats load
}

// The canonical names besides segment's level, message, host and service, and the syntaxes.
const (
	fieldTime     = "time"
	fieldSeverity = "severity" // a number whose value mod 8 Levels names
	fieldBody     = "body"     // read again by auto, the record filling in what it lacks
	fieldPID      = "pid"
	syntaxJSON    = "json"
	syntaxLogfmt  = "logfmt"
)

// defaultFields are json's and logfmt's canonical columns.
var defaultFields = map[string][]string{
	fieldTime:          {segment.ColTimestamp, fieldTime, "timestamp", "@timestamp", "timeUnixNano", "observedTimeUnixNano", "startTimeUnixNano"},
	segment.ColMessage: {"msg", segment.ColMessage},
	segment.ColLevel:   {segment.ColLevel, "lvl", "severity", "log.level"},
}

var (
	jsonFormat   = &Format{Name: syntaxJSON, Syntax: syntaxJSON, Fields: defaultFields}
	logfmtFormat = &Format{Name: syntaxLogfmt, Syntax: syntaxLogfmt, Fields: defaultFields}
)

// formats are the declarable formats in the order auto tries them: those internal/producers
// declares with a match or a require, then json and logfmt; loaded at their first use, so a
// test's producer files count.
var formats = sync.OnceValue(loadFormats)

func loadFormats() []*Format {
	all := append(producers.Section[*Format]("formats"), jsonFormat, logfmtFormat)
	for _, f := range all {
		if f.Match != "" {
			f.match = regexp.MustCompile(f.Match)
		}
		f.sources = map[string]bool{}
		for _, sources := range f.Fields {
			for _, source := range sources {
				name, _, _ := strings.Cut(source, "~")
				f.sources[name] = true
			}
		}
	}
	loose := func(f *Format) int {
		if f.Match+f.Require == "" {
			return 1
		}
		return 0
	}
	slices.SortStableFunc(all, func(a, b *Format) int { return loose(a) - loose(b) })
	return all
}

// parser holds the start of a body the runtime split across lines until its last part,
// and the record a start line opened until the next start line.
type parser struct {
	envelope     string
	format       string
	declared     *Format        // nil for auto and plain
	recordStart  *regexp.Regexp // nil when every body starts a record, unless continuation
	continuation bool           // auto's default with no recordStart: a stack trace's later lines join its first
	partial      strings.Builder
	partialLines int
	open         segment.Record // a value, so holding a record costs no allocation
	detail       []string
	openLines    int // 0 when no record is open
}

// New returns the Parser for one file in cfg's format.
func New(cfg Config) (Parser, error) {
	names := []string{formatAuto, "plain"}
	var declared *Format
	for _, f := range formats() {
		names = append(names, f.Name)
		if f.Name == cfg.Format {
			declared = f
		}
	}
	if !slices.Contains(names, cfg.Format) {
		return nil, fmt.Errorf("unknown log format %q, not one of %s", cfg.Format, strings.Join(names, ", "))
	}
	if !slices.Contains([]string{"", "cri", "docker"}, cfg.Envelope) {
		return nil, fmt.Errorf("unknown log envelope %q", cfg.Envelope)
	}
	p := &parser{envelope: cfg.Envelope, format: cfg.Format, declared: declared, continuation: cfg.Format == formatAuto}
	if cfg.RecordStart != "" {
		re, err := regexp.Compile(cfg.RecordStart)
		if err != nil {
			return nil, fmt.Errorf("record start: %w", err)
		}
		p.recordStart = re
	}
	return p, nil
}

// Parse returns a body before any start line as its own record, and the open record when
// the next start line or its line limit closes it.
func (p *parser) Parse(line string) (segment.Record, bool) {
	ts, body, lines, ok := p.join(line)
	if !ok {
		return segment.Record{}, false
	}
	return p.parseJoined(ts, body, lines)
}

func (p *parser) ParseAll(line string) []segment.Record {
	ts, body, lines, ok := p.join(line)
	if !ok {
		return nil
	}
	parts := expand(body)
	if len(parts) == 0 {
		if r, ok := p.parseJoined(ts, body, lines); ok {
			return []segment.Record{r}
		}
		return nil
	}
	var rows []segment.Record
	if r, ok := p.Flush(); ok {
		rows = append(rows, r)
	}
	for _, part := range parts {
		if r, ok := p.parseJoined(ts, part, 1); ok {
			rows = append(rows, r)
		}
		if r, ok := p.Flush(); ok {
			rows = append(rows, r)
		}
	}
	return rows
}

// parseJoined is Parse after its envelope's parts are joined into body.
func (p *parser) parseJoined(ts time.Time, body string, lines int) (segment.Record, bool) {
	start := p.starts(body)
	switch {
	case !start && p.openLines == 0, p.recordStart == nil && !p.continuation:
		return underlay(p.parseBody(body), segment.Record{Timestamp: ts}), true
	case start:
		r, closed := p.Flush()
		p.open, p.openLines = underlay(p.parseBody(body), segment.Record{Timestamp: ts}), lines
		return r, closed
	}
	p.detail = append(p.detail, body)
	p.openLines += lines
	if p.openLines >= maxRecordLines {
		return p.Flush()
	}
	return segment.Record{}, false
}

func (p *parser) starts(body string) bool {
	if p.recordStart != nil {
		return p.recordStart.MatchString(body)
	}
	return !p.continues(body)
}

// continues reports whether body is a later line of a stack trace or error chain: a frame,
// a cause, a blank line, or an indented line after one of them.
func (p *parser) continues(body string) bool {
	trimmed := strings.TrimLeft(body, " \t")
	indented := len(trimmed) < len(body)
	switch {
	case trimmed == "", indented && p.detail != nil,
		indented && (strings.HasPrefix(trimmed, "at ") || strings.HasPrefix(trimmed, `File "`)),
		strings.HasPrefix(trimmed, "... ") && strings.HasSuffix(trimmed, " more"):
		return true
	}
	return strings.HasPrefix(trimmed, "Caused by:") || strings.HasPrefix(trimmed, "Traceback (most recent call last):")
}

func (p *parser) Flush() (segment.Record, bool) {
	if p.openLines == 0 {
		return segment.Record{}, false
	}
	r := p.open
	if len(p.detail) > 0 {
		r.Columns = append(r.Columns, segment.Column{Name: colDetail, Type: value.TypeString, Value: value.Value{String: strings.Join(p.detail, "\n")}})
	}
	p.open, p.detail, p.openLines = segment.Record{}, nil, 0
	return r, true
}

func (p *parser) Held() int { return p.partialLines + p.openLines }

// join adds line to the parts of a body its envelope split, reporting ok false until the
// last part, and how many lines the body took; a line not in the envelope is a body whole.
func (p *parser) join(line string) (ts time.Time, body string, lines int, ok bool) {
	ts, part, final, ok := unwrap(p.envelope, line)
	if !ok {
		return time.Time{}, line, 1, true
	}
	p.partial.WriteString(part)
	p.partialLines++
	if !final {
		return time.Time{}, "", 0, false
	}
	body, lines = p.partial.String(), p.partialLines
	p.partial.Reset()
	p.partialLines = 0
	return ts, body, lines, true
}

// parseBody reads body in the declared format, or in the first format auto finds.
func (p *parser) parseBody(body string) segment.Record {
	if p.format == formatAuto {
		return parseAuto(body)
	}
	if p.declared != nil {
		if r, ok := p.declared.apply(body); ok {
			return r
		}
	}
	return segment.LogRecord(time.Time{}, "", body, nil)
}

// parseAuto reads body in the first format that reads it, else as text.
func parseAuto(body string) segment.Record {
	for _, f := range formats() {
		if r, ok := f.apply(body); ok {
			return r
		}
	}
	if r, ok := parseText(body); ok {
		return r
	}
	return segment.LogRecord(time.Time{}, "", body, nil)
}

// apply reports false for a line not in f; a body column is read again by auto, the rest
// of the record filling in what the body does not state.
func (f *Format) apply(line string) (segment.Record, bool) {
	if !strings.Contains(line, f.Require) {
		return segment.Record{}, false
	}
	cols, ok := f.columns(line)
	if !ok {
		return segment.Record{}, false
	}
	r, body, hasBody := f.record(cols)
	if hasBody {
		return underlay(parseAuto(body), r), true
	}
	return r, true
}

// columns are line's named groups in f's match, or its keys in f's syntax.
func (f *Format) columns(line string) ([]segment.Column, bool) {
	switch {
	case f.match != nil:
		return f.groups(line)
	case f.Syntax == syntaxLogfmt:
		return parsePairs(line)
	case f.Syntax == syntaxJSON && strings.HasPrefix(strings.TrimSpace(line), "{"):
		if !f.Bytes && !escapesSurrogate(line) {
			return flattenLine(line)
		}
		fields, ok := decodeJSON(line)
		for k, v := range fields {
			if f.Bytes {
				fields[k] = byteStrings(v)
			}
		}
		return flattenJSON(make([]segment.Column, 0, len(fields)), "", fields), ok
	}
	return nil, false
}

func (f *Format) groups(line string) ([]segment.Column, bool) {
	m := f.match.FindStringSubmatchIndex(line)
	if m == nil {
		return nil, false
	}
	cols := make([]segment.Column, 0, len(m)/2)
	for i, name := range f.match.SubexpNames() {
		if name != "" && m[2*i] >= 0 {
			cols = append(cols, stringColumn(name, line[m[2*i]:m[2*i+1]]))
		}
	}
	return cols, true
}

// record gives each column that supplies a canonical name its meaning, in its place, and
// leaves out those f drops.
func (f *Format) record(cols []segment.Column) (r segment.Record, body string, hasBody bool) {
	r.Columns = make([]segment.Column, 0, len(cols))
	zoned := false
	for _, col := range cols {
		name, suffix, ok := f.canonical(col, cols)
		keep := !ok && !slices.ContainsFunc(f.Drop, func(prefix string) bool { return strings.HasPrefix(col.Name, prefix) })
		switch {
		case f.Params != "" && col.Name == f.Params:
			r.Columns, keep = append(r.Columns, params(col.Value.String)...), false
		case !ok:
		case name == fieldTime:
			zoned, keep = f.setTime(&r, col, zoned)
		case name == fieldBody:
			body, hasBody = strings.TrimSuffix(value.Format(col.Type, col.Value), suffix), true
		default:
			r.Columns = f.appendCanonical(r.Columns, name, strings.TrimSuffix(value.Format(col.Type, col.Value), suffix))
		}
		if keep {
			r.Columns = append(r.Columns, col)
		}
	}
	spanEnd(&r, cols)
	return r, body, hasBody
}

// spanEnd gives a span holding both its ends in unix nanoseconds, as OTLP's do, its end as
// its time, which get_trace reads a line's as, and its length as its duration when it
// names none.
func spanEnd(r *segment.Record, cols []segment.Column) {
	start, end := unixNanos(cols, "startTimeUnixNano"), unixNanos(cols, "endTimeUnixNano")
	if start <= 0 || end < start {
		return
	}
	r.Timestamp = time.Unix(0, end).UTC()
	if !slices.ContainsFunc(r.Columns, func(c segment.Column) bool { return c.Name == segment.ColDurationMs }) {
		ms := float64(end-start) / float64(time.Millisecond)
		r.Columns = append(r.Columns, segment.Column{Name: segment.ColDurationMs, Type: value.TypeFloat, Value: value.Value{Float: ms}})
	}
}

// unixNanos is the integer a column of cols named name holds, 0 for none.
func unixNanos(cols []segment.Column, name string) int64 {
	i := slices.IndexFunc(cols, func(c segment.Column) bool { return c.Name == name })
	if i < 0 {
		return 0
	}
	n, _ := strconv.ParseInt(value.Format(cols[i].Type, cols[i].Value), 10, 64)
	return n
}

// setTime gives r col's time unless r's is zoned and col's is not, so a zoned time wins
// over one without, else the later; a col that is no time is kept.
func (f *Format) setTime(r *segment.Record, col segment.Column, zoned bool) (nowZoned, keep bool) {
	ts, colZoned, ok := f.time(col)
	if !ok {
		return zoned, true
	}
	if colZoned || !zoned {
		r.Timestamp, zoned = ts, colZoned
	}
	return zoned, false
}

// canonicalNames are the names record gives a meaning, which a match's group of the same
// name supplies unless Fields lists the name's sources.
var canonicalNames = []string{fieldTime, segment.ColLevel, fieldSeverity, segment.ColMessage, fieldBody, segment.ColHost, segment.ColService, fieldPID}

// canonical is the canonical name col supplies and the suffix its source strips; col
// supplies it unless a source listed before its own is present, except a time.
func (f *Format) canonical(col segment.Column, cols []segment.Column) (name, suffix string, ok bool) {
	fields := f.Fields
	if f.sources != nil && !f.sources[col.Name] {
		fields = nil
	}
	for name, sources := range fields {
		for i, source := range sources {
			if suffix, ok := supplies(col, source); ok && (name == fieldTime || !present(cols, sources[:i])) {
				return name, suffix, true
			}
		}
	}
	_, listed := f.Fields[col.Name]
	return col.Name, "", f.match != nil && !listed && slices.Contains(canonicalNames, col.Name)
}

// supplies reports whether col is source, a name or name~suffix whose value ends in suffix.
func supplies(col segment.Column, source string) (suffix string, ok bool) {
	name, suffix, _ := strings.Cut(source, "~")
	return suffix, col.Name == name && strings.HasSuffix(col.Value.String, suffix)
}

func present(cols []segment.Column, sources []string) bool {
	for _, source := range sources {
		for _, col := range cols {
			if _, ok := supplies(col, source); ok {
				return true
			}
		}
	}
	return false
}

// appendCanonical adds the column a canonical name gives val, none for a pid that is not a
// number or a severity whose number Levels does not name.
func (f *Format) appendCanonical(cols []segment.Column, name, val string) []segment.Column {
	switch name {
	case segment.ColLevel:
		if level, ok := f.Levels[val]; ok {
			return append(cols, stringColumn(segment.ColLevel, level))
		}
		return append(cols, stringColumn(segment.ColLevel, fieldLevel(val)))
	case fieldSeverity:
		if n, err := strconv.Atoi(val); err == nil && n >= 0 {
			val = strconv.Itoa(n % 8)
		}
		if level, ok := f.Levels[val]; ok {
			return append(cols, stringColumn(segment.ColLevel, level))
		}
		return cols
	case fieldPID:
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			return append(cols, segment.Column{Name: name, Type: value.TypeInt, Value: value.Value{Int: n}})
		}
		return cols
	}
	return append(cols, stringColumn(name, val))
}

// time reads col in f's layout, then as columnTime does.
func (f *Format) time(col segment.Column) (ts time.Time, zoned, ok bool) {
	if f.Layout == "" {
		return columnTime(col)
	}
	ts, err := time.ParseInLocation(f.Layout, col.Value.String, time.Local)
	if err != nil {
		return columnTime(col)
	}
	if ts.Year() == 0 {
		ts = withYear(ts)
	}
	return ts, true, true
}

// underlay fills in from under what r does not state: its time, and each column r lacks.
func underlay(r, under segment.Record) segment.Record {
	if r.Timestamp.IsZero() {
		r.Timestamp = under.Timestamp
	}
	r.Columns = slices.Grow(r.Columns, len(under.Columns))
	for _, col := range under.Columns {
		if !slices.ContainsFunc(r.Columns, func(c segment.Column) bool { return c.Name == col.Name }) {
			r.Columns = append(r.Columns, col)
		}
	}
	return r
}

// unwrap strips line's envelope, reporting final false when the next line continues the
// body and ok false when line is not in the envelope.
func unwrap(envelope, line string) (ts time.Time, body string, final, ok bool) {
	switch envelope {
	case "cri":
		return unwrapCRI(line)
	case "docker":
		return unwrapDocker(line)
	}
	return time.Time{}, line, true, true
}

// unwrapCRI reads "<RFC3339Nano time> <stream> <P|F> <body>", where P marks a partial line.
func unwrapCRI(line string) (ts time.Time, body string, final, ok bool) {
	fields := strings.SplitN(line, " ", 4)
	if len(fields) < 4 || (fields[2] != "P" && fields[2] != "F") {
		return time.Time{}, "", false, false
	}
	ts, err := time.Parse(time.RFC3339Nano, fields[0])
	if err != nil {
		return time.Time{}, "", false, false
	}
	return ts, fields[3], fields[2] == "F", true
}

// unwrapDocker reads docker's json-file line, whose log field ends in a newline unless
// docker split it.
func unwrapDocker(line string) (ts time.Time, body string, final, ok bool) {
	var entry struct {
		Log  *string   `json:"log"`
		Time time.Time `json:"time"`
	}
	if err := json.Unmarshal([]byte(line), &entry); err != nil || entry.Log == nil {
		return time.Time{}, "", false, false
	}
	body, final = strings.CutSuffix(*entry.Log, "\n")
	return entry.Time, body, final, true
}

// jsonParsers reuses fastjson's parsers, whose values last only until the parser's next
// Parse, which jsonTree is done with before it returns.
var jsonParsers fastjson.ParserPool

// decodeJSON leaves a line escaping a UTF-16 surrogate to encoding/json, since fastjson
// keeps a lone one's escape as text where encoding/json makes it U+FFFD.
func decodeJSON(line string) (map[string]any, bool) {
	if escapesSurrogate(line) {
		return decodeJSONStd(line)
	}
	if fastjson.Validate(line) != nil { // strict, where Parse alone takes some invalid lines
		return nil, false
	}
	p := jsonParsers.Get()
	defer jsonParsers.Put(p)
	v, err := p.Parse(line)
	if err != nil || v.Type() != fastjson.TypeObject {
		return nil, false
	}
	fields, _ := jsonTree(v).(map[string]any) // an object's tree is a map
	return fields, true
}

func decodeJSONStd(line string) (map[string]any, bool) {
	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber() // tells 120 from 120.5
	var fields map[string]any
	if err := dec.Decode(&fields); err != nil || fields == nil {
		return nil, false
	}
	if _, err := dec.Token(); err != io.EOF { // more than one value on the line
		return nil, false
	}
	return fields, true
}

// escapesSurrogate reports whether line holds a \u escape in D800-DFFF; an escaped
// backslash before "ud8" counts too, which only sends the line the slower way.
func escapesSurrogate(line string) bool {
	for i := strings.Index(line, `\u`); i >= 0 && i+3 < len(line); i = strings.Index(line, `\u`) {
		if (line[i+2] == 'd' || line[i+2] == 'D') && strings.ContainsRune("89abcdefABCDEF", rune(line[i+3])) {
			return true
		}
		line = line[i+2:]
	}
	return false
}

// jsonTree is v as encoding/json decodes it with UseNumber: a later key replacing an
// earlier one, and each invalid UTF-8 byte in a string replaced by U+FFFD.
func jsonTree(v *fastjson.Value) any {
	switch v.Type() {
	case fastjson.TypeObject:
		obj := v.GetObject()
		fields := make(map[string]any, obj.Len())
		obj.Visit(func(key []byte, v *fastjson.Value) { fields[validUTF8(key)] = jsonTree(v) })
		return fields
	case fastjson.TypeArray:
		elems := v.GetArray()
		out := make([]any, len(elems))
		for i, elem := range elems {
			out[i] = jsonTree(elem)
		}
		return out
	case fastjson.TypeString:
		return validUTF8(v.GetStringBytes())
	case fastjson.TypeNumber:
		return json.Number(v.MarshalTo(nil)) // a number marshals as the text it was parsed from
	case fastjson.TypeTrue:
		return true
	case fastjson.TypeFalse:
		return false
	}
	return nil
}

// validUTF8 is b as a string with each byte that is not valid UTF-8 replaced by U+FFFD,
// as encoding/json does.
func validUTF8(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	var out strings.Builder
	for len(b) > 0 {
		r, size := utf8.DecodeRune(b)
		out.WriteRune(r) // utf8.RuneError for an invalid byte, which is U+FFFD
		b = b[size:]
	}
	return out.String()
}

// flattenLine is decodeJSON then flattenJSON, sparing the maps jsonTree would build.
func flattenLine(line string) ([]segment.Column, bool) {
	if fastjson.Validate(line) != nil {
		return nil, false
	}
	p := jsonParsers.Get()
	defer jsonParsers.Put(p)
	v, err := p.Parse(line)
	if err != nil || v.Type() != fastjson.TypeObject {
		return nil, false
	}
	return flattenObject(make([]segment.Column, 0, v.GetObject().Len()), "", v.GetObject()), true
}

// flattenObject is flattenJSON over obj as jsonTree would decode it.
func flattenObject(cols []segment.Column, prefix string, obj *fastjson.Object) []segment.Column {
	type field struct {
		key string
		v   *fastjson.Value
	}
	fields := make([]field, 0, obj.Len())
	obj.Visit(func(key []byte, v *fastjson.Value) { fields = append(fields, field{validUTF8(key), v}) })
	slices.SortStableFunc(fields, func(a, b field) int { return strings.Compare(a.key, b.key) })
	for i, f := range fields {
		if i+1 < len(fields) && fields[i+1].key == f.key {
			continue // a later key replaces an earlier one
		}
		switch name := prefix + f.key; f.v.Type() {
		case fastjson.TypeNull:
		case fastjson.TypeObject:
			cols = flattenObject(cols, name+".", f.v.GetObject())
		case fastjson.TypeArray:
			cols = append(cols, jsonList(name, jsonTree(f.v).([]any))...)
		case fastjson.TypeString:
			cols = append(cols, segment.Column{Name: name, Type: value.TypeString, Value: value.Value{String: validUTF8(f.v.GetStringBytes())}})
		case fastjson.TypeNumber:
			var text [32]byte
			typ, val := jsonNumber(json.Number(f.v.MarshalTo(text[:0])))
			cols = append(cols, segment.Column{Name: name, Type: typ, Value: val})
		default:
			cols = append(cols, segment.Column{Name: name, Type: value.TypeBool, Value: value.Value{Bool: f.v.Type() == fastjson.TypeTrue}})
		}
	}
	return cols
}

// flattenJSON visits keys sorted, so which of two keys naming one field wins is deterministic.
func flattenJSON(cols []segment.Column, prefix string, fields map[string]any) []segment.Column {
	for _, k := range slices.Sorted(maps.Keys(fields)) {
		name := prefix + k
		switch v := fields[k].(type) {
		case nil:
		case map[string]any:
			cols = flattenJSON(cols, name+".", v)
		case []any:
			cols = append(cols, jsonList(name, v)...)
		default:
			typ, val := jsonScalar(v)
			cols = append(cols, segment.Column{Name: name, Type: typ, Value: val})
		}
	}
	return cols
}

// jsonList keeps a list's scalar elements as its own column and gives each key path in
// its object elements a column aligned with them: element i is object i's value, or ""
// where object i has none.
func jsonList(name string, elems []any) []segment.Column {
	if fields := jsonAttributes(elems); len(fields) > 0 && len(fields) == len(elems) {
		return flattenJSON(nil, "", fields)
	}
	var scalars []any
	keyed := map[string][]any{}
	objects := 0
	for _, elem := range elems {
		if obj, ok := elem.(map[string]any); ok {
			collectKeys(keyed, name+".", obj, objects)
			objects++
		} else {
			scalars = append(scalars, elem)
		}
	}
	var cols []segment.Column
	if len(scalars) > 0 || len(keyed) == 0 {
		cols = append(cols, listColumn(name, scalars))
	}
	for _, k := range slices.Sorted(maps.Keys(keyed)) {
		cols = append(cols, listColumn(k, padTo(keyed[k], objects)))
	}
	return cols
}

func jsonAttributes(elems []any) map[string]any {
	var fields map[string]any
	for _, elem := range elems {
		obj, ok := elem.(map[string]any)
		if !ok {
			return nil
		}
		key, ok := obj["key"].(string)
		if !ok || obj["value"] == nil {
			return nil
		}
		if fields == nil {
			fields = make(map[string]any, len(elems))
		}
		fields[key] = attributeValue(obj["value"])
	}
	return fields
}

func attributeValue(v any) any {
	if typed, ok := v.(map[string]any); ok && len(typed) == 1 {
		for kind, scalar := range typed {
			if kind == "intValue" {
				return json.Number(fmt.Sprint(scalar))
			}
			return scalar
		}
	}
	return v
}

// collectKeys sets element i of each key path's list to its value in obj; a nested list
// is a value.
func collectKeys(keyed map[string][]any, prefix string, obj map[string]any, i int) {
	for k, v := range obj {
		switch v := v.(type) {
		case nil:
		case map[string]any:
			collectKeys(keyed, prefix+k+".", v, i)
		default:
			keyed[prefix+k] = append(padTo(keyed[prefix+k], i), v)
		}
	}
}

// padTo extends list to n elements with "", the value of an object missing the key.
func padTo(list []any, n int) []any {
	for len(list) < n {
		list = append(list, "")
	}
	return list
}

// listColumn types a list by the promotion of its elements' types, a string list when empty.
func listColumn(name string, elems []any) segment.Column {
	types := make([]value.Type, len(elems))
	vals := make([]value.Value, len(elems))
	typ := value.Type(0)
	for i, elem := range elems {
		types[i], vals[i] = jsonScalar(elem)
		typ = value.Promote(typ, types[i])
	}
	typ = cmp.Or(typ, value.TypeString)
	for i := range vals {
		vals[i] = value.Coerce(types[i], vals[i], typ)
	}
	return segment.Column{Name: name, Type: typ | value.TypeRepeated, Value: value.Value{List: vals}}
}

// jsonScalar keeps a list, or an object or null inside a list, as its JSON.
func jsonScalar(v any) (value.Type, value.Value) {
	switch v := v.(type) {
	case string:
		return value.TypeString, value.Value{String: v}
	case bool:
		return value.TypeBool, value.Value{Bool: v}
	case json.Number:
		return jsonNumber(v)
	}
	text, _ := json.Marshal(v) // v came from Decode, so it marshals
	return value.TypeJSON, value.JSON(text)
}

func jsonNumber(n json.Number) (value.Type, value.Value) {
	if i, err := n.Int64(); err == nil {
		return value.TypeInt, value.Value{Int: i}
	}
	if f, err := n.Float64(); err == nil {
		return value.TypeFloat, value.Value{Float: f}
	}
	return value.TypeString, value.Value{String: n.String()} // out of float range
}

func parsePairs(line string) ([]segment.Column, bool) {
	var cols []segment.Column
	for rest := strings.TrimSpace(line); rest != ""; rest = strings.TrimLeftFunc(rest, unicode.IsSpace) {
		key, val, ok := strings.Cut(rest, "=")
		if !ok || key == "" || strings.ContainsFunc(key, func(r rune) bool { return r == '"' || unicode.IsSpace(r) }) {
			return nil, false
		}
		if strings.HasPrefix(val, `"`) {
			quoted, err := strconv.QuotedPrefix(val)
			if err != nil {
				return nil, false
			}
			rest = val[len(quoted):]
			val, _ = strconv.Unquote(quoted) // QuotedPrefix accepted it
		} else if n := strings.IndexFunc(val, unicode.IsSpace); n >= 0 {
			val, rest = val[:n], val[n:]
		} else {
			rest = ""
		}
		typ, v := pattern.InferTypedValue(val)
		cols = append(cols, segment.Column{Name: key, Type: typ, Value: v})
	}
	return cols, len(cols) > 0
}

const (
	levelDebug = "debug"
	levelInfo  = "info"
	levelWarn  = "warn"
	levelError = "error"
	levelFatal = "fatal"
)

// columnTime reads a number as seconds, milliseconds, microseconds or nanoseconds since
// 1970, whichever puts it between 2001 and 2255.
func columnTime(col segment.Column) (ts time.Time, zoned, ok bool) {
	n := float64(col.Value.Int)
	switch col.Type {
	case value.TypeString:
		if strings.Trim(col.Value.String, "0123456789") != "" {
			return parseTime(col.Value.String)
		}
		n, _ := strconv.ParseInt(col.Value.String, 10, 64) // digits, so only overflow fails, which no unit puts in range
		return columnTime(segment.Column{Type: value.TypeInt, Value: value.Value{Int: n}})
	case value.TypeFloat:
		n = col.Value.Float
	case value.TypeInt:
	default:
		return time.Time{}, false, false
	}
	for _, unit := range []float64{1e9, 1e6, 1e3, 1} {
		if s := n * unit / 1e9; s >= 1e9 && s < 9e9 {
			if col.Type == value.TypeInt {
				return time.Unix(0, col.Value.Int*int64(unit)).UTC(), true, true
			}
			return time.Unix(0, int64(n*unit)).UTC(), true, true
		}
	}
	return time.Time{}, false, false
}

// fieldLevel also knows npm's levels, which only a level field holds: as a text line's
// first word, http is more often net/http's prefix.
func fieldLevel(word string) string {
	switch strings.ToLower(word) {
	case "http", "verbose", "silly":
		return levelDebug
	}
	return normalizeLevel(word)
}

func normalizeLevel(word string) string {
	switch strings.ToLower(strings.Trim(word, "[]:")) {
	case "trace", levelDebug, "dbg":
		return levelDebug
	case levelInfo, "information", "notice", "log":
		return levelInfo
	case levelWarn, "warning":
		return levelWarn
	case levelError, "err":
		return levelError
	case levelFatal, "panic", "critical", "crit", "alert", "emerg":
		return levelFatal
	}
	return word
}

// parseTime reports zoned false for a time that names no zone, which it reads as UTC.
func parseTime(s string) (ts time.Time, zoned, ok bool) {
	if s == "" || s[0] < '0' || s[0] > '9' { // every layout opens with the year, so text costs no failed parse
		return time.Time{}, false, false
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05Z07:00", "2006-01-02 15:04:05 UTC", "2006/01/02 15:04:05Z07:00", "2006/01/02 15:04:05 UTC",
	} {
		if ts, err := time.Parse(layout, s); err == nil {
			return ts, true, true
		}
	}
	for _, layout := range []string{"2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006/01/02 15:04:05"} {
		if ts, err := time.Parse(layout, s); err == nil {
			return ts, false, true
		}
	}
	return time.Time{}, false, false
}

// parseText reads a header of a time, bracketed or ending in a colon, and a level; a JSON
// object after the time is the record, its own time winning over the header's.
func parseText(body string) (segment.Record, bool) {
	tokens := strings.FieldsFunc(body, func(r rune) bool { return r == ' ' || r == '\t' })
	var ts time.Time
	start := 0
	opening := ""
	if len(tokens) > 0 {
		opening = strings.TrimLeft(tokens[0], "[]:")
	}
	// A time opens with a digit, so a line opening with a word joins no tokens.
	for n := min(3, len(tokens)); n > 0 && strings.IndexAny(opening, "0123456789") == 0; n-- {
		if parsed, _, ok := parseTime(strings.Trim(strings.Join(tokens[:n], " "), "[]:")); ok {
			ts, start = parsed, n
			break
		}
	}
	if start > 0 {
		if r, ok := jsonFormat.apply(textAfter(body, tokens[:start])); ok {
			r.Timestamp = cmp.Or(r.Timestamp, ts)
			return r, true
		}
	}
	level := textLevel(tokens, start, !ts.IsZero())
	if ts.IsZero() && level == "" {
		return segment.Record{}, false
	}
	end := len(tokens)
	var cols []segment.Column
	rest := body
	for i, token := range tokens {
		rest = strings.TrimLeft(rest, " \t")
		if pairs, ok := parsePairs(rest); ok {
			cols, end = pairs, i
			break
		}
		rest = rest[len(token):]
	}
	// The level word stays in the message, so a line filter for it still finds the line.
	r := segment.LogRecord(ts, level, strings.Join(tokens[start:end], " "), nil)
	r.Columns = append(r.Columns, cols...)
	return r, true
}

// textAfter is body past tokens, its first words.
func textAfter(body string, tokens []string) string {
	for _, token := range tokens {
		body = strings.TrimLeft(body, " \t")[len(token):]
	}
	return body
}

func textLevel(tokens []string, start int, hasTime bool) string {
	index := start
	if index < len(tokens) && bracketed(tokens[index]) && normalizeLevel(tokens[index]) == tokens[index] {
		index++
	}
	if index < len(tokens) {
		word := tokens[index]
		if head, _, ok := strings.Cut(word, ":"); ok && head == strings.ToUpper(head) {
			word = head + ":" // Python's LEVEL:logger:message
		}
		level := normalizeLevel(word)
		if slices.Contains([]string{levelDebug, levelInfo, levelWarn, levelError, levelFatal}, level) &&
			(hasTime || word == strings.ToUpper(word) || strings.HasSuffix(word, ":") || bracketed(word)) {
			return level
		}
	}
	return ""
}

func bracketed(word string) bool {
	return strings.HasPrefix(word, "[") && strings.HasSuffix(word, "]")
}

// now is the collector's clock, which gives a time with no year its year.
var now = time.Now

// byteStrings is v with a list of bytes, which a writer may send for a value that is not
// UTF-8, decoded as the string they encode, and so each element of any other list.
func byteStrings(v any) any {
	elems, ok := v.([]any)
	if !ok {
		return v
	}
	b := make([]byte, 0, len(elems))
	for _, elem := range elems {
		n, _ := elem.(json.Number)
		c, err := strconv.ParseUint(string(n), 10, 8)
		if err != nil {
			break
		}
		b = append(b, byte(c))
	}
	if len(b) == len(elems) {
		return validUTF8(b)
	}
	for i, elem := range elems {
		elems[i] = byteStrings(elem)
	}
	return elems
}

// withYear gives a time that states no year the latest year that puts it no
// more than a day after now, so a Dec 31 line read on Jan 1 gets last year.
func withYear(ts time.Time) time.Time {
	limit := now().Add(24 * time.Hour)
	for year := limit.Year(); ; year-- {
		t := time.Date(year, ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), 0, time.Local)
		if !t.After(limit) {
			return t
		}
	}
}

// param is an element's id, or one of its params.
var param = regexp.MustCompile(`\[([^ \]]+)|([^ =\]"]+)="((?:[^"\\]|\\.)*)"`)

// paramEscape undoes the escapes a param value may hold.
var paramEscape = strings.NewReplacer(`\"`, `"`, `\\`, `\`, `\]`, `]`)

// params is a column <id>.<name> for each param of [id name="value"...] elements.
func params(elements string) []segment.Column {
	var cols []segment.Column
	id := ""
	for _, m := range param.FindAllStringSubmatch(elements, -1) {
		if m[1] != "" {
			id = m[1]
			continue
		}
		cols = append(cols, stringColumn(id+"."+m[2], paramEscape.Replace(m[3])))
	}
	return cols
}

func stringColumn(name, s string) segment.Column {
	return segment.Column{Name: name, Type: value.TypeString, Value: value.Value{String: s}}
}

var batchList = regexp.MustCompile(`(?i)"[^"\n]*(records|events|entries|messages|spans)"\s*:\s*\[`)

// expand splits record batches while retaining their envelope fields.
func expand(line string) []string {
	if !strings.Contains(line, "[") || (!strings.HasPrefix(strings.TrimSpace(line), "[") && !batchList.MatchString(line)) {
		return nil
	}
	if fastjson.Validate(line) != nil {
		return nil
	}
	p := jsonParsers.Get()
	defer jsonParsers.Put(p)
	v, _ := p.Parse(line)
	var out []string
	for _, row := range expandJSON(jsonTree(v)) {
		data, _ := json.Marshal(row)
		out = append(out, string(data))
	}
	return out
}

func expandJSON(v any) []map[string]any {
	switch v := v.(type) {
	case []any:
		return expandArray(v)
	case map[string]any:
		if timestamped(v) {
			return nil
		}
		for _, key := range slices.Sorted(maps.Keys(v)) {
			if parts := expandJSON(v[key]); len(parts) > 0 {
				for i, part := range parts {
					row := maps.Clone(v)
					delete(row, key)
					maps.Copy(row, part)
					parts[i] = row
				}
				return parts
			}
		}
	}
	return nil
}

func expandArray(v []any) []map[string]any {
	var out []map[string]any
	for _, elem := range v {
		obj, ok := elem.(map[string]any)
		if !ok {
			return nil
		}
		if timestamped(obj) {
			out = append(out, obj)
			continue
		}
		parts := expandJSON(obj)
		if len(parts) == 0 {
			return nil
		}
		out = append(out, parts...)
	}
	return out
}

func timestamped(obj map[string]any) bool {
	dated := false
	for _, key := range defaultFields[fieldTime] {
		if _, ok := obj[key]; ok {
			dated = true
			break
		}
	}
	if !dated {
		return false
	}
	for _, key := range []string{"body", "message", "attributes", "data", "content", "text", "msg"} {
		if _, ok := obj[key]; ok {
			return true
		}
	}
	return false
}
