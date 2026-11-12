package carry

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/datafocal/datafocal/internal/llm"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

const (
	roleTool      = "tool"
	fieldType     = "type"
	fieldID       = "id"
	fieldModel    = "model"
	fieldName     = "name"
	fieldToolName = "tool_name"
	fieldSkill    = "skill"
	fieldMemoryOp = "memory_op"
	fieldPath     = "path"
	dirMemories   = "memories"
)

var agentName = strings.NewReplacer("_", "", ".", "", "-", "")
var skillTag = regexp.MustCompile(`^\s*<skill\s+[^>]*name=["']([^"']+)["']`)
var runName = regexp.MustCompile(`(session|conversation|run|trace)id$`)
var toolDefinitions = regexp.MustCompile(`(^|\.)tools(\.|$)|tool_definitions`)
var toolName = regexp.MustCompile(`(tool|function)(calls?|requests?)?name$|^(tool|function)$`)

type agentField struct {
	alias, kind                                                   string
	role, typ, definitions, call, interesting, id, context, named bool
	compaction                                                    compactionField
}

// compactionField is what a field says of a compaction: the event it is, whether it
// succeeded, or the context's size before or after it.
type compactionField int8

const (
	notCompaction compactionField = iota
	compactionEvent
	compactionSuccess
	compactionBefore
	compactionAfter
)

func (c *carrier) field(name string) agentField {
	if f, ok := c.names[name]; ok {
		return f
	}
	f := classifyAgent(name)
	lower := strings.ToLower(name)
	f.interesting = f.kind != "" || slices.ContainsFunc([]string{fieldModel, "token", "usage", "cost", roleTool, "function", "call", "role", fieldSkill, "session", "content", "arguments", "args", "gen_ai"}, func(clue string) bool { return strings.Contains(lower, clue) })
	if len(c.names) == maxKeys {
		clear(c.names)
	}
	c.names[name] = f
	return f
}

func classifyAgent(name string) agentField {
	lower := strings.ToLower(name)
	key := agentName.Replace(lower)
	f := agentField{kind: contentKind(name), definitions: toolDefinitions.MatchString(name)}
	switch {
	case strings.HasSuffix(key, "callid"), strings.HasSuffix(key, "tooluseid"):
		f.alias = "tool_call_id"
	case runName.MatchString(key):
		f.alias = "run_id"
	case strings.HasSuffix(key, "role"):
		f.role = true
	case strings.HasSuffix(key, fieldType):
		f.typ = true
	case toolName.MatchString(key):
		f.alias = fieldToolName
	case strings.HasSuffix(key, "skillname"):
		f.alias = "skill_name"
	}
	f.call = strings.HasSuffix(name, ".name") || name == fieldName || toolName.MatchString(key)
	// Context is a field naming the model or the prompt, not one about them, such as a tool
	// schema's model.description, which carried would copy itself onto a session's every line.
	f.context = slices.ContainsFunc([]string{fieldModel, "modelid", "promptid"}, func(end string) bool { return strings.HasSuffix(key, end) })
	f.id = strings.HasSuffix(lower, fieldID)
	f.named = strings.HasSuffix(lower, ".name")
	f.compaction = compactionOf(key)
	return f
}

func (c *carrier) agentFields(r segment.Record, state *memory) segment.Record {
	var role string
	tools := sha256.New()
	definitions, messages, calls := false, false, false
	var runs []segment.Column
	evidence := false // a field naming a model, a prompt or a tool call, which a journal holds none of
	for _, col := range r.Columns {
		var candidate string
		f := c.field(col.Name)
		messages = messages || f.kind != ""
		calls = calls || f.call
		evidence = evidence || f.context || f.alias == "tool_call_id" || f.alias == fieldToolName
		if f.alias == "run_id" {
			runs = append(runs, col)
			continue
		}
		r, candidate = agentColumn(r, col, f)
		if candidate != "" {
			role = candidate
		}
		if f.definitions {
			definitions = true
			_, _ = tools.Write([]byte(col.Name + "=" + value.Format(col.Type, col.Value) + "\n"))
		}
	}
	if definitions {
		r = textField(r, "tools_hash", hex.EncodeToString(tools.Sum(nil)))
	}
	_, usage := column(r, "input_tokens")
	state.agent = state.agent || role != "" || messages || calls || usage || evidence || slices.ContainsFunc(r.Columns, sessionStart)
	// A journal's login session or a build's trace id is no agent's run, nor carried as one.
	if !state.agent {
		return c.agentContent(r, role, messages, calls)
	}
	for _, col := range runs {
		r = alias(r, "run_id", col)
	}
	return c.runFields(c.agentContent(r, role, messages, calls), state)
}

func (c *carrier) agentContent(r segment.Record, role string, messages, calls bool) segment.Record {
	if _, usage := column(r, "input_tokens"); role == "" && usage {
		role = llm.RoleAssistant
	}
	r = sessionFields(r)
	if messages || role != "" {
		r = c.messageFields(r, role)
	}
	if calls {
		return c.callFields(r)
	}
	return memoryLoad(r)
}

func agentColumn(r segment.Record, col segment.Column, f agentField) (segment.Record, string) {
	switch {
	case f.alias != "":
		return alias(r, f.alias, col), ""
	case f.role:
		return r, col.Value.String
	case f.typ:
		return r, eventRole(col.Value.String)
	}
	return r, ""
}

func eventRole(typ string) string {
	role, _, _ := strings.Cut(typ, ".")
	switch role {
	case "user", llm.RoleAssistant, roleTool, "system":
		return role
	case fieldModel:
		return llm.RoleAssistant
	case "toolResult":
		return roleTool
	}
	return ""
}

func sessionFields(r segment.Record) segment.Record {
	if _, held := column(r, "run_id"); held {
		return r
	}
	for _, col := range r.Columns {
		if sessionStart(col) {
			prefix := strings.TrimSuffix(col.Name, fieldType)
			for _, name := range []string{prefix + fieldID, prefix + "payload.id"} {
				if id, ok := column(r, name); ok {
					r = alias(r, "run_id", id)
				}
			}
		}
	}
	return r
}

func sessionStart(col segment.Column) bool {
	return slices.Contains([]string{"session", "session_meta", "session.start"}, col.Value.String)
}

func alias(r segment.Record, name string, col segment.Column) segment.Record {
	col = clone(col)
	col.Name = name
	return llm.Set(r, col)
}

func textField(r segment.Record, name, text string) segment.Record {
	return llm.Set(r, segment.Column{Name: name, Type: value.TypeString, Value: value.Value{String: text}})
}

func (c *carrier) messageFields(r segment.Record, role string) segment.Record {
	lengths := map[string]int{}
	for _, col := range r.Columns {
		kind := c.field(col.Name).kind
		if kind == "" {
			continue
		}
		for i, v := range elements(col) {
			n := utf8.RuneCountInString(v.String)
			lengths["context_"+kind+"_chars"] += n
			lengths["content_"+contentRole(r, col.Name, i, role)+"_chars"] += n
			lengths["content_chars"] += n
			if match := skillTag.FindStringSubmatch(v.String); len(match) > 0 {
				r = textField(r, "skill_name", match[1])
			}
		}
	}
	return contentColumns(r, role, lengths)
}

func contentColumns(r segment.Record, role string, lengths map[string]int) segment.Record {
	for _, name := range []string{"content_user_chars", "content_assistant_chars", "content_tool_chars", "content_system_chars", "context_text_chars", "context_thinking_chars", "context_tool_chars", "content_chars"} {
		if n, ok := lengths[name]; ok && (name != "content_chars" || n > 0) {
			r = llm.Set(r, segment.Column{Name: name, Type: value.TypeInt, Value: value.Value{Int: int64(n)}})
		}
	}
	if role != "" {
		r = textField(r, "content_role", role)
	}
	return r
}

func contentKind(name string) string {
	switch {
	case strings.HasSuffix(name, ".thinking"), strings.HasSuffix(name, ".thoughts.description"):
		return "thinking"
	case strings.HasSuffix(name, ".text"), strings.HasSuffix(name, ".content") && !strings.HasSuffix(name, ".content.content"), name == "response":
		return "text"
	case strings.HasSuffix(name, ".content.content"), strings.Contains(name, ".input."), strings.Contains(name, ".arguments."), strings.Contains(name, ".args."):
		return roleTool
	}
	return ""
}

func contentRole(r segment.Record, name string, i int, fallback string) string {
	for {
		at := strings.LastIndex(name, ".")
		if at < 0 {
			break
		}
		name = name[:at]
		if role, held := column(r, name+".role"); held {
			vs := elements(role)
			if i < len(vs) {
				fallback = vs[i].String
				break
			}
		}
	}
	if name == "response" && fallback == "" {
		return llm.RoleAssistant
	}
	switch fallback {
	case fieldModel:
		return llm.RoleAssistant
	case "toolResult":
		return roleTool
	}
	return fallback
}

func elements(col segment.Column) []value.Value {
	if col.Type.Repeated() {
		return col.Value.List
	}
	return []value.Value{col.Value}
}

func (c *carrier) callFields(r segment.Record) segment.Record {
	for _, col := range r.Columns {
		if !c.field(col.Name).call {
			continue
		}
		prefix := ""
		if at := strings.LastIndex(col.Name, "."); at >= 0 {
			prefix = col.Name[:at+1]
		}
		r = callIdentity(r, prefix, col)
		for i, v := range elements(col) {
			skill := callSkill(r, prefix, v.String, i)
			if skill != "" {
				r = textField(r, "skill_name", skill)
			}
			r = memoryUse(r, prefix, v.String, i)
		}
	}
	return r
}

// memoryUse marks the first call of r that uses memory: the tool_name ingest adds is that call
// again, so a later one never renames it.
func memoryUse(r segment.Record, prefix, name string, i int) segment.Record {
	if _, held := column(r, fieldMemoryOp); held {
		return r
	}
	if op, file := callMemory(r, prefix, name, i); op != "" {
		r = textField(textField(r, fieldMemoryOp, op), "memory_name", file)
	}
	return r
}

func callIdentity(r segment.Record, prefix string, col segment.Column) segment.Record {
	typ, _ := column(r, prefix+fieldType)
	if slices.ContainsFunc(elements(typ), func(v value.Value) bool {
		return slices.Contains([]string{"tool_use", "toolCall", "function_call"}, v.String)
	}) {
		r = alias(r, fieldToolName, col) // tool_call_id is the answer's, so a call counts once
	}
	return r
}

func callSkill(r segment.Record, prefix, name string, i int) string {
	loaders := []string{fieldSkill, "activate_skill", "load_skill"}
	read := strings.HasPrefix(strings.ToLower(name), "read")
	if !slices.Contains(loaders, strings.ToLower(name)) && !read {
		return ""
	}
	for _, col := range callArguments(r, prefix, i) {
		text := col.Value.String
		if read && filepath.Base(text) == "SKILL.md" {
			return filepath.Base(filepath.Dir(text))
		}
		if !read && (strings.HasSuffix(col.Name, ".skill") || strings.HasSuffix(col.Name, ".name")) {
			return text
		}
	}
	return ""
}

// memoryFile is the file a path names when it is an agent's memory: a MEMORY.md, the global
// GEMINI.md, a file under /memories, or one in a memory directory inside a dot-directory.
func memoryFile(path string) (string, bool) {
	base := filepath.Base(path)
	dirs := strings.Split(filepath.ToSlash(filepath.Dir(path)), "/")
	at := slices.IndexFunc(dirs, func(d string) bool { return d == "memory" || d == dirMemories })
	hidden := slices.ContainsFunc(dirs[:max(at, 0)], func(d string) bool { return strings.HasPrefix(d, ".") && len(d) > 1 })
	global := base == "GEMINI.md" && slices.ContainsFunc(dirs, func(d string) bool { return d == ".gemini" })
	if base == "MEMORY.md" || global || at == 1 && dirs[0] == "" && dirs[1] == dirMemories || at > 0 && hidden {
		return base, true
	}
	return "", false
}

// memoryLoad marks a record that puts memory in the context: an entry of a list of files whose
// path names a memory file and whose content the record carries, as an instructions attachment
// does. A tool call's arguments are no load, so a record of calls never reaches here.
func memoryLoad(r segment.Record) segment.Record {
	for _, col := range r.Columns {
		if col.Name != fieldPath && !strings.HasSuffix(col.Name, "."+fieldPath) {
			continue
		}
		content, held := column(r, strings.TrimSuffix(col.Name, fieldPath)+"content")
		if !held {
			continue
		}
		texts, name, chars := elements(content), "", 0
		for i, v := range elements(col) {
			if file, ok := memoryFile(v.String); ok && i < len(texts) {
				name = cmp.Or(name, file)
				chars += utf8.RuneCountInString(texts[i].String)
			}
		}
		if name != "" {
			r = textField(textField(r, fieldMemoryOp, memoryLoaded), "memory_name", name)
			r = llm.Set(r, segment.Column{Name: "memory_chars", Type: value.TypeInt, Value: value.Value{Int: int64(chars)}})
		}
	}
	return r
}

// callMemory is what a tool call does with memory, and to which memory: a memory tool, or a
// file tool reading or writing a memory file.
func callMemory(r segment.Record, prefix, name string, i int) (op, memory string) {
	tool := strings.ToLower(name)
	args := callArguments(r, prefix, i)
	arg := func(key string) string {
		at := slices.IndexFunc(args, func(c segment.Column) bool { return strings.HasSuffix(c.Name, "."+key) })
		if at < 0 {
			return ""
		}
		return args[at].Value.String
	}
	op, tooled := memoryTools[tool]
	if ns, held := column(r, prefix+"namespace"); held && ns.Value.String == dirMemories {
		op, tooled = cmp.Or(op, memoryRead), true
	}
	if tooled {
		if op == "" && arg("command") == "view" {
			op = memoryRead
		}
		op = cmp.Or(op, memoryWritten)
		if file := cmp.Or(arg(fieldPath), arg("file_path"), arg("filename")); file != "" {
			return op, filepath.Base(file)
		}
		return op, cmp.Or(arg("subject"), tool)
	}
	op = fileOp(tool)
	for _, key := range []string{"file_path", fieldPath} {
		if file, ok := memoryFile(arg(key)); ok && op != "" {
			return op, file
		}
	}
	return "", ""
}

// memoryTools are the tools whose every call is memory use, by name: the operation, or none
// where the call's command says.
var memoryTools = map[string]string{
	"memory":          "",
	"save_memory":     memoryWritten,
	"store_memory":    memoryWritten,
	"add_ad_hoc_note": memoryWritten,
	// Voting on a memory rates one the agent recalled.
	"vote_memory": memoryRead,
}

const (
	memoryLoaded  = "load"
	memoryRead    = "read"
	memoryWritten = "write"
)

// fileOp is whether a file tool of this lower-cased name reads or writes the file it names.
func fileOp(tool string) string {
	switch {
	case strings.HasPrefix(tool, "read"), tool == "view":
		return memoryRead
	case slices.ContainsFunc([]string{"write", "edit", "multiedit", "create", "str_replace", "replace"}, func(p string) bool { return strings.HasPrefix(tool, p) }):
		return memoryWritten
	}
	return ""
}

func callArguments(r segment.Record, prefix string, i int) []segment.Column {
	var args []segment.Column
	for _, col := range r.Columns {
		if !strings.HasPrefix(col.Name, prefix) {
			continue
		}
		suffix := strings.TrimPrefix(col.Name, prefix)
		if !strings.HasPrefix(suffix, "input") && !strings.HasPrefix(suffix, "arguments") && !strings.HasPrefix(suffix, "args") {
			continue
		}
		vs := elements(col)
		if i >= len(vs) {
			continue
		}
		if decoded := argumentFields(vs[i].String); len(decoded) > 0 {
			args = append(args, decoded...)
		} else {
			col.Value = vs[i]
			args = append(args, col)
		}
	}
	return args
}

func argumentFields(text string) []segment.Column {
	var object map[string]any
	if json.Unmarshal([]byte(text), &object) != nil {
		return nil
	}
	var args []segment.Column
	for _, key := range []string{fieldSkill, fieldName, fieldPath, "file_path", "filename", "command", "subject"} {
		if v, ok := object[key].(string); ok {
			args = append(args, segment.Column{Name: "." + key, Value: value.Value{String: v}})
		}
	}
	return args
}

func modelFields(r segment.Record, state *memory) segment.Record {
	if model, ok := column(r, fieldModel); ok {
		state.remember(fieldModel, []segment.Column{model})
	} else if prev := state.columns[fieldModel]; len(prev) > 0 {
		r = alias(r, fieldModel, prev[0])
	}
	return r
}

func (c *carrier) runFields(r segment.Record, state *memory) segment.Record {
	r = modelFields(r, state)
	run, ok := column(r, "run_id")
	if !ok {
		if prev := state.columns["run"]; len(prev) > 0 {
			r = alias(r, "run_id", prev[0])
			run, ok = column(r, "run_id")
		}
	}
	if !ok {
		return r
	}
	state.remember("run", []segment.Column{run})
	id := value.Format(run.Type, run.Value)
	if marked, before, after := c.compactionMarker(r); marked {
		return markCompaction(r, state, id, before, after)
	}
	input, usage := column(r, "input_tokens")
	if !usage {
		return r
	}
	total := value.Coerce(input.Type, input.Value, value.TypeFloat).Float
	for _, name := range []string{"cache_read_tokens", "cache_write_tokens"} {
		if c, held := column(r, name); held {
			total += value.Coerce(c.Type, c.Value, value.TypeFloat).Float
		}
	}
	key := "context/" + id
	r = afterCompaction(r, state, id, total, state.columns[key])
	state.remember(key, []segment.Column{{Value: value.Value{Float: total}}})
	return r
}

// compactionEvents are the events agents log a compaction by: Claude Code's transcript and
// OpenTelemetry, Codex, pi, Gemini CLI, Copilot CLI and OpenHands.
var compactionEvents = []string{
	"compact_boundary", "claude_code.compaction", "compacted", "context_compacted", "compaction",
	"gemini_cli.chat_compression", "session.compaction_complete", "Condensation", "condensation",
}

// compactionOf is what the field of name key, lower-cased with separators removed, says of a
// compaction: an event's type, kind or name, its success, or a size before or after.
func compactionOf(key string) compactionField {
	ends := func(ends ...string) bool {
		return slices.ContainsFunc(ends, func(s string) bool { return strings.HasSuffix(key, s) })
	}
	switch {
	case ends(fieldType, "kind", "action", "eventname"):
		return compactionEvent
	case ends("success"):
		return compactionSuccess
	case ends("pretokens", "tokensbefore", "precompactiontokens"):
		return compactionBefore
	case ends("posttokens", "tokensafter", "postcompactiontokens"):
		return compactionAfter
	}
	return notCompaction
}

// compactionMarker is whether r is an agent's record of a compaction, and the context sizes
// before and after it that r reports, each -1 when it reports none. A compaction that failed,
// as Copilot CLI's reports with success false, is none.
func (c *carrier) compactionMarker(r segment.Record) (marked bool, before, after float64) {
	before, after = -1, -1
	failed := false
	for _, col := range r.Columns {
		numeric := col.Type.Numeric() && !col.Type.Repeated()
		switch c.field(col.Name).compaction {
		case notCompaction:
		case compactionEvent:
			marked = marked || col.Type == value.TypeString && slices.Contains(compactionEvents, col.Value.String)
		case compactionSuccess:
			failed = col.Type == value.TypeBool && !col.Value.Bool
		case compactionBefore:
			if numeric {
				before = value.Coerce(col.Type, col.Value, value.TypeFloat).Float
			}
		case compactionAfter:
			if numeric {
				after = value.Coerce(col.Type, col.Value, value.TypeFloat).Float
			}
		}
	}
	return marked && !failed, before, after
}

// markCompaction records the compaction r marks in run id: the tokens it removed when r reports
// both sizes, else what the run's next model call works them out from.
func markCompaction(r segment.Record, state *memory, id string, before, after float64) segment.Record {
	if before >= 0 && after >= 0 {
		r = compactionTokens(r, before-after)
	}
	state.remember("compaction/"+id, []segment.Column{{Value: value.Value{Float: before}}, {Value: value.Value{Bool: before >= 0 && after >= 0}}})
	return r
}

// afterCompaction marks r, a model call sending total tokens of context, as the first since a
// compaction: one its run's records marked, or a fall to under half of the call before, prev.
// The halving keeps a call that sends a little less, as a retried or trimmed one does, from
// counting as one.
func afterCompaction(r segment.Record, state *memory, id string, total float64, prev []segment.Column) segment.Record {
	pending := state.columns["compaction/"+id]
	var removed float64
	switch {
	case len(pending) == 2:
		state.remember("compaction/"+id, nil)
		r = llm.Set(r, afterCompactionColumn)
		if pending[1].Value.Bool {
			return r
		}
		before := pending[0].Value.Float
		if before < 0 && len(prev) > 0 {
			before = prev[0].Value.Float
		}
		removed = max(0, before-total)
	case len(prev) > 0 && total < prev[0].Value.Float/2:
		r = llm.Set(r, afterCompactionColumn)
		removed = prev[0].Value.Float - total
	default:
		return r
	}
	return compactionTokens(r, removed)
}

// afterCompactionColumn marks the first model call after a compaction, which writes the
// compacted context to the cache again.
var afterCompactionColumn = segment.Column{Name: "after_compaction", Type: value.TypeBool, Value: value.Value{Bool: true}}

func compactionTokens(r segment.Record, n float64) segment.Record {
	return llm.Set(r, segment.Column{Name: "compaction_tokens", Type: value.TypeFloat, Value: value.Value{Float: n}})
}
