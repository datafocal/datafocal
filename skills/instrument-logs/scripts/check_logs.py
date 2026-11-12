#!/usr/bin/env python3
"""Score log samples against the contract DataFocal reads best.

Usage: check_logs.py [-h] FILE [FILE ...]    (- reads stdin)

Each file is scored on its own, then all of them together, so a sample per producer
(binary, sidecar, script) shows which one has which defect. Every rule mirrors what
DataFocal's ingest does with a line, so a warning is a field, pattern, latency or trace
DataFocal cannot derive.

Exit status: 0 with no warnings, 1 with warnings, 2 on bad usage.
Standard library only.
"""

import json
import re
import sys
from collections import Counter, defaultdict

LEVELS = {"debug", "info", "warn", "error", "fatal"}
# Spellings DataFocal folds onto those levels (logformat.normalizeLevel).
LEVEL_ALIASES = {"trace", "dbg", "information", "notice", "log", "warning", "err", "panic", "critical", "crit"}
ERROR_LEVELS = {"error", "err", "fatal", "panic", "critical", "crit"}
MESSAGE_KEYS = ("msg", "message")
LEVEL_KEYS = ("level", "lvl", "severity")
# DataFocal reads only these top-level keys as the time, and when several parse, the last in
# sorted order wins (logformat.toRecord over flattenJSON's sorted keys).
TIME_KEYS = ("time", "timestamp", "ts")
# Keys loggers commonly use for the time that DataFocal does not read as one.
OTHER_TIME_KEYS = ("@timestamp", "@t", "Timestamp", "t", "date", "datetime", "asctime")
OTHER_LEVEL_KEYS = ("log.level", "@l", "LogLevel", "levelname", "severity_text", "level_name")
# pino's numeric levels, for counting error lines; DataFocal itself stores them as strings.
PINO_LEVELS = {10: "trace", 20: "debug", 30: "info", 40: "warn", 50: "error", 60: "fatal"}
# A CRI or docker envelope, which the collector strips before parsing the body.
CRI = re.compile(r"^\S+ (stdout|stderr) [FP] ")
# Exactly the layouts logformat.parseTime accepts. Go also takes a fractional second, with a
# dot or a comma, after the seconds of any of them.
RFC3339 = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d([.,]\d+)?(Z|[+-]\d\d:\d\d)$")
SPACED = re.compile(r"^\d{4}([-/])\d\d\1\d\d \d\d:\d\d:\d\d([.,]\d+)?(Z|[+-]\d\d:\d\d| UTC)?$")
ANSI = re.compile(r"\x1b\[[0-9;]*m")
# A JSON object after a prefix: a process manager or wrapper stamping each line.
PREFIXED_JSON = re.compile(r"^[^{]{1,60}?[:\s]\s*\{\"")
# An access line in the message: method, raw path, protocol.
ACCESS_LINE = re.compile(r"\b(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS) /\S*( HTTP/[\d.]+)?")
DIGIT_TOKEN = re.compile(r"\S*\d\S*")
QUOTED = re.compile(r'"(?:[^"\\]|\\.)*"')
# Keys that are data rather than names: ids, hashes, dates, paths.
DATA_KEY = re.compile(r"(^|\.)([0-9a-f]{8,}|\d+|[0-9a-f-]{36})(\.|$)|/")
DURATION_HINT = re.compile(r"(duration|elapsed|latency|took|_time$|_secs?$|_seconds$|_ns$|_us$|"
                           r"(?<=[a-z])(Time|Millis|Secs?|Nanos)$)")
# Id spellings DataFocal does not look up as ids: camelCase, dotted, dashed.
OTHER_ID = re.compile(r"((?<=[a-z0-9])Id|(^|\.)id|-id)$")
# Keys that hold a stack trace, which is long by nature.
STACK_KEY = re.compile(r"(^|[._])(stack|stack_?trace|stacktrace|exc_info|traceback|detail)$", re.I)
# A key=value pair inside a message: a field that was interpolated instead of attached.
PAIR_IN_MSG = re.compile(r"(^|\s)[A-Za-z_][\w.]*=\S")
# An unparsed line that reads like a failure or an exit, which is the line that matters most.
FAILURE_WORDS = re.compile(r"(\b(error|fatal|panic|panicked|failed|exception|traceback|exit|exited|killed)\b"
                           r"|\w(Exception|Error)\b|^Caused by:)", re.I)
UTC_SUFFIX = re.compile(r"(Z|[+-]00:?00| UTC)$")
MAX_COLUMNS_PER_LINE = 40
MAX_DISTINCT_KEYS = 300
SHOW = 5


def flatten(obj, prefix=""):
    """Yields (key, value) as DataFocal flattens JSON: nested objects become dotted keys."""
    for k, v in obj.items():
        name = prefix + k
        if isinstance(v, dict):
            yield from flatten(v, name + ".")
        elif isinstance(v, list):
            objects = [e for e in v if isinstance(e, dict)]
            for e in objects:
                yield from flatten(e, name + ".")
            # DataFocal gives a list its own column only for scalar elements (logformat.jsonList).
            if len(objects) < len(v) or not v:
                yield name, v
        elif v is not None:
            yield name, v


def mask(text):
    """DataFocal's pattern mask: whitespace collapsed, every token holding a digit wildcarded."""
    return " ".join(DIGIT_TOKEN.sub("<*>", t) for t in text.split())


def time_zone(v):
    """'utc', 'offset' or 'none' for a time DataFocal parses, else None."""
    if not isinstance(v, str):
        return None
    s = v.strip()
    if not RFC3339.match(s) and not SPACED.match(s):
        return None
    if UTC_SUFFIX.search(s):
        return "utc"
    return "offset" if re.search(r"[+-]\d\d:\d\d$", s) else "none"


def pick_time(obj):
    """The time key DataFocal would use and its zone class, or (None, None)."""
    found = (None, None)
    for k in TIME_KEYS:
        z = time_zone(obj.get(k))
        if z:
            found = (k, z)
    return found


def infer(v):
    """pattern.InferTypedValue's split: a token that parses as a number is one."""
    for cast in (int, float):
        try:
            return cast(v)
        except ValueError:
            continue
    return v


def parse_pairs(text):
    """logformat.parsePairs: key=value or key="quoted", every token a pair, or None."""
    out, rest = {}, text.strip()
    while rest:
        key, eq, val = rest.partition("=")
        if not eq or not key or '"' in key or any(c.isspace() for c in key):
            return None
        if val.startswith('"'):
            m = QUOTED.match(val)
            if not m:
                return None
            out[key] = json.loads(m.group(0)) if "\\" in m.group(0) else m.group(0)[1:-1]
            rest = val[m.end():]
        else:
            m = re.search(r"\s", val)
            out[key], rest = (infer(val[:m.start()]), val[m.start():]) if m else (infer(val), "")
        rest = rest.lstrip()
    return out or None


def parse_text(line):
    """logformat.parseText: a leading time and/or level word, then free text, then pairs."""
    tokens = line.split()
    start = 0
    for n in range(min(3, len(tokens)), 0, -1):
        if time_zone(" ".join(tokens[:n])):
            start = n
            break
    word = tokens[start] if start < len(tokens) else ""
    level = word.strip("[]:").lower()
    is_level = level in LEVELS | LEVEL_ALIASES and (start or word.isupper() or word.endswith(":") or word.startswith("["))
    if not start and not is_level:
        return None
    obj = {}
    if start:
        obj["ts"] = " ".join(tokens[:start])
    if is_level:
        obj["level"] = level
    end = len(tokens)
    for i in range(start, len(tokens)):
        pairs = parse_pairs(" ".join(tokens[i:]))
        if pairs:
            obj.update(pairs)
            end = i
            break
    # The level word stays in the message, as DataFocal keeps it.
    obj["msg"] = " ".join(tokens[start:end])
    return obj


def parse_line(line):
    """The first of DataFocal's formats that parses line, as (format, fields), or (None, None)."""
    try:
        obj = json.loads(line)
        if isinstance(obj, dict):
            return "json", obj
    except ValueError:
        pass
    pairs = parse_pairs(line)
    if pairs:
        return "logfmt", pairs
    obj = parse_text(line)
    return ("text", obj) if obj else (None, None)


def first(obj, keys):
    for k in keys:
        if k in obj:
            return obj[k]
    return None


class Report:
    def __init__(self, name):
        self.name = name
        self.lines = self.parsed = 0
        self.formats = Counter()
        self.unparsed = []
        self.after_parsed = 0
        self.last_parsed = False
        self.zones = Counter()
        self.has_level = self.has_msg = 0
        self.level_values = Counter()
        self.object_msgs = 0
        self.pairs_in_msg = []
        self.families = defaultdict(set)
        # Per message template: lines, lines with a *_id, lines with duration_ms.
        self.template = defaultdict(lambda: [0, 0, 0])
        self.keys = Counter()
        self.data_keys = set()
        self.wide_lines = 0
        self.id_keys = Counter()
        self.lines_with_id = 0
        self.span_lines = 0
        self.durations = []
        self.other_durations = Counter()
        self.errors = self.errors_with_error = self.errors_with_err = 0
        self.empty_errors = 0
        self.long_values = Counter()
        self.ansi = 0
        self.prefixed_json = []
        self.numeric_times = self.unread_times = 0
        self.unread_time_keys = Counter()
        self.numeric_levels = 0
        self.unread_level_keys = Counter()
        self.other_ids = Counter()
        self.numeric_ids = Counter()
        self.access_lines = []
        self.loggers = {}

    def add(self, raw):
        line = CRI.sub("", raw.rstrip("\n"), count=1)
        if not line.strip():
            return
        self.lines += 1
        if ANSI.search(line):
            self.ansi += 1
        fmt, obj = parse_line(line)
        if obj is None:
            if PREFIXED_JSON.match(line) and len(self.prefixed_json) < SHOW:
                self.prefixed_json.append(line)
            self.unparsed.append(line)
            self.after_parsed += self.last_parsed
            self.last_parsed = False
            return
        self.last_parsed = True
        self.formats[fmt] += 1
        self.parsed += 1
        key, zone = pick_time(obj)
        self.zones[zone or "missing"] += 1
        if not key:
            raw_time = first(obj, TIME_KEYS)
            if isinstance(raw_time, (int, float)) and not isinstance(raw_time, bool):
                self.numeric_times += 1
            elif raw_time is not None:
                self.unread_times += 1
            for k in OTHER_TIME_KEYS:
                if k in obj:
                    self.unread_time_keys[k] += 1
        level = first(obj, LEVEL_KEYS)
        if isinstance(level, (int, float)) and not isinstance(level, bool):
            self.numeric_levels += 1
            level = PINO_LEVELS.get(level)
        elif isinstance(level, str):
            self.has_level += 1
            self.level_values[level.lower()] += 1
        else:
            flat = dict(flatten(obj))
            for k in OTHER_LEVEL_KEYS:
                if k in flat:
                    self.unread_level_keys[k] += 1
                    if isinstance(flat[k], str):
                        level = flat[k]
        msg = first(obj, MESSAGE_KEYS)
        template = None
        if isinstance(msg, dict):
            self.object_msgs += 1
        elif isinstance(msg, str) and msg.strip():
            self.has_msg += 1
            template = mask(msg)
            self.families[" ".join(template.split()[:2])].add(template)
            logger = obj.get("logger") or obj.get("logger_name") or obj.get("name") or obj.get("scope")
            shown = msg[:120] + (f"   [logger {logger}]" if isinstance(logger, str) else "")
            if PAIR_IN_MSG.search(msg) and len(self.pairs_in_msg) < SHOW:
                self.pairs_in_msg.append(shown)
            if ACCESS_LINE.search(msg) and len(self.access_lines) < SHOW:
                self.access_lines.append(shown)
        cols = list(flatten(obj))
        if len(cols) > MAX_COLUMNS_PER_LINE:
            self.wide_lines += 1
        has_id = has_duration = False
        for k, v in cols:
            self.keys[k] += 1
            if DATA_KEY.search(k):
                self.data_keys.add(k)
            if k.endswith("_id") and k != "pattern_id":
                self.id_keys[k] += 1
                has_id = True
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    self.numeric_ids[k] += 1
            elif OTHER_ID.search(k):
                self.other_ids[k] += 1
            if k == "duration_ms" and isinstance(v, (int, float)) and not isinstance(v, bool):
                self.durations.append(v)
                has_duration = True
            elif DURATION_HINT.search(k.rsplit(".", 1)[-1]):
                self.other_durations[k] += 1
            if isinstance(v, str) and len(v) > 2048 and not STACK_KEY.search(k):
                self.long_values[k] += 1
        if template is not None:
            t = self.template[template]
            t[0] += 1
            t[1] += has_id
            t[2] += has_duration
        self.lines_with_id += has_id
        self.span_lines += "span_id" in obj
        if isinstance(level, str) and level.lower() in ERROR_LEVELS:
            self.errors += 1
            keys = {k for k, _ in cols}
            # A string `error`, or an error object DataFocal flattens to error.message.
            has_error = "error" in keys or "error.message" in keys
            self.errors_with_error += has_error
            self.errors_with_err += not has_error and bool(
                keys & {"err", "err.message", "exception", "exc", "exc_info", "stack_trace"})
            err = obj.get("error", obj.get("err"))
            self.empty_errors += err == {} or err == "" or (isinstance(err, dict) and not err.get("message"))

    def render(self):
        out, warns = [f"== {self.name}"], []

        def pct(n, d):
            return f"{100 * n / d:.0f}%" if d else "n/a"

        def rule(ok, text, detail=()):
            out.append(f"  [{'ok' if ok else 'WARN'}] {text}")
            if not ok:
                warns.append(text)
                out.extend(f"         {d}" for d in detail)

        def info(text):
            out.append(f"  [info] {text}")

        n, j = self.lines, self.parsed
        out.append(f"  lines: {n}   parsed into fields: {j} ({pct(j, n)})   "
                   + " ".join(f"{f}={c}" for f, c in self.formats.most_common()))
        if not n:
            rule(False, "no lines: is the logger off (level silent/off, a null handler) in the configuration "
                 "this run used, or its level read from an env file loaded after the logger is built?")
            return out, 1
        rule(not self.ansi, f"lines with ANSI color codes (disable color off a terminal): {self.ansi}")
        rule(not self.prefixed_json, f"JSON lines behind a prefix (a process manager or wrapper stamping each line, "
             f"so none of them parse): {len(self.prefixed_json)}", [u[:120] for u in self.prefixed_json])
        failing = [u for u in self.unparsed if FAILURE_WORDS.search(u)]
        rule(not failing, f"unparsed lines that read like failures or exits: {len(failing)}",
             [u[:120] for u in failing[:SHOW]])
        rule(not self.unparsed, f"lines DataFocal stores as bare text (no time, level or fields): {len(self.unparsed)}"
             f" ({self.after_parsed} directly after a parsed line: stack traces, usage text, subprocess output)",
             [u[:120] for u in self.unparsed[:SHOW] if u not in failing])
        if not j:
            return out, 1
        rule(self.formats["text"] == 0, f"lines with fields only after free text (msg is the text, level word "
             f"included): {self.formats['text']}")
        rule(self.zones["missing"] == 0, f"lines with no parseable ts/time/timestamp: {self.zones['missing']}", [
            d for d in (
                f"{self.numeric_times} have an epoch number: write an RFC 3339 string" if self.numeric_times else "",
                f"{self.unread_times} have a time in a layout DataFocal doesn't parse (it needs RFC 3339 with "
                "a colon in the offset, or 'YYYY-MM-DD hh:mm:ss')" if self.unread_times else "",
                "time under a key DataFocal doesn't read: " + ", ".join(self.unread_time_keys)
                + " (rename to ts)" if self.unread_time_keys else "") if d])
        rule(self.zones["none"] == 0, f"times with no zone (ambiguous across hosts): {self.zones['none']}")
        if self.zones["offset"]:
            info(f"times with a local offset rather than UTC: {self.zones['offset']} (parsed correctly; UTC reads easier)")
        rule(self.has_level == j, f"string level/lvl/severity present: {pct(self.has_level, j)}"
             + (f" ({self.numeric_levels} numeric, which DataFocal stores as the string '30': "
                "configure level labels)" if self.numeric_levels else ""),
             ["level under a key DataFocal doesn't read: " + ", ".join(self.unread_level_keys) + " (rename to level)"]
             if self.unread_level_keys else [])
        odd = sorted(lv for lv in self.level_values if lv not in LEVELS | LEVEL_ALIASES)
        rule(not odd, "level values DataFocal normalizes: " + ("all" if not odd else "not " + ", ".join(odd)))
        rule(self.has_msg == j, f"non-empty string msg/message: {pct(self.has_msg, j)}")
        rule(self.object_msgs == 0, f"msg/message holding an object (gets no pattern): {self.object_msgs}")

        exploding = sorted(((len(v), k) for k, v in self.families.items() if len(v) >= 10), reverse=True)[:SHOW]
        rule(not exploding and not self.pairs_in_msg and not self.access_lines,
             f"constant messages: {len(self.template)} templates over {self.has_msg} lines",
             [f"family with {c} variants: '{k} …'" for c, k in exploding]
             + [f"key=value inside msg: {m}" for m in self.pairs_in_msg]
             + [f"access line in msg (raw path, no route template or duration_ms): {m}" for m in self.access_lines])

        rule(len(self.keys) <= MAX_DISTINCT_KEYS, f"distinct flattened keys: {len(self.keys)} (<= {MAX_DISTINCT_KEYS})")
        rule(self.wide_lines == 0, f"lines with > {MAX_COLUMNS_PER_LINE} flattened keys: {self.wide_lines}")
        rule(not self.data_keys, f"keys made from data (ids, hashes, paths as keys): {len(self.data_keys)}",
             sorted(self.data_keys)[:SHOW])
        rule(not self.long_values, "values over 2 KiB: "
             + (", ".join(f"{k}×{c}" for k, c in self.long_values.most_common(3)) or "none"))

        rule(self.lines_with_id > 0, f"lines carrying a *_id correlation key: {pct(self.lines_with_id, j)}"
              + (" (" + ", ".join(f"{k}×{c}" for k, c in self.id_keys.most_common(6)) + ")" if self.id_keys else ""))
        if self.other_ids:
            rule(False, "id keys DataFocal won't look up as ids (rename to <thing>_id): "
                 + ", ".join(f"{k}×{c}" for k, c in self.other_ids.most_common(5)))
        if self.numeric_ids:
            info("numeric *_id values (looked up fine; DataFocal links services only through string ids): "
                 + ", ".join(k for k, _ in self.numeric_ids.most_common(5)))
        nested = [k for k in self.id_keys if "." in k]
        if nested:
            info("nested id keys (a peer logging the bare key uses a different column): " + ", ".join(nested[:5]))
        info(f"lines with span_id: {pct(self.span_lines, j)}")
        d = self.durations
        rule(bool(d), f"numeric duration_ms: {len(d)} lines", [
            "no latency anywhere: completion events need duration_ms"] if not d else [])
        if self.other_durations:
            rule(False, "other duration keys DataFocal won't treat as latency: "
                 + ", ".join(k for k, _ in self.other_durations.most_common(4)))
        if d and all(isinstance(v, int) for v in d) and sum(v == 0 for v in d) > len(d) / 2:
            rule(False, f"duration_ms truncated to whole ms: {sum(v == 0 for v in d)}/{len(d)} are 0 "
                 "(write fractional milliseconds)")
        if self.errors:
            rule(self.errors_with_error == self.errors,
                 f"error-level lines with an `error` key: {self.errors_with_error}/{self.errors}"
                 + (f" ({self.errors_with_err} use err/exception/stack_trace: add or rename to error)"
                    if self.errors_with_err else ""))
            rule(not self.empty_errors, f"error values with no text (an Error object serialized as {{}}): "
                 f"{self.empty_errors}")
        else:
            info("error-level lines: none in this sample, so error fields are unchecked")

        missing = sorted(((c[0], t) for t, c in self.template.items() if c[1] == 0), reverse=True)[:SHOW]
        if missing and self.id_keys:
            info("most frequent templates with no *_id (check each against the placement map):")
            out.extend(f"         {c}× {t[:100]}" for c, t in missing)
        out.append(f"  warnings: {len(warns)}")
        return out, (1 if warns else 0)


def main(paths):
    if not paths or any(p in ("-h", "--help") for p in paths):
        print(__doc__.strip(), file=sys.stderr if not paths else sys.stdout)
        return 2 if not paths else 0
    total, reports = Report("all files"), []
    for p in paths:
        r = Report(p)
        try:
            f = sys.stdin if p == "-" else open(p, encoding="utf-8", errors="replace")
        except OSError as e:
            print(f"check_logs: {e}", file=sys.stderr)
            return 2
        with f:
            for raw in f:
                r.add(raw)
                total.add(raw)
        reports.append(r)
    if len(reports) > 1:
        reports.append(total)
    code = 0
    for r in reports:
        text, c = r.render()
        print("\n".join(text))
        code = max(code, c)
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
