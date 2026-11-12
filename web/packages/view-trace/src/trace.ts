import type { FieldStat, LogLine, Predicate, Span, TraceResponse } from "@datafocal/api";
import type { TimelineModel } from "@datafocal/render-timeline";
import { errorColor, Flag, serviceColors, type WaterfallModel } from "@datafocal/render-waterfall";
import { logql } from "@datafocal/query";
import type { Destination, TimeRange } from "@datafocal/shell";
import { duration, lineText } from "@datafocal/ui";
import { converse, owners, type StepKind, type Totals } from "./conversation";

// Trace is a get_trace answer with its spans in tree pre-order, each span's parent an index
// in that order, and its log lines, listed in time order under the span they back. Coverage
// under 1 says the read stopped before the whole trace.
export interface Trace {
  readonly res: TraceResponse;
  readonly spans: readonly Span[];
  readonly depth: readonly number[];
  // indent is how far a span's name is indented: an only child stays at its parent's, so a
  // chain of one line after another reads as a list rather than a staircase.
  readonly indent: readonly number[];
  readonly lines: readonly (readonly LogLine[])[];
  readonly t0: number;
  readonly coverage: number;
  // notes say what a row's span did, in place of its service, where it is a conversation's;
  // quiet are a conversation's records that are no message, and turns its turns' spans.
  readonly notes: readonly string[];
  readonly quiet: ReadonlySet<number>;
  readonly turns: readonly number[];
  // kinds are what each conversation's step is, "" for any other span; totals sum its turns.
  readonly kinds: readonly (StepKind | "")[];
  readonly totals: Totals;
  // hues are each service's index in palette, in the order the trace first meets them, so
  // the services of one trace never share a colour while there are colours to go round.
  readonly hues: ReadonlyMap<string, number>;
}

export const ms = (l: LogLine): number => Date.parse(l.timestamp);

// semantic is a field whose value says what kind of record a line is.
const semantic = /(^|[._])(type|kind|name|role|event|action|method|status|op)$/i;

// readable is a span's name as a reader takes it. A name that is a structured record's
// template, "{key key=value …}", is the values of the fields that say what the record is,
// "Bash · tool_result · user", or the template when it has none.
function readable(name: string): string {
  if (!name.startsWith("{")) {
    return name;
  }
  const values = name
    .replace(/^\{|\}?…?$/g, "")
    .split(/\s+/)
    .flatMap((t) => {
      const eq = t.indexOf("=");
      const v = t.slice(eq + 1).replace(/^\[|\]$/g, "");
      return eq > 0 && semantic.test(t.slice(0, eq)) && v !== "" && v !== "<*>" ? [v] : [];
    });
  return values.length > 0 ? [...new Set(values)].join(" · ") : name;
}

// readTrace reads answer and its lines as a Trace. A span get_trace gathers a service's lines
// naming no span into is named by its service, as what it says is its first line's.
export function readTrace(
  answer: TraceResponse,
  lines: readonly LogLine[],
  coverage: number,
): Trace {
  const talk = converse(answer, lines);
  const res = talk.res;
  const kids = res.spans.map((): number[] => []);
  const order: number[] = [];
  const depth: number[] = [];
  const walk = (i: number, d: number) => {
    order.push(i);
    depth.push(d);
    kids[i]?.forEach((k) => walk(k, d + 1));
  };
  res.spans.forEach((s, i) => kids[s.parent]?.push(i));
  res.spans.forEach((s, i) => kids[s.parent] || walk(i, 0));
  const pos = new Map(order.map((i, p) => [i, p]));
  const spans = order
    .flatMap((i) => res.spans[i] ?? [])
    .map((s) => ({
      ...s,
      name: s.id === `svc:${s.service}` ? s.service : readable(s.name),
      parent: pos.get(s.parent) ?? -1,
    }));
  const only = (p: number) => (kids[order[p] ?? -1]?.length ?? 0) === 1;
  const indent: number[] = [];
  spans.forEach((s, i) => {
    const up = indent[s.parent] ?? -1;
    indent[i] = s.parent < 0 ? 0 : only(s.parent) ? up : up + 1;
  });
  const sorted = [...lines].sort((a, b) => ms(a) - ms(b));
  const owner = owners(spans, sorted, res.id_field ?? "span_id");
  const byId = new Map(spans.map((s, i) => [s.id, i]));
  const backing = spans.map((): LogLine[] => []);
  sorted.forEach((l, k) => backing[byId.get(talk.owner.get(l) ?? "") ?? owner[k] ?? -1]?.push(l));
  const t0 = Math.min(...spans.map((s) => s.start_ms));
  const where = (has: (id: string) => boolean) => spans.flatMap((s, i) => (has(s.id) ? [i] : []));
  return {
    res,
    spans,
    depth,
    indent,
    lines: backing,
    t0,
    coverage,
    notes: spans.map((s) => talk.notes.get(s.id) ?? ""),
    quiet: new Set(where((id) => talk.quiet.has(id))),
    turns: where((id) => talk.turns.has(id)),
    kinds: spans.map((s) => talk.kinds.get(s.id) ?? ""),
    totals: talk.totals,
    hues: huesOf(spans),
  };
}

// shown is the spans in pre-order, skipping the descendants of collapsed ones.
export function shown(t: Trace, collapsed: ReadonlySet<number>): number[] {
  const out: number[] = [];
  let hide = Infinity;
  t.depth.forEach((d, i) => {
    if (d <= hide) {
      hide = collapsed.has(i) ? d : Infinity;
      out.push(i);
    }
  });
  return out;
}

// instantLeaves are the spans that take no time and hold no others: a line logged on the
// way rather than work done, which the waterfall folds into its parent's row until asked. In
// a conversation every step is one, so only its records that are no message fold.
export function instantLeaves(t: Trace): ReadonlySet<number> {
  const parents = new Set(t.spans.map((s) => s.parent));
  const turns = new Set(t.turns);
  return new Set(
    t.spans.flatMap((s, i) =>
      t.quiet.has(i) ||
      (!turns.has(s.parent) &&
        s.parent >= 0 &&
        s.dur_ms === 0 &&
        !s.error &&
        !s.group &&
        !parents.has(i))
        ? [i]
        : [],
    ),
  );
}

// clientKind is OpenTelemetry's span kind of a call to another service: kind 3, or named.
const clientKind = /^(3|client|span_kind_client|producer|4|span_kind_producer)$/i;

// isCall is whether line l reports a span waiting on another service's, by its span kind.
export const isCall = (l: LogLine | undefined): boolean =>
  Object.entries(l?.attributes ?? {}).some(
    ([k, v]) => /(^|\.)(span\.)?kind$/.test(k) && clientKind.test(v),
  );

const flags = (s: Span, first: LogLine | undefined) =>
  (s.error ? Flag.error : 0) |
  (s.critical ? Flag.critical : 0) |
  (s.critical_inferred ? Flag.criticalInferred : 0) |
  (s.parent_method === "contained" ? Flag.derivedParent : 0) |
  (s.group ? Flag.group : 0) |
  (isCall(first) ? Flag.call : 0);

// palette is the colours a trace's bars take: a service's by its name, the same wherever it
// is drawn, after which a conversation's steps take their kind's (kindColors).
const palette: readonly string[] = serviceColors;

// serviceHue is service's index in palette: the same for a name in every trace and view.
function serviceHue(service: string): number {
  let h = 0;
  for (const c of service) {
    h = (h * 31 + (c.codePointAt(0) ?? 0)) >>> 0;
  }
  return h % palette.length;
}

export const serviceColor = (service: string): string => palette[serviceHue(service)] ?? "";

// huesOf numbers spans' services into palette in the order they first start.
function huesOf(spans: readonly Span[]): ReadonlyMap<string, number> {
  const hues = new Map<string, number>();
  for (const s of [...spans].sort((a, b) => a.start_ms - b.start_ms)) {
    if (!hues.has(s.service)) {
      hues.set(s.service, hues.size % palette.length);
    }
  }
  return hues;
}

// colorIn is service's colour in trace t.
export const colorIn = (t: Trace, service: string): string =>
  palette[t.hues.get(service) ?? serviceHue(service)] ?? "";

// kindColors are a conversation's turns and steps by what they are, then a background
// service's records, which give way to the conversation.
type Hue = StepKind | "turn" | "background";
const kindOrder: readonly Hue[] = [
  "turn",
  "user",
  "thinking",
  "call",
  "result",
  "assistant",
  "note",
  "background",
];
const kindColors: Readonly<Record<Hue, string>> = {
  turn: "#5b9dff",
  user: "#5b9dff",
  thinking: "#b48cf2",
  call: "#2fc4b2",
  result: "#8a93a3",
  assistant: "#e8a050",
  note: "#5f6878",
  background: "#4a5262",
};

// hues are each span's colour index into the model's palette, and that palette: services by
// name, or in a conversation turns and steps by kind and other services as background.
function hues(t: Trace, order: readonly number[]): { at: number[]; colors: readonly string[] } {
  if (t.turns.length === 0) {
    return { at: order.map((i) => t.hues.get(t.spans[i]?.service ?? "") ?? 0), colors: palette };
  }
  const turns = new Set(t.turns);
  const talking = new Set(t.turns.map((i) => t.spans[i]?.service));
  const kind = (i: number): Hue =>
    turns.has(i)
      ? "turn"
      : t.kinds[i] || (talking.has(t.spans[i]?.service) ? "note" : "background");
  return {
    at: order.map((i) => kindOrder.indexOf(kind(i))),
    colors: kindOrder.map((k) => kindColors[k]),
  };
}

// endOf is what is written past span i's bar: how long it took, or for an instant how far
// into its parent it happened.
function endOf(t: Trace, i: number): string {
  const s = t.spans[i];
  if (!s) {
    return "";
  }
  if (s.group) {
    return `${s.group.count} × p50 ${duration(s.group.p50_ms)}`;
  }
  const from = t.spans[s.parent]?.start_ms ?? t.t0;
  return s.dur_ms > 0 ? duration(s.dur_ms) : `+${duration(s.start_ms - from)}`;
}

const compact = new Intl.NumberFormat("en", { notation: "compact" });

// facts are the numbers a span's lines carry, by field name, as "gets 4 · bytes_read 1.2K".
const facts = (s: Span | undefined): string =>
  Object.entries(s?.sums ?? {})
    .sort(([a], [b]) => (a < b ? -1 : 1))
    .map(([k, v]) => `${k} ${compact.format(v)}`)
    .join(" · ");

// factsOf is what span i's lines counted, none in a conversation, whose steps' notes say what
// they cost and whose services' roots sum every line of a transcript.
function factsOf(t: Trace, i: number): string {
  return t.turns.length > 0 ? "" : facts(t.spans[i]);
}

// waterfallModel draws the spans order lists, one row each, labelled with what their first
// line said; the flame graph draws it too.
export function waterfallModel(t: Trace, order: readonly number[]): WaterfallModel {
  const row = new Map(order.map((i, r) => [i, r]));
  const ss = order.flatMap((i) => t.spans[i] ?? []);
  const hue = hues(t, order);
  const ticks = order.flatMap((i, r) => (t.lines[i] ?? []).map((l) => ({ r, l })));
  return {
    spans: {
      start: Float64Array.from(ss, (s) => s.start_ms - t.t0),
      dur: Float32Array.from(ss, (s) => s.dur_ms),
      self: Float32Array.from(ss, (s) => s.self_ms),
      depth: Uint16Array.from(order, (i) => t.depth[i] ?? 0),
      parent: Int32Array.from(ss, (s) => row.get(s.parent) ?? -1),
      flags: Uint8Array.from(order, (i) => {
        const s = t.spans[i];
        return s ? flags(s, t.lines[i]?.[0]) : 0;
      }),
      group: Uint32Array.from(ss, (_, i) => i),
    },
    names: order.map((i) => {
      const s = t.spans[i];
      const first = t.lines[i]?.[0];
      const name = s?.group ? `${s.name} ×${s.group.count}` : (s?.name ?? "");
      return first && !t.notes[i] ? lineText(first) || name : name;
    }),
    services: Uint16Array.from(hue.at),
    palette: hue.colors,
    ends: order.map((i) => endOf(t, i)),
    ticks: {
      span: Uint32Array.from(ticks, (k) => k.r),
      t: Float64Array.from(ticks, (k) => ms(k.l) - t.t0),
      error: Uint8Array.from(ticks, (k) => Number(k.l.level === "error")),
    },
    gaps: [],
    facts: order.map((i) => factsOf(t, i)),
  };
}

// freeRow is the first row from r on that none of taken's bars cover over [s, e).
function freeRow(
  taken: readonly (readonly (readonly [number, number])[])[],
  r: number,
  s: number,
  e: number,
): number {
  while (taken[r]?.some(([a, b]) => s < b && a < e)) {
    r++;
  }
  return r;
}

// flameModel is m laid out as a flame graph whose bars never overlap: each span on the first
// row below its parent's that is free over its time, so spans running at once at one depth,
// as concurrent calls or a second service's root are, stack rather than cover each other.
export function flameModel(m: WaterfallModel): WaterfallModel {
  const { start, dur, parent } = m.spans;
  const taken: (readonly [number, number])[][] = [];
  const row = new Uint16Array(start.length);
  for (let i = 0; i < start.length; i++) {
    const [s, e] = [start[i] ?? 0, (start[i] ?? 0) + Math.max(dur[i] ?? 0, 1e-6)];
    const up = parent[i] ?? -1;
    const r = freeRow(taken, up < 0 ? 0 : (row[up] ?? 0) + 1, s, e);
    (taken[r] ??= []).push([s, e]);
    row[i] = r;
  }
  return { ...m, spans: { ...m.spans, depth: row } };
}

// maxKinds is the most kinds a timeline tells apart; past it a span is drawn unnamed.
const maxKinds = 255;

// lanesModel draws the spans order lists, in start order, on a lane per service, its name
// left to the column beside the lanes, each bar named by its span in its service's colour as
// the waterfall draws it, or red for an error.
export function lanesModel(t: Trace, order: readonly number[]): TimelineModel {
  const services = [...new Set(t.spans.map((s) => s.service))];
  const ss = order.flatMap((i) => t.spans[i] ?? []);
  const kinds: string[] = [];
  const colors: string[] = [];
  const byKey = new Map<string, number>();
  const kindOf = (s: Span) => {
    const lane = services.indexOf(s.service);
    const named = byKey.size < maxKinds - services.length * 2;
    const key = `${lane}\u0000${named ? s.name : ""}\u0000${s.error}`;
    let k = byKey.get(key);
    if (k === undefined) {
      k = kinds.length;
      byKey.set(key, k);
      kinds.push(named ? s.name : "");
      colors.push(s.error ? errorColor : colorIn(t, s.service));
    }
    return k;
  };
  const kind = Uint8Array.from(ss, kindOf);
  return {
    lanes: services.map((id) => ({ id, label: "", depth: 0, parent: -1 })),
    kinds,
    colors,
    steps: {
      start: Float64Array.from(ss, (s) => s.start_ms - t.t0),
      dur: Float32Array.from(ss, (s) => s.dur_ms),
      lane: Uint16Array.from(ss, (s) => services.indexOf(s.service)),
      kind,
      group: Uint32Array.from(ss, (s) => s.group?.count ?? 1),
    },
    markers: [],
  };
}

// match selects a trace's lines: its value in the field it was read by, or any it joined.
export function match(res: TraceResponse): Predicate[] {
  const each = (res.fields ?? [res.field]).map((field) => ({ field, op: "eq", value: res.value }));
  return each.length > 1 ? [{ any: each }] : each;
}

export const traceLogs = (t: Trace): Destination => ({
  view: "logs",
  query: logql(match(t.res)),
  title: `Trace ${t.res.value}`,
});

export const spanLogs = (s: Span): Extract<Destination, { view: "logs" }> => ({
  view: "logs",
  query: logql(s.where),
  title: `${s.name} · ${s.service}`,
});

// traceSpan is the trace's span with a minute either side, so a running total's rate, as a
// resource's, has a sample before the trace starts.
export function traceSpan(t: Trace): TimeRange {
  const end = Math.max(...t.spans.map((x) => x.start_ms + x.dur_ms));
  const at = (ms: number) => new Date(ms).toISOString();
  return { from: at(t.t0 - 60_000), to: at(end + 60_000) };
}

export const lineLogs = (s: Span, l: LogLine): Destination => ({
  view: "logs",
  query: logql(s.where),
  focus: { segment: l.segment ?? "", row: l.row },
  title: `${s.name} · ${l.timestamp}`,
});

// scope reads the field="value" terms of an arriving where, the rest left unread.
export const scope = (where: string): Predicate[] =>
  [...where.matchAll(/([\w.]+)="?([^"\s]+)"?/g)].map(([, field, value]) => ({
    field: field ?? "",
    op: "eq",
    value: value ?? "",
  }));

// searchTerm is a Logs search term: an optional "-", then a field compared with a value
// (field:value, field>=n, field~text), a quoted phrase or a word.
export const searchTerm: RegExp =
  /\s*(-)?(?:([A-Za-z_][\w.-]*)(:|>=|<=|>|<|~)("[^"]*"|\S+)|("[^"]*"|\S+))/gy;
const unquote = (s: string) => (/^".*"$/.test(s) ? s.slice(1, -1) : s);
const escapeRegex = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// compare are the predicate ops of a term's comparisons, and their negations.
const compare: Readonly<Record<string, readonly [string, string]>> = {
  ":": ["eq", "ne"],
  ">": ["gt", "le"],
  ">=": ["ge", "lt"],
  "<": ["lt", "ge"],
  "<=": ["le", "gt"],
  "~": ["regex", "noregex"],
};

// searchPredicate is what a term finds: a field compared with a value, else a word or phrase
// in the message ignoring case, either excluded when not.
function searchPredicate(
  not: string | undefined,
  field: string | undefined,
  op: string | undefined,
  value: string,
): Predicate {
  const [is, isnt] = compare[op ?? ""] ?? ["eq", "ne"];
  if (field) {
    const v = unquote(value);
    return { field, op: not ? isnt : is, value: op === "~" ? `(?i)${escapeRegex(v)}` : v };
  }
  return {
    field: "message",
    op: not ? "noregex" : "regex",
    value: `(?i)${escapeRegex(unquote(value))}`,
  };
}

// search is the predicates a Logs plain search finds a line by, OR between two terms taking
// either of them.
export function search(q: string): Predicate[] {
  const out: Predicate[] = [];
  let or = false;
  for (const [, not, field, op, value, text = ""] of q.matchAll(searchTerm)) {
    if (text === "OR" && !not) {
      or = out.length > 0;
      continue;
    }
    const p = searchPredicate(not, field, op, value ?? text);
    const prev = or ? out.pop() : undefined;
    out.push(prev ? { any: [...(prev.any ?? [prev]), p] } : p);
    or = false;
  }
  return out;
}

// A name ending in an id word, or naming a unit of work, says its values each gather lines;
// pattern_id is the template column, which gathers lines by shape rather than by request.
const idName = /(^|[._-])id$|[a-z0-9]Id$/;
const unitName = /^(trace|session|transaction|correlation|conversation|task|job|run)$/i;
const leaf = (name: string) => name.slice(name.lastIndexOf(".") + 1);
const linkKey = (name: string) => name.toLowerCase().replace(/[^a-z0-9]/g, "");
const perValue = (f: FieldStat) => f.lines / Math.max(1, f.cardinality);
// unit is whether a field names a unit of work, as itself or as its id: session, run_id, taskId.
const unit = (name: string) => unitName.test(leaf(name).replace(/[._-]?id$/i, ""));

// opLeaves are the last parts of a field's name that say it names an operation, best first;
// a field under a resource's name (k8s.pod.name) names that resource instead.
const opLeaves = ["name", "operation", "op", "route", "endpoint", "method"];
const resourceWord =
  /(^|[._])(k8s|pod|node|host|namespace|container|service|cluster|image|process|deployment|telemetry|scope|sdk)[._]/i;

// operationField is the rank_fields field naming what a line's operation is, as OpenTelemetry's
// span name does, best first by its name then by the lines holding it; undefined when none.
export function operationField(ranked: readonly FieldStat[]): string | undefined {
  const rank = (f: FieldStat) => opLeaves.indexOf(leaf(f.name).toLowerCase());
  return ranked
    .filter(
      (f) => f.type !== "num" && f.cardinality > 1 && rank(f) >= 0 && !resourceWord.test(f.name),
    )
    .sort((a, b) => rank(a) - rank(b) || b.lines - a.lines || (a.name < b.name ? -1 : 1))[0]?.name;
}

// Ends says a trace's lines report how long each took, as spans logged when they end do, so
// its root is told by its last line (index.ts rootAt), and the field naming its operation.
export interface Ends {
  readonly op?: string;
}

// traceEnds is Ends for the rank_fields fields, undefined when no line reports a duration.
export function traceEnds(ranked: readonly FieldStat[]): Ends | undefined {
  if (!ranked.some((f) => f.name === "duration_ms")) {
    return undefined;
  }
  const op = operationField(ranked);
  return op ? { op } : {};
}

// correlations are the rank_fields fields a trace can be gathered by, best first: non-numeric
// fields of more than one value whose name says id or unit of work, trace_id leading, then
// those naming a unit of work (run_id, sessionId), then those with the most lines to a value,
// so an id each line has its own of, like a request's, ranks under a unit of work's. A field
// of one value in range (a model's id) gathers every line into one trace, so it is left out,
// as is a field another names as its parent (span_id, uuid), which links spans within a trace
// rather than gathering one.
export function correlations(ranked: readonly FieldStat[]): string[] {
  const keys = ranked.map((f) => linkKey(f.name)).filter((k) => k.startsWith("parent"));
  const links = new Set(keys.flatMap((k) => [k, k.slice("parent".length)]));
  return ranked
    .filter(
      (f) =>
        f.type !== "num" &&
        f.cardinality > 1 &&
        f.name !== "pattern_id" &&
        !links.has(linkKey(f.name)) &&
        (f.type === "id" || idName.test(leaf(f.name)) || unitName.test(leaf(f.name))),
    )
    .sort(
      (a, b) =>
        Number(b.name === "trace_id") - Number(a.name === "trace_id") ||
        Number(unit(b.name)) - Number(unit(a.name)) ||
        perValue(b) - perValue(a) ||
        (a.name < b.name ? -1 : 1),
    )
    .map((f) => f.name);
}
