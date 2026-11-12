import type {
  DataFocalClient,
  LogLine,
  Span,
  TraceShapeRequest,
  TraceShapeResponse,
} from "@datafocal/api";
import {
  navigate,
  Pivots,
  type TimeRange,
  useEvidence,
  useFocus,
  useParam,
} from "@datafocal/shell";
import { duration, type Evidence, Inspector, lineText, Show, useAnswer } from "@datafocal/ui";
import { createElement, Fragment, type ReactElement, useState } from "react";
import { colorIn, isCall, lineLogs, spanLogs, type Trace, traceSpan } from "./trace";

export const button = (label: string, onClick: () => void, title?: string): ReactElement =>
  createElement("button", { key: label, type: "button", className: "tbtn", title, onClick }, label);

const facts = (rows: readonly (readonly [string, string])[]) => () =>
  createElement(
    "dl",
    null,
    rows.flatMap(([k, v]) =>
      v === ""
        ? []
        : [createElement("dt", { key: k }, k), createElement("dd", { key: `${k}=` }, v)],
    ),
  );

// rules are how span s was found: its parent and duration, observed in its lines or derived
// from what, and whether it is on the critical path.
function rules(t: Trace, s: Span): [string, string][] {
  const parent = !s.parent_method
    ? "none, a root"
    : s.parent_method === "read"
      ? `observed: its lines name it in ${t.res.parent_field ?? "a parent field"}`
      : "derived: the nearest span whose time contains it";
  const timed =
    s.duration_method === "reported"
      ? "observed: its lines report it"
      : "derived: from its first line to its last";
  return [
    ["Parent", parent],
    ["Duration", `${timed}; ${lasted(t, s)}`],
    ["Critical path", s.critical_inferred ? "inferred" : s.critical ? "yes" : "no"],
  ];
}

// spanEvidence is what backs span s: the rules it was found by, and its lines.
function spanEvidence(t: Trace, s: Span, lines: readonly LogLine[]): Evidence {
  const { query, title = s.name } = spanLogs(s);
  const shown = lines.map((l) => ({
    time: l.timestamp.slice(11, 23),
    service: l.service ?? s.service,
    message: lineText(l),
    error: l.level === "error",
  }));
  return { title, lines: shown, computed: facts(rules(t, s))(), query };
}

// fieldValues is each field the lines carry, with how many of them hold each of its values.
function fieldValues(lines: readonly LogLine[]): Map<string, Map<string, number>> {
  const fields = new Map<string, Map<string, number>>();
  for (const l of lines) {
    for (const [k, v] of Object.entries(l.attributes ?? {})) {
      const values = fields.get(k) ?? new Map<string, number>();
      values.set(v, (values.get(v) ?? 0) + 1);
      fields.set(k, values);
    }
  }
  return fields;
}

// split is the fields lines carry, by name: those every line holds one value of, and the
// rest, those on the most lines first.
function split(lines: readonly LogLine[]) {
  const fields = [...fieldValues(lines)].sort(([a], [b]) => (a < b ? -1 : 1));
  const shared = fields.filter(([, vs]) => vs.size === 1 && [...vs.values()][0] === lines.length);
  const carried = (vs: Map<string, number>) => [...vs.values()].reduce((a, b) => a + b, 0);
  const varied = fields
    .filter((f) => !shared.includes(f))
    .sort(([, a], [, b]) => carried(b) - carried(a));
  return { fields, shared, varied };
}

// Attributes are what a span's lines say: the fields every line holds one value of, which a
// click copies or adds to the list's search as field:value, then those that vary, those on
// the most lines first, each with how many values it holds and its commonest.
function Attributes(p: { lines: readonly LogLine[] }): ReactElement {
  const [q, setQ] = useParam("q");
  const term = (k: string, v: string) => `${k}:${/^[^\s"]+$/.test(v) ? v : JSON.stringify(v)}`;
  const { fields, shared, varied } = split(p.lines);
  const row = (k: string, v: string, note?: string) =>
    createElement(
      "tr",
      { key: k },
      createElement("th", null, k),
      createElement(
        "td",
        null,
        createElement("code", null, v),
        note && createElement("small", null, note),
        createElement(
          "span",
          { className: "tr-aacts" },
          button("Copy", () => void navigator.clipboard.writeText(v), `Copy ${k}`),
          button(
            "Filter",
            () => setQ(`${q ?? ""} ${term(k, v)}`.trim()),
            `List the traces with ${k}:${v}`,
          ),
        ),
      ),
    );
  const top = (vs: Map<string, number>) => [...vs].sort((a, b) => b[1] - a[1])[0] ?? ["", 0];
  const heading = (text: string) =>
    createElement(
      "tr",
      { key: text },
      createElement("td", { colSpan: 2, className: "tr-ah" }, text),
    );
  if (fields.length === 0) {
    return createElement("p", { className: "tr-empty" }, "These lines carry no fields.");
  }
  return createElement(
    "table",
    { className: "tr-attrs" },
    createElement(
      "tbody",
      null,
      p.lines.length > 1 && shared.length > 0 && heading(`On all ${p.lines.length} lines`),
      shared.map(([k, vs]) => row(k, top(vs)[0])),
      varied.length > 0 && heading("Varies"),
      varied.map(([k, vs]) => {
        const [v, n] = top(vs);
        return row(
          k,
          v,
          vs.size > 1 ? `${vs.size} values · this on ${n}` : `on ${n} of ${p.lines.length}`,
        );
      }),
    ),
  );
}

// held are the lines span i backs: for the span get_trace gathers a service's lines into,
// every line of that service in the trace, wherever its steps went; else its own, or for a
// span backing none, those of the spans under it, which follow it in pre-order.
function held(t: Trace, i: number): readonly LogLine[] {
  const s = t.spans[i];
  const own = t.lines[i] ?? [];
  const out: LogLine[] = [];
  if (s && s.id === `svc:${s.service}`) {
    out.push(...t.lines.flat().filter((l) => l.service === s.service));
  } else if (own.length > 0) {
    return own;
  } else {
    for (let k = i + 1; k < t.spans.length && (t.depth[k] ?? 0) > (t.depth[i] ?? 0); k++) {
      out.push(...(t.lines[k] ?? []));
    }
  }
  return out.sort((a, b) => Date.parse(a.timestamp) - Date.parse(b.timestamp));
}

// said is what a line says: its text, or for a record with none, its first few fields.
const said = (l: LogLine) =>
  lineText(l) ||
  Object.entries(l.attributes ?? {})
    .slice(0, 4)
    .map(([k, v]) => `${k}=${v}`)
    .join(" ");

// shownLines is the most lines the Lines tab lists; Logs opens the rest.
const shownLines = 200;

// LinesTab lists a span's lines, each opening in Logs, the first shownLines of them.
function LinesTab(p: { s: Span; lines: readonly LogLine[] }): ReactElement {
  const more = p.lines.length - shownLines;
  return createElement(
    "div",
    { className: "tr-lines" },
    p.lines.slice(0, shownLines).map((l, k) =>
      createElement(
        "button",
        {
          key: k,
          type: "button",
          className: l.level === "error" ? "err" : undefined,
          onClick: () => navigate(lineLogs(p.s, l)),
        },
        createElement("time", null, l.timestamp.slice(11, 23)),
        createElement("span", null, l.service),
        createElement("span", null, said(l)),
      ),
    ),
    more > 0 && button(`${more} more in Logs`, () => navigate(spanLogs(p.s))),
  );
}

// useSampled is whether service has resource samples over the trace's time, so Resources
// is offered only where it shows something; undefined until known.
function useSampled(client: DataFocalClient, t: Trace, service: string): boolean | undefined {
  const time = traceSpan(t);
  const req = {
    selector: { time, where: [{ field: "service", op: "eq", value: service }] },
    group_by: "service",
    summary_only: true,
  };
  const answer = useAnswer(JSON.stringify(req), () =>
    client.call("resource_usage", req).then((e) => e.data.groups.length > 0),
  );
  return typeof answer === "boolean" ? answer : undefined;
}

// SpanInspector shows span i: its facts, its lines' attributes and the lines themselves, each
// opening in Logs. Evidence opens how its parent, duration and critical path were found with
// its lines, Focus looks into it over range, and × closes the panel.
export function SpanInspector(p: {
  client: DataFocalClient;
  t: Trace;
  i: number;
  range: TimeRange;
  close: () => void;
}): ReactElement | null {
  const { t, i } = p;
  const s = t.spans[i];
  const showEvidence = useEvidence();
  const focus = useFocus();
  const sampled = useSampled(p.client, t, s?.service ?? "");
  if (!s) {
    return null;
  }
  const lines = held(t, i);
  const { query, title = s.name } = spanLogs(s);
  return createElement(Inspector, {
    title: s.name,
    kicker: kickerOf(t, i),
    meta: createElement(SpanMeta, { s, line: lines[0], color: colorIn(t, s.service) }),
    hero: heroOf(t, s),
    tabKey: "trace-span",
    onClose: p.close,
    actions: [
      button("Span logs", () => navigate(spanLogs(s)), "This span's lines (L)"),
      createElement(Pivots, {
        key: "pivots",
        from: "trace",
        omit: sampled ? ["logs"] : ["logs", "resources"],
        subject: { where: s.where, service: s.service, time: traceSpan(t), title },
      }),
      button("Evidence", () => showEvidence(spanEvidence(t, s, lines))),
      button("Focus", () => focus({ title, query, selector: { time: p.range }, slice: s.where })),
    ],
    tabs: [
      { id: "Overview", label: "Overview", render: () => createElement(Overview, { t, i, lines }) },
      {
        id: "Attributes",
        label: "Attributes",
        count: fieldValues(lines).size,
        render: () => createElement(Attributes, { lines }),
      },
      {
        id: "Lines",
        label: "Lines",
        count: lines.length,
        render: () => createElement(LinesTab, { s, lines }),
      },
    ],
  });
}

// stepWords say what a conversation's step is.
const stepWords: Readonly<Record<string, string>> = {
  user: "Prompt",
  thinking: "Model call",
  call: "Tool call",
  result: "Tool result",
  assistant: "Reply",
  note: "Harness note",
};

// kickerOf says what span i is: a conversation's turn or step, a service's lines, a call to
// another service, or a span.
function kickerOf(t: Trace, i: number): string {
  const s = t.spans[i];
  if (t.turns.includes(i)) {
    return "Turn";
  }
  if (t.kinds[i]) {
    return stepWords[t.kinds[i] ?? ""] ?? "Step";
  }
  if (s && s.id === `svc:${s.service}`) {
    return "Service";
  }
  return isCall(t.lines[i]?.[0]) ? "Call to another service" : "Span";
}

// heroOf is span s's headline: how long it took and where in the trace, as a share of it.
function heroOf(t: Trace, s: Span) {
  const whole = Math.max(1, ...t.spans.map((x) => x.start_ms + x.dur_ms)) - t.t0;
  const from = s.start_ms - t.t0;
  const share = s.dur_ms / whole;
  return {
    value: s.dur_ms > 0 ? duration(s.dur_ms) : "an instant",
    caption:
      s.dur_ms > 0
        ? `${Math.round(share * 100)}% of the trace, from +${duration(from)}`
        : `at +${duration(from)}`,
    start: from / whole,
    share,
    color: colorIn(t, s.service),
  };
}

// first is the value of the first of names line l carries.
const first = (l: LogLine | undefined, names: readonly string[]) =>
  names.map((n) => l?.attributes?.[n]).find((v) => v !== undefined && v !== "");

// SpanMeta is a line on span s: its service, and what its first line says it served.
function SpanMeta(p: { s: Span; line: LogLine | undefined; color: string }): ReactElement {
  const { s, line, color } = p;
  const method = first(line, ["http.method", "http.request.method", "rpc.system"]);
  const route = first(line, ["http.route", "url.path", "http.target", "rpc.method"]);
  const status = first(line, [
    "http.status_code",
    "http.response.status_code",
    "rpc.grpc.status_code",
  ]);
  const bad = Number(status) >= 500 || s.error;
  return createElement(
    Fragment,
    null,
    createElement(
      "span",
      { className: "tr-chip" },
      createElement("span", { className: "tr-sq", ref: tinted(color) }),
      s.service,
    ),
    (method || route) &&
      createElement("code", null, [method, route?.split("/").at(-1)].filter(Boolean).join(" ")),
    status && createElement("span", { className: bad ? "tr-pill bad" : "tr-pill ok" }, status),
  );
}

// groups are the fields that say where a span ran and what it served, each by the names it
// is logged under, OpenTelemetry's and the collector's.
const groups: readonly (readonly [string, readonly (readonly [string, readonly string[]])[]])[] = [
  [
    "Request",
    [
      ["method", ["http.method", "http.request.method"]],
      ["route", ["http.route", "url.path", "http.target", "http.url"]],
      ["status", ["http.status_code", "http.response.status_code"]],
      ["rpc", ["rpc.service", "rpc.method"]],
      ["database", ["db.system", "db.statement", "db.query.text"]],
    ],
  ],
  [
    "Where it ran",
    [
      ["namespace", ["k8s.namespace.name", "namespace"]],
      ["pod", ["k8s.pod.name", "pod"]],
      ["node", ["k8s.node.name", "host.name", "host"]],
      ["container", ["k8s.container.name", "container"]],
    ],
  ],
];

// Children is the time spent in span i's own spans, longest first, each placed in i's time.
function Children({ t, i }: { t: Trace; i: number }): ReactElement | null {
  const s = t.spans[i];
  const kids = t.spans
    .flatMap((k, n) => (k.parent === i && k.dur_ms > 0 ? [[k, n] as const] : []))
    .sort(([a], [b]) => b.dur_ms - a.dur_ms)
    .slice(0, 6);
  if (!s || s.dur_ms <= 0 || kids.length === 0) {
    return null;
  }
  return createElement(
    "ul",
    { className: "tr-kids" },
    kids.map(([k, n]) =>
      createElement(
        "li",
        { key: n },
        createElement("span", null, k.name),
        createElement(
          "span",
          { className: "tr-kbar" },
          createElement("span", {
            ref: (el: HTMLElement | null) => {
              el?.style.setProperty("left", `${((k.start_ms - s.start_ms) / s.dur_ms) * 100}%`);
              el?.style.setProperty("width", `${Math.max(1, (k.dur_ms / s.dur_ms) * 100)}%`);
              el?.style.setProperty("--c", colorIn(t, k.service));
            },
          }),
        ),
        createElement("span", null, duration(k.dur_ms)),
      ),
    ),
  );
}

// Overview is what span i is at a glance: the time its own spans took, what its first line
// said, what it served and where it ran, then its facts.
function Overview(p: { t: Trace; i: number; lines: readonly LogLine[] }): ReactElement | null {
  const { t, i, lines } = p;
  const s = t.spans[i];
  if (!s) {
    return null;
  }
  const said = lines[0] && lineText(lines[0]);
  const kids = createElement(Children, { t, i });
  return createElement(
    Fragment,
    null,
    t.spans.some((k) => k.parent === i && k.dur_ms > 0) &&
      s.dur_ms > 0 &&
      createElement(Fragment, null, createElement("h3", null, "Time inside it"), kids),
    said &&
      createElement(
        Fragment,
        null,
        createElement("h3", null, "Said"),
        createElement("pre", { className: "tr-said" }, said),
      ),
    groups.map(([heading, fields]) => {
      const rows = fields.flatMap(([k, names]) => {
        const v = first(lines[0], names);
        return v === undefined ? [] : [[k, v] as const];
      });
      return (
        rows.length > 0 &&
        createElement(Fragment, { key: heading }, createElement("h3", null, heading), facts(rows)())
      );
    }),
    createElement("h3", null, "Span"),
    facts(summary(t, s))(),
  );
}

// lasted is how long s took: as its lines report, or for an instant, the time until the
// trace's next line, which its bar spans.
function lasted(t: Trace, s: Span): string {
  if (s.dur_ms > 0) {
    return `${duration(s.dur_ms)}, ${duration(s.self_ms)} self`;
  }
  const next = Math.min(...t.spans.map((x) => x.start_ms).filter((x) => x > s.start_ms));
  return Number.isFinite(next)
    ? `an instant; the next line came ${duration(next - s.start_ms)} later`
    : "an instant, the trace's last";
}

function summary(t: Trace, s: Span): [string, string][] {
  const i = t.spans.indexOf(s);
  const first = t.lines[i]?.[0];
  const note = t.notes[i];
  const spoken: [string, string][] = [];
  if (first) {
    spoken.push(["Said", lineText(first)]);
  }
  if (note) {
    spoken.push(["Did", note]);
  }
  const rows: [string, string][] = [
    ...spoken,
    ["Service", s.service],
    ["Start", `+${duration(s.start_ms - t.t0)}`],
    [
      "Duration",
      s.dur_ms > 0 ? `${duration(s.dur_ms)}, ${duration(s.self_ms)} self` : "an instant",
    ],
    ["Lines", String(s.lines)],
    ["Error", s.error ? "yes" : "no"],
  ];
  const g = s.group;
  if (g) {
    rows.push(["Group", `${g.count} spans, p50 ${g.p50_ms} ms, p95 ${g.p95_ms} ms`]);
  }
  return rows;
}

const pct = (v: number) => `${(v * 100).toFixed(1)}%`;

// Shape is the merged tree of the traces starting at this trace's root service, with how
// this trace differs from it.
export function Shape(p: { client: DataFocalClient; range: TimeRange; t: Trace }): ReactElement {
  const req: TraceShapeRequest = {
    selector: { time: p.range },
    root: [{ field: "service", op: "eq", value: p.t.spans[0]?.service ?? "" }],
    field: p.t.res.field,
    compare: p.t.res.value,
  };
  const shape = useAnswer(JSON.stringify(req), () =>
    p.client.call("trace_shape", req).then((e) => e.data),
  );
  const root = p.t.spans[0]?.service ?? "";
  return createElement(
    "div",
    { className: "tr-shape" },
    createElement(Show<TraceShapeResponse>, {
      answer: shape,
      render: (res) => createElement(ShapeTable, { res, root }),
    }),
  );
}

// diffWords say how this trace differs from the merged tree at a node.
const diffWords: Readonly<Record<string, string>> = {
  extra: "only in this trace",
  missing: "missing here",
  slower: "slower here",
};

// took is a node's time, or a dash for an instant.
const took = (ms: number) => (ms > 0 ? duration(ms) : "–");

// Meter is a share as a short bar beside its percentage.
const meter = (share: number, className: string) =>
  createElement(
    "span",
    { className: `tr-meter ${className}` },
    createElement(
      "b",
      null,
      createElement("i", {
        ref: (el: HTMLElement | null) => el?.style.setProperty("width", pct(share)),
      }),
    ),
    createElement("span", null, pct(share)),
  );

// ShapeRow is a node of a shape's tree in the order the table lists it, at its depth.
interface ShapeRow {
  readonly i: number;
  readonly depth: number;
  // ours is whether this trace has it or an operation under it.
  readonly ours: boolean;
}

// shapeRows is res's tree in pre-order, each node's children those this trace has first.
function shapeRows(res: TraceShapeResponse): ShapeRow[] {
  const kids = new Map<number, number[]>();
  res.nodes.forEach((n, i) => kids.set(n.parent, [...(kids.get(n.parent) ?? []), i]));
  const ours = new Map<number, boolean>();
  const has = (i: number): boolean => {
    let v = ours.get(i);
    if (v === undefined) {
      v = Boolean(res.nodes[i]?.in_trace) || (kids.get(i) ?? []).some(has);
      ours.set(i, v);
    }
    return v;
  };
  const out: ShapeRow[] = [];
  const walk = (i: number, depth: number) => {
    out.push({ i, depth, ours: has(i) });
    const sorted = [...(kids.get(i) ?? [])].sort((a, b) => Number(has(b)) - Number(has(a)));
    sorted.forEach((k) => walk(k, depth + 1));
  };
  [...(kids.get(-1) ?? [])]
    .sort((a, b) => Number(has(b)) - Number(has(a)))
    .forEach((i) => walk(i, 0));
  return out;
}

// thisTrace says how this trace stands at a node: how it differs, that it has it, or neither.
const thisTrace = (n: TraceShapeResponse["nodes"][number]) =>
  n.diff
    ? createElement("span", { className: "tr-diff" }, diffWords[n.diff] ?? n.diff)
    : n.in_trace
      ? createElement("span", { className: "tr-here" }, "here")
      : "–";

// ShapeRowView is one operation of the shape, indented under its caller.
function ShapeRowView({ n, depth }: { n: TraceShapeResponse["nodes"][number]; depth: number }) {
  return createElement(
    "tr",
    { className: n.diff ? `d-${n.diff}` : n.in_trace ? "d-here" : undefined },
    createElement(
      "td",
      { className: "tr-op" },
      createElement(
        "span",
        {
          ref: (el: HTMLElement | null) => el?.style.setProperty("padding-left", `${depth * 16}px`),
        },
        createElement("span", { className: "tr-op-s" }, n.service),
        ` ${n.name}`,
      ),
    ),
    createElement("td", null, meter(n.presence, "")),
    createElement("td", null, took(n.p50_ms)),
    createElement("td", null, took(n.p95_ms)),
    createElement("td", null, n.error_share > 0 ? meter(n.error_share, "bad") : "–"),
    createElement("td", null, thisTrace(n)),
  );
}

// ShapeTable is the merged tree of res's traces, a row an operation indented under its
// caller, with how often it happens, how long it takes, how often it fails, and how this
// trace stands at it. The operations this trace has come first; the others fold away until
// asked for.
function ShapeTable({ res, root }: { res: TraceShapeResponse; root: string }): ReactElement {
  const [all, setAll] = useState(false);
  const rows = shapeRows(res);
  const anyOurs = rows.some((r) => r.ours);
  const shown = rows.filter((r) => all || !anyOurs || r.ours);
  const others = rows.length - shown.length;
  const head = ["Operation", "In traces", "p50", "p95", "Errors", "This trace"];
  return createElement(
    "div",
    null,
    createElement(
      "p",
      { className: "tr-shape-h" },
      `What ${res.traces.toLocaleString("en")} traces starting at `,
      createElement("b", null, root),
      " usually do, merged into one tree: how often each operation happens, how long it takes and how often it fails. The last column marks what this trace does and where it differs.",
    ),
    createElement(
      "table",
      { "aria-label": `Shape of ${res.traces} traces` },
      createElement(
        "thead",
        null,
        createElement(
          "tr",
          null,
          head.map((h) => createElement("th", { key: h }, h)),
        ),
      ),
      createElement(
        "tbody",
        null,
        shown.flatMap((r) => {
          const n = res.nodes[r.i];
          return n ? [createElement(ShapeRowView, { key: r.i, n, depth: r.depth })] : [];
        }),
      ),
    ),
    (others > 0 || all) &&
      anyOurs &&
      button(all ? "Show only this trace's operations" : `Show ${others} other operations`, () =>
        setAll(!all),
      ),
  );
}

// Stat is one of a trace's headline numbers.
interface Stat {
  readonly k: string;
  readonly v: string;
}

// promptOf is a conversation's opening prompt: its first turn a person's message began.
function promptOf(t: Trace): string | undefined {
  const asked = t.turns.find((i) =>
    t.spans.some((s, k) => s.parent === i && t.kinds[k] === "user"),
  );
  return t.spans[asked ?? t.turns[0] ?? -1]?.name.replace(/^\d+\. /, "");
}

// rootOf is the span a trace is of: its longest root.
const rootOf = (t: Trace) =>
  t.spans.filter((s) => s.parent < 0).sort((a, b) => b.dur_ms - a.dur_ms)[0];

// statsOf are what a trace's header says of it: for a conversation its turns, steps, spend
// and tools, else its spans, services and errors; then how long it ran and when it began.
function statsOf(t: Trace): Stat[] {
  // A conversation lasts from its first turn to its last, whatever runs beside it.
  const spans = t.turns.length > 0 ? t.turns.flatMap((i) => t.spans[i] ?? []) : t.spans;
  const from = Math.min(...spans.map((s) => s.start_ms));
  const end = Math.max(...spans.map((s) => s.start_ms + s.dur_ms)) - from;
  const lasted = { k: "Duration", v: duration(end) };
  const began = { k: "Started", v: `${new Date(from).toISOString().slice(11, 19)} UTC` };
  if (t.turns.length > 0) {
    const { steps, cost, tools } = t.totals;
    const top = tools.slice(0, 2).map(([n, k]) => `${n} ×${k}`);
    return [
      lasted,
      { k: "Turns", v: String(t.turns.length) },
      { k: "Steps", v: String(steps) },
      ...(cost > 0 ? [{ k: "Model cost", v: `$${cost.toFixed(2)}` }] : []),
      ...(top.length > 0 ? [{ k: "Tools", v: top.join(" · ") }] : []),
      began,
    ];
  }
  return [
    lasted,
    { k: "Spans", v: String(t.res.total) },
    { k: "Services", v: String(new Set(t.spans.map((s) => s.service)).size) },
    { k: "Errors", v: String(t.spans.filter((s) => s.error).length) },
    began,
  ];
}

// tinted sets an element's --c to color.
const tinted = (color: string) => (el: HTMLElement | null) => el?.style.setProperty("--c", color);

// Legend is a trace's services by how many of its spans each logged, as a strip and a key.
function Legend({ t }: { t: Trace }): ReactElement | null {
  const counts = new Map<string, number>();
  t.spans.forEach((s, i) => {
    if (!t.quiet.has(i)) {
      counts.set(s.service, (counts.get(s.service) ?? 0) + 1);
    }
  });
  if (counts.size < 2 || t.turns.length > 0) {
    return null;
  }
  const each = [...counts];
  const strip = each.map(([name, n]) =>
    createElement("span", {
      key: name,
      ref: (el: HTMLElement | null) => {
        el?.style.setProperty("--c", colorIn(t, name));
        el?.style.setProperty("flex-grow", String(n));
      },
    }),
  );
  return createElement(
    "div",
    { className: "tr-legend" },
    createElement("div", { className: "tr-strip", "aria-hidden": true }, strip),
    createElement(
      "ul",
      { "aria-label": "Services by spans" },
      each.map(([name, n]) =>
        createElement(
          "li",
          { key: name },
          createElement("span", { className: "tr-sq", ref: tinted(colorIn(t, name)) }),
          name,
          createElement("span", null, n),
        ),
      ),
    ),
  );
}

// titleOf is what a trace is: a conversation's opening prompt, else its root's operation.
const titleOf = (t: Trace): string =>
  (t.turns.length > 0 ? promptOf(t) : rootOf(t)?.name) ?? t.res.value;

// TraceSummary heads a trace: what it is, its opening prompt for a conversation or else its
// root's operation, whether it erred, its id, its headline numbers and its services.
export function TraceSummary({ t }: { t: Trace }): ReactElement {
  const talk = t.turns.length > 0;
  const errors = t.spans.filter((s) => s.error).length;
  const title = titleOf(t);
  const copy = () => void navigator.clipboard.writeText(t.res.value);
  return createElement(
    "section",
    { className: "tr-sum", "aria-label": "Trace summary" },
    createElement(
      "div",
      { className: "tr-sum-h" },
      talk && createElement("span", { className: "tr-pill talk" }, "Session"),
      createElement("h1", { title }, title),
      createElement(
        "span",
        { className: errors > 0 ? "tr-pill bad" : "tr-pill ok" },
        errors > 0 ? `${errors} error${errors === 1 ? "" : "s"}` : "OK",
      ),
      createElement("span", { className: "tr-id" }, `${t.res.field} ${t.res.value}`),
      button("Copy id", copy, `Copy ${t.res.value}`),
    ),
    talk && createElement("p", { className: "tr-sum-sub" }, t.spans[t.turns[0] ?? -1]?.service),
    createElement(
      "dl",
      { className: "tr-stats" },
      statsOf(t).map(({ k, v }) =>
        createElement(
          "div",
          { key: k },
          createElement("dt", null, k),
          createElement("dd", null, v),
        ),
      ),
    ),
    createElement(Legend, { t }),
  );
}
