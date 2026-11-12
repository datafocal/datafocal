import {
  type AggregateRequest,
  type FieldStat,
  type TraceRequest,
  type DataFocalClient,
  type LineCursor,
  type LogLine,
  type Selector,
  type Series,
  ToolError,
  type TraceResponse,
} from "@datafocal/api";
import {
  navigate,
  resolve,
  type TimeRange,
  useParams,
  useQueryRange,
  type ViewProps,
} from "@datafocal/shell";
import { Chip, duration, Show, Splitter, useAnswer } from "@datafocal/ui";
import { createElement, type FormEvent, type ReactElement, useMemo } from "react";
import { TraceBody } from "./body";
import {
  correlations,
  type Ends,
  match,
  readTrace,
  scope,
  search,
  searchTerm,
  serviceColor,
  type Trace,
  traceEnds,
} from "./trace";
import "./trace.css";

// TraceView is the trace navigate opens beside the list of traces in range, gathered by the
// field it names or else the best correlation field the logs carry, the trace's arriving
// where scoping the list and naming the service whose first span it selects.
export function TraceView({ client }: ViewProps): ReactElement {
  const params = useParams();
  const range = useQueryRange();
  const { traceId: picked, field: named, where = "", span, title } = params;
  const filters: List = useMemo(() => {
    const { min, max, start: began, q, sort } = params;
    return { where, min, max, start: began, q, sort };
  }, [params, where]);
  const preds = useMemo(() => scope(where), [where]);
  const service = preds.filter((p) => p.field === "service").at(-1)?.value;
  const gathering = useGathering(client, range, named);
  const { fields, names, ends } = gathering;
  const { list, field } = useTraces(client, range, filters, gathering);
  const { id, by } = shownOf(list, picked, named, field);
  const trace = useTrace(client, range, id, by);
  return createElement(
    "div",
    { className: "tr" },
    title !== undefined &&
      typeof history.state === "string" &&
      createElement(Chip, { title: "Back", onClick: () => history.back() }, `← Back · ${title}`),
    createElement(TraceList, { list, filters, field, fields, names, ends, current: id }),
    createElement(Splitter, {
      label: "the trace list",
      name: "list",
      initial: 300,
      min: 220,
      max: 560,
      edge: "left",
    }),
    id === undefined
      ? createElement("p", { className: "tr-none" }, "No trace to show.")
      : createElement(Show<Trace>, {
          answer: trace,
          render: (t) =>
            createElement(TraceBody, {
              key: id,
              client,
              range,
              t,
              start: arrival(t, service, span),
            }),
        }),
  );
}

// shownOf is the trace to show and the field to read it by. Arriving with no trace named
// shows the first listed, read by the field the list gathers by; one named on arrival is
// read by the field named with it, or get_trace's default.
function shownOf(
  list: Series[] | Error | undefined,
  picked: string | undefined,
  named: string | undefined,
  field: string | undefined,
): { id: string | undefined; by: string | undefined } {
  if (picked !== undefined) {
    return { id: picked, by: named };
  }
  return { id: Array.isArray(list) ? list[0]?.key : undefined, by: field };
}

// useTrace reads the trace valued id under field, or get_trace's default field, and its lines.
function useTrace(
  client: DataFocalClient,
  range: TimeRange,
  id: string | undefined,
  field: string | undefined,
) {
  return useAnswer(id === undefined ? null : JSON.stringify([id, field, range]), async () => {
    const req = { value: id ?? "", time: range, limit: 10_000, ...(field && { field }) };
    const { data: res, meta } = await whole(client, range, req);
    const selector = { time: spanned(res) ?? range, where: match(res) };
    const limit = res.spans.reduce((n, s) => n + s.lines, 0);
    return readTrace(res, await linesOf(client, selector, limit), meta.coverage);
  });
}

// pageLines is the most lines one read of a trace asks for, so a session of thousands of lines
// costs the querier a page at a time rather than all of them in one answer.
const pageLines = 1000;

// linesOf is the newest limit lines selector matches, read a page at a time.
async function linesOf(
  client: DataFocalClient,
  selector: Selector,
  limit: number,
): Promise<LogLine[]> {
  const lines: LogLine[] = [];
  for (let after: LineCursor | undefined; lines.length < limit; ) {
    const page = await client.call("search_logs", {
      selector,
      limit: Math.min(pageLines, limit - lines.length),
      ...(after && { after }),
    });
    lines.push(...(page.data.lines ?? []));
    after = page.data.next;
    if (!after) {
      break;
    }
  }
  return lines;
}

// cutOff is whether res's spans run over an edge of range: its start, or an end before now,
// as a range ending now meets a trace still going rather than one cut off.
function cutOff(res: TraceResponse, range: TimeRange, now: number): boolean {
  const covered = spanned(res);
  const [from, to] = [resolve(range.from, now), resolve(range.to, now)];
  const edge = Math.max(1000, (to - from) / 100);
  return Boolean(
    covered &&
      (resolve(covered.from, now) <= from + edge ||
        (resolve(covered.to, now) >= to - edge && to < now - edge)),
  );
}

// unreadable are the refusals of a get_trace with no time: of a field that is not an id, and
// of a trace too long to hold; either leaves the trace as the range holds it.
const unreadable = new Set(["unbounded_scan", "too_large"]);

// whole is get_trace's answer to req over all of the trace's time, wherever its lines are: it
// is read over the range, and again with no time only when its spans run over an edge of the
// range, which an id's membership filter keeps to the segments holding the trace.
async function whole(client: DataFocalClient, range: TimeRange, req: TraceRequest) {
  const first = await client.call("get_trace", req);
  if (!cutOff(first.data, range, Date.now())) {
    return first;
  }
  const { time: _, ...anytime } = req;
  return client.call("get_trace", anytime).catch((e: unknown) => {
    if (e instanceof ToolError && unreadable.has(e.code)) {
      return first;
    }
    throw e;
  });
}

// spanned is the time res's spans cover, which its lines are read over: get_trace answers a
// trace overlapping the range whole, its spans before the range's start among them.
function spanned(res: TraceResponse): TimeRange | undefined {
  if (res.spans.length === 0) {
    return undefined;
  }
  const from = Math.min(...res.spans.map((s) => s.start_ms));
  const to = Math.max(...res.spans.map((s) => s.start_ms + s.dur_ms)) + 1;
  return { from: new Date(from).toISOString(), to: new Date(to).toISOString() };
}

// useGathering is the fields to gather traces by, in the order to try them: the one named,
// or else the fields rank_fields offers, best first, or get_trace's default when it offers
// none, undefined until known; the fields a span filter can name, best first; and whether
// its lines report durations (Ends).
function useGathering(
  client: DataFocalClient,
  range: TimeRange,
  named: string | undefined,
): {
  candidates: readonly string[] | undefined;
  fields: readonly string[];
  names: readonly string[];
  ends?: Ends;
} {
  const ranked = useAnswer(JSON.stringify(range), () =>
    client.call("rank_fields", { selector: { time: range } }).then((e) => gathered(e.data.fields)),
  );
  const known = ranked && !(ranked instanceof Error) ? ranked : blank;
  const best = known.fields.length > 0 ? known.fields : ["trace_id"];
  // A list waits for rank_fields, which says whether lines report durations, even with a
  // field named, so it is asked once.
  const candidates = ranked === undefined ? undefined : named ? [named] : best;
  return { candidates, ...known };
}

// gathered is what rank_fields' fields say: those gathering traces, best first, those a span
// filter can name, and whether lines report durations.
function gathered(ranked: readonly FieldStat[]) {
  const ends = traceEnds(ranked);
  return {
    fields: correlations(ranked),
    names: ranked.slice(0, filterNames).map((f) => f.name),
    ...(ends && { ends }),
  };
}
const blank: ReturnType<typeof gathered> = { fields: [], names: [] };

// filterNames is the most fields the span filter offers to name.
const filterNames = 80;

// arrival is the span named on arrival, or else the service's first span that is not one of a
// conversation's records, else a conversation's first prompt, or the first span, or -1.
function arrival(t: Trace, service: string | undefined, span: string | undefined): number {
  const named = t.spans.findIndex((s) => s.id === span);
  const own = t.spans.flatMap((s, i) =>
    s.service === service && !t.quiet.has(i) ? [[s.start_ms, i] as const] : [],
  );
  // A conversation opens on its first prompt, what it was asked.
  const asked = t.spans.findIndex((_, i) => t.kinds[i] === "user");
  const first = service === undefined && asked >= 0 ? asked : t.spans.length > 0 ? 0 : -1;
  return named >= 0 ? named : (own.sort((a, b) => a[0] - b[0])[0]?.[1] ?? first);
}

// measures are what the list reads of each trace: its time, errors, lines and services, and
// the service of its first line.
const measures = [
  { measure: "min:ts" },
  { measure: "max:ts" },
  { measure: "count", where: [{ field: "level", op: "eq", value: "error" }] },
  { measure: "count" },
  { measure: "distinct:service" },
  { measure: "span:ts" },
  { measure: "first:service" },
];
// endMeasures are, for lines reporting durations, the service at a trace's last line and the
// duration it reports, by which its root is told (rootAt), then given an operation field the
// operation of its last line.
const endMeasures = (op: string | undefined) => [
  { measure: "last:service" },
  { measure: "last:duration_ms" },
  ...(op ? [{ measure: `last:${op}` }] : []),
];
// firstService and the rest are those measures' indexes in a list request.
const firstService = 6;
const [lastService, lastReported, lastOp] = [7, 8, 9] as const;
// start and extent are the measures of a trace's start and duration.
const [start, extent] = [0, 5];

// lineCount is the measure counting a trace's lines; a value one line carries is no trace.
const lineCount = 3;
// sorts are the list's orders by the measures they sort on, newest first by default.
const newest = "0:desc";
const sorts: Readonly<Record<string, string>> = { start: newest, duration: "5:desc" };

// listLimit is the most traces the list holds.
const listLimit = 100;

// List is what the trace list shows: the arriving where and the list's own filters.
type List = {
  readonly [k in "where" | "min" | "max" | "start" | "q" | "sort"]?: string | undefined;
};

// open shows the traces field gathers under list, and the one valued traceId when named.
function open(field: string, list: List, traceId?: string): void {
  const set = Object.entries({ ...list, traceId }).filter(([, v]) => v);
  navigate({ view: "trace", field, ...Object.fromEntries(set) });
}

// reads are the measures a list reads, given Ends those of a trace's last line too.
const reads = (ends: Ends | undefined) => [...measures, ...(ends ? endMeasures(ends.op) : [])];

// listRequest asks for the traces in range under the list, gathered by field. Several service
// terms ask for the traces touching each, and a search for those with a line it finds, so each
// is a measure to have, leaving the others over all of a trace's lines.
export function listRequest(
  range: TimeRange,
  list: List,
  field: string,
  ends?: Ends,
): AggregateRequest {
  const read = reads(ends);
  const preds = scope(list.where ?? "");
  const services = preds.filter((w) => w.field === "service");
  const each = services.length > 1 ? services.map((w) => [w]) : [];
  const found = search(list.q ?? "");
  const having = [...each, ...(found.length > 0 ? [found] : [])];
  const bound = (index: number, op: string, ms: string | undefined) =>
    ms ? [{ index, op, value: Number(ms) }] : [];
  const [from, to] = list.start?.split(",") ?? [];
  return {
    selector: { time: range, where: preds.filter((w) => !each.flat().includes(w)) },
    measure: "",
    group_by: field,
    bins: "all",
    measures: [...read, ...having.map((where) => ({ measure: "count", where }))],
    having: [
      { index: lineCount, op: "gt", value: 1 },
      ...having.map((_, i) => ({ index: read.length + i, op: "gt", value: 0 })),
      ...bound(extent, "ge", list.min),
      ...bound(extent, "le", list.max),
      ...bound(start, "ge", from),
      ...bound(start, "le", to),
    ],
    sort: sorts[list.sort ?? ""] ?? newest,
    limit: listLimit,
  };
}

// useTraces is the traces in range under list, gathered by the first of candidates that
// gathers any, and that field; the list is undefined until the candidates are known.
function useTraces(
  client: DataFocalClient,
  range: TimeRange,
  list: List,
  { candidates, ends }: { candidates: readonly string[] | undefined; ends?: Ends },
): { list: Series[] | Error | undefined; field: string | undefined } {
  const tried = candidates?.slice(0, maxTried);
  const listed = useAnswer(tried ? JSON.stringify([range, list, tried, ends]) : null, async () => {
    for (const field of tried ?? []) {
      // The empty key is the lines without the field, which no trace gathers.
      const series = (
        await listing(client, listRequest(range, list, field, ends), () =>
          listRequest(range, list, field),
        )
      ).filter((s) => s.key !== "");
      if (series.length > 0) {
        return { field, series };
      }
    }
    return { field: tried?.[0] ?? "", series: [] };
  });
  if (listed instanceof Error || !listed) {
    return { list: listed, field: candidates?.[0] };
  }
  return { list: listed.series, field: listed.field };
}

// listing is the traces req lists, or where the lines it reads hold no duration to tell a
// trace's root by, as a list scoped to a few services' may not though the range's do, those
// plain lists.
async function listing(
  client: DataFocalClient,
  req: AggregateRequest,
  plain: () => AggregateRequest,
): Promise<Series[]> {
  const answer = await client
    .call("aggregate", req)
    .catch((e: unknown) =>
      req.measures?.length === plain().measures?.length
        ? Promise.reject(e)
        : client.call("aggregate", plain()),
    );
  return answer.data.series;
}

// maxTried is the most fields useTraces tries before saying none gathers a trace.
const maxTried = 4;

// clock is a time's hour, minute and second.
const clock = (ms: number) =>
  Number.isFinite(ms) && ms > 0 ? new Date(ms).toISOString().slice(11, 19) : "";

// rootAt is whether a listed trace's root logged its last line: where that line reports
// lasting about as long as the whole trace, as a root span logged when it ends does; else its
// first line is its root's. A duration logged as text comes back as the measure's label.
const rootAt = (s: Series) => {
  const whole = measured(s, extent);
  return whole > 0 && measured(s, lastReported) >= whole * 0.9;
};

// measured is a listed trace's measure m, read from its label where it is text.
const measured = (s: Series, m: number) =>
  s.measure_values?.[m]?.[0] || Number(s.measure_labels?.[m]?.[0]) || 0;

// rootOf is the service of a listed trace's root and, given an operation field, its operation
// where the root is the span its last line reports: a first: or last: measure reads the first
// or last line holding the field, which at a trace's start, as a log's or a transcript's, may
// be any step's rather than the root's.
function rootOf(
  s: Series,
  ends: Ends | undefined,
): { service: string | undefined; name: string | undefined } {
  const last = ends !== undefined && rootAt(s);
  const label = (m: number) => s.measure_labels?.[m]?.[0] || undefined;
  return {
    service: label(last ? lastService : firstService),
    name: last && ends?.op ? label(lastOp) : undefined,
  };
}

// entryFacts are a listed trace's start, end, lines and services.
function entryFacts(s: Series): readonly [number, number, number, number] {
  const [from = 0, to = 0, , lines = 0, services = 0] =
    s.measure_values?.map((m) => m[0] ?? 0) ?? [];
  return [from, to, lines, services];
}

// Entry is one listed trace: its root's colour, what its root did or else the service it
// began in, and how long it ran, as a bar against the longest listed; then that service and
// its value, when it began, its lines and services, and how many of its lines a search finds.
function Entry(p: {
  s: Series;
  field: string;
  ends: Ends | undefined;
  list: List;
  current: boolean;
  longest: number;
  found: boolean;
}): ReactElement {
  const { from, to, lines, services, title, key, color, matched } = cardOf(p.s, p.ends, p.found);
  return createElement(
    "button",
    {
      type: "button",
      "aria-current": p.current,
      title: p.s.key,
      onClick: () => open(p.field, p.list, p.s.key),
    },
    createElement(
      "span",
      { className: "tr-e" },
      createElement("span", { className: "tr-sq", ref: tint(color) }),
      createElement("span", { className: "tr-root" }, title),
      createElement("span", { className: "tr-dur" }, duration(to - from)),
    ),
    createElement(
      "span",
      { className: "tr-lbar", "aria-hidden": true },
      createElement("span", {
        ref: (el: HTMLElement | null) =>
          el?.style.setProperty("width", `${((to - from) / Math.max(1, p.longest)) * 100}%`),
      }),
    ),
    createElement("span", { className: "tr-key" }, key),
    createElement(
      "span",
      { className: "tr-meta" },
      `${clock(from)} · ${lines} lines · ${services} service${services === 1 ? "" : "s"}`,
      matched !== undefined && createElement("b", null, `${matched} match`),
    ),
  );
}

// cardOf is what listed trace s's card says: its time, lines and services, its title and the
// line under it, its root's colour, and given a search how many of its lines it finds.
function cardOf(s: Series, ends: Ends | undefined, found: boolean) {
  const [from, to, lines, services] = entryFacts(s);
  const root = rootOf(s, ends);
  return {
    from,
    to,
    lines,
    services,
    title: root.name ?? root.service ?? "?",
    key: root.name && root.service ? `${root.service} · ${s.key}` : s.key,
    color: serviceColor(root.service ?? ""),
    matched: found ? (s.measure_values?.at(-1)?.[0] ?? 0) : undefined,
  };
}

// tint sets an element's --c to color.
const tint = (color: string) => (el: HTMLElement | null) => el?.style.setProperty("--c", color);

// TraceList is the traces in range under the list's span filters, gathered by field, a card
// each, longest bar first in the order asked.
function TraceList(p: {
  list: Series[] | Error | undefined;
  filters: List;
  field: string | undefined;
  fields: readonly string[];
  names: readonly string[];
  ends: Ends | undefined;
  current: string | undefined;
}): ReactElement {
  const { field } = p;
  if (field === undefined) {
    return createElement("nav", { className: "tr-list", "aria-label": "Traces" }, "Loading…");
  }
  const count = Array.isArray(p.list) ? p.list.length : undefined;
  return createElement(
    "nav",
    { className: "tr-list", "aria-label": "Traces" },
    createElement(Filters, { field, names: p.names, list: p.filters }),
    createElement(Gather, { field, fields: p.fields, list: p.filters, count }),
    createElement(Show<Series[]>, {
      answer: p.list,
      render: (series) =>
        series.length === 0
          ? createElement("p", null, `No trace of more than one line in this range by ${field}.`)
          : listed(series, { field, list: p.filters, ends: p.ends, current: p.current }),
    }),
  );
}

// listed is a card for each listed trace.
function listed(
  series: readonly Series[],
  p: { field: string; list: List; ends: Ends | undefined; current: string | undefined },
) {
  const longest = Math.max(...series.map((s) => entryFacts(s)[1] - entryFacts(s)[0]));
  const found = search(p.list.q ?? "").length > 0;
  return createElement(
    "div",
    { className: "tr-cards" },
    series.map((s) =>
      createElement(Entry, {
        key: s.key,
        s,
        ...p,
        current: s.key === p.current,
        longest,
        found,
      }),
    ),
  );
}

// pickedFields is the most fields the picker offers to gather traces by, best first.
const pickedFields = 5;

// sortWords name the list's orders.
const sortWords: Readonly<Record<string, string>> = { start: "Newest", duration: "Slowest" };

// Gather is how many traces the list holds, the field gathering them, of the best few, and
// their order.
function Gather(p: {
  field: string;
  fields: readonly string[];
  list: List;
  count: number | undefined;
}): ReactElement {
  const { list } = p;
  const pick = (
    label: string,
    value: string,
    options: readonly [string, string][],
    to: (v: string) => void,
  ) =>
    createElement(
      "select",
      {
        "aria-label": label,
        value,
        onChange: (e: { target: HTMLSelectElement }) => to(e.target.value),
      },
      options.map(([v, text]) => createElement("option", { key: v, value: v }, text)),
    );
  const fields = [...new Set([p.field, ...p.fields.slice(0, pickedFields)])];
  return createElement(
    "div",
    { className: "tr-gather" },
    createElement(
      "span",
      null,
      p.count === undefined ? "Traces" : `${p.count}${p.count === listLimit ? "+" : ""} traces`,
      " by",
    ),
    pick(
      "Gather traces by",
      p.field,
      fields.map((f) => [f, f]),
      (f) => open(f, list),
    ),
    pick("Sort traces by", list.sort ?? "start", Object.entries(sortWords), (v) =>
      open(p.field, { ...list, sort: v }),
    ),
  );
}

// Term is one of the list's span filters as a chip says it, and the list without it.
interface Term {
  readonly field: string;
  readonly op: string;
  readonly value: string;
  readonly without: List;
}

// termsOf are list's filters as chips: the services and fields its traces' lines carry, its
// least and most duration, the start a box narrowed it to, and its search's terms.
function termsOf(list: List): Term[] {
  const where = scope(list.where ?? "");
  const words = [...(list.q ?? "").matchAll(searchTerm)].map((m) => m[0].trim()).filter(Boolean);
  return [
    ...where.map((w, k) => ({
      field: w.field ?? "",
      op: "=",
      value: w.value ?? "",
      without: {
        ...list,
        where: where
          .filter((_, j) => j !== k)
          .map((x) => `${x.field}="${x.value}"`)
          .join(" "),
      },
    })),
    ...(list.min
      ? [{ field: "duration", op: "≥", value: `${list.min} ms`, without: { ...list, min: "" } }]
      : []),
    ...(list.max
      ? [{ field: "duration", op: "≤", value: `${list.max} ms`, without: { ...list, max: "" } }]
      : []),
    ...(list.start
      ? [
          {
            field: "started",
            op: "in",
            value: list.start.split(",").map(Number).map(clock).join("–"),
            without: { ...list, start: "" },
          },
        ]
      : []),
    ...words.map((w, k) => ({
      field: "",
      op: "",
      value: w,
      without: { ...list, q: words.filter((_, j) => j !== k).join(" ") },
    })),
  ];
}

// ops are the span filter's comparisons, by what the editor offers, as a search writes each.
const ops: readonly (readonly [string, string])[] = [
  ["=", ":"],
  ["≠", "!"],
  [">", ">"],
  ["≥", ">="],
  ["<", "<"],
  ["≤", "<="],
  ["contains", "~"],
];

// added is list with the span filter field op value: a service or a duration bound as the list
// keeps them, any other field as a search term, and a bare word as a word searched for.
export function added(list: List, field: string, op: string, value: string): List {
  const q = `${list.q ?? ""} ${termOf(field, op, value)}`.trim();
  return kept(list, field, op, value) ?? { ...list, q };
}

// kept is list with a filter it keeps apart from its search: a service, or a duration bound
// in ms, or s with an s; undefined for any other.
function kept(list: List, field: string, op: string, value: string): List | undefined {
  const ms = String(Number.parseFloat(value) * (/\ds$/.test(value) ? 1000 : 1));
  if (field === "service" && op === ":") {
    return { ...list, where: `${list.where ?? ""} service="${value}"`.trim() };
  }
  if (field !== "duration") {
    return undefined;
  }
  if (op.startsWith(">")) {
    return { ...list, min: ms };
  }
  return op.startsWith("<") ? { ...list, max: ms } : undefined;
}

// termOf is a search term comparing field with value by op, or for no field the word value.
function termOf(field: string, op: string, value: string): string {
  const v = /^[^\s"]+$/.test(value) ? value : JSON.stringify(value);
  if (!field) {
    return v;
  }
  return op === "!" ? `-${field}:${v}` : `${field}${op}${v}`;
}

// Chips are the list's span filters, each removable.
function Chips({ field, list }: { field: string; list: List }): ReactElement {
  return createElement(
    "ul",
    { className: "tr-chips", "aria-label": "Span filters" },
    termsOf(list).map((t) =>
      createElement(
        "li",
        { key: `${t.field}${t.op}${t.value}` },
        t.field && createElement("span", null, t.field),
        t.op && createElement("b", null, t.op),
        t.value,
        createElement(
          "button",
          {
            type: "button",
            "aria-label": `Remove ${t.field} ${t.op} ${t.value}`.trim(),
            onClick: () => open(field, t.without),
          },
          "×",
        ),
      ),
    ),
  );
}

// Filters are the list's span filters as chips, each removable, and an editor adding one:
// a field its lines carry, a comparison and a value.
function Filters(p: { field: string; names: readonly string[]; list: List }): ReactElement {
  const { list } = p;
  const submit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const form = new FormData(e.currentTarget);
    const get = (k: string) => {
      const v = form.get(k);
      return typeof v === "string" ? v.trim() : "";
    };
    if (get("value")) {
      open(p.field, added(list, get("field"), get("op") || ":", get("value")));
    }
  };
  return createElement(
    "div",
    { className: "tr-filters" },
    createElement("h2", null, "Traces with a span where"),
    createElement(Chips, { field: p.field, list }),
    createElement(
      "form",
      { key: JSON.stringify(list), className: "tr-add", onSubmit: submit },
      createElement("input", {
        name: "field",
        list: "tr-fields",
        "aria-label": "Field",
        placeholder: "field",
      }),
      createElement(
        "datalist",
        { id: "tr-fields" },
        ["service", "duration", ...p.names].map((n) =>
          createElement("option", { key: n, value: n }),
        ),
      ),
      createElement(
        "select",
        { name: "op", "aria-label": "Comparison" },
        ops.map(([text, op]) => createElement("option", { key: op, value: op }, text)),
      ),
      createElement("input", { name: "value", "aria-label": "Value", placeholder: "value" }),
      createElement("button", { type: "submit", className: "tbtn" }, "Add"),
    ),
  );
}
