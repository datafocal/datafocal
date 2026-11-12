import type { LogLine, Predicate, Span, TraceResponse } from "@datafocal/api";
import { agentStep, firstValue } from "@datafocal/ui";

// A conversation is the derived or instant spans of a service whose lines carry a message
// role, as a transcript's are, put into turns of one prompt each and named by what they say.
// Spans reporting their own duration, as calls do, are left be.

// Talk is a get_trace answer with its conversations put into turns, and what the view shows
// of each span by its id: a note in place of its service, whether it says nothing, and
// whether it is a turn.
export interface Talk {
  readonly res: TraceResponse;
  // owner is the span, by id, of each line a conversation's step holds though no id names it.
  readonly owner: ReadonlyMap<LogLine, string>;
  readonly notes: ReadonlyMap<string, string>;
  readonly quiet: ReadonlySet<string>;
  readonly turns: ReadonlySet<string>;
  // kinds are what each step is, by id: a person's prompt (user), the model thinking, a tool
  // call or result, the model's reply (assistant), or a harness's note.
  readonly kinds: ReadonlyMap<string, StepKind>;
  // totals sum the conversations' turns: their steps, what their model calls cost, and the
  // tools they called, most first.
  readonly totals: Totals;
}

export type StepKind = "user" | "thinking" | "call" | "result" | "assistant" | "note";

export interface Totals {
  readonly steps: number;
  readonly cost: number;
  readonly tools: readonly (readonly [string, number])[];
}

const none: Totals = { steps: 0, cost: 0, tools: [] };

const role = "content_role";
// container is a span holding others rather than a step: the span get_trace gathers a
// service's lines naming no span into, or a group of repeated steps. In a conversation it
// holds records, not the turns it overlaps.
const container = (s: Span | undefined) => s?.id === `svc:${s?.service}` || s?.group !== undefined;

// callKey is a field naming the model call a line is part of.
const callKey = /(^|\.)(message|request|response)[._]?id$/i;

// owners is the span each line backs: the one its id names, else its service's, as readTrace
// reads them.
export function owners(spans: readonly Span[], lines: readonly LogLine[], id: string): number[] {
  const byKey = new Map<string, number>();
  spans.forEach((s, i) => {
    for (const p of s.where[0]?.any ?? s.where.slice(0, 1)) {
      byKey.set(`${p.field}=${p.value}`, i);
    }
  });
  return lines.map(
    (l) => byKey.get(`${id}=${l.attributes?.[id]}`) ?? byKey.get(`service=${l.service}`) ?? -1,
  );
}

const flat = (s: string, n = 160) => {
  const one = s.replace(/\s+/g, " ").trim();
  return one.length > n ? `${one.slice(0, n - 1)}…` : one;
};

const isResult = (l: LogLine) => l.attributes?.["tool_call_id"] !== undefined;
const isCall = (l: LogLine) => agentStep(l)?.kind === "call";
// isNote is a line of the harness's own in a conversation: a system message, or a user's line
// that is neither a person's message nor a tool's result, as a reminder or an interruption is.
const isNote = (l: LogLine) => {
  const who = l.attributes?.[role];
  return who === undefined || who === "system" || (who === "user" && !isResult(l) && !isPrompt(l));
};

// message is the text of a line's own message, not of a file or reminder it attaches.
const message = /(^|\.)(message|content|prompt|text)$/;
const spoken = (l: LogLine) =>
  Object.entries(l.attributes ?? {}).find(
    ([k, v]) => message.test(k) && !k.includes("attachment") && firstValue(v).trim() !== "",
  )?.[1];

// isPrompt is a person's message: a user's line answering no tool call, flagged as no
// harness note, whose text a harness did not wrap in a tag or brackets, as it does reminders
// and interruptions.
function isPrompt(l: LogLine): boolean {
  const a = l.attributes ?? {};
  const meta = Object.entries(a).some(([k, v]) => /(^|\.)is_?meta$/i.test(k) && v === "true");
  const text = firstValue(spoken(l)).trim();
  return a[role] === "user" && !isResult(l) && !isCall(l) && !meta && /^[^<[]/.test(text);
}

// stepName says what a conversation's line does (@datafocal/ui's agentStep).
const stepName = (l: LogLine) => agentStep(l)?.label ?? "";

const compact = new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 });
const num = (l: LogLine, f: string) => Number(l.attributes?.[f] ?? 0) || 0;

// Call is one model call's spend and the input it sent, cache included.
interface Call {
  readonly cost: number;
  readonly input: number;
}

// calls are the model calls lines log, each once where several lines name the same call.
function calls(lines: readonly LogLine[], seen = new Set<string>()): Call[] {
  return lines.flatMap((l) => {
    if (l.attributes?.["input_tokens"] === undefined) {
      return [];
    }
    const id = Object.entries(l.attributes).find(([k]) => callKey.test(k))?.[1];
    if (id !== undefined && seen.has(id)) {
      return [];
    }
    if (id !== undefined) {
      seen.add(id);
    }
    const input =
      num(l, "input_tokens") + num(l, "cache_read_tokens") + num(l, "cache_write_tokens");
    return [{ cost: num(l, "cost_usd"), input }];
  });
}

const usd = (v: number) => `$${v < 1 ? v.toFixed(3) : v.toFixed(2)}`;

// stepNote is what a step is, or for the first line of a model call, what the call cost and
// how much input it sent.
function stepNote(l: LogLine, cs: readonly Call[]): string {
  const c = cs[0];
  if (c) {
    return `${c.cost > 0 ? `${usd(c.cost)} · ` : ""}${compact.format(c.input)} in`;
  }
  return isCall(l) ? "tool call" : isResult(l) ? "tool result" : (l.attributes?.[role] ?? "");
}

// totalsOf sums steps: those but the harness's notes, what their model calls cost, and the
// tools they called, most first.
function totalsOf(steps: readonly (readonly LogLine[])[]): Totals {
  const lines = steps.flat();
  const tools = new Map<string, number>();
  for (const l of lines.filter(isCall)) {
    const t = firstValue(l.attributes?.["tool_name"]);
    tools.set(t, (tools.get(t) ?? 0) + 1);
  }
  return {
    steps: steps.filter((ls) => ls[0] && !isNote(ls[0])).length,
    cost: calls(lines).reduce((n, c) => n + c.cost, 0),
    tools: [...tools].sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1)),
  };
}

// turnNote is what a turn did: its steps but the harness's notes, the tools it called most,
// and what its calls cost.
function turnNote(steps: readonly (readonly LogLine[])[]): string {
  const { steps: said, cost, tools } = totalsOf(steps);
  const top = tools.slice(0, 3).map(([t, n]) => `${t} ×${n}`);
  return [`${said} steps`, ...top, ...(cost > 0 ? [usd(cost)] : [])].join(" · ");
}

// kindOf is what a conversation's line l does.
function kindOf(l: LogLine): StepKind {
  if (isResult(l)) {
    return "result";
  }
  if (isCall(l)) {
    return "call";
  }
  if (isPrompt(l)) {
    return "user";
  }
  if (isNote(l)) {
    return "note";
  }
  return stepName(l) === "thinking" ? "thinking" : "assistant";
}

// Turn is the spans of one prompt, in time order.
interface Turn {
  readonly steps: number[];
}

// Ctx is what a conversation is read from: the answer, each span's lines, the spans holding
// others, and the conversation's steps.
interface Ctx {
  readonly res: TraceResponse;
  readonly byspan: readonly (readonly LogLine[])[];
  readonly parents: ReadonlySet<number>;
  readonly grouped: ReadonlySet<number>;
}

const parentOf = (c: Ctx, i: number) => c.res.spans[i]?.parent ?? -1;

// outermost is the outermost of the containers holding container i.
function outermost(c: Ctx, i: number): number {
  let up = i;
  while (container(c.res.spans[parentOf(c, up)])) {
    up = parentOf(c, up);
  }
  return up;
}

// thread is the root of span i below any container: one transcript's chain, as an agent's
// and each subagent's are. A step holding nothing directly in a container, as get_trace
// derives from lines naming no span, is of its outermost container's thread.
function thread(c: Ctx, i: number): number {
  let at = i;
  while (parentOf(c, at) >= 0 && !container(c.res.spans[parentOf(c, at)])) {
    at = parentOf(c, at);
  }
  const up = parentOf(c, at);
  return c.parents.has(at) || up < 0 ? at : outermost(c, up);
}

const lineMs = (l: LogLine) => Date.parse(l.timestamp);

// patternSteps are the steps get_trace derived from runs of one pattern's lines, by service
// and pattern.
function patternSteps(res: TraceResponse): Map<string, number[]> {
  const byPattern = new Map<string, number[]>();
  res.spans.forEach((s, i) => {
    const p = s.where[0];
    if (!container(s) && p?.field === "pattern_id") {
      const key = `${s.service}/${p.value}`;
      byPattern.set(key, [...(byPattern.get(key) ?? []), i]);
    }
  });
  return byPattern;
}

// stepsOf are the lines each span holds: those its id names, and for a step get_trace derived
// from a run of one pattern's lines, which no id names, those of its pattern from its start on.
function stepsOf(res: TraceResponse, lines: readonly LogLine[]): LogLine[][] {
  const owner = owners(res.spans, lines, res.id_field ?? "span_id");
  const byspan = res.spans.map((): LogLine[] => []);
  const byPattern = patternSteps(res);
  const start = (i: number) => res.spans[i]?.start_ms ?? 0;
  lines.forEach((l, k) => {
    const held = owner[k] ?? -1;
    const steps = container(res.spans[held])
      ? (byPattern.get(`${l.service}/${l.pattern_id}`) ?? [])
      : [];
    const step = steps.filter((i) => start(i) <= lineMs(l)).sort((a, b) => start(b) - start(a))[0];
    byspan[step ?? held]?.push(l);
  });
  return byspan;
}

// turnsOf splits each thread's steps, in time order, into turns, each from one person's
// message to the next; a harness's note starts no thread but joins the turn open when logged.
function turnsOf(c: Ctx, order: readonly number[]): Turn[] {
  const turns: Turn[] = [];
  const current = new Map<number, Turn>();
  for (const i of order) {
    const l = c.byspan[i]?.[0];
    const root = thread(c, i);
    const open = current.get(root) ?? (!l || isNote(l) ? turns.at(-1) : undefined);
    if (open && !(l && isPrompt(l))) {
      open.steps.push(i);
      continue;
    }
    const t = { steps: [i] };
    turns.push(t);
    current.set(root, t);
  }
  return turns;
}

// turnName is what a turn's prompt said, or what it began with when it carries on one the
// lines read do not hold, as one begun before the range or after a compaction does.
function turnName(n: number, lines: readonly LogLine[]): string {
  const prompt = lines.find(isPrompt);
  const opening = lines.find((l) => !isNote(l) && stepName(l) !== "thinking");
  if (prompt) {
    return `${n}. ${flat(firstValue(spoken(prompt)))}`;
  }
  return `${n}. ${opening ? `continued · ${stepName(opening)}` : "session setup"}`;
}

// turnParent is the span the turn's first step was under, past the steps and containers.
function turnParent(c: Ctx, step: number): number {
  let parent = parentOf(c, step);
  while (c.grouped.has(parent) || container(c.res.spans[parent])) {
    parent = parentOf(c, parent);
  }
  return parent;
}

// turnSpan is the span standing for turn n: named by its prompt, lasting until its thread's
// next turn starts, until, or else its last step, and holding its trace's lines, as no field
// names a turn.
function turnSpan(c: Ctx, t: Turn, n: number, until: number | undefined): Span {
  const steps = t.steps.flatMap((i) => c.res.spans[i] ?? []);
  const start = Math.min(...steps.map((s) => s.start_ms));
  const end = until ?? Math.max(...steps.map((s) => s.start_ms));
  const parent = turnParent(c, t.steps[0] ?? -1);
  const where: Predicate[] = steps[0]?.where.slice(1) ?? [];
  return {
    name: turnName(
      n,
      t.steps.flatMap((i) => c.byspan[i] ?? []),
    ),
    service: steps[0]?.service ?? "",
    start_ms: start,
    dur_ms: Math.max(0, end - start),
    self_ms: 0,
    error: steps.some((s) => s.error),
    parent,
    id: `turn:${n}`,
    parent_method: parent < 0 ? "" : "contained",
    duration_method: "observed",
    lines: steps.reduce((k, s) => k + s.lines, 0),
    where,
    critical: false,
    critical_inferred: false,
  };
}

// conversing are the services whose derived or instant spans carry a message, and those
// spans, in time order: the conversations' steps.
function conversing(res: TraceResponse, byspan: readonly (readonly LogLine[])[]) {
  const derived = (s: Span) => s.duration_method !== "reported";
  const talks = new Set(
    res.spans.flatMap((s, i) =>
      derived(s) && byspan[i]?.some((l) => l.attributes?.[role]) ? [s.service] : [],
    ),
  );
  const order = res.spans
    .flatMap((s, i) => (derived(s) && !container(s) && talks.has(s.service) ? [i] : []))
    .sort((a, b) => (res.spans[a]?.start_ms ?? 0) - (res.spans[b]?.start_ms ?? 0) || a - b);
  return { talks, order };
}

// Placed is a span in its new place: its old index, or the turn it stands for.
type Placed = Span & { readonly old: number; readonly turn: number };

// placed is the spans the conversations leave, then each turn and its steps in time order, so
// they read in the order they happened.
function placed(c: Ctx, turns: readonly Turn[], made: readonly Span[]): Placed[] {
  const out: Placed[] = c.res.spans.flatMap((s, i) =>
    c.grouped.has(i) ? [] : [{ ...s, old: i, turn: -1 }],
  );
  turns.forEach((t, n) => {
    const turn = made[n];
    if (turn) {
      out.push({ ...turn, old: -1, turn: n });
    }
    for (const i of t.steps) {
      const s = c.res.spans[i];
      if (s) {
        out.push({ ...s, old: i, turn: -1 });
      }
    }
  });
  return out;
}

// Shown is what converse tells readTrace of each span, filled in as the spans are placed.
interface Shown {
  readonly notes: Map<string, string>;
  readonly quiet: Set<string>;
  readonly billed: Set<string>;
  readonly kinds: Map<string, StepKind>;
}

// step is a conversation's step in its turn n's span: named by what its first line does, its
// note its model call or what it is, and quiet when it is a record or a harness's note.
function step(c: Ctx & { shown: Shown }, s: Span, old: number, parent: number): Span {
  const shown = c.shown;
  const l = c.byspan[old]?.[0];
  if (!l) {
    shown.notes.set(s.id, "no line read");
    return { ...s, parent };
  }
  if (l.attributes?.[role] === undefined) {
    shown.quiet.add(s.id);
    shown.notes.set(s.id, s.name);
    return { ...s, parent };
  }
  if (isNote(l)) {
    shown.quiet.add(s.id);
  }
  shown.kinds.set(s.id, kindOf(l));
  shown.notes.set(s.id, stepNote(l, calls(c.byspan[old] ?? [], shown.billed)));
  return { ...s, name: stepName(l), parent };
}

// converse puts res's conversations into turns, and names every step by what its first line
// says.
export function converse(res: TraceResponse, lines: readonly LogLine[]): Talk {
  const byspan = stepsOf(res, lines);
  const { talks, order } = conversing(res, byspan);
  if (order.length === 0) {
    const blank = { notes: new Map(), quiet: new Set<string>(), kinds: new Map() };
    return { res, owner: new Map(), ...blank, turns: new Set(), totals: none };
  }
  const parents = new Set(res.spans.map((s) => s.parent));
  const c: Ctx = { res, byspan, parents, grouped: new Set(order) };
  const turns = turnsOf(c, order);
  const next = (n: number) => {
    const root = thread(c, turns[n]?.steps[0] ?? -1);
    const later = turns.slice(n + 1).find((t) => thread(c, t.steps[0] ?? -1) === root);
    return later && res.spans[later.steps[0] ?? -1]?.start_ms;
  };
  const made = turns.map((t, n) => turnSpan(c, t, n + 1, next(n)));
  const out = placed(c, turns, made);
  const at = new Map(out.flatMap((s, k) => (s.old >= 0 ? [[s.old, k] as const] : [])));
  const turnAt = new Map(out.flatMap((s, k) => (s.turn >= 0 ? [[s.turn, k] as const] : [])));
  const turnOf = new Map(turns.flatMap((t, n) => t.steps.map((i) => [i, n])));
  const shown: Shown = { notes: new Map(), quiet: new Set(), billed: new Set(), kinds: new Map() };
  const spans = out.map(({ old, turn, ...s }): Span => {
    if (turn >= 0) {
      shown.notes.set(s.id, turnNote(turns[turn]?.steps.map((i) => byspan[i] ?? []) ?? []));
    }
    const n = turnOf.get(old);
    return n === undefined
      ? { ...s, parent: at.get(s.parent) ?? -1 }
      : step({ ...c, shown }, s, old, turnAt.get(n) ?? -1);
  });
  for (const s of spans.filter((x) => container(x) && talks.has(x.service))) {
    shown.quiet.add(s.id);
    shown.notes.set(s.id, s.group ? `${s.group.count} steps alike` : "records naming no step");
  }
  const owner = new Map(
    order.flatMap((i) => (byspan[i] ?? []).map((l) => [l, res.spans[i]?.id ?? ""] as const)),
  );
  const totals = totalsOf(order.map((i) => byspan[i] ?? []));
  const turnIds = new Set(made.map((s) => s.id));
  return { res: { ...res, spans }, owner, ...shown, turns: turnIds, totals };
}
