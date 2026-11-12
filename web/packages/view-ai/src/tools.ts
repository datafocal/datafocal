import type {
  AggregateRequest,
  AggregateResponse,
  MeasureSpec,
  Predicate,
  Series,
} from "@datafocal/api";
import { logql } from "@datafocal/query";
import { Absent, Meter } from "@datafocal/ui";
import { createElement, type ReactElement } from "react";
import type { Field, Fields } from "./fields";
import {
  breakdown,
  button,
  failure,
  grouped,
  measures,
  num,
  Panel,
  pct,
  type Row,
  type Scope,
  selectorOf,
  sliceQuery,
  spec,
  sum,
  type TabProps,
  Table,
  tokens,
  total,
  useAggregate,
  useRunCount,
  valueAt,
} from "./panel";
import { bucketAxis, Crumbs, Facts, Lede, Plot } from "./parts";
import { inMs, ms } from "./stats";

// output is the column a tool call's output length is read from: the canonical length of
// tool content, else the length of the result block.
const output = (f: Fields): string | undefined => f.toolOutput;

// answered keeps a measure to the one line answering each call, so a call counts once and
// its size is its output's, not its input's.
const answered = (f: Fields, m: MeasureSpec): MeasureSpec =>
  f.callId ? { ...m, where: [{ field: f.callId.name, op: "exists" }] } : m;

const ledgerMeasures = (f: Fields) => {
  const out = output(f);
  return measures({
    calls: answered(f, { measure: "count" }),
    errors: f.toolError && {
      measure: "count",
      where: [{ field: f.toolError.name, op: "eq", value: "true" }],
    },
    p95: spec("p95", f.duration),
    p50out: out ? answered(f, { measure: `p50:${out}` }) : undefined,
    p95out: out ? answered(f, { measure: `p95:${out}` }) : undefined,
    out: out ? answered(f, { measure: `sum:${out}` }) : undefined,
    runs: spec("distinct", f.run),
  });
};

const ledgerRequest = (f: Fields): Partial<AggregateRequest> | null =>
  f.tool ? { group_by: f.tool.name, measures: ledgerMeasures(f).list, bins: "all" } : null;

// spans times each group of by by the span of its lines, rolled up by kind over the groups,
// or per label of a second measure. A turn's lines share its turn field, and a tool call's
// lines its toolCall field, so these time them when no duration column does.
const spans = (by: Field, ms: string[], kind: string): Partial<AggregateRequest> => ({
  group_by: by.name,
  measures: ms.map((measure) => ({ measure })),
  bins: "all",
  limit: 10_000,
  rollup: {
    measure: `${kind}:values[0]`,
    bins: "all",
    ...(ms.length > 1 && { group_by: "label[1]" }),
  },
});
const turnTime = (f: Fields, kind: string): Partial<AggregateRequest> | null =>
  f.turn ? spans(f.turn, [`span:${f.turn.name}`], kind) : null;
const callTime = (f: Fields, kind: string) =>
  f.toolCall && f.tool
    ? spans(f.toolCall, [`span:${f.toolCall.name}`, `first:${f.tool.name}`], kind)
    : null;

// ToolStat is a tool's line of the ledger.
export interface ToolStat {
  readonly tool: string;
  readonly calls: number;
  readonly errors: number;
  readonly p95: number;
  readonly p50out: number;
  readonly p95out: number;
  readonly out: number;
  readonly runs: number;
}

// useTools is each tool's numbers, largest output first, with p95 time from its calls'
// lines when no duration column reports it.
export function useTools(scope: Scope): readonly ToolStat[] | Error | undefined {
  const f = scope.fields;
  const m = ledgerMeasures(f);
  const answer = useAggregate(scope, ledgerRequest(f));
  // A duration the logs report may be on no tool's lines, so each call is also timed by the
  // span of its lines, read where no duration is.
  const calls = useAggregate(scope, callTime(f, "p95"));
  if (answer instanceof Error || !answer) {
    return answer;
  }
  const timed = calls instanceof Error ? undefined : calls;
  const p95 = (key: string) => {
    const s = timed?.series.find((c) => c.key === key);
    return s ? (valueAt(s, 0, 0) ?? Number.NaN) : Number.NaN;
  };
  const stat = (s: Series): ToolStat => {
    const get = (k: Parameters<typeof m.at>[0]) =>
      m.at(k) < 0 ? Number.NaN : (valueAt(s, m.at(k), 0) ?? 0);
    const reported = m.at("p95") >= 0 ? (valueAt(s, m.at("p95"), 0) ?? null) : null;
    const p = reported ? inMs(f.duration?.name ?? "", reported) : p95(s.key);
    return {
      tool: s.key,
      calls: get("calls"),
      errors: get("errors"),
      p95: p,
      p50out: get("p50out"),
      p95out: get("p95out"),
      out: get("out"),
      runs: get("runs"),
    };
  };
  return grouped(answer, "")[0]
    .series.map(stat)
    .sort((a, b) => (b.out || b.calls) - (a.out || a.calls));
}

const errorRate = (t: ToolStat) => (t.calls ? t.errors / t.calls : Number.NaN);

function ledgerRows(
  ts: readonly ToolStat[],
  totalRuns: number,
  open: TabProps["open"],
  scope: Scope,
): Row[] {
  const allOut = sum(ts.map((t) => t.out || 0)) || 1;
  const top = ts[0]?.out || 1;
  return ts.map((t) => ({
    key: t.tool,
    cells: [
      String(t.calls),
      pct(errorRate(t)),
      ms(t.p95),
      `${tokens(t.p50out)} / ${tokens(t.p95out)}`,
      createElement(Meter, { text: tokens(t.out), share: t.out / top }),
      pct(t.out / allOut),
      totalRuns ? pct(t.runs / totalRuns) : "—",
    ],
    query: sliceQuery(scope, scope.fields.tool, t.tool),
    onPick: () => open("tools", t.tool),
  }));
}

const ledgerColumns = [
  "Tool",
  "Calls",
  "Error rate",
  "p95 time",
  "Output p50 / p95",
  "Output tokens",
  "Share of output",
  "Runs using it",
];

function Ledger(p: TabProps): ReactElement {
  const tools = useTools(p.scope);
  const totalRuns = useRunCount(p.scope);
  return createElement(Panel<readonly ToolStat[]>, {
    title: "Tool ledger",
    answer: p.scope.fields.tool ? tools : new Absent("no tool name on any line"),
    render: (ts) => [
      createElement(
        "p",
        { key: "w", className: "ai-note" },
        "Sorted by output: what each tool hands back is re-sent on every later call until the context is compacted. Tokens are estimated at four characters each.",
      ),
      createElement(Table, {
        key: "t",
        label: "Tools",
        columns: ledgerColumns,
        rows: ledgerRows(ts, totalRuns, p.open, p.scope),
      }),
    ],
  });
}

// TurnTime splits turn time by the duration columns, or when the logs report none, into each
// tool's calls and the rest, the model and other.
function TurnTime({ scope }: { scope: Scope }): ReactElement {
  const f = scope.fields;
  const reported = f.durations.length > 0;
  const durations = useAggregate(
    scope,
    reported ? { measures: f.durations.map((d) => ({ measure: `sum:${d}` })), bins: "all" } : null,
  );
  const turns = useAggregate(scope, reported ? null : turnTime(f, "sum"));
  const calls = useAggregate(scope, reported ? null : callTime(f, "sum"));
  const rows = (r: AggregateResponse): Row[] => {
    if (reported) {
      const totals = f.durations.map((_, i) => total(r, i));
      return f.durations.map((d, i) => ({
        key: d,
        cells: [ms(totals[i] ?? 0), pct((totals[i] ?? 0) / (sum(totals) || 1))],
        query: logql([...scope.filters, { field: d, op: "gt", value: "0" }]),
      }));
    }
    const all = total(r);
    const tools = calls instanceof Error || !calls ? [] : breakdown(grouped(calls, "")[0]);
    const row = (key: string, v: number, query: string) => ({
      key,
      cells: [ms(v), pct(v / (all || 1))],
      query,
    });
    return [
      ...tools.map((t) => row(t.key, t.value, sliceQuery(scope, f.tool, t.key))),
      row("model and other", all - sum(tools.map((t) => t.value)), logql(scope.filters)),
    ];
  };
  return createElement(Panel<AggregateResponse>, {
    title: "Where turn time goes",
    answer: reported ? durations : calls && (failure(calls) ?? turns),
    render: (r) =>
      createElement(Table, {
        label: "Turn time",
        columns: [reported ? "Duration" : "Spent in", "Total", "Share"],
        rows: rows(r),
      }),
  });
}

export function ToolsTab(p: TabProps): ReactElement {
  if (p.picked !== undefined) {
    return createElement(ToolPage, { ...p, tool: p.picked });
  }
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Lede, {
      title: "Tools",
      text: "Each tool's calls, error rate, time and output size, from the lines that log its calls and results.",
    }),
    createElement(Ledger, p),
    createElement(TurnTime, { scope: p.scope }),
  );
}

const sizeBuckets = [0, 500, 1e3, 2e3, 5e3, 10e3, 20e3, 50e3];

// perCall reads each call of tool's output, labelled with its run.
const perCallRequest = (f: Fields): Partial<AggregateRequest> | null => {
  const out = output(f);
  return f.callId && out && f.run
    ? {
        group_by: f.callId.name,
        measures: [{ measure: `sum:${out}` }, { measure: `last:${f.run.name}` }],
        bins: "all",
        limit: 10_000,
        sort: "0:desc",
      }
    : null;
};

function ToolPage(p: TabProps & { tool: string }): ReactElement {
  const f = p.scope.fields;
  const tools = useTools(p.scope);
  const where: Predicate[] = [
    ...(selectorOf(p.scope).where ?? []),
    { field: f.tool?.name ?? "", op: "eq", value: p.tool },
  ];
  const calls = useAggregate(p.scope, perCallRequest(f), { ...selectorOf(p.scope), where });
  const t = tools instanceof Error || !tools ? undefined : tools.find((x) => x.tool === p.tool);
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Crumbs, { steps: [["Tools", () => p.open("tools")], [p.tool]] }),
    t && createElement(ToolFacts, { t, ...p }),
    createElement(Panel<AggregateResponse>, {
      title: "Output of each call",
      answer: perCallRequest(f) ? calls : new Absent("no tool call id to tell its calls apart"),
      render: (r) => createElement(Sizes, { r, open: p.openRun }),
    }),
  );
}

function ToolFacts(p: TabProps & { t: ToolStat }): ReactElement {
  const { t } = p;
  return createElement(
    "div",
    null,
    createElement(Facts, {
      facts: [
        ["Calls", num(t.calls)],
        ["Error rate", pct(errorRate(t))],
        ["p95 time", ms(t.p95)],
        ["Output p50 / p95", `${tokens(t.p50out)} / ${tokens(t.p95out)}`, "tokens"],
        ["Output", tokens(t.out), "tokens in all"],
      ],
    }),
    button("Compare runs that call it ›", () =>
      p.compare({ field: p.scope.fields.tool?.name ?? "", value: t.tool }),
    ),
  );
}

// Sizes is how many calls hand back each size of output, how much output each size adds up
// to, and the largest calls, each opening its run.
function Sizes(p: { r: AggregateResponse; open: (run: string) => void }): ReactElement {
  const calls = grouped(p.r, "")[0].series.map((s) => ({
    out: valueAt(s, 0, 0) ?? 0,
    run: s.measure_labels?.[1]?.[0] ?? "",
    key: s.key,
  }));
  const bucket = (chars: number) => sizeBuckets.filter((lo) => chars / 4 >= lo).length - 1;
  const counts = sizeBuckets.map((_, i) => calls.filter((c) => bucket(c.out) === i).length);
  const outs = sizeBuckets.map((_, i) =>
    sum(calls.filter((c) => bucket(c.out) === i).map((c) => c.out / 4)),
  );
  const x = bucketAxis(
    "output tokens per call",
    sizeBuckets.map((b) => `${num(b)}+`),
  );
  const xs = sizeBuckets.map((_, i) => i);
  return createElement(
    "div",
    null,
    createElement(
      "div",
      { className: "g2" },
      createElement(Plot, {
        kind: "bars",
        xs,
        ys: [counts],
        series: ["calls"],
        label: "Calls by output size",
        x,
        y: { label: "calls", format: (v) => String(Math.round(v)) },
      }),
      createElement(Plot, {
        kind: "bars",
        xs,
        ys: [outs],
        series: ["output tokens"],
        label: "Output tokens by output size",
        x,
        y: { label: "output tokens", format: (v) => tokens(v * 4) },
      }),
    ),
    createElement(Table, {
      label: "Largest outputs",
      columns: ["Call", "Output tokens", "Run"],
      rows: calls
        .slice(0, 10)
        .map((c) => ({ key: c.key, cells: [tokens(c.out), c.run], onPick: () => p.open(c.run) })),
    }),
  );
}
