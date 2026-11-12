import { type AggregateRequest, type AggregateResponse, binStarts } from "@datafocal/api";
import { Absent, Meter } from "@datafocal/ui";
import { createElement, type ReactElement, useState } from "react";
import type { Fields, Kind } from "./fields";
import {
  inputAt,
  money,
  num,
  Panel,
  pct,
  type Row,
  type Scope,
  sum,
  type Tab,
  type TabProps,
  Table,
  type Usage,
  usageMeasure,
  useAggregate,
  useUsage,
  valueAt,
} from "./panel";
import { type Fact, Facts, Lede, type Part, Plot, Stack } from "./parts";
import { RunsBy } from "./runlist";
import {
  type AfterCompaction,
  quantile,
  type RunStat,
  useAfterCompaction,
  useRunStats,
} from "./stats";

// Contributor is a part of what fills the window: one kind of block from one role, and its
// share of all content; tool content opens the tool ledger.
export interface Contributor extends Part {
  readonly chars: number;
  readonly to?: Tab;
}

// names say what each kind of content is by whose message holds it. Tool content in the
// model's own message is a call's input; anywhere else it is a result handed back.
const names: Readonly<Record<string, string>> = {
  "tool/result": "Tool results",
  "tool/call": "Tool call inputs",
  thinking: "Thinking",
  "text/assistant": "Assistant replies",
  "text/user": "User messages",
  "text/system": "System prompt",
};

function nameOf(kind: Kind, role: string): string {
  const key =
    kind === "tool"
      ? role === "assistant"
        ? "tool/call"
        : "tool/result"
      : kind === "thinking"
        ? kind
        : `${kind}/${role}`;
  return names[key] ?? `${role || "unattributed"} ${kind}`;
}

// makeupRequest sums each kind of block's length by the role of the message holding it.
export const makeupRequest = (f: Fields): Partial<AggregateRequest> | null => {
  const kinds = Object.values(f.kinds);
  return f.contentRole && kinds.length
    ? {
        group_by: f.contentRole.name,
        measures: kinds.map((k) => ({ measure: `sum:${k}` })),
        bins: "all",
      }
    : null;
};

// contributors are r's role and kind cells, largest first, as shares of all content.
export function contributors(r: AggregateResponse, f: Fields): Contributor[] {
  const kinds = Object.keys(f.kinds).filter((k): k is Kind => k in f.kinds);
  const cells = r.series.flatMap((s) =>
    kinds.map((kind, i) => ({ kind, role: s.key, chars: valueAt(s, i, 0) ?? 0 })),
  );
  const all = sum(cells.map((c) => c.chars)) || 1;
  const merged = new Map<string, Contributor>();
  for (const c of cells.filter((x) => x.chars > 0)) {
    const label = nameOf(c.kind, c.role);
    const chars = (merged.get(label)?.chars ?? 0) + c.chars;
    const share = chars / all;
    merged.set(label, {
      key: label,
      label,
      share,
      title: `${label}: ${pct(share)}`,
      chars,
      ...(c.kind === "tool" && { to: "tools" as const }),
    });
  }
  return [...merged.values()].sort((a, b) => b.chars - a.chars);
}

// Makeup is the window's contributors, each scaled from its share of content to the input
// the calls were billed for, so the parts add up to the Tokens the KPIs show.
function Makeup(p: { scope: Scope; usage: Usage; open: TabProps["open"] }): ReactElement {
  const f = p.scope.fields;
  const [picked, setPicked] = useState<string>();
  const answer = useAggregate(p.scope, makeupRequest(f));
  const billed = sum(p.usage.input);
  const rows = (parts: readonly Contributor[]): Row[] =>
    parts.map((c) => ({
      key: c.label,
      cells: [
        pct(c.share),
        createElement(Meter, {
          text: num(c.share * billed),
          share: c.share / (parts[0]?.share || 1),
        }),
      ],
      ...(c.to && { onPick: () => p.open(c.to ?? "tools") }),
    }));
  return createElement(Panel<AggregateResponse>, {
    title: "Input by contributor",
    answer: makeupRequest(f) ? answer : missing,
    render: (r) => {
      const parts = contributors(r, f);
      return [
        createElement(
          "p",
          { key: "w", className: "ai-note" },
          `Shares come from the length of each block of content, scaled to the ${num(billed)} input tokens billed (${p.usage.source}).`,
        ),
        createElement(Stack, { key: "s", parts, picked, pick: setPicked }),
        createElement(Table, {
          key: "t",
          label: "Context makeup",
          columns: ["Contributor", "Share", "Input tokens"],
          rows: rows(parts),
        }),
      ];
    },
  });
}

const missing = new Absent(
  "no content lengths, so input cannot be split by what fills it. Capture message content in the agent's telemetry, or log request bodies at the gateway.",
);

// growthRequest reads the input each run's calls sent, by their order in the run.
const growthRequest = (f: Fields, usage: Usage) =>
  f.run && usage.inputs.length
    ? {
        group_by: f.run.name,
        ordinal_of: f.run.name,
        measures: usage.inputs.flatMap((x) => usageMeasure(f, "sum", x) ?? []),
        bins: "1",
        limit: 10_000,
      }
    : null;

// growth is the median and p90 input at each step of a run, over the runs reaching it.
function growth(r: AggregateResponse, usage: Usage) {
  const steps = new Map<number, number[]>();
  for (const s of r.series) {
    binStarts(s).forEach((step, bin) => {
      // A line sending no input is no model call, or a repeat of one counted once.
      const input = inputAt(usage.inputs, s, bin);
      if (input > 0) {
        steps.set(step, [...(steps.get(step) ?? []), input]);
      }
    });
  }
  // Steps are pooled into at most 60 spans, so a few runs draw a trend rather than noise.
  const width = Math.max(1, Math.ceil(Math.max(0, ...steps.keys()) / 60));
  const spans = new Map<number, number[]>();
  for (const [step, vs] of steps) {
    const at = Math.floor(step / width) * width;
    spans.set(at, [...(spans.get(at) ?? []), ...vs]);
  }
  const points = [...spans].sort(([a], [b]) => a - b);
  return {
    xs: points.map(([t]) => t + 1),
    median: points.map(([, vs]) => quantile(vs, 0.5)),
    p90: points.map(([, vs]) => quantile(vs, 0.9)),
  };
}

// drops says where the median run's input falls by half: a compaction or a fresh context,
// which it only guesses at where the logs mark no compaction.
function drops(g: ReturnType<typeof growth>, logged: boolean): string {
  const at = g.xs.filter((_, i) => i > 0 && (g.median[i] ?? 0) < (g.median[i - 1] ?? 0) / 2);
  if (at.length === 0) {
    return logged
      ? "The median run's input never halves."
      : "The median run's input never halves: no compaction shows in the window.";
  }
  const first = at.slice(0, 3).join(", ");
  const halves = `The median run's input halves ${at.length} times, first around step ${first}`;
  return logged ? `${halves}.` : `${halves}: a compaction or a fresh context.`;
}

// Compaction is what the runs' compactions removed from the context, and what the first call
// after each paid to write the compacted context to the cache again.
function Compaction(p: { scope: Scope; usage: Usage }): ReactElement {
  const f = p.scope.fields;
  const runs = useRunStats(p.scope);
  const after = useAfterCompaction(p.scope);
  if (!f.compaction) {
    return createElement(Facts, { facts: [["Compactions", "0", "none logged in the window"]] });
  }
  if (runs instanceof Error || !runs) {
    return createElement(Facts, { facts: [["Compactions", runs ? "—" : "…"]] });
  }
  const n = sum(runs.map((r) => r.compactions));
  const removed = sum(runs.map((r) => r.compacted));
  const compacting = runs.filter((r) => r.compactions > 0).length;
  const facts: Fact[] = [
    ["Compactions", num(n), `in ${num(compacting)} of ${num(runs.length)} runs`],
    ["Tokens removed", num(removed), n ? `${num(removed / n)} per compaction` : "tokens"],
  ];
  if (!(after instanceof Error) && after?.calls) {
    facts.push(...afterFacts(after, runs, p.usage, f));
  }
  return createElement(Facts, { facts });
}

// afterFacts set the first calls after a compaction beside every call: the cache they write
// again, what they spend, and how much they read from the cache.
function afterFacts(
  a: AfterCompaction,
  runs: readonly RunStat[],
  usage: Usage,
  f: Scope["fields"],
): Fact[] {
  const typical = sum(runs.map((r) => r.cacheWrite)) / (sum(runs.map((r) => r.calls)) || 1);
  const rewritten = a.cacheWrite / a.calls;
  const read = sum(usage.cache) / (sum(usage.input) || 1);
  return [
    [
      "Cache rewritten after",
      num(rewritten),
      typical
        ? `tokens per first call after · ${(rewritten / typical).toFixed(1)}× a typical call's`
        : "tokens per first call after",
    ],
    [
      "Spend on the call after",
      money(f, a.cost),
      `${pct(a.cost / (sum(usage.cost) || 1))} of spend over ${num(a.calls)} calls`,
    ],
    ["Hit rate after", pct(a.cacheRead / (a.input || 1)), `vs ${pct(read)} over every call`],
  ];
}

function Growth(p: { scope: Scope; usage: Usage }): ReactElement {
  const answer = useAggregate(p.scope, growthRequest(p.scope.fields, p.usage));
  return createElement(Panel<AggregateResponse>, {
    title: "Input per model call, by step",
    answer,
    render: (r) => {
      const g = growth(r, p.usage);
      return [
        createElement(Plot, {
          key: "c",
          kind: "line",
          xs: g.xs,
          ys: [g.median, g.p90],
          series: ["median run", "p90 run"],
          label: "Input tokens by step of the run",
          x: { label: "step of the run" },
          y: { label: "input tokens", format: num },
        }),
        createElement(
          "p",
          { key: "d", className: "ai-note" },
          drops(g, Boolean(p.scope.fields.compaction)),
        ),
      ];
    },
  });
}

const buckets = [0, 10e3, 25e3, 50e3, 100e3, 150e3, 200e3, 1e6];

// Peaks is how many runs reach each peak context, the most input one call sent, and the runs
// themselves, the largest first.
function Peaks(p: { scope: Scope; open: (run: string) => void }): ReactElement {
  return createElement(Panel<readonly RunStat[]>, {
    title: "Peak context per run",
    answer: useRunStats(p.scope),
    render: (runs) =>
      createElement(RunsBy, {
        label: "peak input tokens",
        runs,
        of: (r) => r.peak,
        lows: buckets.slice(0, -1),
        format: num,
        order: "desc",
        open: p.open,
      }),
  });
}

export function ContextTab(p: TabProps): ReactElement {
  const usage = useUsage(p.scope);
  const withUsage = (u: Usage) => [
    createElement(Compaction, { key: "c", scope: p.scope, usage: u }),
    createElement(Makeup, { key: "m", scope: p.scope, usage: u, open: p.open }),
    createElement(
      "div",
      { key: "g", className: "g2" },
      createElement(Growth, { scope: p.scope, usage: u }),
      createElement(Peaks, { scope: p.scope, open: p.openRun }),
    ),
  ];
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Lede, {
      title: "What fills the context window",
      text: "Input billed, split by the length of each kind of content the runs logged, how it grows across a run, and what compacting it removed and cost.",
    }),
    createElement(Panel<Usage>, { title: "Context", answer: usage, render: withUsage }),
  );
}
