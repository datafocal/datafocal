import { Absent } from "@datafocal/ui";
import { createElement, type ReactElement } from "react";
import {
  num,
  Panel,
  pct,
  type Row,
  sum,
  type TabProps,
  Table,
  type Usage,
  useUsage,
} from "./panel";
import { type Fact, Facts, Lede, Plot } from "./parts";
import { RunsBy } from "./runlist";
import {
  type AfterCompaction,
  hitRate,
  type RunStat,
  useAfterCompaction,
  useRunStats,
} from "./stats";

// Change is something that can change between a run's calls, counted per run.
interface Change {
  readonly label: string;
  readonly count: (r: RunStat) => number;
}

const changes: readonly Change[] = [
  { label: "Model changed", count: (r) => r.modelChanges },
  { label: "Tool list changed", count: (r) => r.toolListChanges },
  { label: "Compaction", count: (r) => r.compactions },
];

const bands = [0, 0.3, 0.5, 0.6, 0.7, 0.8, 0.9];

function Changes(p: { runs: readonly RunStat[]; openRun: (run: string) => void }): ReactElement {
  const rows = changes.map((c): Row => {
    const hit = p.runs.filter((r) => c.count(r) > 0);
    const most = [...hit].sort((a, b) => c.count(b) - c.count(a))[0];
    return {
      key: c.label,
      cells: [num(sum(p.runs.map(c.count))), num(hit.length)],
      ...(most && { onPick: () => p.openRun(most.run) }),
    };
  });
  return createElement(
    "section",
    { className: "panel", "aria-label": "Changes within a run" },
    createElement("h3", null, "Changes within a run"),
    createElement(
      "p",
      { className: "ai-note" },
      "How often each changed between two calls of the same run. Click one to open the run where it changed most.",
    ),
    createElement(Table, { label: "Changes", columns: ["Change", "Times", "Runs"], rows }),
  );
}

function Spread(p: { runs: readonly RunStat[]; openRun: (run: string) => void }): ReactElement {
  return createElement(
    "section",
    { className: "panel", "aria-label": "Hit rate per run" },
    createElement("h3", null, "Hit rate per run"),
    createElement(RunsBy, {
      label: "share of input read from cache",
      runs: p.runs,
      of: hitRate,
      lows: bands,
      format: (v) => `${Math.round(v * 100)}%`,
      order: "asc",
      open: p.openRun,
    }),
  );
}

// rewritten is the cache writes of the first calls after a compaction, as a fact, where the
// logs show any.
function rewritten(after: AfterCompaction | Error | undefined, written: number): Fact[] {
  return after instanceof Error || !after?.calls
    ? []
    : [
        [
          "Rewritten after compaction",
          num(after.cacheWrite),
          `tokens · ${pct(after.cacheWrite / (written || 1))} of writes`,
        ],
      ];
}

function Body(p: TabProps & { usage: Usage; runs: readonly RunStat[] }): ReactElement {
  const { usage, runs } = p;
  const after = useAfterCompaction(p.scope);
  const [read, input] = [sum(usage.cache), sum(usage.input)];
  const written = sum(runs.map((r) => r.cacheWrite));
  const low = runs.filter((r) => hitRate(r) < 0.5).length;
  return createElement(
    "div",
    null,
    createElement(Facts, {
      facts: [
        ["Hit rate", pct(read / (input || 1)), "of input read from cache"],
        ["Read from cache", num(read), "tokens"],
        ["Written to cache", num(written), "tokens"],
        ["Written per read", read ? (written / read).toFixed(2) : "—", "tokens"],
        ...rewritten(after, written),
        ["Runs under 50%", `${num(low)} of ${num(runs.length)}`],
      ],
    }),
    createElement(
      "section",
      { className: "panel", "aria-label": "Hit rate over time" },
      createElement("h3", null, "Hit rate over time"),
      createElement(Plot, {
        kind: "line",
        xs: usage.bins,
        ys: [usage.cache.map((v, i) => (usage.input[i] ? v / (usage.input[i] ?? 1) : Number.NaN))],
        series: ["hit rate"],
        label: "Cache hit rate over time",
        x: { time: true },
        y: { label: "read from cache", format: pct },
      }),
    ),
    createElement(
      "div",
      { className: "g2" },
      createElement(Changes, { runs, openRun: p.openRun }),
      createElement(Spread, { runs, openRun: p.openRun }),
    ),
  );
}

export function CacheTab(p: TabProps): ReactElement {
  const usage = useUsage(p.scope);
  const runs = useRunStats(p.scope);
  const answer =
    usage instanceof Error || !usage
      ? usage
      : runs instanceof Error || !runs
        ? runs
        : { usage, runs };
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Lede, {
      title: "Prompt cache",
      text: "The share of input read from cache, over the window and per run, and the tokens written to it.",
    }),
    createElement(Panel<{ usage: Usage; runs: readonly RunStat[] }>, {
      title: "Cache",
      answer: p.scope.fields.cacheRead ? answer : new Absent("no cache-read token counts"),
      render: (a) => createElement(Body, { ...p, ...a }),
    }),
  );
}
