import type { Selector } from "@datafocal/api";
import { resolve, type TimeRange } from "@datafocal/shell";
import { Failure } from "@datafocal/ui";
import { createElement, type ReactElement, type ReactNode } from "react";
import {
  num,
  pct,
  type Scope,
  selectorOf,
  type Split,
  type TabProps,
  usd,
  useAggregate,
} from "./panel";
import { Lede, Note } from "./parts";
import { RunTable } from "./runlist";
import { type Delta, delta, hitRate, type RunStat, useRunStats } from "./stats";

// enough is the fewest runs a side needs before a difference is drawn (§6.3).
const enough = 20;

// Metric is a per-run number Compare reads, and which way is better: lower, higher, or
// neither, for counts that say how a run behaves rather than how well.
interface Metric {
  readonly label: string;
  readonly of: (r: RunStat) => number;
  readonly format: (v: number) => string;
  readonly better: "lower" | "higher" | "neither";
  // priced metrics read cost, which logs without a cost or priced usage lack.
  readonly priced?: boolean;
}

const metrics: readonly Metric[] = [
  { label: "Cost per run", of: (r) => r.cost, format: usd, better: "lower", priced: true },
  { label: "Peak context", of: (r) => r.peak, format: num, better: "lower" },
  { label: "Cache hit", of: hitRate, format: pct, better: "higher" },
  { label: "Model calls per run", of: (r) => r.calls, format: num, better: "neither" },
  { label: "Tool calls per run", of: (r) => r.toolCalls, format: num, better: "neither" },
  { label: "Compactions per run", of: (r) => r.compactions, format: num, better: "neither" },
];

const isTime = (v: unknown): v is { at: string } =>
  v instanceof Object && "at" in v && typeof v.at === "string";
const isSlice = (v: unknown): v is { field: string; value: string } =>
  v instanceof Object &&
  "field" in v &&
  "value" in v &&
  typeof v.field === "string" &&
  typeof v.value === "string";

// splitOf is the split a URL names, none when it names one in no form this view wrote.
const splitOf = (json: string | undefined): Split | undefined => {
  try {
    const v: unknown = JSON.parse(json ?? "null");
    return isTime(v) ? { at: v.at } : isSlice(v) ? { field: v.field, value: v.value } : undefined;
  } catch {
    return undefined;
  }
};

// midpoint is the middle of range, the split Compare opens on.
const midpoint = (range: TimeRange): string => {
  const now = Date.now();
  const [from, to] = [resolve(range.from, now), resolve(range.to, now)];
  return new Date(Math.round((from + to) / 2 / 60_000) * 60_000).toISOString();
};

const halves = (scope: Scope, at: string): [Selector, Selector] => {
  const s = selectorOf(scope);
  return [
    { ...s, time: { from: scope.range.from, to: at } },
    { ...s, time: { from: at, to: scope.range.to } },
  ];
};

// verdict says whether the interval clears zero, in the metric's better direction.
function verdict(m: Metric, d: Delta, thin: boolean): [string, string] {
  if (thin) {
    return ["too few runs", ""];
  }
  if (m.better === "neither" || (d.lo <= 0 && d.hi >= 0)) {
    return ["no clear change", ""];
  }
  const up = d.lo > 0;
  return (up ? m.better === "higher" : m.better === "lower") ? ["better", "ok"] : ["worse", "warn"];
}

// Interval draws a delta's 95% interval on a scale of −100% … +100%, zero in the middle.
function Interval(p: { d: Delta; tone: string }): ReactElement {
  const x = (v: number) => 50 + Math.max(-1, Math.min(1, v)) * 50;
  return createElement(
    "svg",
    {
      viewBox: "0 0 100 14",
      preserveAspectRatio: "none",
      className: "ai-ci",
      role: "img",
      "aria-label": `${pct(p.d.lo)} to ${pct(p.d.hi)}`,
    },
    createElement("line", { x1: 50, x2: 50, y1: 0, y2: 14, className: "zero" }),
    createElement("rect", {
      x: x(p.d.lo),
      y: 5,
      width: Math.max(0.5, x(p.d.hi) - x(p.d.lo)),
      height: 4,
      rx: 2,
      className: p.tone || "none",
    }),
  );
}

const signed = (v: number) => `${v >= 0 ? "+" : "−"}${pct(Math.abs(v))}`;

function Forest(p: Sides): ReactElement {
  const thin = p.a.length < enough || p.b.length < enough;
  const row = (m: Metric) => {
    const d = delta(p.a.map(m.of).filter(Number.isFinite), p.b.map(m.of).filter(Number.isFinite));
    const [word, tone] = verdict(m, d, thin);
    return createElement(
      "tr",
      { key: m.label },
      createElement("td", null, m.label),
      createElement("td", null, m.format(d.before)),
      createElement("td", null, m.format(d.after)),
      createElement("td", null, signed(d.change)),
      createElement("td", null, createElement(Interval, { d, tone })),
      createElement("td", null, createElement("span", { className: `ai-pill ${tone}` }, word)),
    );
  };
  const head = [
    "Metric",
    p.names[0],
    p.names[1],
    "Change",
    "95% interval (−100% … +100%)",
    "Verdict",
  ];
  return createElement(
    "table",
    { "aria-label": "Comparison", className: "ai-forest" },
    createElement(
      "thead",
      null,
      createElement(
        "tr",
        null,
        head.map((h) => createElement("th", { key: h }, h)),
      ),
    ),
    createElement("tbody", null, metrics.filter((m) => p.priced || !m.priced).map(row)),
  );
}

interface Sides {
  readonly a: readonly RunStat[];
  readonly b: readonly RunStat[];
  readonly names: [string, string];
  readonly priced: boolean;
  // open opens a run's calls.
  readonly open: (run: string) => void;
}

function Result(p: Sides): ReactNode {
  const thin = p.a.length < enough || p.b.length < enough;
  return [
    createElement(
      "p",
      { key: "n", className: "ai-note" },
      `Medians per run with a 95% bootstrap interval, resampled with a fixed seed. ${num(p.a.length)} runs ${p.names[0].toLowerCase()}, ${num(p.b.length)} ${p.names[1].toLowerCase()}.`,
    ),
    thin &&
      createElement(
        Note,
        { key: "t", title: "Not enough runs to call a difference." },
        `Each side needs at least ${enough} runs; the numbers are shown, the verdicts are not.`,
      ),
    createElement(Forest, { key: "f", ...p }),
    createElement(
      "div",
      { key: "r", className: "g2" },
      p.names.map((name, i) =>
        createElement(RunTable, {
          key: name,
          label: `Runs ${name.toLowerCase()}`,
          runs: [...(i === 0 ? p.a : p.b)].sort((x, y) => y.cost - x.cost),
          open: p.open,
        }),
      ),
    ),
  ];
}

type Runs = readonly RunStat[] | Error | undefined;

// How is what a comparison shows beside its two sides.
type How = Pick<Sides, "names" | "priced" | "open">;

function show(a: Runs, b: Runs, how: How): ReactElement {
  const bad = [a, b].find((x) => x instanceof Error);
  if (bad instanceof Error) {
    return createElement(Failure, { what: "Cannot compare", message: bad.message });
  }
  if (!a || !b || a instanceof Error || b instanceof Error) {
    return createElement("p", null, "Loading…");
  }
  return createElement("div", null, Result({ a, b, ...how }));
}

function ByTime(p: { scope: Scope; at: string; open: (run: string) => void }): ReactElement {
  const [before, after] = halves(p.scope, p.at);
  const priced = Boolean(p.scope.fields.cost);
  return show(useRunStats(p.scope, before), useRunStats(p.scope, after), {
    names: ["Before", "After"],
    priced,
    open: p.open,
  });
}

// ByValue sets the runs with a line carrying field = value against the runs without one,
// each run's numbers read over all its lines.
function ByValue(p: {
  scope: Scope;
  field: string;
  value: string;
  open: (run: string) => void;
}): ReactElement {
  const f = p.scope.fields;
  const all = useRunStats(p.scope);
  const s = selectorOf(p.scope);
  const where = [...(s.where ?? []), { field: p.field, op: "eq", value: p.value }];
  const req = f.run ? { group_by: f.run.name, measure: "count", bins: "all", limit: 10_000 } : null;
  const members = useAggregate(p.scope, req, { ...s, where });
  if (members instanceof Error || !members || all instanceof Error || !all) {
    return show(members instanceof Error ? members : all, all, {
      names: ["Without", "With"],
      priced: Boolean(f.cost),
      open: p.open,
    });
  }
  const keys = new Set(members.series.map((x) => x.key));
  const [a, b] = [all.filter((r) => !keys.has(r.run)), all.filter((r) => keys.has(r.run))];
  return show(a, b, { names: ["Without", "With"], priced: Boolean(f.cost), open: p.open });
}

// Controls picks the split: a time in the window, or a value of one of the split fields.
function Controls(p: TabProps & { chosen: Split }): ReactElement {
  const dims = p.scope.fields.dims;
  const field = "field" in p.chosen ? p.chosen.field : "";
  const values = useAggregate(
    p.scope,
    field ? { group_by: field, measure: "count", bins: "all" } : null,
  );
  const options =
    values instanceof Error || !values
      ? []
      : values.series.filter((x) => x.key !== "").map((x) => x.key);
  const onKind = (e: { target: HTMLSelectElement }) =>
    p.compare(
      e.target.value === ""
        ? { at: midpoint(p.scope.range) }
        : { field: e.target.value, value: "" },
    );
  // The time is UTC, as the bins are.
  const onAt = (e: { target: HTMLInputElement }) =>
    e.target.value && p.compare({ at: new Date(`${e.target.value}Z`).toISOString() });
  const onValue = (e: { target: HTMLSelectElement }) => p.compare({ field, value: e.target.value });
  const select = (
    label: string,
    value: string,
    onChange: (e: { target: HTMLSelectElement }) => void,
    opts: readonly (readonly [string, string])[],
  ) =>
    createElement(
      "label",
      null,
      `${label} `,
      createElement(
        "select",
        { value, onChange },
        opts.map(([v, l]) => createElement("option", { key: v, value: v }, l)),
      ),
    );
  return createElement(
    "div",
    { className: "ai-bar" },
    select("Split", field, onKind, [
      ["", "Before / after a time"],
      ...dims.map((d) => [d.name, `Runs by ${d.label}`] as const),
    ]),
    "at" in p.chosen
      ? createElement(
          "label",
          null,
          "At (UTC) ",
          createElement("input", {
            type: "datetime-local",
            value: p.chosen.at.slice(0, 16),
            onChange: onAt,
          }),
        )
      : select("With", p.chosen.value, onValue, [
          ["", "pick a value"],
          ...options.map((o) => [o, o] as const),
        ]),
  );
}

export function CompareTab(p: TabProps): ReactElement {
  const split = splitOf(p.split) ?? { at: midpoint(p.scope.range) };
  const body =
    "at" in split
      ? createElement(ByTime, { scope: p.scope, at: split.at, open: p.openRun })
      : split.value
        ? createElement(ByValue, {
            scope: p.scope,
            field: split.field,
            value: split.value,
            open: p.openRun,
          })
        : createElement("p", null, "Pick a value to compare its runs against the rest.");
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Lede, {
      title: "Did the change help?",
      text: "Medians per run on each side of a split: before and after a time, or runs with and without a value.",
    }),
    createElement(Controls, { ...p, chosen: split }),
    createElement("section", { className: "panel", "aria-label": "Before and after" }, body),
  );
}
