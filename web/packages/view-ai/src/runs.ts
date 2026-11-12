import type { AggregateResponse, Series } from "@datafocal/api";
import { Menu, type MenuItem } from "@datafocal/ui";
import { createElement, type ReactElement, useState } from "react";
import { outcome } from "./fields";
import { usesMeasure } from "./memory";
import {
  callsMeasure,
  failure,
  inputAt,
  measures,
  num,
  Panel,
  peaks,
  peaksOf,
  type Scope,
  type TabProps,
  usageMeasure,
  usd,
  useAggregate,
  usePatterns,
  useUsage,
  valueAt,
} from "./panel";

// rowHeight matches ai.css; overscan rows render either side of the visible ones.
const rowHeight = 28;
const overscan = 10;

type Column = "steps" | "cost" | "input" | "peak" | "memory" | "outcome";
const columns: readonly { id: Column; label: string; format: (v: number) => string }[] = [
  { id: "steps", label: "Steps", format: num },
  { id: "cost", label: "Spend", format: usd },
  { id: "input", label: "Input tokens", format: num },
  { id: "peak", label: "Peak context", format: num },
  { id: "memory", label: "Memory uses", format: num },
  { id: "outcome", label: "Ends on", format: String },
];

// atLeast keeps the groups whose measure i is at least min, when the logs carry it.
const atLeast = (i: number, min: number) =>
  min > 0 && i >= 0 ? [{ index: i, op: "ge", value: min }] : [];

// useEndings names the pattern each run ends on, labelled by measure i of runs, by its text.
function useEndings(scope: Scope, runs: AggregateResponse | Error | undefined, i: number) {
  const ending = (s: Series) => s.measure_labels?.[i]?.[0] ?? "";
  const patterns = usePatterns(
    scope,
    runs instanceof Error || !runs ? [] : runs.series.map(ending),
  );
  return (s: Series) => patterns.get(ending(s)) ?? ending(s);
}

// useRuns reads each run's columns over at least minSpend, and value is a run's column.
function useRuns(scope: Scope, minSpend: number) {
  const f = scope.fields;
  const usage = useUsage(scope);
  const inputs = usage instanceof Error ? [] : (usage?.inputs ?? []);
  const m = measures({
    steps: { measure: "count" },
    cost: usageMeasure(f, "sum", f.cost),
    outcome: { measure: `last:${outcome}` },
    calls: callsMeasure(f),
    memory: usesMeasure(f),
  });
  const runs = useAggregate(
    scope,
    f.run && usage
      ? {
          group_by: f.run.name,
          measures: [...m.list, ...inputs.flatMap((x) => usageMeasure(f, "sum", x) ?? [])],
          bins: "all",
          limit: 10_000,
          // The costliest runs are the ones kept when there are more than the limit.
          sort: `${Math.max(m.at("cost"), 0)}:desc`,
          having: atLeast(m.at("cost"), minSpend),
        }
      : null,
  );
  const peakAnswer = useAggregate(scope, usage ? peaks(f, inputs) : null);
  const peak =
    peakAnswer instanceof Error || !peakAnswer
      ? new Map<string, number>()
      : peaksOf(peakAnswer, f, inputs);
  const answer = failure(usage, peakAnswer) ?? runs;
  const value = (s: Series, c: Column) => {
    if (c === "peak") {
      return peak.get(s.key) ?? 0;
    }
    return c === "input" ? inputAt(inputs, s, 0, m.list.length) : (valueAt(s, m.at(c), 0) ?? 0);
  };
  const present = (c: Column) => (c === "input" || c === "peak" ? inputs.length > 0 : m.at(c) >= 0);
  const label = useEndings(scope, runs, m.at("outcome"));
  // A unit of work that made no model call, such as another program's pod, is no run.
  const isRun = (s: Series) => m.at("calls") < 0 || (valueAt(s, m.at("calls"), 0) ?? 0) > 0;
  return { answer, value, present, label, isRun };
}

// RunsTab is one row per run, filtered by a minimum spend, sorted by the column whose
// header was clicked, and showing the columns chosen in the menu.
export function RunsTab({ scope, openRun }: TabProps): ReactElement {
  const [minSpend, setMinSpend] = useState(0);
  const [sort, setSort] = useState<Column>("cost");
  const [hidden, setHidden] = useState<readonly Column[]>([]);
  const { answer, value, present, label, isRun } = useRuns(scope, minSpend);
  const shown = columns.filter((c) => present(c.id) && !hidden.includes(c.id));
  const cells = (s: Series) =>
    shown.map((c) => (c.id === "outcome" ? label(s) : c.format(value(s, c.id))));
  const toggle = (c: Column) =>
    setHidden(hidden.includes(c) ? hidden.filter((h) => h !== c) : [...hidden, c]);
  const menu = columns.map((c) => ({
    label: c.label,
    checked: !hidden.includes(c.id),
    run: () => toggle(c.id),
  }));
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Bar, { minSpend, setMinSpend, columns: menu }),
    createElement(Panel<AggregateResponse>, {
      title: "Runs",
      answer,
      render: (r) =>
        createElement(Rows, {
          header: header(scope.fields.run?.name ?? "", shown, sort, setSort),
          series: r.series.filter(isRun).sort((a, b) => value(b, sort) - value(a, sort)),
          cells,
          open: (s: Series) => openRun(s.key),
        }),
    }),
  );
}

const header = (
  run: string,
  shown: readonly (typeof columns)[number][],
  sort: Column,
  setSort: (c: Column) => void,
) => [
  createElement("span", { key: "run", role: "columnheader" }, run),
  ...shown.map((c) =>
    createElement(
      "button",
      {
        key: c.id,
        type: "button",
        role: "columnheader",
        "aria-sort": c.id === sort ? "descending" : undefined,
        onClick: () => setSort(c.id),
      },
      c.label,
    ),
  ),
];

function Bar(p: {
  minSpend: number;
  setMinSpend: (v: number) => void;
  columns: readonly MenuItem[];
}): ReactElement {
  const onChange = (e: { target: HTMLInputElement }) => p.setMinSpend(Number(e.target.value));
  return createElement(
    "div",
    { className: "ai-bar" },
    createElement(
      "label",
      null,
      "Min spend $ ",
      createElement("input", { type: "number", min: 0, value: p.minSpend, onChange }),
    ),
    createElement(Menu, { label: "Columns", sections: [{ items: p.columns }] }),
  );
}

// Rows renders only the runs in view and overscan either side, in a spacer as tall as all.
function Rows(p: {
  header: readonly ReactElement[];
  series: readonly Series[];
  cells: (s: Series) => readonly string[];
  open: (s: Series) => void;
}): ReactElement {
  const [top, setTop] = useState(0);
  const [body, setBody] = useState<HTMLDivElement | null>(null);
  const height = body?.clientHeight ?? 0;
  const first = Math.max(0, Math.floor(top / rowHeight) - overscan);
  const last = Math.min(p.series.length, Math.ceil((top + height) / rowHeight) + overscan);
  const row = (s: Series, i: number) =>
    createElement(
      "div",
      {
        key: s.key,
        role: "row",
        ref: (el: HTMLDivElement | null) =>
          el?.style.setProperty("top", `${(first + i) * rowHeight}px`),
        onClick: () => p.open(s),
      },
      [s.key, ...p.cells(s)].map((c, j) => createElement("span", { key: j, role: "cell" }, c)),
    );
  return createElement(
    "div",
    {
      role: "table",
      "aria-label": "Runs",
      "aria-rowcount": p.series.length + 1,
      className: "ai-runs",
    },
    createElement("div", { role: "row", className: "ai-runs-h" }, p.header),
    createElement(
      "div",
      {
        className: "ai-runs-b",
        ref: setBody,
        onScroll: (e: { currentTarget: HTMLDivElement }) => setTop(e.currentTarget.scrollTop),
      },
      createElement(
        "div",
        {
          ref: (el: HTMLDivElement | null) =>
            el?.style.setProperty("height", `${p.series.length * rowHeight}px`),
        },
        p.series.slice(first, last).map(row),
      ),
    ),
  );
}
