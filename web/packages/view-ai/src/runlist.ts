import { createElement, type ReactElement, useState } from "react";
import { num, pct, type Row, Table, usd } from "./panel";
import { bucketAxis, Plot } from "./parts";
import { hitRate, type RunStat } from "./stats";

// shown is the most runs a list shows, the first by its order.
const shown = 20;

const row = (r: RunStat, open: (run: string) => void): Row => ({
  key: r.run,
  cells: [num(r.calls), usd(r.cost), pct(hitRate(r)), num(r.peak)],
  onPick: () => open(r.run),
});

// RunTable lists runs, each opening the calls it made.
export function RunTable(p: {
  label: string;
  runs: readonly RunStat[];
  open: (run: string) => void;
}): ReactElement {
  return createElement(
    "div",
    null,
    createElement(Table, {
      label: p.label,
      columns: ["Run", "Model calls", "Spend", "Cache hit", "Peak context"],
      rows: p.runs.slice(0, shown).map((r) => row(r, p.open)),
    }),
    p.runs.length > shown &&
      createElement("p", { className: "ai-note" }, `${shown} of ${num(p.runs.length)} runs.`),
  );
}

// RunsBy is how many runs fall in each band of a per-run number, and the runs themselves in
// its order, those of one band when its button is pressed; each opens the calls it made.
export function RunsBy(p: {
  label: string;
  runs: readonly RunStat[];
  of: (r: RunStat) => number;
  // lows are each band's lower bound, ascending; a band runs to the next one's.
  lows: readonly number[];
  format: (v: number) => string;
  order: "asc" | "desc";
  open: (run: string) => void;
}): ReactElement {
  const [picked, setPicked] = useState<number>();
  const band = (v: number) => p.lows.filter((lo) => v >= lo).length - 1;
  const known = p.runs.filter((r) => Number.isFinite(p.of(r)));
  const counts = p.lows.map((_, i) => known.filter((r) => band(p.of(r)) === i).length);
  const labels = p.lows.map((lo) => `${p.format(lo)}+`);
  const listed = known
    .filter((r) => picked === undefined || band(p.of(r)) === picked)
    .sort((a, b) => (p.order === "asc" ? p.of(a) - p.of(b) : p.of(b) - p.of(a)));
  return createElement(
    "div",
    null,
    createElement(Plot, {
      kind: "bars",
      xs: p.lows.map((_, i) => i),
      ys: [counts],
      series: ["runs"],
      label: `Runs by ${p.label}`,
      x: bucketAxis(p.label, labels),
      // A count's ticks between whole numbers would print as the whole numbers beside them.
      y: { label: "runs", format: (v) => (Number.isInteger(v) ? String(v) : "") },
    }),
    createElement(
      "div",
      { className: "ai-dims", role: "group", "aria-label": `Runs by ${p.label}` },
      labels.map((l, i) =>
        createElement(
          "button",
          {
            key: l,
            type: "button",
            className: "tbtn",
            "aria-pressed": picked === i,
            onClick: () => setPicked(picked === i ? undefined : i),
          },
          `${l} · ${counts[i]}`,
        ),
      ),
    ),
    createElement(RunTable, { label: `Runs by ${p.label}`, runs: listed, open: p.open }),
  );
}
