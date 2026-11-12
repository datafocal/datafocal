import type { SLOStatusResponse } from "@datafocal/api";
import { Chart, type Kpi, Panel } from "@datafocal/ui";
import { createElement as h, type ReactElement } from "react";
import { at, pct } from "./doc";
import { count } from "./parts";

// An SLO's overview over any range, from the counts its store keeps a step, so it reads no
// segment whatever the range: its bad units a step, its SLI over its window against the
// objective, and its budget left. A drag across any of them narrows the range.

type Detail = SLOStatusResponse["slo"];

// unitOf is what the SLO counts: slices for a time-slice SLO, else events.
export const unitOf = (doc: unknown): "slices" | "events" =>
  at(doc, ["spec", "budgetingMethod"]) === "Occurrences" ? "events" : "slices";

const totals = (d: Detail) =>
  d.points.reduce((t, p) => ({ bad: t.bad + (p.valid - p.good), valid: t.valid + p.valid }), {
    bad: 0,
    valid: 0,
  });

// badTile is the bad units over the range, of the valid ones.
export function badTile(d: Detail, unit: "slices" | "events"): Kpi {
  const t = totals(d);
  // A store still counting the range, as after a restart, says how much of it it holds.
  const counted = d.coverage < 1 ? ` · ${pct(d.coverage, 0)} counted` : "";
  return {
    label: `Bad ${unit}`,
    value: count(t.bad),
    detail: `of ${count(t.valid)} in range${counted}`,
    tone: t.bad > 0 ? "bad" : undefined,
  };
}

const xsOf = (d: Detail) => d.points.map((p) => Date.parse(p.at));

const chart = (p: {
  d: Detail;
  title: string;
  kind: "bars" | "line";
  ys: readonly (readonly (number | null)[])[];
  series: readonly string[];
  format: (v: number) => string;
  brush: (from: number, to: number) => void;
}): ReactElement =>
  h(
    Panel,
    { key: p.title, title: p.title },
    h(Chart, {
      kind: p.kind,
      data: [xsOf(p.d), ...p.ys.map((y) => [...y])],
      width: 320,
      height: 160,
      label: p.title,
      x: { time: true },
      y: { format: p.format },
      series: p.series,
      onBrush: p.brush,
    }),
  );

// Overview is the bad units a step, the SLI against the objective, and the budget left.
export function Overview(p: {
  d: Detail;
  unit: "slices" | "events";
  brush: (from: number, to: number) => void;
}): ReactElement {
  const { d, unit, brush } = p;
  return h(
    "div",
    { className: "g3" },
    chart({
      d,
      title: `Bad ${unit} a step`,
      kind: "bars",
      ys: [d.points.map((x) => x.valid - x.good)],
      series: [`bad ${unit}`],
      format: count,
      brush,
    }),
    chart({
      d,
      title: "SLI over its window",
      kind: "line",
      ys: [d.points.map((x) => x.sli ?? null), d.points.map(() => d.objective)],
      series: ["SLI", "objective"],
      format: (v) => pct(v, 1),
      brush,
    }),
    chart({
      d,
      title: "Budget left",
      kind: "line",
      ys: [
        d.points.map((x) => (x.budget_remaining == null ? null : Math.max(-1, x.budget_remaining))),
      ],
      series: ["budget left"],
      format: (v) => pct(v, 0),
      brush,
    }),
  );
}
