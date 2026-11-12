import {
  type AggregateResponse,
  binStarts,
  type DataFocalClient,
  type TimeSpec,
} from "@datafocal/api";
import { type Answer, Chart, Panel, useAnswer } from "@datafocal/ui";
import { createElement as h, type ReactElement } from "react";
import { clock, count, measured } from "./parts";
import { meets, type Slices } from "./scopes";

// A time-slice SLO's slices: its own metric a slice against its objective, and the slices
// that fail it, each narrowing the page to itself.

type SliceState = "good" | "bad" | "none";

export interface Slice {
  readonly from: number;
  readonly to: number;
  readonly value: number | null;
  readonly state: SliceState;
}

const opText: Readonly<Record<string, string>> = {
  lt: "<",
  lte: "≤",
  le: "≤",
  gt: ">",
  gte: "≥",
  ge: "≥",
  eq: "=",
};
// field is what a measure reads, as "p99:duration_ms" reads duration_ms.
const field = (measure: string) => measure.split(":")[1] ?? measure;
const objective = (s: Slices): string =>
  `${opText[s.op] ?? s.op} ${measured(field(s.measure), s.value)}`;

const values = (r: AggregateResponse) => r.series[0]?.values ?? [];

// stateOf judges a slice as the SLO does: a count's slice holds zero, not nothing, while
// anything at all was collected then, and a slice nothing was collected in says nothing.
function stateOf(
  s: Slices,
  v: number | null | undefined,
  collected: number | null | undefined,
): SliceState {
  if (s.counted) {
    return collected ? (meets(s.op, v ?? 0, s.value) ? "good" : "bad") : "none";
  }
  return v == null ? "none" : meets(s.op, v, s.value) ? "good" : "bad";
}

// useSlices reads each slice's value and, for a count, every row collected a slice.
export function useSlices(
  client: DataFocalClient,
  s: Slices | undefined,
  time: TimeSpec,
): Answer<Slice[] | undefined> {
  return useAnswer(JSON.stringify(["slices", s, time]), async (): Promise<Slice[] | undefined> => {
    if (!s) {
      return undefined;
    }
    const read = (where: Slices["where"], measure: string) =>
      client.call("aggregate", {
        selector: { time, where: [...where] },
        measure,
        group_by: "none",
        bins: s.bins,
      });
    const [metric, all] = await Promise.all([
      read(s.where, s.measure),
      s.counted ? read([], "count") : undefined,
    ]);
    const xs = metric.data.series[0] ? binStarts(metric.data.series[0]) : [];
    const step = metric.data.series[0]?.step ?? 0;
    const collected = all ? values(all.data) : [];
    return xs.map((from, i) => {
      const v = values(metric.data)[i];
      return { from, to: from + step, value: v ?? null, state: stateOf(s, v, collected[i]) };
    });
  });
}

// SliceChart is the SLO's metric a slice against its objective; a drag narrows the range.
export function SliceChart(p: {
  s: Slices;
  slices: readonly Slice[];
  brush: (from: number, to: number) => void;
}): ReactElement {
  const xs = p.slices.map((x) => x.from);
  return h(
    Panel,
    { title: `${p.s.measure} a slice` },
    h(Chart, {
      kind: "line",
      data: [
        xs,
        p.slices.map((x) => (x.state === "none" ? null : (x.value ?? 0))),
        xs.map(() => p.s.value),
      ],
      width: 480,
      height: 160,
      label: `${p.s.measure} a slice against its objective`,
      x: { time: true },
      y: { format: (v: number) => measured(field(p.s.measure), v) },
      series: [p.s.measure, `objective ${objective(p.s)}`],
      onBrush: p.brush,
    }),
  );
}

// BadSlices are the slices that fail the objective, newest first, each narrowing the page to
// itself.
export function BadSlices(p: {
  s: Slices;
  slices: readonly Slice[];
  open: (x: Slice) => void;
}): ReactElement {
  const bad = p.slices.filter((x) => x.state === "bad").reverse();
  const valid = p.slices.filter((x) => x.state !== "none").length;
  if (bad.length === 0) {
    return h(
      Panel,
      { title: "Bad slices" },
      h("p", null, "No slice in range fails the objective."),
    );
  }
  return h(
    Panel,
    { title: "Bad slices", big: [String(bad.length), `of ${count(valid)} slices in range`] },
    h(
      "table",
      { "aria-label": "Bad slices", className: "slo-requests" },
      h(
        "thead",
        null,
        h(
          "tr",
          null,
          ["Slice", p.s.measure, "Objective"].map((n) => h("th", { key: n }, n)),
        ),
      ),
      h(
        "tbody",
        null,
        bad.slice(0, 30).map((x) =>
          h(
            "tr",
            {
              key: x.from,
              tabIndex: 0,
              onClick: () => p.open(x),
              onKeyDown: (e: { key: string }) => e.key === "Enter" && p.open(x),
            },
            h("td", null, `${clock(x.from)} – ${clock(x.to)}`),
            h("td", null, measured(field(p.s.measure), x.value ?? 0)),
            h("td", null, objective(p.s)),
          ),
        ),
      ),
    ),
  );
}
