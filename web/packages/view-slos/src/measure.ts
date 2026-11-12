import { binStarts, type DataFocalClient, type Predicate, type TimeSpec } from "@datafocal/api";
import { Chart, Panel, Show, useAnswer } from "@datafocal/ui";
import { createElement as h, type ReactElement } from "react";
import { measured } from "./parts";

// For an event SLO whose events are bad by a bound on a number, as a cost over a dollar or a
// request over a second, that number over the valid events a bin, its median, p95 and most,
// against the bound: how far the events run from it. A field of few values, as a status, is
// one a bound sorts rather than measures, and has no chart.

const bounds = ["gt", "ge", "lt", "le"];

// boundOf is the predicate on a number that makes an event bad, if one does.
export const boundOf = (badness: readonly Predicate[]): Predicate | undefined =>
  badness.find(
    (p) => bounds.includes(p.op ?? "") && p.value !== undefined && Number.isFinite(Number(p.value)),
  );

// fewest is how many values a field needs to be measured rather than sorted.
const fewest = 20;

interface Spread {
  readonly xs: number[];
  readonly ys: (number | null)[][];
}

function useSpread(
  client: DataFocalClient,
  time: TimeSpec,
  where: readonly Predicate[],
  field: string,
) {
  return useAnswer(
    JSON.stringify(["spread", time, where, field]),
    async (): Promise<Spread | undefined> => {
      const read = (measure: string, bins: string) =>
        client.call("aggregate", {
          selector: { time, where: [...where] },
          measure,
          group_by: "none",
          bins,
        });
      const distinct = (await read(`distinct:${field}`, "all")).data.series[0]?.values?.[0] ?? 0;
      if (distinct < fewest) {
        return undefined;
      }
      const series = await Promise.all(
        ["p50", "p95", "max"].map((m) => read(`${m}:${field}`, "auto")),
      );
      const first = series[0]?.data.series[0];
      return {
        xs: first ? binStarts(first) : [],
        ys: series.map((r) => (r.data.series[0]?.values ?? []).map((v) => v ?? null)),
      };
    },
  );
}

// FieldChart is the bad events' number over the valid ones a bin, against the bound.
export function FieldChart(p: {
  client: DataFocalClient;
  time: TimeSpec;
  valid: readonly Predicate[];
  bound: Predicate;
  brush: (from: number, to: number) => void;
}): ReactElement {
  const field = p.bound.field ?? "";
  const limit = Number(p.bound.value);
  const spread = useSpread(p.client, p.time, p.valid, field);
  return h(Show<Spread | undefined>, {
    answer: spread,
    render: (s) =>
      s &&
      h(
        Panel,
        { title: `${field} a bin` },
        h(Chart, {
          kind: "line",
          data: [s.xs, ...s.ys, s.xs.map(() => limit)],
          width: 480,
          height: 160,
          label: `${field} over the valid events a bin, against its bound`,
          x: { time: true },
          y: { format: (v: number) => measured(field, v) },
          series: ["p50", "p95", "max", `bound ${measured(field, limit)}`],
          onBrush: p.brush,
        }),
      ),
  });
}
