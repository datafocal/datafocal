import {
  type AggregateRequest,
  type AggregateResponse,
  binStarts,
  type ExplainSpikeResponse,
} from "@datafocal/api";
import { logql } from "@datafocal/query";
import { Chart, useAnswer } from "@datafocal/ui";
import { createElement, type ReactElement, useState } from "react";
import { type Fields, outcome } from "./fields";
import {
  breakdown,
  callsMeasure,
  eq,
  Panel,
  pct,
  type Scope,
  type TabProps,
  selectorOf,
  Table,
  useAggregate,
  usePatterns,
} from "./panel";

// endingsRequest counts the runs ending on each pattern. A run is a unit of work that made a
// model call, so the rollup keeps the groups that did.
function endingsRequest(f: Fields): Partial<AggregateRequest> | null {
  const calls = callsMeasure(f);
  return f.run
    ? {
        group_by: f.run.name,
        measures: [{ measure: `last:${outcome}` }, ...(calls ? [calls] : [])],
        bins: "all",
        rollup: {
          measure: "count",
          group_by: "label[0]",
          bins: "auto",
          ...(calls && { where: [{ index: 1, op: "gt", value: 0 }] }),
        },
      }
    : null;
}

// OutcomesTab ranks how runs end, charts the chosen ending's share over time, and explains what
// sets the lines of that ending apart from the rest.
export function OutcomesTab({ scope }: TabProps): ReactElement {
  const [chosen, setChosen] = useState<string>();
  const endings = useAggregate(scope, endingsRequest(scope.fields));
  const ids = endings instanceof Error || !endings ? [] : endings.series.map((s) => s.key);
  const text = usePatterns(scope, chosen === undefined ? ids : [...ids, chosen]);
  const named = (id: string) => text.get(id) ?? id;
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Panel<AggregateResponse>, {
      title: "How runs end",
      answer: endings,
      render: (r) => {
        const shares = breakdown(r);
        const key = chosen ?? shares[0]?.key;
        // Every series of a rollup shares its bins, so a bin's runs are summed across them.
        const ends = r.series.find((s) => s.key === key);
        const bins = ends
          ? binStarts(ends).flatMap((t, bin) => {
              const v = ends.values?.[bin];
              return v == null ? [] : [{ t, bin, v }];
            })
          : [];
        const day = (bin: number) => r.series.reduce((n, s) => n + (s.values?.[bin] ?? 0), 0);
        return [
          createElement(Table, {
            key: "t",
            label: "Run outcomes",
            columns: ["Ends on", "Runs", "Share"],
            rows: shares.map((s) => ({
              key: named(s.key),
              cells: [String(s.value), pct(s.share)],
              query: `${logql(scope.filters)} | ${outcome}=${JSON.stringify(s.key)}`,
              onPick: () => setChosen(s.key),
            })),
          }),
          createElement(Chart, {
            key: "d",
            kind: "sparkline",
            data: [bins.map((b) => b.t), bins.map((b) => b.v / (day(b.bin) || 1))],
            width: 320,
            height: 48,
            label: `Share of runs ending on ${named(key ?? "")} over time`,
          }),
        ];
      },
    }),
    chosen !== undefined && createElement(Why, { scope, chosen, name: named(chosen) }),
  );
}

// Why compares the lines of runs ending on chosen against the rest.
function Why({
  scope,
  chosen,
  name,
}: {
  scope: Scope;
  chosen: string;
  name: string;
}): ReactElement {
  const selector = selectorOf(scope);
  const slice = [{ field: outcome, op: "eq", value: chosen }];
  const why = useAnswer(JSON.stringify([selector, slice]), () =>
    scope.client.call("explain_spike", { selector, slice }).then((e) => e.data),
  );
  return createElement(Panel<ExplainSpikeResponse>, {
    title: `Why runs end on ${name}`,
    answer: why,
    render: (r) =>
      createElement(Table, {
        label: "What sets them apart",
        columns: ["Field value", "In these runs", "Elsewhere"],
        rows: r.shifts.map((s) => ({
          key: `${s.field}=${s.value}`,
          cells: [pct(s.slice_share), pct(s.baseline_share)],
          query: logql([...scope.filters, eq(s.field, s.value)]),
        })),
      }),
  });
}
