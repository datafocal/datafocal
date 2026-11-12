import type { AggregateRequest, AggregateResponse, Series } from "@datafocal/api";
import { logql } from "@datafocal/query";
import { Absent, Meter } from "@datafocal/ui";
import { createElement, type ReactElement } from "react";
import type { Fields } from "./fields";
import {
  callsMeasure,
  measures,
  money,
  num,
  Panel,
  pct,
  perBin,
  type Row,
  type Scope,
  sliceQuery,
  spec,
  sum,
  type TabProps,
  Table,
  usageMeasure,
  usd,
  useAggregate,
  valueAt,
} from "./panel";
import { Lede, Plot } from "./parts";
import { inMs, ms } from "./stats";

const modelMeasures = (f: Fields) =>
  measures({
    cost: usageMeasure(f, "sum", f.cost),
    input: usageMeasure(f, "sum", f.input),
    read: usageMeasure(f, "sum", f.cacheRead),
    write: usageMeasure(f, "sum", f.cacheWrite),
    output: usageMeasure(f, "sum", f.output),
    calls: callsMeasure(f),
    runs: spec("distinct", f.run),
  });

export const modelRequest = (f: Fields): Partial<AggregateRequest> | null =>
  f.model ? { group_by: f.model.name, measures: modelMeasures(f).list, bins: "all" } : null;

// ModelStat is what one model was sent, answered and cost.
export interface ModelStat {
  readonly model: string;
  readonly cost: number;
  readonly input: number;
  readonly read: number;
  readonly output: number;
  readonly calls: number;
  readonly runs: number;
}

export function modelStats(r: AggregateResponse, f: Fields): ModelStat[] {
  const m = modelMeasures(f);
  const stat = (s: Series): ModelStat => {
    const get = (k: Parameters<typeof m.at>[0]) =>
      m.at(k) < 0 ? 0 : (valueAt(s, m.at(k), 0) ?? 0);
    const [input, read, write] = [get("input"), get("read"), get("write")];
    return {
      model: s.key,
      cost: get("cost"),
      // Reading more from cache than all input is only possible when input excludes it.
      input: f.excludesCache || read > input ? input + read + write : input,
      read,
      output: get("output"),
      calls: get("calls"),
      runs: get("runs"),
    };
  };
  return r.series
    .filter((s) => s.key !== "")
    .map(stat)
    .sort((a, b) => b.cost - a.cost);
}

function rows(stats: readonly ModelStat[], p: TabProps): Row[] {
  const spend = sum(stats.map((s) => s.cost)) || 1;
  const top = stats[0]?.cost || 1;
  const f = p.scope.fields;
  return stats.map((s) => ({
    key: s.model,
    cells: [
      num(s.calls),
      num(s.runs),
      createElement(Meter, { text: money(f, s.cost), share: s.cost / top }),
      f.cost ? pct(s.cost / spend) : "—",
      s.calls ? money(f, s.cost / s.calls) : "—",
      num(s.input),
      num(s.output),
      pct(s.input ? s.read / s.input : Number.NaN),
    ],
    query: sliceQuery(p.scope, f.model, s.model),
    onPick: () => p.compare({ field: f.model?.name ?? "", value: s.model }),
  }));
}

function Ledger(p: TabProps): ReactElement {
  const f = p.scope.fields;
  const answer = useAggregate(p.scope, modelRequest(f));
  return createElement(Panel<AggregateResponse>, {
    title: "Per model",
    answer: f.model ? answer : new Absent("no model on any line"),
    render: (r) => [
      createElement(
        "p",
        { key: "w", className: "ai-note" },
        "Click a model to compare the runs that use it against the rest.",
      ),
      createElement(Table, {
        key: "t",
        label: "Models",
        columns: [
          "Model",
          "Calls",
          "Runs",
          "Spend",
          "Share",
          "Per call",
          "Input tokens",
          "Output tokens",
          "Cache hit",
        ],
        rows: rows(modelStats(r, f), p),
      }),
    ],
  });
}

// Trend is each of the costliest models' spend over the window.
function Trend({ scope }: { scope: Scope }): ReactElement {
  const f = scope.fields;
  const cost = usageMeasure(f, "sum", f.cost);
  const answer = useAggregate(
    scope,
    f.model && cost ? { group_by: f.model.name, measures: [cost], bins: "auto" } : null,
  );
  return createElement(Panel<AggregateResponse>, {
    title: "Spend by model over time",
    answer,
    query:
      f.model &&
      f.cost &&
      `sum by (${f.model.name}) (sum_over_time(${logql(scope.filters)} | unwrap ${f.cost.name} [1h]))`,
    render: (r) => {
      const top = r.series.filter((s) => s.key !== "").slice(0, 5);
      const [bins] = perBin(r);
      const ys = top.map((s) => {
        const [xs, vs] = perBin({ series: [s] });
        return bins.map((t) => vs[xs.indexOf(t)] ?? 0);
      });
      return createElement(Plot, {
        kind: "line",
        xs: bins,
        ys,
        series: top.map((s) => s.key),
        label: "Spend by model over time",
        x: { time: true },
        y: { label: "spend", format: usd },
      });
    },
  });
}

// Latency is time to first token by model, where the logs report it.
function Latency({ scope }: { scope: Scope }): ReactElement {
  const f = scope.fields;
  const req =
    f.model && f.ttft
      ? {
          group_by: f.model.name,
          measures: [spec("p50", f.ttft), spec("p95", f.ttft)].flatMap((s) => s ?? []),
          bins: "all",
        }
      : null;
  const answer = useAggregate(scope, req);
  return createElement(Panel<AggregateResponse>, {
    title: "Time to first token",
    answer: req ? answer : new Absent("no time-to-first-token field"),
    render: (r) =>
      createElement(Table, {
        label: "Time to first token",
        columns: ["Model", "p50", "p95"],
        rows: r.series.map((s) => ({
          key: s.key,
          cells: [0, 1].map((i) => ms(inMs(f.ttft?.name ?? "", valueAt(s, i, 0) ?? Number.NaN))),
          query: sliceQuery(scope, f.model, s.key),
        })),
      }),
  });
}

export function ModelsTab(p: TabProps): ReactElement {
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Lede, {
      title: "Models",
      text: "Calls, tokens, cache hit, spend and latency per model.",
    }),
    createElement(Ledger, p),
    createElement(
      "div",
      { className: "g2" },
      createElement(Trend, { scope: p.scope }),
      createElement(Latency, { scope: p.scope }),
    ),
  );
}
