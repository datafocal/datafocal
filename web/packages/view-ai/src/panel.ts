import {
  type AggregateRequest,
  type AggregateResponse,
  binStarts,
  type DataFocalClient,
  type MeasureSpec,
  type Predicate,
  type Selector,
  type Series,
} from "@datafocal/api";
import { logql } from "@datafocal/query";
import { navigate, type TimeRange } from "@datafocal/shell";
import { Absent, type Answer, isAbsent, Show, useAnswer } from "@datafocal/ui";
import { createElement, type ReactElement, type ReactNode } from "react";
import type { Field, Fields } from "./fields";

// Scope is what every panel narrows by and calls through.
export interface Scope {
  readonly client: DataFocalClient;
  readonly range: TimeRange;
  // filters are the drill-down's.
  readonly filters: readonly Predicate[];
  readonly fields: Fields;
}

// Tab is a tab of the view, which item drills into one thing on: a tool, a skill, a model.
export type Tab =
  | "overview"
  | "context"
  | "tools"
  | "skills"
  | "memory"
  | "cache"
  | "models"
  | "quota"
  | "compare"
  | "runs"
  | "run"
  | "outcomes"
  | "map";

export interface TabProps {
  readonly scope: Scope;
  // pivot narrows the whole page to a slice.
  readonly pivot: (p: Predicate) => void;
  // picked is the run the Run tab shows, which openRun sets and switches to, or what another
  // tab drills into, which open sets along with the tab.
  readonly picked: string | undefined;
  readonly openRun: (run: string) => void;
  readonly open: (tab: Tab, item?: string) => void;
  // split is what Compare compares, which compare sets and switches to.
  readonly split: string | undefined;
  readonly compare: (split: Split) => void;
}

// Split is what Compare sets apart: the runs that carry a value of a field against the rest,
// or the runs before a time against those after it.
export type Split = { readonly field: string; readonly value: string } | { readonly at: string };

export const eq = (field: string, value: string): Predicate => ({ field, op: "eq", value });

// sliceQuery is the Logs query for the lines of one value of field under the page's filters.
export const sliceQuery = (scope: Scope, field: Field | undefined, value: string): string =>
  logql([...scope.filters, eq(field?.name ?? "", value)]);

// spec reads kind over field, or is undefined when the logs lack the field.
export const spec = (kind: string, field: Field | undefined): MeasureSpec | undefined =>
  field && { measure: `${kind}:${field.name}` };

// usageMeasure is the measure reading kind over field once per call, distinct_by the call
// role; per line when fields.call is absent.
export const usageMeasure = (
  f: Fields,
  kind: "sum" | "max",
  field: Field | undefined,
): MeasureSpec | undefined =>
  field && { measure: `${kind}:${field.name}`, ...(f.call && { distinct_by: f.call.name }) };

// callsMeasure counts model calls: the lines carrying input, each call once where a source
// names its calls and repeats one across lines, and every line where it does not.
export const callsMeasure = (f: Fields): MeasureSpec | undefined =>
  f.input && {
    measure: "count",
    where: [{ field: f.input.name, op: "exists" }],
    ...(f.call && { distinct_by: f.call.name }),
  };

// measures lists the present specs in order, at giving each name's index among them.
export function measures<K extends string>(
  specs: Record<K, MeasureSpec | undefined>,
): { list: MeasureSpec[]; at: (k: K) => number } {
  const present = Object.entries<MeasureSpec | undefined>(specs).filter(([, s]) => s);
  return {
    list: present.flatMap(([, s]) => s ?? []),
    at: (k) => present.findIndex(([name]) => name === k),
  };
}

const absent = new Absent("the logs carry no field for it");

// failure is the first of answers that failed, rather than finding the logs lack it.
export const failure = (...answers: readonly unknown[]): Error | undefined =>
  answers.find((a): a is Error => a instanceof Error && !isAbsent(a));

// selectorOf selects the lines under the page's filters. It asks for no field beyond them: a
// measure reads only the lines carrying its field, so a source that lacks one is still counted
// for the rest.
export const selectorOf = (scope: Scope): Selector => ({
  time: scope.range,
  where: [...scope.filters],
});

// useAggregate is the answer to req over the page's lines, or over selector's when given.
export function useAggregate(
  scope: Scope,
  req: Partial<AggregateRequest> | null,
  selector: Selector = selectorOf(scope),
): AggregateResponse | Error | undefined {
  // A request whose measures all read fields the logs lack asks nothing.
  const asks = req?.measures?.length !== 0 || Boolean(req?.measure);
  const full = req && asks ? { selector, measure: "", group_by: "", ...req } : null;
  const answer = useAnswer(full && JSON.stringify(full), async () =>
    full ? (await scope.client.call("aggregate", full)).data : Promise.reject(absent),
  );
  return full ? answer : absent;
}

// usePatterns is the text of each of the pattern ids given, as list_patterns reads it over
// the page's lines; an id it does not find is shown as itself.
export function usePatterns(scope: Scope, ids: readonly string[]): ReadonlyMap<string, string> {
  const wanted = [...new Set(ids)].filter(Boolean).sort();
  const any = wanted.map((value): Predicate => ({ field: "pattern_id", op: "eq", value }));
  const selector = { time: scope.range, where: [...scope.filters, { any }] };
  const found = useAnswer(wanted.length ? JSON.stringify(selector) : null, () =>
    scope.client.call("list_patterns", { selector, limit: wanted.length }).then((e) => e.data),
  );
  return found instanceof Error || !found
    ? new Map()
    : new Map(found.patterns.map((p) => [p.pattern_id, p.pattern]));
}

export const sum = (vs: readonly number[]): number => vs.reduce((a, b) => a + b, 0);

// useRunCount is how many runs have lines under the page's filters. It counts the groups of
// the run field: a distinct with no group_by is one per pattern, not one for the window.
export function useRunCount(scope: Scope): number {
  const run = scope.fields.run;
  const r = useAggregate(
    scope,
    run ? { group_by: run.name, measure: "count", bins: "all", limit: 10_000 } : null,
  );
  return r instanceof Error || !r ? 0 : r.series.filter((s) => s.key !== "").length;
}

// Usage is the spend and token sums per bin, which the KPIs, cache trend and costs share.
// input is the total input calls sent, the sum of the inputs fields, and source says which.
export interface Usage {
  readonly bins: number[];
  readonly cost: number[];
  readonly input: number[];
  readonly output: number[];
  readonly cache: number[];
  readonly inputs: readonly Field[];
  readonly source: string;
}

const byModel = (f: Fields) => f.model?.name ?? "";

export function useUsage(scope: Scope): Usage | Error | undefined {
  const f = scope.fields;
  const m = measures({
    cost: usageMeasure(f, "sum", f.cost),
    input: usageMeasure(f, "sum", f.input),
    output: usageMeasure(f, "sum", f.output),
    cache: usageMeasure(f, "sum", f.cacheRead),
    write: usageMeasure(f, "sum", f.cacheWrite),
  });
  // Grouped by model, as every call has one: with no group_by the engine groups by pattern,
  // a series for every shape of line. Lines naming no model are the "" group, still summed.
  const r = useAggregate(scope, { group_by: byModel(f), measures: m.list, bins: "auto" });
  if (r instanceof Error || !r) {
    return r;
  }
  const [bins] = perBin(r);
  const at = (k: "cost" | "input" | "output" | "cache" | "write") =>
    m.at(k) < 0 ? bins.map(() => 0) : perBin(r, m.at(k))[1];
  const [input, cache, write] = [at("input"), at("cache"), at("write")];
  // Reading more from cache than all input is only possible when input excludes the cache.
  const excludes = f.excludesCache || sum(cache) > sum(input);
  return {
    bins,
    cost: at("cost"),
    output: at("output"),
    cache,
    input: excludes ? input.map((v, i) => v + (cache[i] ?? 0) + (write[i] ?? 0)) : input,
    inputs: [f.input, ...(excludes ? [f.cacheRead, f.cacheWrite] : [])].flatMap((x) => x ?? []),
    source: excludes
      ? "input + cache read + cache write, as input excludes the cache"
      : (f.input?.source ?? ""),
  };
}

// inputAt is the total input in a series' bin whose measures from first read inputs.
export const inputAt = (inputs: readonly Field[], s: Series, bin: number, first = 0): number =>
  sum(inputs.map((_, i) => valueAt(s, first + i, bin) ?? 0));

// peaks reads each run's peak context, the most input one call sent: per call when the logs
// name calls, each labelled with its run, else per run over its lines.
export const peaks = (f: Fields, inputs: readonly Field[]): Partial<AggregateRequest> | null =>
  f.run && inputs.length
    ? {
        group_by: (f.call ?? f.run).name,
        measures: [
          ...inputs.map((x) => ({ measure: `max:${x.name}` })),
          ...(f.call ? [{ measure: `last:${f.run.name}` }] : []),
        ],
        bins: "all",
        limit: 10_000,
      }
    : null;

export function peaksOf(
  r: AggregateResponse,
  f: Fields,
  inputs: readonly Field[],
): Map<string, number> {
  const byRun = new Map<string, number>();
  for (const s of r.series) {
    const run = f.call ? s.key && s.measure_labels?.[inputs.length]?.[0] : s.key;
    if (run) {
      byRun.set(run, Math.max(byRun.get(run) ?? 0, inputAt(inputs, s, 0)));
    }
  }
  return byRun;
}

// valueAt is measure i's value in bin, or the series' value when the request had one
// measure; null where the series has no value in that bin.
export const valueAt = (s: Series, i: number, bin: number): number | null =>
  (s.measure_values ? s.measure_values[i]?.[bin] : s.values?.[bin]) ?? null;

// seriesTotal is measure i summed over s's bins.
const seriesTotal = (s: Series, i: number): number =>
  sum(binStarts(s).map((_, bin) => valueAt(s, i, bin) ?? 0));

export function total(res: AggregateResponse, i = 0): number {
  return sum(res.series.map((s) => seriesTotal(s, i)));
}

export interface Share {
  readonly key: string;
  readonly value: number;
  readonly share: number;
}

// perBin is measure i summed over the groups in each bin, as uPlot's x and y arrays.
export function perBin(res: AggregateResponse, i = 0): [number[], number[]] {
  const bins = new Map<number, number>();
  for (const s of res.series) {
    binStarts(s).forEach((t, bin) => {
      const v = valueAt(s, i, bin);
      if (v !== null) {
        bins.set(t, (bins.get(t) ?? 0) + v);
      }
    });
  }
  const ts = [...bins.keys()].sort((a, b) => a - b);
  return [ts, ts.map((t) => bins.get(t) ?? 0)];
}

// grouped is res without the lines lacking its group_by field, which the engine keys "" and
// are no value of it, and a note of how many lines that was when measure lines counts them.
export function grouped(
  res: AggregateResponse,
  field: string,
  lines?: number,
): [AggregateResponse, ReactNode] {
  const none = res.series.find((s) => s.key === "");
  const kept = { ...res, series: res.series.filter((s) => s !== none) };
  const n =
    none && lines !== undefined && lines >= 0 ? total({ ...res, series: [none] }, lines) : 0;
  return [kept, n > 0 && createElement("p", { key: "none" }, `${num(n)} lines carry no ${field}`)];
}

// breakdown is each group's total of the measures given, else of measure 0, and its share of
// all the groups'.
export function breakdown(res: AggregateResponse, ...measures: number[]): Share[] {
  const of = (s: Series) => sum((measures.length ? measures : [0]).map((i) => seriesTotal(s, i)));
  const all = sum(res.series.map(of));
  return res.series.map((s) => {
    const value = of(s);
    return { key: s.key, value, share: all ? value / all : 0 };
  });
}

const compact = new Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 });
const dollars = new Intl.NumberFormat("en", { style: "currency", currency: "USD" });
export const num = (v: number): string => compact.format(v);
export const usd = (v: number): string => dollars.format(v);
// money is v in dollars when the logs carry a cost, and a dash rather than $0.00 when not.
export const money = (f: Fields, v: number): string => (f.cost ? usd(v) : "—");
export const pct = (v: number): string => (Number.isFinite(v) ? `${(v * 100).toFixed(1)}%` : "—");
// tokens is a character count as tokens, at four characters a token.
export const tokens = (chars: number): string => num(chars / 4);

export interface Row {
  readonly key: string;
  readonly cells: readonly ReactNode[];
  // query, when set, finds the row's lines in Logs; onPick, when set, is what clicking its
  // key does.
  readonly query?: string;
  readonly onPick?: () => void;
}

export const button = (label: string, onClick: () => void, title?: string): ReactElement =>
  createElement("button", { type: "button", className: "link", title, onClick }, label);

export const openLogs = (query: string, title: string): void =>
  navigate({ view: "logs", query, title });

// pageSize is the aggregate tool's default page of groups, the most a ranked table shows.
const pageSize = 100;

export function Table(p: {
  label: string;
  columns: readonly string[];
  rows: readonly Row[];
}): ReactElement {
  const head = [...p.columns, ""].map((c, i) => createElement("th", { key: i }, c));
  return createElement(
    "table",
    { "aria-label": p.label },
    createElement("thead", null, createElement("tr", null, head)),
    createElement(
      "tbody",
      null,
      p.rows.slice(0, pageSize).map((r) =>
        createElement(
          "tr",
          { key: r.key },
          createElement("td", null, r.onPick ? button(r.key, r.onPick) : r.key),
          r.cells.map((c, i) => createElement("td", { key: i }, c)),
          createElement(
            "td",
            null,
            r.query !== undefined &&
              button("Logs", () => openLogs(r.query ?? "", `${p.label}: ${r.key}`), "Open in Logs"),
          ),
        ),
      ),
    ),
  );
}

// Panel titles a section and shows its answer once it arrives, or why it cannot; given its
// chart's LogQL, it offers to add the chart to a saved view.
export function Panel<T>(p: {
  title: string;
  answer: Answer<T>;
  render: (v: T) => ReactNode;
  query?: string | undefined;
}): ReactElement {
  const { query } = p;
  const add = query && button("Add to view", () => navigate({ view: "views", query }));
  return createElement(
    "section",
    { className: "panel", "aria-label": p.title },
    createElement("h3", null, p.title, add),
    createElement(Show<T>, { answer: p.answer, render: p.render }),
  );
}
