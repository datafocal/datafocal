import type { AggregateResponse, MeasureSpec, Predicate, Selector } from "@datafocal/api";
import {
  callsMeasure,
  measures,
  peaks,
  peaksOf,
  type Scope,
  selectorOf,
  spec,
  total,
  usageMeasure,
  useAggregate,
  type Usage,
  useUsage,
  valueAt,
} from "./panel";

// RunStat is what one run spent and did: the numbers every per-run comparison reads.
export interface RunStat {
  readonly run: string;
  readonly cost: number;
  // input is all the input its calls were billed for, read from cache or not.
  readonly input: number;
  readonly cacheRead: number;
  readonly cacheWrite: number;
  readonly calls: number;
  readonly toolCalls: number;
  readonly modelChanges: number;
  readonly toolListChanges: number;
  readonly compactions: number;
  // compacted is the tokens its compactions removed from the context.
  readonly compacted: number;
  readonly peak: number;
}

const runMeasures = (scope: Scope) => {
  const f = scope.fields;
  return measures({
    cost: usageMeasure(f, "sum", f.cost),
    input: usageMeasure(f, "sum", f.input),
    read: usageMeasure(f, "sum", f.cacheRead),
    write: usageMeasure(f, "sum", f.cacheWrite),
    calls: callsMeasure(f),
    tools: f.tool && { measure: "count", where: [{ field: f.tool.name, op: "exists" }] },
  });
};

// changeMeasures are asked apart from the rest: a column some sources log, such as a
// tool-list hash, may be in no segment of part of the window, and the engine refuses a
// request with a measure reading a column it has nowhere.
const changeMeasures = (scope: Scope) => {
  const f = scope.fields;
  return measures({
    models: spec("changes", f.model),
    toolLists: spec("changes", f.toolList),
    compactions: f.compaction && {
      measure: "count",
      where: [{ field: f.compaction.name, op: "exists" }],
    },
    compacted: spec("sum", f.compaction),
  });
};

// usePeaks is each run's peak context, read once usage says which columns are input; none
// while it loads or where the logs cannot tell it.
function usePeaks(scope: Scope, usage: Usage | undefined, selector: Selector) {
  const inputs = usage?.inputs ?? [];
  const peak = useAggregate(scope, usage ? peaks(scope.fields, inputs) : null, selector);
  return peak instanceof Error || !peak
    ? new Map<string, number>()
    : peaksOf(peak, scope.fields, inputs);
}

const runRequest = (scope: Scope, list: MeasureSpec[], ready: boolean) =>
  ready && scope.fields.run
    ? { group_by: scope.fields.run.name, measures: list, bins: "all", limit: 10_000 }
    : null;

// useRunStats reads the numbers of every run that made a model call over the page's lines,
// or selector's.
export function useRunStats(
  scope: Scope,
  selector: Selector = selectorOf(scope),
): readonly RunStat[] | Error | undefined {
  const usage = useUsage(scope);
  const loaded = usage instanceof Error ? undefined : usage;
  const [m, c] = [runMeasures(scope), changeMeasures(scope)];
  const runs = useAggregate(scope, runRequest(scope, m.list, loaded !== undefined), selector);
  const changes = useAggregate(scope, runRequest(scope, c.list, loaded !== undefined), selector);
  const peak = usePeaks(scope, loaded, selector);
  if (usage instanceof Error) {
    return usage;
  }
  // Until usage loads, runs asks nothing, which is no answer yet rather than none.
  if (!usage) {
    return undefined;
  }
  if (runs instanceof Error) {
    return runs;
  }
  if (!runs) {
    return undefined;
  }
  const excludes = scope.fields.excludesCache || usage.inputs.length > 1;
  return statsOf(runs, m.at, peak, excludes).map((r) => withChanges(r, changes, c.at));
}

type ChangeKey = Parameters<ReturnType<typeof changeMeasures>["at"]>[0];

// withChanges adds a run's changes, none where they could not be read.
function withChanges(
  r: RunStat,
  changes: AggregateResponse | Error | undefined,
  at: (k: ChangeKey) => number,
): RunStat {
  const s = changes instanceof Error ? undefined : changes?.series.find((x) => x.key === r.run);
  const get = (k: ChangeKey) => (!s || at(k) < 0 ? 0 : (valueAt(s, at(k), 0) ?? 0));
  return {
    ...r,
    modelChanges: get("models"),
    toolListChanges: get("toolLists"),
    compactions: get("compactions"),
    compacted: get("compacted"),
  };
}

type RunKey = Parameters<ReturnType<typeof runMeasures>["at"]>[0];

function statsOf(
  r: AggregateResponse,
  at: (k: RunKey) => number,
  peaksByRun: Map<string, number>,
  excludes: boolean,
): RunStat[] {
  // A unit of work that made no model call, such as another program's pod, is no run.
  return r.series
    .filter((s) => s.key !== "" && (at("calls") < 0 || (valueAt(s, at("calls"), 0) ?? 0) > 0))
    .map((s) => {
      const get = (k: RunKey) => (at(k) < 0 ? 0 : (valueAt(s, at(k), 0) ?? 0));
      const [read, write] = [get("read"), get("write")];
      return {
        run: s.key,
        cost: get("cost"),
        input: get("input") + (excludes ? read + write : 0),
        cacheRead: read,
        cacheWrite: write,
        calls: get("calls"),
        toolCalls: get("tools"),
        modelChanges: 0,
        toolListChanges: 0,
        compactions: 0,
        compacted: 0,
        peak: peaksByRun.get(s.key) ?? 0,
      };
    });
}

// AfterCompaction is what the first model calls after a compaction spent and sent: each
// writes the compacted context to the cache again, as its prefix is new.
export interface AfterCompaction {
  readonly calls: number;
  readonly cost: number;
  // input is all the input the calls were billed for, read from cache or not.
  readonly input: number;
  readonly cacheRead: number;
  readonly cacheWrite: number;
}

// useAfterCompaction reads the first calls after a compaction over the page's lines; absent
// where the logs show no compaction.
export function useAfterCompaction(scope: Scope): AfterCompaction | Error | undefined {
  const usage = useUsage(scope);
  const m = runMeasures(scope);
  const r = useAggregate(scope, afterRequest(scope, m.list, usage), afterSelector(scope));
  if (usage instanceof Error) {
    return usage;
  }
  if (r instanceof Error) {
    return r;
  }
  const excludes = scope.fields.excludesCache || (usage?.inputs.length ?? 0) > 1;
  return r && usage ? afterOf(r, m.at, excludes) : undefined;
}

const afterRequest = (scope: Scope, list: MeasureSpec[], usage: Usage | Error | undefined) =>
  scope.fields.afterCompaction && usage && !(usage instanceof Error)
    ? { group_by: scope.fields.model?.name ?? "", measures: list, bins: "all" }
    : null;

// afterSelector is the page's lines that are a first call after a compaction.
function afterSelector(scope: Scope): Selector {
  const base = selectorOf(scope);
  const after: Predicate = { field: scope.fields.afterCompaction?.name ?? "", op: "exists" };
  return { ...base, where: [...(base.where ?? []), after] };
}

// afterOf is r's totals over the first calls after a compaction, its input with the cache
// where excludes says input leaves the cache out.
function afterOf(r: AggregateResponse, at: (k: RunKey) => number, excludes: boolean) {
  const get = (k: RunKey) => (at(k) < 0 ? 0 : total(r, at(k)));
  const [read, write] = [get("read"), get("write")];
  return {
    calls: get("calls"),
    cost: get("cost"),
    input: get("input") + (excludes ? read + write : 0),
    cacheRead: read,
    cacheWrite: write,
  };
}

// hitRate is the share of input a run's calls read from the cache.
export const hitRate = (r: RunStat): number => (r.input ? r.cacheRead / r.input : Number.NaN);

// quantile is the nearest-rank quantile q of vs, as the engine's p<NN>.
export function quantile(vs: readonly number[], q: number): number {
  const sorted = [...vs].sort((a, b) => a - b);
  return sorted[Math.max(0, Math.ceil(q * sorted.length) - 1)] ?? 0;
}

// random is a seeded generator (mulberry32), so a resample is the same on every load of the
// same answer, as the determinism contract asks.
function random(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const resample = (vs: readonly number[], next: () => number) =>
  vs.map(() => vs[Math.floor(next() * vs.length)] ?? 0);

// Delta is how much the median of after differs from before's, relative to before's, with a
// 95% bootstrap interval.
export interface Delta {
  readonly before: number;
  readonly after: number;
  readonly change: number;
  readonly lo: number;
  readonly hi: number;
}

// delta compares the medians of two samples, resampling each rounds times.
export function delta(before: readonly number[], after: readonly number[], rounds = 500): Delta {
  const next = random(before.length * 7919 + after.length);
  const rel = (a: number, b: number) => (a === 0 ? (b === 0 ? 0 : 1) : (b - a) / a);
  const [mb, ma] = [quantile(before, 0.5), quantile(after, 0.5)];
  const changes = Array.from({ length: rounds }, () =>
    rel(quantile(resample(before, next), 0.5), quantile(resample(after, next), 0.5)),
  );
  return {
    before: mb,
    after: ma,
    change: rel(mb, ma),
    lo: quantile(changes, 0.025),
    hi: quantile(changes, 0.975),
  };
}

// ms is a duration in the largest units that keep it short.
export function ms(v: number): string {
  if (!Number.isFinite(v)) {
    return "—";
  }
  if (v < 1000) {
    return `${Math.round(v)} ms`;
  }
  if (v < 60_000) {
    return `${(v / 1000).toFixed(1)} s`;
  }
  return `${Math.floor(v / 60_000)}m ${Math.round((v % 60_000) / 1000)}s`;
}

// inMs is a value of field in ms: a field says ms in its name, and otherwise, as OpenTelemetry's
// time_to_first_token, counts seconds.
export const inMs = (field: string, v: number): number =>
  /(_ms|Ms|millis\w*)$/.test(field) ? v : v * 1000;
