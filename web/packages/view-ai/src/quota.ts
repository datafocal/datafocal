import {
  type AggregateResponse,
  binStarts,
  type MeasureSpec,
  type Predicate,
  type Selector,
  type Series,
} from "@datafocal/api";
import { logql } from "@datafocal/query";
import { navigate, useTimeRange } from "@datafocal/shell";
import { Absent, Meter } from "@datafocal/ui";
import { createElement, type ReactElement } from "react";
import { type Fields, type Quota, source } from "./fields";
import {
  grouped,
  num,
  Panel,
  type Scope,
  selectorOf,
  sliceQuery,
  type TabProps,
  Table,
  useAggregate,
  valueAt,
} from "./panel";
import { Facts, Lede, Plot } from "./parts";

// QuotaStat is a window's latest and highest used share, when it resets and how often the
// logs read it.
export interface QuotaStat {
  // source is the log source reporting it, whose account the window is.
  readonly source: string;
  readonly window: string;
  readonly now: number;
  readonly peak: number;
  readonly resets: string;
  readonly readings: number;
}

// windowMeasures are four per window: its latest and highest share, its latest reset time
// and its readings.
const windowMeasures = (q: Quota): MeasureSpec[] => [
  { measure: `last:${q.used}` },
  { measure: `max:${q.used}` },
  { measure: q.resets ? `last:${q.resets}` : "count" },
  { measure: "count", where: [{ field: q.used, op: "exists" }] },
];

const accountMeasures = (f: Fields): MeasureSpec[] =>
  [
    f.quotaLimited && {
      measure: "count",
      where: [{ field: f.quotaLimited.name, op: "exists" }],
    },
    f.credits && { measure: `last:${f.credits.name}` },
  ].flatMap((m) => m ?? []);

const statOf = (s: Series, q: Quota, i: number): QuotaStat => ({
  source: s.key,
  window: q.window,
  now: valueAt(s, i * 4, 0) ?? Number.NaN,
  peak: valueAt(s, i * 4 + 1, 0) ?? Number.NaN,
  resets: q.resets ? (s.measure_labels?.[i * 4 + 2]?.[0] ?? "") : "",
  readings: valueAt(s, i * 4 + 3, 0) ?? 0,
});

// Account is the quota windows' numbers, the limits hit and the credits left.
export interface Account {
  readonly windows: readonly QuotaStat[];
  readonly limited: number;
  readonly credits: number;
}

// quotaLines narrows selector to the lines reporting quota, so sources reporting none are no
// group of the answer.
function quotaLines(f: Fields, selector: Selector): Selector {
  const any = [...f.quotas.map((q) => q.used), f.quotaLimited?.name, f.credits?.name]
    .flatMap((name) => name ?? [])
    .map((field): Predicate => ({ field, op: "exists" }));
  return { ...selector, where: [...(selector.where ?? []), { any }] };
}

// useQuota reads every window the logs report, per log source, over the page's lines or
// selector's: two sources' 5h windows are two accounts' and are not combined.
export function useQuota(
  scope: Scope,
  selector: Selector = selectorOf(scope),
): Account | Error | undefined {
  const f = scope.fields;
  const extra = accountMeasures(f);
  const asks = f.quotas.length > 0 || extra.length > 0;
  const answer = useAggregate(
    scope,
    asks
      ? {
          group_by: source,
          measures: [...f.quotas.flatMap(windowMeasures), ...extra],
          bins: "all",
        }
      : null,
    quotaLines(f, selector),
  );
  return answer instanceof Error || !answer ? answer : accountOf(f, answer.series);
}

// accountOf is each source's windows, then the limits all hit and the latest credits any has.
function accountOf(f: Fields, series: readonly Series[]): Account {
  const at = f.quotas.length * 4;
  const get = (s: Series, i: number) => valueAt(s, i, 0) ?? Number.NaN;
  const credits = series.map((s) => get(s, at + (f.quotaLimited ? 1 : 0)));
  return {
    windows: series.flatMap((s) =>
      f.quotas.map((q, i) => statOf(s, q, i)).filter((w) => w.readings > 0),
    ),
    limited: f.quotaLimited ? series.reduce((n, s) => n + (get(s, at) || 0), 0) : 0,
    credits: f.credits ? (credits.find(Number.isFinite) ?? Number.NaN) : Number.NaN,
  };
}

// label names a window by its source too when several sources report windows.
export const label = (a: Account, w: QuotaStat): string =>
  new Set(a.windows.map((x) => x.source)).size > 1 ? `${w.source} · ${w.window}` : w.window;

// highest is the window closest to its limit now.
export const highest = (a: Account): QuotaStat | undefined =>
  [...a.windows].sort((x, y) => y.now - x.now)[0];

export const share = (v: number): string => (Number.isFinite(v) ? `${v.toFixed(1)}%` : "—");

// when is a reset time as the page's readers see it, in their own zone.
const when = (at: string): string => (at ? new Date(at).toLocaleString() : "—");

function Windows(p: { a: Account; scope: Scope }): ReactElement {
  const f = p.scope.fields;
  return createElement(
    "div",
    null,
    createElement(Facts, {
      facts: [
        ["Windows", num(p.a.windows.length)],
        ["Highest now", share(highest(p.a)?.now ?? Number.NaN), "of a window"],
        ["Limits hit", f.quotaLimited ? num(p.a.limited) : "—", "lines"],
        ["Credits left", Number.isFinite(p.a.credits) ? num(p.a.credits) : "—", "latest"],
      ],
    }),
    createElement(Table, {
      label: "Quota windows",
      columns: ["Window", "Used now", "Peak", "Resets", "Readings"],
      rows: p.a.windows.map((w) => ({
        key: label(p.a, w),
        cells: [
          createElement(Meter, { text: share(w.now), share: w.now / 100 }),
          share(w.peak),
          when(w.resets),
          num(w.readings),
        ],
      })),
    }),
  );
}

// highestPerBin is measure i's highest value over the sources in each bin: a share is no sum.
function highestPerBin(r: AggregateResponse, i: number): Map<number, number> {
  const bins = new Map<number, number>();
  for (const s of r.series) {
    binStarts(s).forEach((t, bin) => {
      const v = valueAt(s, i, bin);
      if (v !== null) {
        bins.set(t, Math.max(bins.get(t) ?? v, v));
      }
    });
  }
  return bins;
}

// logsAround opens the lines under the page's filters over the bin starting at, step ms wide,
// and one either side: the prompts and runs that moved the quota then.
function logsAround(scope: Scope, at: number, step: number): void {
  const iso = (t: number) => new Date(t).toISOString();
  navigate({
    view: "logs",
    query: logql(scope.filters),
    time: `${iso(at - step)},${iso(at + 2 * step)}`,
  });
}

// Trend is each window's highest share per bin, so the climb to a limit and its reset show.
// A drag across it narrows the page to that span, and a click opens the lines around then.
function Trend({ scope }: { scope: Scope }): ReactElement {
  const f = scope.fields;
  const [, setRange] = useTimeRange();
  const answer = useAggregate(
    scope,
    {
      group_by: source,
      measures: f.quotas.map((q) => ({ measure: `max:${q.used}` })),
      bins: "auto",
    },
    quotaLines(f, selectorOf(scope)),
  );
  return createElement(Panel<AggregateResponse>, {
    title: "Quota over time",
    answer,
    render: (r) => {
      const bins = f.quotas.map((_, i) => highestPerBin(r, i));
      const xs = [...new Set(bins.flatMap((b) => [...b.keys()]))].sort((a, b) => a - b);
      const iso = (t: number) => new Date(t).toISOString();
      return createElement(Plot, {
        kind: "line",
        xs,
        ys: bins.map((b) => xs.map((t) => b.get(t) ?? Number.NaN)),
        series: f.quotas.map((q) => q.window),
        label: "Quota used over time",
        x: { label: "time", time: true },
        y: { label: "used", format: share },
        height: 240,
        onBrush: (from, to) => setRange({ from: iso(from), to: iso(to) }),
        onPick: (at) => logsAround(scope, at, (xs[1] ?? at) - (xs[0] ?? at)),
      });
    },
  });
}

// RunClimb is how far a run saw each window climb: from its first reading to its highest.
interface RunClimb {
  readonly run: string;
  readonly climbs: readonly number[];
}

// climbsOf are r's runs by the climb they saw in the first window, the largest first.
function climbsOf(r: AggregateResponse, f: Fields): RunClimb[] {
  return r.series
    .filter((s) => s.key !== "")
    .map((s) => ({
      run: s.key,
      climbs: f.quotas.map((_, i) => {
        const [first, peak] = [valueAt(s, i * 2, 0), valueAt(s, i * 2 + 1, 0)];
        return first === null || peak === null ? Number.NaN : peak - first;
      }),
    }))
    .sort((a, b) => (b.climbs[0] || 0) - (a.climbs[0] || 0));
}

const points = (v: number): string => (Number.isFinite(v) ? `+${v.toFixed(1)} pts` : "—");

// Runs is how far each run saw the quota climb, so the runs that spent it stand out. Runs at
// the same time share the account, so each one's climb holds the others' use too.
function Runs(p: TabProps): ReactElement {
  const f = p.scope.fields;
  const answer = useAggregate(
    p.scope,
    f.run
      ? {
          group_by: f.run.name,
          measures: f.quotas.flatMap((q) => [
            { measure: `first:${q.used}` },
            { measure: `max:${q.used}` },
          ]),
          bins: "all",
          limit: 10_000,
        }
      : null,
    quotaLines(f, selectorOf(p.scope)),
  );
  return createElement(Panel<AggregateResponse>, {
    title: "Quota by run",
    answer: f.run ? answer : new Absent("no run: the quota lines name no session or run"),
    render: (r) => {
      const runs = climbsOf(r, f);
      const most = f.quotas.map((_, i) => Math.max(0, ...runs.map((x) => x.climbs[i] || 0)) || 1);
      return [
        createElement(
          "p",
          { key: "n" },
          "Each run's climb is from its first reading to its highest. Runs at the same time share the account, so each climb holds the others' use too.",
        ),
        createElement(Table, {
          key: "t",
          label: "Quota by run",
          columns: ["Run", ...f.quotas.map((q) => `${q.window} climb`)],
          rows: runs.map((x) => ({
            key: x.run,
            cells: x.climbs.map((v, i) =>
              createElement(Meter, { text: points(v), share: (v || 0) / (most[i] ?? 1) }),
            ),
            query: sliceQuery(p.scope, f.run, x.run),
            onPick: () => p.openRun(x.run),
          })),
        }),
      ];
    },
  });
}

// Plans is the subscriptions the lines name, by how many lines name each.
function Plans({ scope }: { scope: Scope }): ReactElement {
  const plan = scope.fields.plan;
  const answer = useAggregate(
    scope,
    plan ? { group_by: plan.name, measure: "count", bins: "all" } : null,
  );
  return createElement(Panel<AggregateResponse>, {
    title: "Plans",
    answer: plan ? answer : new Absent("no plan: the logs name no subscription"),
    render: (r) =>
      createElement(Table, {
        label: "Plans",
        columns: ["Plan", "Lines"],
        rows: grouped(r, "")[0].series.map((s) => ({
          key: s.key,
          cells: [num(valueAt(s, 0, 0) ?? 0)],
          query: sliceQuery(scope, plan, s.key),
        })),
      }),
  });
}

// absent says what the logs must carry for quota to show, and where to read how to log it.
const absent = new Absent(
  "no quota. Quota shows when the logs report a subscription window's used share, a limit hit or the credits left; an agent that reports its quota only to its status line can append it to a log",
);

export function QuotaTab(p: TabProps): ReactElement {
  const f = p.scope.fields;
  const account = useQuota(p.scope);
  const any = f.quotas.length > 0 || f.quotaLimited || f.credits;
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Lede, {
      title: "Quota",
      text: "How much of each subscription window the agents have used, when it resets, the limits they hit and the runs that used it, from what the agents log about their account. Drag across the chart to narrow the range, or click it for the lines around then.",
    }),
    createElement(Panel<Account>, {
      title: "Quota windows",
      answer: any ? account : absent,
      render: (a) => createElement(Windows, { a, scope: p.scope }),
    }),
    f.quotas.length > 0 && createElement(Trend, { scope: p.scope }),
    f.quotas.length > 0 && createElement(Runs, p),
    createElement(Plans, { scope: p.scope }),
  );
}
