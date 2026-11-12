import type { DataFocalClient, LogLine, Predicate, TimeSpec } from "@datafocal/api";
import { duration, type Kpi, Kpis, Panel, Show, useAnswer } from "@datafocal/ui";
import { createElement as h, type ReactElement } from "react";
import { clock, measured, valueOf } from "./parts";
import { meets, type Slices } from "./scopes";
import type { Slice } from "./slices";

// Where a time-slice SLO's metric is heading: its rate since it last turned back, as a quota's
// use since its window reset, carried to when it crosses the objective and, where the definition
// names the field that says when the window resets, to the reset.

interface Trend {
  readonly at: number;
  readonly value: number;
  // perMs is the metric's change a millisecond since it last turned back.
  readonly perMs: number;
}

// trendOf is the run of slices since the metric last moved away from the objective's bound,
// and its rate.
function trendOf(s: Slices, slices: readonly Slice[]): Trend | undefined {
  const points = slices.flatMap((x) =>
    x.state === "none" || x.value == null ? [] : [{ at: x.to, value: x.value }],
  );
  const upper = s.op.startsWith("l");
  // A step back toward the bound, as a reset, ends the run.
  const turned = (a: number, b: number) => (upper ? a > b : a < b);
  const last = points.at(-1);
  const back = points.reduce(
    (found, x, i) => (i > 0 && turned(points[i - 1]?.value ?? x.value, x.value) ? i : found),
    0,
  );
  const first = points[back];
  if (!first || !last || last.at === first.at) {
    return undefined;
  }
  return {
    at: last.at,
    value: last.value,
    perMs: (last.value - first.value) / (last.at - first.at),
  };
}

// resetOf reads a reset time as logged: RFC 3339, or epoch seconds or milliseconds.
function resetOf(v: string): number | undefined {
  const n = Number(v);
  if (v !== "" && Number.isFinite(n)) {
    return n < 1e12 ? n * 1000 : n;
  }
  const t = Date.parse(v);
  return Number.isNaN(t) ? undefined : t;
}

// useReset is when the metric's window resets, from the newest line carrying the field.
function useReset(client: DataFocalClient, s: Slices, time: TimeSpec) {
  return useAnswer(JSON.stringify(["reset", s.resetsAt, s.where, time]), async () => {
    if (!s.resetsAt) {
      return undefined;
    }
    const where: Predicate[] = [...s.where, { field: s.resetsAt, op: "exists" }];
    const lines: LogLine[] =
      (await client.call("search_logs", { selector: { time, where }, limit: 1 })).data.lines ?? [];
    return lines[0] && resetOf(valueOf(lines[0], s.resetsAt));
  });
}

const field = (measure: string) => measure.split(":")[1] ?? measure;

// tiles are the metric now, its rate, when it crosses the objective, and its value at reset.
function tiles(s: Slices, t: Trend, reset: number | undefined): Kpi[] {
  const f = field(s.measure);
  const crossing = t.perMs !== 0 ? t.at + (s.value - t.value) / t.perMs : Number.NaN;
  const heading = crossing > t.at && (reset === undefined || crossing < reset);
  const out: Kpi[] = [
    { label: "Now", value: measured(f, t.value), detail: clock(t.at) },
    {
      label: "Rate",
      value: `${t.perMs >= 0 ? "+" : ""}${measured(f, t.perMs * 3_600_000)}`,
      detail: "an hour, since it last turned back",
    },
    heading
      ? {
          label: `Crosses ${measured(f, s.value)}`,
          value: clock(crossing),
          detail: `in ${duration(crossing - t.at)}`,
          tone: "bad",
        }
      : {
          label: `Crosses ${measured(f, s.value)}`,
          value: "—",
          detail: reset === undefined ? "not at this rate" : "not before it resets",
        },
  ];
  if (reset !== undefined) {
    const projected = t.value + t.perMs * (reset - t.at);
    out.push({
      label: "At reset",
      value: measured(f, projected),
      detail: `resets ${clock(reset)}`,
      tone: meets(s.op, projected, s.value) ? undefined : "bad",
    });
  }
  return out;
}

// Projection is where the metric is heading at its current rate, once it has a rate.
export function Projection(p: {
  client: DataFocalClient;
  s: Slices;
  slices: readonly Slice[];
  time: TimeSpec;
}): ReactElement | null {
  const reset = useReset(p.client, p.s, p.time);
  const t = trendOf(p.s, p.slices);
  if (!t) {
    return null;
  }
  return h(
    Panel,
    { title: "At this rate" },
    h(Show<number | undefined>, {
      answer: reset,
      render: (r) => h(Kpis, { tiles: tiles(p.s, t, r) }),
    }),
  );
}
