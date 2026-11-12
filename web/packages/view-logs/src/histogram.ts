import {
  binStarts,
  type DataFocalClient,
  type Selector,
  type Series,
  type TimeSpec,
} from "@datafocal/api";
import {
  type Baseline,
  baselineRange,
  Compare,
  describeBaseline,
  resolve,
  type TimeRange,
  useCompare,
  useParam,
} from "@datafocal/shell";
import { type ColorToken, cssColor, Show } from "@datafocal/ui";
import {
  createElement,
  type PointerEvent,
  type ReactElement,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import { useAsync } from "./lines";
import { severity } from "./row";

export interface HistogramProps {
  readonly client: DataFocalClient;
  readonly selector: Selector;
  readonly snapshot: string;
  // onBrush narrows the time range to the span dragged across.
  readonly onBrush: (r: TimeRange) => void;
  // onJump ends the range at the end of the bin clicked, to read what led up to it.
  readonly onJump: (at: string) => void;
}

// Band is a layer of the stacked bars: the series key it takes, its label and its colour.
interface Band {
  readonly key: string;
  readonly label: string;
  readonly color: ColorToken;
}

// levels are the bands by level, bottom up. Every level a producer writes folds into one, so
// each spelling of error is one colour.
const levels: readonly Band[] = [
  { key: "info", label: "other", color: "--line-2" },
  { key: "warn", label: "warn", color: "--warn" },
  { key: "error", label: "error", color: "--err" },
];

// palette colours a field's top values, most lines first; the rest of its lines are one band.
const palette = [
  "--cat-1",
  "--cat-2",
  "--cat-3",
  "--cat-4",
  "--cat-5",
] as const satisfies readonly ColorToken[];

// rest is the key of the series of the lines outside a field's top values, which no value of
// the field can be.
const rest = "\u0000rest";

// bandsOf is the bands series stack into, bottom up: by level, the levels; by another field,
// the lines outside its top values, then each top value.
function bandsOf(by: string, series: readonly Series[]): readonly Band[] {
  if (by === "level") {
    return levels;
  }
  const top = series.filter((s) => s.key !== rest);
  return [
    { key: rest, label: "other", color: "--line-2" },
    ...top.map(
      (s, i): Band => ({
        key: s.key,
        label: s.key === "" ? "(none)" : s.key,
        color: palette[i] ?? "--line-2",
      }),
    ),
  ];
}

// Stack is the lines in each bin by band, on the bins' start times.
interface Stack {
  readonly bands: readonly Band[];
  readonly t: readonly number[];
  readonly step: number;
  readonly counts: readonly (readonly number[])[];
  readonly total: number;
}

// stack sums series, grouped by by, into bands, on the bins they share. A series no band
// takes goes to the first, which is the lines outside the others.
function stack(series: readonly Series[], by: string, bands = bandsOf(by, series)): Stack {
  const t = series[0] ? binStarts(series[0]) : [];
  const counts = bands.map(() => t.map(() => 0));
  const bandOf = (key: string) => (by === "level" ? severity(key) : key);
  let total = 0;
  for (const s of series) {
    const k = bands.findIndex((b) => b.key === bandOf(s.key));
    const band = counts[Math.max(k, 0)] ?? [];
    t.forEach((_, bin) => {
      const n = s.values?.[bin] ?? 0;
      band[bin] = (band[bin] ?? 0) + n;
      total += n;
    });
  }
  return { bands, t, step: series[0]?.step ?? 0, counts, total };
}

// span is the time s covers, from its first bin's start to its last bin's end.
const span = (s: Stack): [number, number] => [s.t[0] ?? 0, (s.t.at(-1) ?? 0) + s.step];

const barsHeight = 132;
const axisHeight = 16;

// tickLabel is a tick's time: the hour and minute, with the day when the range spans days.
function tickLabel(t: number, days: boolean): string {
  const iso = new Date(t).toISOString();
  return days ? `${iso.slice(5, 10)} ${iso.slice(11, 16)}` : iso.slice(11, 16);
}

// totals are the lines in each of s's bins.
const totals = (s: Stack) => s.t.map((_, bin) => s.counts.reduce((a, c) => a + (c[bin] ?? 0), 0));

// Pen is what the bars draw with: the canvas's context, width and theme, and the height a
// count stands for, as a fraction of the bars' height.
interface Pen {
  readonly ctx: CanvasRenderingContext2D;
  readonly width: number;
  readonly el: Element;
  readonly scale: (n: number) => number;
}

// scaleOf is linear up to tallest, or square-root when it is over 20 times the median bar
// with lines, so one spike does not flatten the rest.
function scaleOf(bars: readonly number[]) {
  const tallest = Math.max(1, ...bars);
  const lined = bars.filter((n) => n > 0).sort((a, b) => a - b);
  const median = lined[Math.floor(lined.length / 2)] ?? tallest;
  const root = tallest > 20 * median;
  const scale = (n: number) => (root ? Math.sqrt(n / tallest) : n / tallest);
  return { scale, root, tallest };
}

// drawBars stacks s's bands bottom up in each of n bins.
function drawBars({ ctx, width, el, scale }: Pen, s: Stack, n: number): void {
  const w = width / n;
  const gap = w > 4 ? 1 : 0;
  const top = Array.from({ length: n }, () => barsHeight);
  const below = Array.from({ length: n }, () => 0);
  s.bands.forEach((band, k) => {
    ctx.fillStyle = cssColor(band.color, el);
    s.counts[k]?.slice(0, n).forEach((c, bin) => {
      const under = below[bin] ?? 0;
      const h = c > 0 ? Math.max(1, (scale(under + c) - scale(under)) * (barsHeight - 4)) : 0;
      const y = (top[bin] ?? barsHeight) - h;
      ctx.fillRect(bin * w, y, w - gap, h);
      top[bin] = y;
      below[bin] = under + c;
    });
  });
}

// drawScale labels the top of the bars with the count it stands for, and a root scale as one.
function drawScale(ctx: CanvasRenderingContext2D, tallest: number, root: boolean, el: Element) {
  ctx.fillStyle = cssColor("--ink-3", el);
  ctx.font = "10px ui-monospace, monospace";
  ctx.textAlign = "left";
  ctx.textBaseline = "top";
  ctx.fillText(`${count(tallest)}${root ? " (√ scale)" : ""}`, 2, 0);
}

// drawAxis rules the bars' base and labels evenly spaced times under it.
function drawAxis(ctx: CanvasRenderingContext2D, s: Stack, width: number, el: Element): void {
  ctx.fillStyle = cssColor("--line", el);
  ctx.fillRect(0, barsHeight, width, 1);
  ctx.fillStyle = cssColor("--ink-3", el);
  ctx.font = "10px ui-monospace, monospace";
  ctx.textBaseline = "top";
  const [t0, t1] = span(s);
  const ticks = Math.max(2, Math.floor(width / 140));
  for (let i = 0; i <= ticks; i++) {
    ctx.textAlign = i === 0 ? "left" : i === ticks ? "right" : "center";
    ctx.fillText(
      tickLabel(t0 + ((t1 - t0) * i) / ticks, t1 - t0 > 86_400_000),
      (width * i) / ticks,
      barsHeight + 4,
    );
  }
}

// drawBaseline draws the baseline's lines in each of n bins as a step line over the bars,
// in the insight colour with a soft fill under it, so a bin that rose above its baseline
// stands clear of the line and one that fell sits under it.
function drawBaseline({ ctx, width, el, scale }: Pen, was: Stack, n: number): void {
  const w = width / n;
  const y = (c: number) => barsHeight - scale(c) * (barsHeight - 4);
  const line = new Path2D();
  totals(was)
    .slice(0, n)
    .forEach((c, bin) => {
      const at = y(c);
      if (bin === 0) {
        line.moveTo(0, at);
      }
      line.lineTo(bin * w, at);
      line.lineTo((bin + 1) * w, at);
    });
  const area = new Path2D(line);
  area.lineTo(Math.min(n, was.t.length) * w, barsHeight);
  area.lineTo(0, barsHeight);
  ctx.fillStyle = cssColor("--insight-soft", el);
  ctx.fill(area);
  ctx.strokeStyle = cssColor("--insight", el);
  ctx.lineWidth = 1.5;
  ctx.lineJoin = "round";
  ctx.stroke(line);
}

// draw paints s into canvas, width CSS pixels wide, in the theme of its parent; comparing,
// the baseline's volume runs over the bars as a line.
function draw(canvas: HTMLCanvasElement, s: Stack, was: Stack, width: number): void {
  const dpr = devicePixelRatio;
  canvas.width = Math.round(width * dpr);
  canvas.height = Math.round((barsHeight + axisHeight) * dpr);
  const ctx = canvas.getContext("2d");
  const el = canvas.parentElement;
  if (!ctx || !el || s.t.length === 0) {
    return;
  }
  ctx.scale(dpr, dpr);
  const n = s.t.length;
  const { scale, root, tallest } = scaleOf([...totals(s), ...totals(was).slice(0, n)]);
  const pen = { ctx, width, el, scale };
  drawBars(pen, s, n);
  if (was.t.length > 0) {
    drawBaseline(pen, was, n);
  }
  drawAxis(ctx, s, width, el);
  drawScale(ctx, tallest, root, el);
}

// Drag is the part of the histogram's width a drag covers, from 0 to 1.
type Drag = readonly [number, number];

// drag follows the drag e starts, on the window so it carries on past the histogram's edge.
function drag(e: PointerEvent, onMove: (d: Drag) => void, onEnd: (d: Drag) => void): void {
  const r = e.currentTarget.getBoundingClientRect();
  const at = (x: number) => Math.min(Math.max((x - r.left) / r.width, 0), 1);
  const across = (x: number): Drag => [
    Math.min(at(e.clientX), at(x)),
    Math.max(at(e.clientX), at(x)),
  ];
  const move = (m: MouseEvent) => onMove(across(m.clientX));
  const up = (u: MouseEvent) => {
    removeEventListener("pointermove", move);
    removeEventListener("pointerup", up);
    onEnd(across(u.clientX));
  };
  addEventListener("pointermove", move);
  addEventListener("pointerup", up);
}

const count = (n: number) => n.toLocaleString("en-US");

// versus says how the range's lines compare with the baseline's: its count and how many times
// the rate it had the range's is, or that it has too few to compare.
function versus(s: Stack, was: Stack, b: Baseline): string {
  const label = describeBaseline(b);
  if (was.total === 0) {
    return `no lines ${label}`;
  }
  if (was.total < s.total * 0.05) {
    return `only ${count(was.total)} lines ${label}, too few to compare`;
  }
  // Per millisecond, so a typed-in range of another length still compares.
  const rate = (x: Stack) => x.total / Math.max(1, x.t.length * x.step);
  const r = rate(s) / rate(was);
  return `${count(was.total)} ${label} (×${r >= 10 ? Math.round(r) : r.toFixed(1)})`;
}

interface LegendProps {
  readonly s: Stack;
  readonly was: Stack;
  readonly compare: Baseline | null;
  // loaded is whether was is the answer for compare, which may hold no lines, not a load pending.
  readonly loaded: boolean;
  readonly by: string;
  // onLevel stacks the bars by level again.
  readonly onLevel: () => void;
}

// Legend is the total and how it compares with the baseline's, the field the bars stack by
// unless level, what each band's colour stands for, and the choice of baseline.
function Legend({ s, was, compare, loaded, by, onLevel }: LegendProps): ReactElement {
  return createElement(
    "div",
    { className: "hist-h" },
    createElement("b", null, count(s.total)),
    "lines",
    loaded &&
      compare &&
      createElement("span", { className: "hist-vs" }, `· ${versus(s, was, compare)}`),
    by !== "level" &&
      createElement(
        "button",
        { type: "button", className: "chip hist-by", title: "Stack by level", onClick: onLevel },
        `by ${by} ×`,
      ),
    s.bands.map((b) =>
      createElement(
        "span",
        { key: b.key, className: "hist-k" },
        createElement("i", {
          "aria-hidden": true,
          ref: (el: HTMLElement | null) => el?.style.setProperty("background", `var(${b.color})`),
        }),
        b.label,
      ),
    ),
    compare &&
      createElement(
        "span",
        { className: "hist-k was" },
        createElement("i", { "aria-hidden": true }),
        `line: ${describeBaseline(compare)}`,
      ),
    createElement(
      "span",
      { className: "hist-tip" },
      "drag to narrow time · click to read up to then",
    ),
    createElement(Compare),
  );
}

// useWidth is el's width, kept as it resizes.
function useWidth(el: Element | null | undefined): number {
  const [width, setWidth] = useState(0);
  useEffect(() => {
    if (!el) {
      return;
    }
    const observer = new ResizeObserver(() => setWidth(el.clientWidth));
    observer.observe(el);
    return () => observer.disconnect();
  }, [el]);
  return width;
}

const none = { now: [], was: [] };

// Counted is what volume counts: the selector's lines at the snapshot, in bins, and the
// values of the field it counts them by, which are its top values unless named.
interface Counted {
  readonly selector: Selector;
  readonly snapshot: string;
  readonly bins: string;
  readonly values?: readonly string[];
}

// volume is the lines over time by level or, by another field, by its values with the lines
// outside them, counted off the total by level, as one series more.
async function volume(
  client: DataFocalClient,
  q: Counted,
  by: string,
  signal: AbortSignal,
): Promise<Series[]> {
  const { snapshot, bins } = q;
  const count = async (group_by: string, selector = q.selector, limit?: number) =>
    (
      await client.call(
        "aggregate",
        { selector, snapshot, measure: "count", group_by, bins, ...(limit && { limit }) },
        { signal },
      )
    ).data.series;
  if (by === "level") {
    return count("level");
  }
  const among = q.values && {
    ...q.selector,
    where: [
      ...(q.selector.where ?? []),
      { any: q.values.map((value) => ({ field: by, op: "eq" as const, value })) },
    ],
  };
  const [top, all] = await Promise.all([
    q.values?.length === 0 ? [] : count(by, among, palette.length),
    count("level"),
  ]);
  const values = (all[0]?.values ?? []).map((_, bin) => {
    const sum = (ss: Series[]) => ss.reduce((a, s) => a + (s.values?.[bin] ?? 0), 0);
    return Math.max(0, sum(all) - sum(top));
  });
  return all[0] ? [...top, { ...all[0], key: rest, values }] : top;
}

// ladder is the bin width for a span of up to each length, so a preset's own span gets the
// finer bars: a day five-minute ones, a week hourly ones.
const ladder: readonly (readonly [number, string])[] = [
  [3_600_000, "15s"],
  [21_600_000, "1m"],
  [86_400_000, "5m"],
  [604_800_000, "1h"],
];

// binsFor is the bin width for time's span, or auto when it has no span to measure.
function binsFor(time: TimeSpec = {}): string {
  const now = Date.now();
  const span = resolve(time.to ?? "now", now) - resolve(time.from ?? "now", now);
  if (!(span > 0)) {
    return "auto";
  }
  return ladder.find(([upTo]) => span <= upTo)?.[1] ?? "1d";
}

// useVolume is the selector's lines over time by by and, comparing, over the baseline range
// on the same bins and by the same values, so its bars line up with the range's bin for bin.
function useVolume(p: HistogramProps, by: string, compare: Baseline | null) {
  const { client, selector, snapshot } = p;
  const load = useCallback(
    async (signal: AbortSignal) => {
      const bins = binsFor(selector.time);
      const now = await volume(client, { selector, snapshot, bins }, by, signal);
      if (!compare) {
        return { now, was: [] };
      }
      const baseline = {
        selector: { ...selector, time: baselineRange(selector.time ?? {}, compare) },
        snapshot,
        bins,
        values: now.filter((s) => s.key !== rest).map((s) => s.key),
      };
      return { now, was: await volume(client, baseline, by, signal) };
    },
    [client, selector, snapshot, by, compare],
  );
  return useAsync(load);
}

// described is what the bars show, for a reader that cannot see them.
function described(s: Stack, was: Stack, by: string, compare: Baseline | null): string {
  const beside = compare ? `, beside ${count(was.total)} ${describeBaseline(compare)}` : "";
  return `${count(s.total)} lines over time, by ${by}${beside}`;
}

// Histogram is the query's volume over time, stacked by level or the field the page names,
// which a drag across narrows the time range to; comparing, the baseline range's volume
// stands beside it in the same bands.
export function Histogram(p: HistogramProps): ReactElement {
  const { onBrush } = p;
  const [compare] = useCompare();
  const [param, setBy] = useParam("by");
  const by = param ?? "level";
  const series = useVolume(p, by, compare);
  const loaded = series !== undefined;
  const [canvas, setCanvas] = useState<HTMLCanvasElement | null>(null);
  const [dragged, setDragged] = useState<Drag>();
  const both = series instanceof Error ? none : (series ?? none);
  const s = useMemo(() => stack(both.now, by), [both.now, by]);
  const was = useMemo(() => stack(both.was, by, s.bands), [both.was, by, s.bands]);
  const width = useWidth(canvas?.parentElement);
  useEffect(() => {
    if (canvas && width > 0) {
      draw(canvas, s, was, width);
    }
  }, [canvas, s, was, width]);
  if (series instanceof Error) {
    return createElement(Show, { answer: series, render: () => null });
  }
  const [t0, t1] = span(s);
  const time = (f: number) => new Date(t0 + f * (t1 - t0)).toISOString();
  return createElement(
    "section",
    { className: "hist-w", "aria-label": "Volume" },
    createElement(Legend, { s, was, compare, loaded, by, onLevel: () => setBy(null) }),
    createElement(
      "div",
      {
        className: "hist",
        title: "Drag across to narrow time, or click to read the lines up to then",
        onPointerDown: (e: PointerEvent) =>
          drag(e, setDragged, ([a, b]) => {
            setDragged(undefined);
            if (b - a > 0.005) {
              onBrush({ from: time(a), to: time(b) });
            } else if (s.t.length > 0) {
              const bin = Math.min(s.t.length - 1, Math.floor(a * s.t.length));
              p.onJump(new Date((s.t[bin] ?? 0) + s.step).toISOString());
            }
          }),
      },
      createElement("canvas", {
        ref: setCanvas,
        role: "img",
        "aria-label": described(s, was, by, loaded ? compare : null),
      }),
      dragged &&
        createElement("div", {
          className: "brush",
          ref: (el: HTMLDivElement | null) => {
            el?.style.setProperty("left", `${dragged[0] * 100}%`);
            el?.style.setProperty("width", `${(dragged[1] - dragged[0]) * 100}%`);
          },
        }),
    ),
  );
}
