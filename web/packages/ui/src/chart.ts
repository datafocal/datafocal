import { createElement, type ReactElement, useEffect, useState } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { type ColorToken, cssColor } from "./tokens";

// Axis is how a chart's axis reads: what it measures and how its values print. A time axis
// takes milliseconds since the epoch.
export interface Axis {
  readonly label?: string;
  readonly format?: (v: number) => string;
  readonly time?: boolean;
}

export interface ChartProps {
  // sparkline and histogram are bare marks beside a number; line and bars carry axes, a
  // grid and a legend that reads each series' value under the cursor.
  readonly kind: "sparkline" | "histogram" | "line" | "bars";
  // data is uPlot's: the x values, then one array of y values per series.
  readonly data: uPlot.AlignedData;
  readonly width: number;
  readonly height: number;
  // label describes the chart to assistive technology.
  readonly label: string;
  readonly x?: Axis;
  readonly y?: Axis;
  // series names each y series, in data's order, for the legend.
  readonly series?: readonly string[];
  // onBrush makes a drag across a line or bar chart select the x span it covers, in x's
  // units (milliseconds on a time axis), rather than nothing.
  readonly onBrush?: (from: number, to: number) => void;
  // onPick makes a click on a line chart pick the bin under the cursor and the line nearest
  // it, as x in its units and the y series' index in data's order, less the x.
  readonly onPick?: (x: number, series: number) => void;
}

const palette: readonly ColorToken[] = ["--cat-1", "--cat-3", "--cat-2", "--cat-4", "--cat-5"];

const printed = (a: Axis | undefined) => (v: number | null) =>
  v === null ? "—" : (a?.format?.(v) ?? String(v));

// tickFont is the axes' values, measured to fit a value axis to its widest one.
const tickFont = "11px ui-monospace, SFMono-Regular, Menlo, monospace";

// fitted is a value axis' width: its widest value, the tick and the gap beside it, so a
// value such as "100.0 cores" neither clips nor runs under the axis label.
function fitted(u: uPlot, values: string[] | null): number {
  u.ctx.save();
  u.ctx.font = tickFont;
  const widest = Math.max(0, ...(values ?? []).map((v) => u.ctx.measureText(v).width));
  u.ctx.restore();
  return Math.ceil(widest + 18);
}

const axis = (a: Axis | undefined, ink: string, grid: string): uPlot.Axis => ({
  stroke: ink,
  font: tickFont,
  grid: { stroke: grid, width: 1 },
  ticks: { stroke: grid, width: 1 },
  ...(a?.label && { label: a.label }),
  ...(!a?.time && { size: fitted }),
  ...(a?.format && !a.time && { values: (_: uPlot, splits: number[]) => splits.map(printed(a)) }),
});

// latest is a series' last value, which a line chart's legend reads off the cursor.
const latest = (ys: uPlot.AlignedData[number] | undefined): number | null => {
  for (let i = (ys?.length ?? 0) - 1; i >= 0; i--) {
    const v = ys?.[i];
    if (v !== null && v !== undefined) {
      return v;
    }
  }
  return null;
};

const stamp = uPlot.fmtDate("{YYYY}-{MM}-{DD} {h}:{mm}{aa}");

const bare = (kind: ChartProps["kind"]) => kind === "sparkline" || kind === "histogram";

// colorOf is the y series at i's colour.
const colorOf = (i: number, el: HTMLElement) =>
  cssColor(palette[i % palette.length] ?? "--accent", el);

// total is a series' values summed, which a bar chart's legend reads off the cursor.
const total = (ys: uPlot.AlignedData[number] | undefined): number | null => {
  let n = 0;
  for (const v of ys ?? []) {
    n += v ?? 0;
  }
  return ys?.length ? n : null;
};

// isolated are the bins of a line holding a value between two that hold none, which a line
// cannot draw, so they show as points: a container living within one bin is one.
function isolated(ys: uPlot.AlignedData[number] | undefined): number[] | null {
  const at: number[] = [];
  ys?.forEach((v, i) => {
    if (v != null && ys[i - 1] == null && ys[i + 1] == null) {
      at.push(i);
    }
  });
  return at.length > 0 ? at : null;
}

// ySeries are the y series' styles: bars for a histogram or bar chart, else lines. Off the
// cursor, the legend reads each line's last value and each bar series' total rather than none.
function ySeries(p: ChartProps, el: HTMLElement): uPlot.Series[] {
  const bars = (p.kind === "histogram" || p.kind === "bars") && uPlot.paths.bars;
  const rest = bars ? total : latest;
  return Array.from({ length: p.data.length - 1 }, (_, i): uPlot.Series => {
    const stroke = colorOf(i, el);
    return {
      stroke,
      label: p.series?.[i] ?? p.label,
      value: (u: uPlot, v: number | null, s: number, idx: number | null) =>
        printed(p.y)(idx === null ? rest(u.data[s]) : v),
      ...(bars
        ? { fill: stroke, paths: bars({ size: [0.8] }) }
        : {
            width: 2,
            points: {
              filter: (u: uPlot, s: number, show: boolean) => (show ? null : isolated(u.data[s])),
            },
          }),
    };
  });
}

// xValue is what the legend says of x: the cursor's, or off it, the last for a line chart
// and "all" for a bar chart, whose series it sums.
function xValue(p: ChartProps, u: uPlot, v: number | null, idx: number | null): string {
  if (idx === null) {
    if (p.kind === "bars" || p.kind === "histogram") {
      return "all";
    }
    return p.x?.time ? "latest" : printed(p.x)(latest(u.data[0]));
  }
  return p.x?.time ? stamp(new Date((v ?? 0) * 1000)) : printed(p.x)(v);
}

// brushed hands the span a drag selected to onBrush, in x's units, and clears the selection,
// which the page's own range then shows.
const brushed = (p: ChartProps, time: boolean) => (u: uPlot) => {
  const { left, width } = u.select;
  if (width < 3 || !p.onBrush) {
    return;
  }
  const at = (x: number) => u.posToVal(x, "x") * (time ? 1000 : 1);
  u.setSelect({ left: 0, top: 0, width: 0, height: 0 }, false);
  p.onBrush(at(left), at(left + width));
};

// nearest is the y series whose value in bin idx lies nearest y, or -1 when none has one.
function nearest(u: uPlot, idx: number, y: number): number {
  let [best, gap] = [-1, Number.POSITIVE_INFINITY];
  for (let s = 1; s < u.data.length; s++) {
    const v = u.data[s]?.[idx];
    if (v != null && Math.abs(v - y) < gap) {
      [best, gap] = [s, Math.abs(v - y)];
    }
  }
  return best;
}

// picking hands a click's bin and nearest line to onPick. uPlot stops the click that ends a
// drag, so a drag only brushes.
const picking = (p: ChartProps, time: boolean) => (u: uPlot) => {
  u.over.addEventListener("click", () => {
    const { idx, top } = u.cursor;
    if (!p.onPick || idx == null || top == null) {
      return;
    }
    const s = nearest(u, idx, u.posToVal(top, "y"));
    if (s > 0) {
      p.onPick((u.data[0][idx] ?? 0) * (time ? 1000 : 1), s - 1);
    }
  });
};

// options are uPlot's for p: bare marks, or marks with axes, a grid and a live legend.
function options(p: ChartProps, el: HTMLElement, width: number): uPlot.Options {
  const plain = bare(p.kind);
  const [ink, grid] = [cssColor("--ink-3", el), cssColor("--line", el)];
  const time = Boolean(p.x?.time);
  return {
    width,
    height: p.height,
    legend: { show: !plain, live: true, markers: { fill: (_, s) => colorOf(s - 1, el) } },
    // The cursor reads values; a drag selects a span for onBrush, and zooms nothing itself,
    // which nothing on the page could undo.
    cursor: {
      show: !plain,
      drag: { x: Boolean(p.onBrush), y: false, setScale: false },
    },
    hooks: { setSelect: [brushed(p, time)], ready: [picking(p, time)] },
    scales: { x: { time } },
    axes: plain ? [{ show: false }, { show: false }] : [axis(p.x, ink, grid), axis(p.y, ink, grid)],
    series: [
      {
        label: p.x?.label ?? "x",
        value: (u: uPlot, v: number | null, _s: number, idx: number | null) => xValue(p, u, v, idx),
      },
      ...ySeries(p, el),
    ],
  };
}

// Chart draws a sparkline or a histogram with uPlot: no axes, legend or cursor; or a line or
// bar chart with them, as wide as the space it is given when that is wider than width.
export function Chart(p: ChartProps): ReactElement {
  const [el, setEl] = useState<HTMLDivElement | null>(null);
  useEffect(() => {
    if (!el) {
      return;
    }
    const width = bare(p.kind) ? p.width : Math.max(p.width, el.clientWidth);
    // uPlot's time scale counts seconds; the engine's bins count milliseconds.
    const data: uPlot.AlignedData = p.x?.time
      ? [Array.from(p.data[0], (t) => t / 1000), ...p.data.slice(1)]
      : p.data;
    const plot = new uPlot(options(p, el, width), data, el);
    return () => plot.destroy();
  }, [el, p]);
  return createElement("div", {
    ref: setEl,
    role: "img",
    "aria-label": p.label,
    className: bare(p.kind) ? undefined : p.onPick ? "chart pickable" : "chart",
  });
}
