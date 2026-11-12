import {
  createHitIndex,
  createTextWidths,
  type DrawStats,
  fitText,
  type Hit,
  type HitIndex,
  type Renderer,
  type TextContext,
  type Viewport,
} from "@datafocal/render-core";

// Flag holds the bits of a span's flags. criticalInferred marks a critical-path span whose
// membership rests on a derived parent link; group marks a span standing for a ×N group;
// call marks a span waiting on another service's, drawn as an outline.
export const Flag = {
  error: 1,
  critical: 2,
  criticalInferred: 4,
  derivedParent: 8,
  group: 16,
  call: 32,
} as const;

// WaterfallModel is a trace, columnar, its spans in tree pre-order. group is the index of the
// ×N span a span collapses into, or its own; services indexes a palette; ends, when set, is
// what is written after each span's bar, as its duration; ticks are the backing log lines,
// by span then time; gaps are the "no evidence" stretches between siblings at depth; facts
// are what each span's lines counted, written past its bar after its end text. palette, when
// set, is the colours services indexes, in place of serviceColors.
export interface WaterfallModel {
  spans: {
    start: Float64Array;
    dur: Float32Array;
    self: Float32Array;
    depth: Uint16Array;
    parent: Int32Array;
    flags: Uint8Array;
    group: Uint32Array;
  };
  names: readonly string[];
  services: Uint16Array;
  palette?: readonly string[];
  ends?: readonly string[];
  ticks: { span: Uint32Array; t: Float64Array; error: Uint8Array };
  gaps: readonly { t0: number; t1: number; depth: number }[];
  facts: readonly string[];
}

// FlameModel is the spans a flame graph draws, one row per depth.
export type FlameModel = Pick<WaterfallModel, "spans" | "names" | "services" | "palette">;

// Canvas is the part of an OffscreenCanvas the renderers use.
interface Canvas {
  width: number;
  height: number;
  getContext(id: "2d"): Context | null;
}

type Context = TextContext &
  Pick<
    OffscreenCanvasRenderingContext2D,
    | "fillStyle"
    | "strokeStyle"
    | "textBaseline"
    | "setTransform"
    | "setLineDash"
    | "clearRect"
    | "beginPath"
    | "rect"
    | "roundRect"
    | "arc"
    | "lineWidth"
    | "moveTo"
    | "lineTo"
    | "fill"
    | "stroke"
    | "fillText"
  >;

// serviceColors tell services apart without reading as a status, after ui's categorical
// tokens, which a worker can't read.
export const serviceColors: readonly string[] = [
  "#5b9dff",
  "#2fc4b2",
  "#e8a050",
  "#b48cf2",
  "#f07aa8",
  "#8fb8de",
  "#c9b27a",
  "#6fcfe8",
  "#d39bd6",
  "#a3b86c",
  "#9aa3b5",
];
export const errorColor: string = "#ef5b5b";
const pathColor = "#1f2328";
const linkColor = "#8a8f98";
// tickColor marks a line on its bar, darkening its foot rather than striping it.
const tickColor = "rgba(0, 0, 0, 0.4)";
const mutedColor = "#8a8f98";
const gapColor = "#d0d7de";
const textColor = "#1f2328";
const font = "500 11px sans-serif";
const hatch = [3, 3];
// pad is the share of a row above and below a flame graph's bars, which hold their names;
// a waterfall's, named beside them, are slimmer (slim). minLabel is the narrowest labelled
// bar in pixels, and minBar the narrowest drawn; dot is an instant's radius; bins is a ×N
// histogram's bucket count.
const pad = 0.2;
const slim = 0.31;
const dot = 3.5;
const radius = 3;
const minLabel = 20;
const minBar = 3;
const bins = 8;

// Label is text at x, y in room pixels, on its bar unless it is written beside it, muted;
// one ending at x rather than starting there is written before it.
interface Label {
  text: string;
  x: number;
  y: number;
  room: number;
  beside?: boolean;
  ending?: boolean;
}

// Frame is one render's state: the model, each span's row (-1 when collapsed), the scale
// from data to CSS pixels, the resolution bucket, what it drew and the labels it draws last.
interface Frame {
  ctx: Context;
  m: FlameModel;
  rows: Int32Array;
  v: Viewport;
  x(t: number): number;
  y(row: number): number;
  rowH: number;
  pad: number;
  res: number;
  stats: DrawStats;
  labels: Label[];
  // outline is whether the bars being drawn are outlines, whose labels sit on no fill.
  outline: boolean;
  // ink is the colour of what sits on the page rather than on a bar, a group's histogram;
  // muted is text beside a bar and the critical path's underline.
  ink: string;
  muted: string;
}

// Theme is the colours and font a renderer takes from the page, which a worker can't read:
// ink for what sits on the page, muted for text beside a bar.
export interface Theme {
  readonly ink: string;
  readonly muted?: string;
  readonly font?: string;
}

// Layout is a model prepared once: each span's row, its label, and what to draw over bars.
interface Layout {
  rows: Int32Array;
  // pad is the share of a row above and below its bars.
  pad: number;
  label: (i: number) => string;
  // beside is what is written after a span's bar.
  beside: (i: number) => string;
  overlay: (f: Frame) => void;
}

// createWaterfallRenderer draws a trace as its indented tree of spans, one row each.
export function createWaterfallRenderer(
  canvas: Canvas,
  theme: Theme = { ink: pathColor },
): Renderer<WaterfallModel> {
  return createRenderer(canvas, waterfall, theme);
}

// createFlameRenderer draws a trace's spans as a flame graph, one row per depth.
export function createFlameRenderer(
  canvas: Canvas,
  theme: Theme = { ink: pathColor },
): Renderer<FlameModel> {
  const flame = (m: FlameModel): Layout => ({
    rows: Int32Array.from(m.spans.depth),
    pad,
    label: (i) => at(m.names, i),
    beside: () => "",
    overlay: () => {},
  });
  return createRenderer(canvas, flame, theme);
}

function createRenderer<M extends FlameModel>(
  canvas: Canvas,
  prepare: (m: M) => Layout,
  theme: Theme,
): Renderer<M> {
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    throw new Error("render-waterfall: no 2d context");
  }
  const widths = createTextWidths();
  let model: M | null = null;
  let layout: Layout | null = null;
  let index: HitIndex | null = null;
  let view: Viewport | null = null;
  return {
    resize(width, height, dpr) {
      canvas.width = Math.round(width * dpr);
      canvas.height = Math.round(height * dpr);
    },
    render(m, v) {
      if (m !== model || !layout) {
        model = m;
        layout = prepare(m);
        index = createHitIndex(spanBoxes(m, layout.rows, layout.pad));
      }
      view = v;
      const f = frame(ctx, m, layout.rows, v);
      f.pad = layout.pad;
      ctx.font = theme.font ?? font;
      f.ink = theme.ink;
      f.muted = theme.muted ?? mutedColor;
      drawSpans(f, layout);
      layout.overlay(f);
      drawLabels(f, widths);
      return f.stats;
    },
    hitTest(x, y): Hit | null {
      if (!index || !view) {
        return null;
      }
      const t = view.x0 + (x * (view.x1 - view.x0)) / view.width;
      const row = view.y0 + (y * (view.y1 - view.y0)) / view.height;
      const i = index.top(t, row);
      return i < 0 ? null : { kind: "span", index: i };
    },
    dispose() {
      model = null;
      layout = null;
      index = null;
      view = null;
    },
  };
}

// spanBoxes returns each span's bar in data space, a collapsed span's out of every row.
function spanBoxes(m: FlameModel, rows: Int32Array, pad: number): Float64Array {
  const { start, dur } = m.spans;
  const boxes = new Float64Array(start.length * 4);
  // An instant, drawn as a dot, is hit a little either side of when it happened.
  const reach = Math.max(...start.map((t, i) => t + at(dur, i))) / 400;
  for (let i = 0; i < start.length; i++) {
    const row = at(rows, i) < 0 ? -2 : at(rows, i);
    const [t, d] = [at(start, i), at(dur, i)];
    const [t0, t1] = d === 0 ? [t - reach, t + reach] : [t, t + d];
    boxes.set([t0, row + pad, t1, row + 1 - pad], i * 4);
  }
  return boxes;
}

// waterfall lays a trace out a row per span, skipping ×N members, and counts and bins each
// group's member durations.
function waterfall(m: WaterfallModel): Layout {
  const { dur, group, depth, start } = m.spans;
  const rows = new Int32Array(dur.length);
  const members = new Map<number, number[]>();
  let row = 0;
  group.forEach((g, i) => {
    rows[i] = g === i ? row++ : -1;
    if (g !== i) {
      const ds = members.get(g) ?? [at(dur, g)];
      ds.push(at(dur, i));
      members.set(g, ds);
    }
  });
  const hists = [...members].flatMap(([g, ds]) => histogram(g, ds));
  // A gap goes in the row of the last span at its depth to start by its start.
  const gapRows = m.gaps.map((g) => {
    let r = -1;
    rows.forEach((ri, i) => {
      r = ri >= 0 && depth[i] === g.depth && at(start, i) <= g.t0 ? ri : r;
    });
    return r;
  });
  return {
    rows,
    pad: slim,
    // The waterfall names each row beside it, so a bar says only how long it took.
    label: () => "",
    beside: (i) => [m.ends?.[i], m.facts[i]].filter(Boolean).join(" · "),
    overlay: (f) => {
      drawTicks(f, m.ticks);
      drawGaps(f, m.gaps, gapRows);
      drawHistograms(f, hists);
      drawLinks(f);
    },
  };
}

// Bin is one bar of a ×N group's duration histogram: its span, its bucket of bins from the
// least duration to the greatest, and its height as a share of the tallest.
interface Bin {
  span: number;
  bin: number;
  share: number;
}

// histogram returns the bins of group g's member durations ds that hold any.
function histogram(g: number, ds: number[]): Bin[] {
  const lo = Math.min(...ds);
  const range = Math.max(...ds) - lo || 1;
  const h = new Uint32Array(bins);
  for (const d of ds) {
    const b = Math.min(bins - 1, Math.floor(((d - lo) / range) * bins));
    h[b] = at(h, b) + 1;
  }
  const top = Math.max(...h);
  return [...h].flatMap((n, bin) => (n > 0 ? [{ span: g, bin, share: n / top }] : []));
}

function frame(ctx: Context, m: FlameModel, rows: Int32Array, v: Viewport): Frame {
  const kx = v.width / (v.x1 - v.x0);
  const ky = v.height / (v.y1 - v.y0);
  ctx.setTransform(v.dpr, 0, 0, v.dpr, 0, 0);
  ctx.clearRect(0, 0, v.width, v.height);
  ctx.textBaseline = "middle";
  return {
    ctx,
    m,
    rows,
    v,
    x: (t) => (t - v.x0) * kx,
    y: (row) => (row - v.y0) * ky,
    rowH: ky,
    pad,
    res: 2 ** Math.ceil(Math.log2((v.x1 - v.x0) / v.width)),
    stats: { drawCalls: 0, primitives: 0, labels: 0 },
    labels: [],
    outline: false,
    ink: pathColor,
    muted: mutedColor,
  };
}

// drawSpans fills the bars one path per service, a call's outlined, the error path's in its
// own color, and underlines the critical path in muted ink, where it rests on a derived link
// as well.
function drawSpans(f: Frame, layout: Layout): void {
  const { flags } = f.m.spans;
  const has = (bit: number) => (i: number) => (at(flags, i) & bit) !== 0;
  const [err, call] = [has(Flag.error), has(Flag.call)];
  const palette = f.m.palette ?? serviceColors;
  const paint = (color: string, keep: (i: number) => boolean) => {
    drawBars(f, { color }, (i) => keep(i) && !call(i), layout);
    drawBars(f, { color, outline: true }, (i) => keep(i) && call(i), layout);
  };
  palette.forEach((color, s) =>
    paint(color, (i) => !err(i) && at(f.m.services, i) % palette.length === s),
  );
  paint(errorColor, err);
  const critical = (i: number) => (at(flags, i) & (Flag.critical | Flag.criticalInferred)) !== 0;
  fillRects(f, f.muted, f.rows.length, (i) => {
    const row = at(f.rows, i);
    const t = at(f.m.spans.start, i);
    const x = f.x(t);
    return critical(i) && visible(f, row)
      ? [x, f.y(row + 1 - f.pad) + 1, Math.max(minBar, f.x(t + at(f.m.spans.dur, i)) - x), 1]
      : null;
  });
}

// drawBars fills the bars of the spans keep picks in one path, or given outline strokes them,
// labelling each as layout says.
function drawBars(
  f: Frame,
  { color, outline = false }: { color: string; outline?: boolean },
  keep: (i: number) => boolean,
  layout: Layout,
): void {
  const ends = new Map<number, number>();
  f.outline = outline;
  f.ctx.beginPath();
  for (let i = 0; i < f.rows.length; i++) {
    if (keep(i)) {
      drawBar(f, i, ends, layout);
    }
  }
  f.outline = false;
  if (outline) {
    f.ctx.strokeStyle = color;
    f.ctx.lineWidth = 1.5;
    f.ctx.stroke();
    f.ctx.lineWidth = 1;
  } else {
    f.ctx.fillStyle = color;
    f.ctx.fill();
  }
  f.stats.drawCalls++;
}

// drawBar adds span i's bar unless it is out of the window or under bars its row already
// has, given the bucket each row's bars end at. Following the Perfetto track model, it snaps
// a bar to buckets of res, so a row gets at most one bar a pixel.
function drawBar(f: Frame, i: number, ends: Map<number, number>, layout: Layout): void {
  const row = at(f.rows, i);
  const t = at(f.m.spans.start, i);
  const end = t + at(f.m.spans.dur, i);
  const b1 = Math.max(Math.ceil(end / f.res), Math.floor(t / f.res) + 1);
  const seen = ends.get(row) ?? -Infinity;
  if (!visible(f, row) || end < f.v.x0 || t > f.v.x1 || b1 <= seen) {
    return;
  }
  ends.set(row, b1);
  const { x, w } = addShape(f, i, Math.max(Math.floor(t / f.res), seen), b1);
  const [y, text, beside] = [f.y(row + 0.5), layout.label(i), layout.beside(i)];
  if (text && w >= minLabel) {
    f.labels.push({ text, x: x + 4, y, room: w - 8, beside: f.outline });
  }
  if (beside) {
    f.labels.push(besideLabel(f, beside, { x, w, y }));
  }
}

// addShape adds span i's shape over buckets b0 to b1 to the path, and says where it went: a
// rounded bar, or for an instant a dot where it happened, not a bar standing for no time.
function addShape(f: Frame, i: number, b0: number, b1: number): { x: number; w: number } {
  const row = at(f.rows, i);
  const t = at(f.m.spans.start, i);
  f.stats.primitives++;
  if (at(f.m.spans.dur, i) === 0 && (at(f.m.spans.flags, i) & Flag.group) === 0) {
    f.ctx.moveTo(f.x(t) + dot, f.y(row + 0.5));
    f.ctx.arc(f.x(t), f.y(row + 0.5), dot, 0, 2 * Math.PI);
    return { x: f.x(t) - dot, w: 2 * dot };
  }
  const x = f.x(b0 * f.res);
  const w = Math.max(minBar, f.x(b1 * f.res) - x);
  const h = f.rowH * (1 - 2 * f.pad);
  f.ctx.roundRect(x, f.y(row + f.pad), w, h, Math.min(radius, w / 2, h / 2));
  return { x, w };
}

// besideLabel is where text beside a bar from x, w wide, goes: after it, else before it, else
// on its end when the bar is wide enough to hold it.
function besideLabel(
  f: Frame,
  text: string,
  { x, w, y }: { x: number; w: number; y: number },
): Label {
  const after = x + w + 6;
  if (after + 60 <= f.v.width) {
    return { text, x: after, y, room: f.v.width - after, beside: true };
  }
  if (x >= 66) {
    return { text, x: x - 6, y, room: 60, beside: true, ending: true };
  }
  return { text, x: Math.max(x + 4, f.v.width - 64), y, room: Math.min(60, w - 8) };
}

// visible reports whether row is in the window.
function visible(f: Frame, row: number): boolean {
  return row >= 0 && row + 1 >= f.v.y0 && row <= f.v.y1;
}

// Rect is x, y, width and height in CSS pixels.
type Rect = [number, number, number, number];

// fillRects fills the rects rect gives for k from 0 to n, skipping nulls, in one path.
function fillRects(f: Frame, color: string, n: number, rect: (k: number) => Rect | null): void {
  f.ctx.fillStyle = color;
  f.ctx.beginPath();
  for (let k = 0; k < n; k++) {
    const r = rect(k);
    if (r) {
      f.ctx.rect(...r);
      f.stats.primitives++;
    }
  }
  f.ctx.fill();
  f.stats.drawCalls++;
}

// drawTicks marks each log line at the foot of its span's bar, an error's the bar's full
// height in red, so a span of many lines reads as a bar, not a barcode.
function drawTicks(f: Frame, ticks: WaterfallModel["ticks"]): void {
  const row = (k: number) => at(f.rows, at(ticks.span, k));
  const h = f.rowH * (1 - 2 * f.pad);
  fillRects(f, tickColor, ticks.t.length, (k) =>
    at(ticks.error, k) === 0 && visible(f, row(k))
      ? [f.x(at(ticks.t, k)), f.y(row(k) + 1 - f.pad) - h / 3, 1, h / 3]
      : null,
  );
  fillRects(f, errorColor, ticks.t.length, (k) =>
    at(ticks.error, k) !== 0 && visible(f, row(k))
      ? [f.x(at(ticks.t, k)), f.y(row(k) + f.pad), 2, h]
      : null,
  );
}

// drawGaps fills each "no evidence" gap in its row, labelled where it fits.
function drawGaps(f: Frame, gaps: WaterfallModel["gaps"], rows: number[]): void {
  fillRects(f, gapColor, gaps.length, (n) => {
    const { t0, t1 } = at(gaps, n);
    const row = at(rows, n);
    if (!visible(f, row)) {
      return null;
    }
    const text = "no evidence";
    f.labels.push({ text, x: f.x(t0) + 2, y: f.y(row + 0.5), room: f.x(t1) - f.x(t0) - 4 });
    return [f.x(t0), f.y(row + f.pad), f.x(t1) - f.x(t0), f.rowH * (1 - 2 * f.pad)];
  });
}

// drawHistograms draws each ×N group's bins along the bottom of its bar, where it is wide
// enough to label.
function drawHistograms(f: Frame, bars: readonly Bin[]): void {
  const { start, dur } = f.m.spans;
  fillRects(f, f.ink, bars.length, (k) => {
    const { span, bin, share } = at(bars, k);
    const row = at(f.rows, span);
    const x = f.x(at(start, span));
    const w = f.x(at(start, span) + at(dur, span)) - x;
    const h = f.rowH * (0.5 - f.pad) * share;
    return visible(f, row) && w >= minLabel
      ? [x + (bin * w) / bins, f.y(row + 1 - f.pad) - h, w / bins - 1, h]
      : null;
  });
}

// drawLinks strokes each span's link down from its parent's bar: read links solid, derived
// ones hatched, and the error path from where it started to where it was reported.
function drawLinks(f: Frame): void {
  const { flags } = f.m.spans;
  const derived = (i: number) => (at(flags, i) & Flag.derivedParent) !== 0;
  const errorPath = (i: number, p: number) => (at(flags, i) & at(flags, p) & Flag.error) !== 0;
  const paths = [
    { color: linkColor, dash: [], keep: (i: number) => !derived(i) },
    { color: linkColor, dash: hatch, keep: derived },
    { color: errorColor, dash: [], keep: errorPath },
  ];
  for (const { color, dash, keep } of paths) {
    f.ctx.strokeStyle = color;
    f.ctx.setLineDash(dash);
    f.ctx.beginPath();
    f.m.spans.parent.forEach((p, i) => {
      const row = at(f.rows, i);
      const prow = p < 0 ? -1 : at(f.rows, p);
      if (prow < 0 || row < 0 || row < f.v.y0 || prow > f.v.y1 || !keep(i, p)) {
        return;
      }
      const x = f.x(at(f.m.spans.start, i));
      f.ctx.moveTo(x, f.y(prow + 1 - f.pad));
      f.ctx.lineTo(x, f.y(row + f.pad));
      f.stats.primitives++;
    });
    f.ctx.stroke();
    f.stats.drawCalls++;
  }
  f.ctx.setLineDash([]);
}

// drawLabels writes the labels that fit their room and fall in the canvas: on a bar in dark
// text, beside one muted.
// A label overlapping the one written before it in its row is left out, so bars meeting in
// a flame graph's row keep their names apart.
function drawLabels(f: Frame, width: (ctx: TextContext, text: string) => number): void {
  let [row, end] = [Number.NaN, -Infinity];
  for (const l of f.labels.sort((a, b) => a.y - b.y || a.x - b.x)) {
    [row, end] = l.y === row ? [row, end] : [l.y, -Infinity];
    const text = fitText(width, f.ctx, l.text, l.room);
    const x = l.ending ? l.x - width(f.ctx, text) : l.x;
    f.ctx.fillStyle = l.beside ? f.muted : textColor;
    if (text && x >= end && x < f.v.width && l.y >= 0 && l.y <= f.v.height) {
      end = x + width(f.ctx, text) + 4;
      f.ctx.fillText(text, x, l.y);
      f.stats.drawCalls++;
      f.stats.labels++;
    }
  }
}

// at is a[i] for an i the model keeps in range.
function at<T>(a: ArrayLike<T>, i: number): T {
  const v = a[i];
  if (v === undefined) {
    throw new RangeError(`render-waterfall: index ${i} out of range`);
  }
  return v;
}
