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

// Lane is one session's row. A subagent's lane comes after its parent's, whose index is
// parent (-1 for none), one depth deeper.
export interface Lane {
  id: string;
  label: string;
  depth: number;
  parent: number;
}

// Marker is an event at time t in a lane: a context compaction, a gate result, or a
// collision, where two sessions touched the same value in overlapping time, drawn as a
// connector from lane to lane `to`.
export interface Marker {
  kind: "compaction" | "collision" | "gate-pass" | "gate-fail";
  t: number;
  lane: number;
  to?: number;
}

// TimelineModel is a run, columnar. Steps are in start order; kind indexes kinds, coloured
// by colors when set, and group counts the repeats a step stands for, labelled ×N above one. fill is each lane's context
// tokens, lane by lane in time order. track is the correlated log lines in time order, drawn
// in a row below the lanes.
export interface TimelineModel {
  lanes: readonly Lane[];
  kinds: readonly string[];
  colors?: readonly string[];
  steps: {
    start: Float64Array;
    dur: Float32Array;
    lane: Uint16Array;
    kind: Uint8Array;
    group: Uint32Array;
  };
  fill?: { lane: Uint16Array; t: Float64Array; tokens: Float32Array };
  markers: readonly Marker[];
  track?: { t: Float64Array; severity: Uint8Array; text: readonly string[] };
}

// Canvas is the part of an OffscreenCanvas the renderer uses.
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
    | "clearRect"
    | "beginPath"
    | "rect"
    | "moveTo"
    | "lineTo"
    | "fill"
    | "stroke"
    | "fillText"
  >;

const kindColors = ["#4e79a7", "#f28e2b", "#59a14f", "#b07aa1", "#76b7b2", "#edc948", "#9c755f"];
const markerColors: Record<Marker["kind"], string> = {
  compaction: "#8a8f98",
  collision: "#e15759",
  "gate-pass": "#59a14f",
  "gate-fail": "#e15759",
};
// severityColors is debug, info, warn and error.
const severityColors = ["#8a8f98", "#4e79a7", "#edc948", "#e15759"];
const textColor = "#1f2328";
const font = "500 11px sans-serif";
// pad is the share of a row above and below its bars; minLabel is the narrowest labelled
// bar in pixels.
const pad = 0.15;
const minLabel = 20;

// Label is text at x, y in room pixels, a lane's in ink, else on a bar in dark text.
interface Label {
  text: string;
  x: number;
  y: number;
  room: number;
  lane?: boolean;
}

// Frame is one render's state: the model, the scale from data to CSS pixels, what it drew
// and the labels it will draw last.
interface Frame {
  ctx: Context;
  m: TimelineModel;
  v: Viewport;
  x(t: number): number;
  y(row: number): number;
  rowH: number;
  stats: DrawStats;
  labels: Label[];
  // ink is the colour of text and lines that sit on the page rather than on a bar.
  ink: string;
}

// Theme is the colours and font a renderer takes from the page, which a worker can't read.
export interface Theme {
  readonly ink: string;
  readonly font?: string;
}

// createTimelineRenderer draws run timelines into canvas, which is an OffscreenCanvas in a
// worker in the app, its text in theme's ink.
export function createTimelineRenderer(
  canvas: Canvas,
  theme: Theme = { ink: textColor },
): Renderer<TimelineModel> {
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    throw new Error("render-timeline: no 2d context");
  }
  const widths = createTextWidths();
  let model: TimelineModel | null = null;
  let index: HitIndex | null = null;
  let maxTokens = 0;
  let view: Viewport | null = null;
  return {
    resize(width, height, dpr) {
      canvas.width = Math.round(width * dpr);
      canvas.height = Math.round(height * dpr);
    },
    render(m, v) {
      if (m !== model) {
        model = m;
        index = createHitIndex(stepBoxes(m));
        maxTokens = m.fill?.tokens.reduce((a, b) => Math.max(a, b), 0) ?? 0;
      }
      view = v;
      const f = frame(ctx, m, v);
      ctx.font = theme.font ?? font;
      f.ink = theme.ink;
      drawSteps(f);
      drawFill(f, maxTokens);
      drawMarkers(f);
      drawTrack(f);
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
      return i < 0 ? null : { kind: "step", index: i };
    },
    dispose() {
      model = null;
      index = null;
      view = null;
    },
  };
}

// stepBoxes returns each step's bar in data space: its time by its row less the padding.
function stepBoxes(m: TimelineModel): Float64Array {
  const { start, dur, lane } = m.steps;
  const boxes = new Float64Array(start.length * 4);
  for (let i = 0; i < start.length; i++) {
    boxes[i * 4] = at(start, i);
    boxes[i * 4 + 1] = at(lane, i) + pad;
    boxes[i * 4 + 2] = at(start, i) + at(dur, i);
    boxes[i * 4 + 3] = at(lane, i) + 1 - pad;
  }
  return boxes;
}

function frame(ctx: Context, m: TimelineModel, v: Viewport): Frame {
  const kx = v.width / (v.x1 - v.x0);
  const ky = v.height / (v.y1 - v.y0);
  ctx.setTransform(v.dpr, 0, 0, v.dpr, 0, 0);
  ctx.clearRect(0, 0, v.width, v.height);
  ctx.textBaseline = "middle";
  const labels = m.lanes.map((l, row) => ({
    text: l.label,
    x: 4 + 12 * l.depth,
    y: (row + 0.5 - v.y0) * ky,
    room: Infinity,
    lane: true,
  }));
  return {
    ctx,
    m,
    v,
    x: (t) => (t - v.x0) * kx,
    y: (row) => (row - v.y0) * ky,
    rowH: ky,
    stats: { drawCalls: 0, primitives: 0, labels: 0 },
    labels,
    ink: textColor,
  };
}

// drawSteps fills the visible steps one path per kind. Following the Perfetto track model,
// it snaps each step to buckets of the window's time per pixel rounded up to a power of
// two, so a step keeps its shape while the window pans, and skips a step whose buckets its
// lane has already filled, so a frame draws at most one bar per pixel, lane and kind.
function drawSteps(f: Frame): void {
  const res = 2 ** Math.ceil(Math.log2((f.v.x1 - f.v.x0) / f.v.width));
  for (let k = 0; k < f.m.kinds.length; k++) {
    const ends = new Float64Array(f.m.lanes.length).fill(-Infinity);
    f.ctx.fillStyle = f.m.colors?.[k] ?? at(kindColors, k % kindColors.length);
    f.ctx.beginPath();
    for (let i = 0; i < f.m.steps.start.length; i++) {
      if (f.m.steps.kind[i] === k) {
        drawStep(f, i, ends, res);
      }
    }
    f.ctx.fill();
    f.stats.drawCalls++;
  }
}

// drawStep adds step i's bar to the path unless it is out of the window or under bars its
// lane already has, given the bucket each lane's bars end at and the bucket width res.
function drawStep(f: Frame, i: number, ends: Float64Array, res: number): void {
  const { start, dur, lane, kind, group } = f.m.steps;
  const t = at(start, i);
  const end = t + at(dur, i);
  const row = at(lane, i);
  const b1 = Math.max(Math.ceil(end / res), Math.floor(t / res) + 1);
  if (end < f.v.x0 || t > f.v.x1 || b1 <= at(ends, row)) {
    return;
  }
  const b0 = Math.max(Math.floor(t / res), at(ends, row));
  ends[row] = b1;
  const x = f.x(b0 * res);
  const w = f.x(b1 * res) - x;
  f.ctx.rect(x, f.y(row + pad), w, f.rowH * (1 - 2 * pad));
  f.stats.primitives++;
  if (w >= minLabel) {
    const name = at(f.m.kinds, at(kind, i));
    const n = at(group, i);
    const text = n > 1 ? `${name} ×${n}` : name;
    f.labels.push({ text, x: x + 4, y: f.y(row + 0.5), room: w - 8 });
  }
}

// drawFill strokes each lane's context tokens as a line rising from the bottom of its row.
function drawFill(f: Frame, maxTokens: number): void {
  const fill = f.m.fill;
  if (!fill) {
    return;
  }
  f.ctx.strokeStyle = f.ink;
  f.ctx.beginPath();
  for (let i = 0; i < fill.t.length; i++) {
    const row = at(fill.lane, i);
    const share = at(fill.tokens, i) / maxTokens;
    const x = f.x(at(fill.t, i));
    const y = f.y(row + 1 - pad - share * (1 - 2 * pad));
    if (i === 0 || fill.lane[i - 1] !== row) {
      f.ctx.moveTo(x, y);
    } else {
      f.ctx.lineTo(x, y);
      f.stats.primitives++;
    }
  }
  f.ctx.stroke();
  f.stats.drawCalls++;
}

// drawMarkers strokes each marker down its row, or across the rows between the lanes a
// collision joins, one path per kind.
function drawMarkers(f: Frame): void {
  for (const [kind, color] of Object.entries(markerColors)) {
    const ms = f.m.markers.filter((m) => m.kind === kind);
    if (ms.length === 0) {
      continue;
    }
    f.ctx.strokeStyle = color;
    f.ctx.beginPath();
    for (const m of ms) {
      const x = f.x(m.t);
      f.ctx.moveTo(x, f.y(Math.min(m.lane, m.to ?? m.lane)));
      f.ctx.lineTo(x, f.y(Math.max(m.lane, m.to ?? m.lane) + 1));
    }
    f.ctx.stroke();
    f.stats.drawCalls++;
    f.stats.primitives += ms.length;
  }
}

// drawTrack ticks each log line in the row below the lanes, one path per severity, and
// labels it in the room before the next.
function drawTrack(f: Frame): void {
  const track = f.m.track;
  if (!track) {
    return;
  }
  const row = f.m.lanes.length;
  for (let s = 0; s < severityColors.length; s++) {
    f.ctx.fillStyle = at(severityColors, s);
    f.ctx.beginPath();
    for (let i = 0; i < track.t.length; i++) {
      if (track.severity[i] === s) {
        f.ctx.rect(f.x(at(track.t, i)), f.y(row + pad), 1, f.rowH * (1 - 2 * pad));
        f.stats.primitives++;
      }
    }
    f.ctx.fill();
    f.stats.drawCalls++;
  }
  for (let i = 0; i < track.t.length; i++) {
    const x = f.x(at(track.t, i));
    const room = (i + 1 < track.t.length ? f.x(at(track.t, i + 1)) : f.v.width) - x - 4;
    if (room >= minLabel) {
      f.labels.push({ text: at(track.text, i), x: x + 3, y: f.y(row + 0.5), room, lane: true });
    }
  }
}

// drawLabels writes the labels that fit their room and fall in the canvas, left to right in
// each row, skipping one that would overlap the label written before it.
function drawLabels(f: Frame, width: (ctx: TextContext, text: string) => number): void {
  let row = Number.NaN;
  let end = -Infinity;
  for (const l of f.labels.sort((a, b) => a.y - b.y || a.x - b.x)) {
    [row, end] = l.y === row ? [row, end] : [l.y, -Infinity];
    const text = fitText(width, f.ctx, l.text, l.room);
    if (text && l.x >= end && l.x < f.v.width && l.y >= 0 && l.y <= f.v.height) {
      f.ctx.fillStyle = l.lane ? f.ink : textColor;
      f.ctx.fillText(text, l.x, l.y);
      end = l.x + width(f.ctx, text);
      f.stats.drawCalls++;
      f.stats.labels++;
    }
  }
}

// at is a[i] for an i the model keeps in range.
function at<T>(a: ArrayLike<T>, i: number): T {
  const v = a[i];
  if (v === undefined) {
    throw new RangeError(`render-timeline: index ${i} out of range`);
  }
  return v;
}
