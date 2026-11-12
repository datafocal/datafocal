import Flatbush from "flatbush";

// Viewport is the data range a renderer draws, [x0, x1] by [y0, y1], into a canvas of
// width by height CSS pixels at dpr device pixels each.
export interface Viewport {
  x0: number;
  x1: number;
  y0: number;
  y1: number;
  width: number;
  height: number;
  dpr: number;
}

// DrawStats is what one render drew, the counts the performance budgets hold.
export interface DrawStats {
  drawCalls: number;
  primitives: number;
  labels: number;
}

// Hit is the item under a point: its kind and its index in the model.
export interface Hit {
  kind: string;
  index: number;
}

// Renderer draws a Model into a canvas, so every renderer is measured the same way.
export interface Renderer<Model> {
  resize(width: number, height: number, dpr: number): void;
  render(model: Model, view: Viewport): DrawStats;
  hitTest(x: number, y: number): Hit | null;
  dispose(): void;
}

// HitIndex finds the box under a point among boxes packed once into a static R-tree
// (flatbush), so a lookup descends the tree instead of scanning every box.
export interface HitIndex {
  // top returns the highest-numbered box containing (x, y), the one drawn last, or -1.
  top(x: number, y: number): number;
}

// createHitIndex packs boxes, given as [minX, minY, maxX, maxY] for each box in turn.
export function createHitIndex(boxes: Float64Array): HitIndex {
  const n = boxes.length / 4;
  if (n === 0) {
    return { top: () => -1 };
  }
  const tree = new Flatbush(n);
  for (let i = 0; i < boxes.length; i += 4) {
    tree.add(boxes[i] ?? 0, boxes[i + 1] ?? 0, boxes[i + 2] ?? 0, boxes[i + 3] ?? 0);
  }
  tree.finish();
  return { top: (x, y) => Math.max(-1, ...tree.search(x, y, x, y)) };
}

// TextContext is the part of a 2D context that measures text.
export interface TextContext {
  font: string;
  measureText(text: string): { width: number };
}

// createTextWidths returns a measure of text in ctx's current font, cached per (font, text):
// measuring is slow and a renderer labels the same strings every frame.
// fitText is text as it fits room pixels given width: whole, cut short with an ellipsis, or
// "" when not even a few characters do.
export function fitText(
  width: (ctx: TextContext, text: string) => number,
  ctx: TextContext,
  text: string,
  room: number,
): string {
  const whole = width(ctx, text);
  if (whole <= room) {
    return text;
  }
  for (let n = Math.floor((text.length * room) / whole); n >= 3; n--) {
    const cut = `${text.slice(0, n).trimEnd()}…`;
    if (width(ctx, cut) <= room) {
      return cut;
    }
  }
  return "";
}

export function createTextWidths(): (ctx: TextContext, text: string) => number {
  const widths = new Map<string, number>();
  return (ctx, text) => {
    const key = `${ctx.font}\n${text}`;
    let w = widths.get(key);
    if (w === undefined) {
      w = ctx.measureText(text).width;
      widths.set(key, w);
    }
    return w;
  };
}
