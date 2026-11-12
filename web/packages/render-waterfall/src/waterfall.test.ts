import { expect, test } from "vitest";
import { createFlameRenderer, createWaterfallRenderer, Flag, type WaterfallModel } from ".";

// trace is seven spans: a critical path through an error that started in a span with a
// derived parent, a ×3 group, and a hatched sibling, with ticks, a "no evidence" gap and
// what one span's lines counted.
function trace(): WaterfallModel {
  const { error, critical, criticalInferred, derivedParent, group } = Flag;
  return {
    spans: {
      start: new Float64Array([0, 5, 10, 50, 62, 76, 60]),
      dur: new Float32Array([100, 40, 20, 10, 12, 20, 30]),
      self: new Float32Array([10, 20, 20, 10, 12, 20, 30]),
      depth: new Uint16Array([0, 1, 2, 1, 1, 1, 1]),
      parent: new Int32Array([-1, 0, 1, 0, 0, 0, 0]),
      flags: new Uint8Array([
        critical,
        critical | error,
        critical | criticalInferred | error | derivedParent,
        group,
        0,
        0,
        derivedParent,
      ]),
      group: new Uint32Array([0, 1, 2, 3, 3, 3, 6]),
    },
    names: ["GET /checkout", "charge", "db.query", "cache.get", "cache.get", "cache.get", "emit"],
    services: new Uint16Array([0, 1, 2, 3, 3, 3, 1]),
    ticks: {
      span: new Uint32Array([1, 1, 2, 2, 6]),
      t: new Float64Array([6, 40, 12, 28, 61]),
      error: new Uint8Array([0, 0, 1, 1, 0]),
    },
    gaps: [{ t0: 45, t1: 50, depth: 1 }],
    facts: ["", "", "bytes_read 1.2K · gets 4", "", "", "", ""],
  };
}

// recorder returns a canvas whose context writes each draw to log.
function recorder(log: string[]) {
  const call =
    (name: string) =>
    (...args: unknown[]) => {
      log.push(`${name} ${args.join(" ")}`);
    };
  const textBaseline: CanvasTextBaseline = "alphabetic";
  const ctx = {
    font: "",
    fillStyle: "",
    strokeStyle: "",
    textBaseline,
    measureText: (text: string) => ({ width: 6 * text.length }),
    setTransform: call("setTransform"),
    setLineDash: call("setLineDash"),
    clearRect: call("clearRect"),
    beginPath: call("beginPath"),
    rect: call("rect"),
    roundRect: call("roundRect"),
    arc: call("arc"),
    lineWidth: 1,
    moveTo: call("moveTo"),
    lineTo: call("lineTo"),
    fill: () => log.push(`fill ${ctx.fillStyle}`),
    stroke: () => log.push(`stroke ${ctx.strokeStyle}`),
    fillText: call("fillText"),
  };
  return { width: 0, height: 0, getContext: () => ctx };
}

const view = { x0: 0, x1: 100, y0: 0, y1: 5, width: 800, height: 100, dpr: 2 };

test("the waterfall and flame draw lists match the golden, run after run", async () => {
  const draws = () => {
    const log: string[] = ["waterfall"];
    log.push(JSON.stringify(createWaterfallRenderer(recorder(log)).render(trace(), view)), "flame");
    log.push(JSON.stringify(createFlameRenderer(recorder(log)).render(trace(), view)));
    return `${log.join("\n")}\n`;
  };
  const first = draws();
  expect(draws()).toBe(first);
  await expect(first).toMatchFileSnapshot("../testdata/draw.golden");
});

test("hitTest finds the span under each of 1,000 points among 10,000 within 1 ms", () => {
  const n = 10_000;
  const m = trace();
  m.spans = {
    start: Float64Array.from({ length: n }, (_, i) => (i * 7919) % 1000),
    dur: Float32Array.from({ length: n }, (_, i) => 1 + ((i * 104_729) % 500)),
    self: new Float32Array(n),
    depth: new Uint16Array(n),
    parent: new Int32Array(n).fill(-1),
    flags: new Uint8Array(n),
    group: Uint32Array.from({ length: n }, (_, i) => i),
  };
  m.services = new Uint16Array(n);
  m.names = new Array<string>(n).fill("span");
  const v = { ...view, x1: 1500, y1: n, height: n * 20 };
  const r = createWaterfallRenderer(recorder([]));
  r.render(m, v);
  const misses: string[] = [];
  const began = performance.now();
  for (let k = 0; k < 1000; k++) {
    const [t, row] = [(k * 7) % 1500, (k * 9973) % n];
    const y = (row + 0.5) * 20;
    const end = (m.spans.start[row] ?? 0) + (m.spans.dur[row] ?? 0);
    const want = t >= (m.spans.start[row] ?? 0) && t <= end ? row : -1;
    const got = r.hitTest((t * v.width) / v.x1, y)?.index ?? -1;
    if (got !== want) {
      misses.push(`(${t}, ${row}): got ${got}, want ${want}`);
    }
  }
  expect(misses).toEqual([]);
  expect((performance.now() - began) / 1000).toBeLessThanOrEqual(1);
});
