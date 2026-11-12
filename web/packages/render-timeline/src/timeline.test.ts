import { expect, test } from "vitest";
import { createTimelineRenderer, type TimelineModel } from ".";

// rng is a linear congruential generator: a deterministic source of numbers in [0, 1).
function rng(seed: number): () => number {
  let s = seed;
  return () => {
    s = (Math.imul(s, 1664525) + 1013904223) >>> 0;
    return s / 2 ** 32;
  };
}

// run returns n back-to-back steps over four lanes, the last a subagent of the first, with
// a context fill, a marker of each kind and a log line every 20 steps.
function run(n: number, seed: number): TimelineModel {
  const r = rng(seed);
  const ends = [0, 0, 0, 0];
  const steps = {
    start: new Float64Array(n),
    dur: new Float32Array(n),
    lane: new Uint16Array(n),
    kind: new Uint8Array(n),
    group: new Uint32Array(n),
  };
  for (let i = 0; i < n; i++) {
    const lane = Math.floor(r() * 4);
    steps.start[i] = ends[lane] ?? 0;
    steps.dur[i] = 1 + Math.floor(r() * 2000);
    ends[lane] = (steps.start[i] ?? 0) + (steps.dur[i] ?? 0);
    steps.lane[i] = lane;
    steps.kind[i] = Math.floor(r() * 3);
    steps.group[i] = r() < 0.1 ? 2 + Math.floor(r() * 5) : 1;
  }
  const ticks = Array.from({ length: n / 20 }, (_, i) => i * 4000);
  return {
    lanes: [
      { id: "s0", label: "session 0", depth: 0, parent: -1 },
      { id: "s1", label: "session 1", depth: 0, parent: -1 },
      { id: "s2", label: "session 2", depth: 0, parent: -1 },
      { id: "a0", label: "subagent", depth: 1, parent: 0 },
    ],
    kinds: ["tool", "model", "think"],
    steps,
    fill: {
      lane: new Uint16Array(ticks.map((_, i) => (i < ticks.length / 2 ? 0 : 1))),
      t: new Float64Array(ticks),
      tokens: new Float32Array(ticks.map((t) => t % 50_000)),
    },
    markers: [
      { kind: "compaction", t: 50_000, lane: 0 },
      { kind: "collision", t: 60_000, lane: 1, to: 2 },
      { kind: "gate-pass", t: 70_000, lane: 3 },
      { kind: "gate-fail", t: 80_000, lane: 2 },
    ],
    track: {
      t: new Float64Array(ticks),
      severity: new Uint8Array(ticks.map((_, i) => i % 4)),
      text: ticks.map((_, i) => `line ${i}`),
    },
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
    clearRect: call("clearRect"),
    beginPath: call("beginPath"),
    rect: call("rect"),
    moveTo: call("moveTo"),
    lineTo: call("lineTo"),
    fill: () => log.push(`fill ${ctx.fillStyle}`),
    stroke: () => log.push(`stroke ${ctx.strokeStyle}`),
    fillText: call("fillText"),
  };
  return { width: 0, height: 0, getContext: () => ctx };
}

const view = { x0: 0, x1: 120_000, y0: 0, y1: 5, width: 800, height: 200, dpr: 2 };

test("the draw list of a 200-step run matches the golden, run after run", async () => {
  const draws = () => {
    const log: string[] = [];
    const stats = createTimelineRenderer(recorder(log)).render(run(200, 1), view);
    return `${log.join("\n")}\n${JSON.stringify(stats)}\n`;
  };
  const first = draws();
  expect(draws()).toBe(first);
  await expect(first).toMatchFileSnapshot("../testdata/draw-200.golden");
});

// under returns the last step whose bar, its time by its row less 0.15 above and below,
// holds (t, row), by checking every step.
function under(m: TimelineModel, t: number, row: number): number {
  let want = -1;
  m.steps.start.forEach((start, i) => {
    const lane = m.steps.lane[i] ?? 0;
    const inT = t >= start && t <= start + (m.steps.dur[i] ?? 0);
    if (inT && row >= lane + 0.15 && row <= lane + 0.85) {
      want = i;
    }
  });
  return want;
}

test("hitTest returns the last-drawn step under each of 10,000 points", () => {
  const m = run(200, 2);
  const r = createTimelineRenderer(recorder([]));
  r.render(m, view);
  const rand = rng(3);
  const misses: string[] = [];
  for (let n = 0; n < 10_000; n++) {
    const x = rand() * view.width;
    const y = rand() * view.height;
    const t = (x * view.x1) / view.width;
    const row = (y * view.y1) / view.height;
    const want = under(m, t, row);
    const got = r.hitTest(x, y)?.index ?? -1;
    if (got !== want) {
      misses.push(`(${x}, ${y}): got ${got}, want ${want}`);
    }
  }
  expect(misses).toEqual([]);
});
