import type { Metrics } from "./harness";

const runs = 6;

// ab measures main and the PR alternately (ABAB), runs times each, and returns each key's
// 95% interval for the PR's change relative to main: 0.1 is +10%.
export async function ab(
  measure: (url: string) => Promise<Metrics>,
  keys: readonly (keyof Metrics)[],
  mainURL: string,
  prURL: string,
): Promise<Partial<Record<keyof Metrics, [number, number]>>> {
  const main: Metrics[] = [];
  const pr: Metrics[] = [];
  for (let i = 0; i < runs; i++) {
    main.push(await measure(mainURL));
    pr.push(await measure(prURL));
  }
  return Object.fromEntries(
    keys.map((k) => [
      k,
      interval(
        main.map((m) => m[k]),
        pr.map((m) => m[k]),
      ),
    ]),
  );
}

// interval is the 95% interval of (mean(b) - mean(a)) / mean(a), by the normal
// approximation to Welch's t-test, as Tachometer reports it.
function interval(a: number[], b: number[]): [number, number] {
  const ma = mean(a);
  const diff = mean(b) - ma;
  const half = 1.96 * Math.sqrt(variance(a) / a.length + variance(b) / b.length);
  return [(diff - half) / ma, (diff + half) / ma];
}

function mean(xs: number[]): number {
  return xs.reduce((s, x) => s + x, 0) / xs.length;
}

function variance(xs: number[]): number {
  const m = mean(xs);
  return xs.reduce((s, x) => s + (x - m) ** 2, 0) / (xs.length - 1);
}
