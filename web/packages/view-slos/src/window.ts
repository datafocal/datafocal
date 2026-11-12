import type { TimeSpec } from "@datafocal/api";
import { useQueryRange } from "@datafocal/shell";
import { createElement as h, type ReactElement } from "react";
import { iso } from "./parts";

// The span the page reads raw lines over: its range when that is a day or less, else the
// range's last day, so what scans segments costs the same over a month as over a day. The
// overview, from the store's counts, covers the whole range; a drag across it narrows the range,
// and so the window, to what it covers.

const day = 24 * 3_600_000;

export interface Analysis {
  readonly time: TimeSpec;
  // capped marks a window shorter than the range.
  readonly capped: boolean;
  readonly from: number;
  readonly to: number;
}

export function useAnalysis(): Analysis {
  const range = useQueryRange();
  const [from, to] = [Date.parse(range.from), Date.parse(range.to)];
  if (to - from <= day) {
    return { time: range, capped: false, from, to };
  }
  return { time: { from: iso(to - day), to: range.to }, capped: true, from: to - day, to };
}

const stamp = (ms: number) =>
  new Date(ms).toLocaleString([], {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });

// WindowStrip says the window the parts below it read, when it is shorter than the range.
export function WindowStrip({ a }: { a: Analysis }): ReactElement | null {
  return a.capped
    ? h(
        "div",
        { className: "slo-window", role: "note" },
        h("small", null, "Below: the range's last day"),
        h("b", null, `${stamp(a.from)} → ${stamp(a.to)}`),
      )
    : null;
}
