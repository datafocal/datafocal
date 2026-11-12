import { type Axis, Chart } from "@datafocal/ui";
import { createElement, type ReactElement, type ReactNode } from "react";

// The parts every lever's page is built of: facts over a page, a note naming what the logs
// lack, a stacked share bar and charts with axes; a bar inside a cell is @datafocal/ui's
// Meter.

// Fact is a labelled number, with the unit or denominator behind it.
export type Fact = readonly [label: string, value: string, unit?: string];

export const Facts = (p: { facts: readonly Fact[] }): ReactElement =>
  createElement(
    "div",
    { className: "ai-facts" },
    p.facts.map(([label, value, unit]) =>
      createElement(
        "div",
        { key: label },
        createElement("small", null, label),
        createElement("b", null, value),
        unit && createElement("span", null, unit),
      ),
    ),
  );

// Note says what a panel cannot show and what to log so it can.
export const Note = (p: { title: string; children?: ReactNode }): ReactElement =>
  createElement(
    "div",
    { className: "ai-empty" },
    createElement("b", null, p.title),
    " ",
    p.children,
  );

const grow = (share: number) => (el: HTMLElement | null) =>
  el?.style.setProperty("flex-grow", String(Math.max(share, 0.002)));

export interface Part {
  readonly key: string;
  readonly label: string;
  readonly share: number;
  readonly title: string;
}

// Stack is the parts of a whole side by side, each as wide as its share, with a legend.
export const Stack = (p: {
  parts: readonly Part[];
  picked?: string | undefined;
  pick?: ((key: string) => void) | undefined;
}): ReactElement =>
  createElement(
    "div",
    null,
    createElement(
      "div",
      { className: "ai-stack", role: "list" },
      p.parts.map((x, i) =>
        createElement("button", {
          key: x.key,
          type: "button",
          role: "listitem",
          title: x.title,
          "aria-label": x.title,
          "aria-pressed": p.picked === x.key,
          className: `cat-${(i % 5) + 1}`,
          ref: grow(x.share),
          onClick: () => p.pick?.(x.key),
        }),
      ),
    ),
    createElement(
      "div",
      { className: "ai-legend" },
      p.parts.map((x, i) =>
        createElement(
          "span",
          { key: x.key },
          createElement("i", { className: `cat-${(i % 5) + 1}` }),
          x.label,
          createElement("small", null, `${(x.share * 100).toFixed(1)}%`),
        ),
      ),
    ),
  );

// Plot is a line or bar chart with labelled axes, at least 320×120 as every chart that
// stands alone.
export const Plot = (p: {
  kind: "line" | "bars";
  xs: readonly number[];
  ys: readonly (readonly number[])[];
  label: string;
  x: Axis;
  y: Axis;
  series?: readonly string[];
  height?: number;
  onBrush?: (from: number, to: number) => void;
  onPick?: (x: number, series: number) => void;
}): ReactElement =>
  createElement(Chart, {
    kind: p.kind,
    // A value that is not a number is a gap, which uPlot leaves undrawn when it is null.
    data: [[...p.xs], ...p.ys.map((y) => y.map((v) => (Number.isFinite(v) ? v : null)))],
    width: 320,
    height: p.height ?? 200,
    label: p.label,
    x: p.x,
    y: p.y,
    ...(p.series && { series: p.series }),
    ...(p.onBrush && { onBrush: p.onBrush }),
    ...(p.onPick && { onPick: p.onPick }),
  });

// bucketAxis labels a bar chart whose x is the index of a bucket by the bucket's lower bound,
// and leaves the ticks uPlot puts between buckets blank.
export const bucketAxis = (label: string, lows: readonly string[]): Axis => ({
  label,
  format: (i) => (Number.isInteger(i) ? (lows[i] ?? "") : ""),
});

// Crumbs is the drill path to the page: each step but the last goes back to it.
export const Crumbs = (p: {
  steps: readonly (readonly [label: string, go?: () => void])[];
}): ReactElement =>
  createElement(
    "nav",
    { className: "ai-crumbs", "aria-label": "Drill path" },
    p.steps.map(([label, go], i) => [
      i > 0 && createElement("span", { key: `s${i}`, "aria-hidden": true }, "›"),
      go
        ? createElement("button", { key: label, type: "button", onClick: go }, label)
        : createElement("b", { key: label }, label),
    ]),
  );

// Lede says in a sentence what a page is for and how to read it.
export const Lede = (p: { title: string; text: string }): ReactElement =>
  createElement(
    "header",
    { className: "ai-lede" },
    createElement("h2", null, p.title),
    createElement("p", null, p.text),
  );
