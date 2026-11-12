import { createElement, type ReactElement } from "react";
import { resolve, type TimeRange, useTimeRange } from "./url";

const presets = ["15m", "1h", "6h", "24h", "7d", "30d"];

// Presets set the range to a span ending now.
export function Presets(): ReactElement {
  const [range, setRange] = useTimeRange();
  return createElement(
    "div",
    { role: "group", "aria-label": "Time range" },
    presets.map((p) =>
      createElement(
        "button",
        {
          key: p,
          type: "button",
          className: "tbtn",
          "aria-pressed": range.from === `now-${p}` && range.to === "now",
          onClick: () => setRange({ from: `now-${p}`, to: "now" }),
        },
        p,
      ),
    ),
  );
}

// clock is t as month-day and UTC minute: "10-28 14:35".
export function clock(t: string, now: number): string {
  return new Date(resolve(t, now)).toISOString().slice(5, 16).replace("T", " ");
}

// rangeText reads r: "last 1h" for a span ending now, else its UTC bounds.
function rangeText(r: TimeRange, now: number = Date.now()): string {
  return r.to === "now" && r.from.startsWith("now-")
    ? `last ${r.from.slice(4)}`
    : `${clock(r.from, now)} → ${clock(r.to, now)} UTC`;
}

// RangeText is the range the page shows, in words.
export function RangeText(): ReactElement {
  const [range] = useTimeRange();
  return createElement("span", { className: "range" }, rangeText(range));
}
