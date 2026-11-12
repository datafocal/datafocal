import { type ChangeEvent, createElement, type ReactElement } from "react";
import { clock } from "./range";
import { type Baseline, baselineRange, useCompare, useTimeRange } from "./url";

// offsets are the baselines compare offers by name, each a step back from the range.
const offsets = [
  { offset: "period", label: "the period before" },
  { offset: "1d", label: "a day earlier" },
  { offset: "7d", label: "a week earlier" },
];

// describeBaseline names b as a phrase after a count: "a week earlier", or the range's
// bounds.
export function describeBaseline(b: Baseline): string {
  if ("offset" in b) {
    return offsets.find((o) => o.offset === b.offset)?.label ?? `${b.offset} earlier`;
  }
  const now = Date.now();
  return `in ${clock(b.from, now)} → ${clock(b.to, now)} UTC`;
}

// local is t as a datetime-local input's value, in UTC; utc reads one back, or undefined
// while it is incomplete.
const local = (t: string) => new Date(t).toISOString().slice(0, 16);
const utc = (v: string) => (Number.isNaN(Date.parse(`${v}Z`)) ? undefined : `${v}:00Z`);

// Compare picks what the range is compared against: nothing, a step back from it, or a
// range typed in, which starts as the period before.
export function Compare(): ReactElement {
  const [baseline, setBaseline] = useCompare();
  const [time] = useTimeRange();
  const chosen = baseline === null ? "" : "offset" in baseline ? baseline.offset : "custom";
  const onChoose = (e: ChangeEvent<HTMLSelectElement>) => {
    const v = e.target.value;
    setBaseline(
      v === "" ? null : v === "custom" ? baselineRange(time, { offset: "period" }) : { offset: v },
    );
  };
  const custom = baseline && !("offset" in baseline) ? baseline : undefined;
  const bound = (key: "from" | "to") =>
    createElement("input", {
      type: "datetime-local",
      "aria-label": `Compare ${key} (UTC)`,
      value: custom ? local(custom[key]) : "",
      onChange: (e: ChangeEvent<HTMLInputElement>) => {
        const at = utc(e.target.value);
        if (custom && at) {
          setBaseline({ ...custom, [key]: at });
        }
      },
    });
  return createElement(
    "span",
    { className: "compare" },
    createElement(
      "select",
      { "aria-label": "Compare", value: chosen, onChange: onChoose },
      createElement("option", { value: "" }, "No comparison"),
      offsets.map((o) =>
        createElement("option", { key: o.offset, value: o.offset }, `vs ${o.label}`),
      ),
      createElement("option", { value: "custom" }, "vs a range…"),
    ),
    custom && bound("from"),
    custom && "→",
    custom && bound("to"),
    custom && createElement("small", null, "UTC"),
  );
}
