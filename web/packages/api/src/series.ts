import type { Series } from "./generated/tools";

// binStarts is each bin's start: start + i × step for every bin of the series.
export function binStarts(s: Series): number[] {
  const bins = (s.values ?? s.measure_values?.[0] ?? []).length;
  return Array.from({ length: bins }, (_, i) => s.start + i * s.step);
}
