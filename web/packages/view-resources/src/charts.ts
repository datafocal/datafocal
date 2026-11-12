import type { ResourceUse } from "@datafocal/api";
import { type Axis, Chart } from "@datafocal/ui";
import { createElement, type ReactElement } from "react";
import { type Detail, details, partTitles, type Stat, stats } from "./format";
import { type LogsOf, openLogs } from "./logs";

// shown is how many rows a chart draws a line each for, the rest summed into one more: the
// chart's palette has five colours.
const shown = 4;

// Brush narrows the page's range to the span dragged across a chart, in epoch ms.
export type Brush = (from: number, to: number) => void;

// Line is one line a chart draws: its name, a value per bin, null where it has none, and
// the Logs query of what it charts, which a click on it opens.
interface Line {
  readonly name: string;
  readonly values: readonly (number | null)[];
  readonly logs: string;
}

const along = (bins: readonly number[], values: readonly (number | null)[] | undefined) =>
  bins.map((_, i) => values?.[i] ?? null);

// breakdown is a line for each of the n rows using the most of stat, and one for the rest of
// the rows summed, so the lines account for the whole of the scope's use.
function breakdown(
  rows: readonly ResourceUse[],
  stat: Stat,
  bins: readonly number[],
  logs: { row: (key: string) => string; rest: string },
): Line[] {
  const n = shown;
  const ranked = [...rows].sort((a, b) => stats[stat].rank(b) - stats[stat].rank(a));
  const lines = ranked
    .slice(0, n)
    .map((u) => ({ name: u.key, values: along(bins, u[stat]), logs: logs.row(u.key) }));
  const rest = ranked.slice(n);
  if (rest.length === 0) {
    return lines;
  }
  const values = bins.map((_, i) => {
    const vs = rest.map((u) => u[stat]?.[i]).filter((v): v is number => v != null);
    return vs.length === 0 ? null : vs.reduce((a, v) => a + v, 0);
  });
  return [...lines, { name: `${rest.length} others`, values, logs: logs.rest }];
}

// partLines are a use's parts over time, as its detail names them.
const partLines = (u: ResourceUse, bins: readonly number[], logs: string): Line[] =>
  (u.parts ?? []).map((p) => ({
    name: partTitles[p.name] ?? p.name,
    values: along(bins, p.values),
    logs,
  }));

// Trend charts lines over the window, its heading opening the resource's page where open is
// given, a drag across it narrowing the range, and a click on a line opening its logs
// around the bin clicked, to read what a spike was.
function Trend(p: {
  title: string;
  bins: readonly number[];
  lines: readonly Line[];
  format: (n: number | null) => string;
  brush: Brush;
  open?: () => void;
  wide?: boolean;
}): ReactElement {
  const y: Axis = { format: (n) => p.format(n) };
  return createElement(
    "section",
    { className: p.wide ? "res-panel wide" : "res-panel", "aria-label": p.title },
    createElement(
      "h3",
      null,
      p.open
        ? createElement(
            "button",
            { type: "button", className: "link", title: `Open ${p.title}`, onClick: p.open },
            `${p.title} ›`,
          )
        : p.title,
    ),
    createElement(Chart, {
      kind: "line",
      data: [[...p.bins], ...p.lines.map((l) => [...l.values])],
      width: 320,
      height: p.wide ? 240 : 180,
      label: `${p.title} over time`,
      x: { label: "time", time: true },
      y,
      series: p.lines.map((l) => l.name),
      onBrush: p.brush,
      onPick: (at: number, s: number) => {
        const line = p.lines[s];
        if (line) {
          openLogs(line.logs, at, (p.bins[1] ?? at) - (p.bins[0] ?? at));
        }
      },
    }),
  );
}

// RowLogs are the Logs queries of a chart's rows, and of the rest of them summed.
export interface RowLogs {
  readonly row: (key: string) => string;
  readonly rest: string;
}

// Charts are the overview: CPU, memory, the network each way and disk where samples carry
// it, each the busiest rows and the rest, each opening its resource's page.
export function Charts(p: {
  bins: readonly number[];
  rows: readonly ResourceUse[];
  logs: RowLogs;
  disk: boolean;
  brush: Brush;
  open: (d: Detail) => void;
}): ReactElement {
  const trend = (stat: Stat, detail: Detail) =>
    createElement(Trend, {
      key: stat,
      title: stats[stat].title,
      bins: p.bins,
      lines: breakdown(p.rows, stat, p.bins, p.logs),
      format: stats[stat].format,
      brush: p.brush,
      open: () => p.open(detail),
    });
  return createElement(
    "div",
    { className: "res-grid" },
    trend("cpu_cores", "cpu"),
    trend("memory_bytes", "memory"),
    trend("rx_bytes_per_sec", "network"),
    trend("tx_bytes_per_sec", "network"),
    p.disk && trend("disk_bytes", "disk"),
  );
}

// DetailCharts are a resource's page: its use by group, the busiest and the rest, then
// its parts summed over the scope, then each host's own use.
export function DetailCharts(p: {
  detail: Detail;
  bins: readonly number[];
  rows: readonly ResourceUse[];
  total: ResourceUse;
  hosts: readonly ResourceUse[];
  label: string;
  logs: RowLogs;
  scope: LogsOf;
  brush: Brush;
}): ReactElement {
  const d = details[p.detail];
  const parts = partLines(p.total, p.bins, p.scope.scope);
  const hostLogs = { row: p.scope.host, rest: p.scope.scope };
  const trend = (key: string, title: string, lines: Line[], format: (n: number | null) => string) =>
    createElement(Trend, { key, title, bins: p.bins, lines, format, brush: p.brush, wide: true });
  return createElement(
    "div",
    { className: "res-grid" },
    d.stats.map((s) =>
      trend(
        s,
        `${stats[s].title} by ${p.label}`,
        breakdown(p.rows, s, p.bins, p.logs),
        stats[s].format,
      ),
    ),
    parts.length > 0 && trend("parts", `${d.title} by part`, parts, d.part),
    p.hosts.length > 0 &&
      d.stats.map((s) =>
        trend(
          `host:${s}`,
          `${stats[s].title}, each host's own`,
          breakdown(p.hosts, s, p.bins, hostLogs),
          stats[s].format,
        ),
      ),
  );
}
