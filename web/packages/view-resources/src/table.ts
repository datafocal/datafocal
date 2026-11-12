import type { ResourceUse } from "@datafocal/api";
import { navigate } from "@datafocal/shell";
import { createElement, type ReactElement, type ReactNode } from "react";
import { bytes, cores, type Detail, details, partTitles } from "./format";

export const button = (text: string, onClick: () => void, title?: string): ReactElement =>
  createElement("button", { type: "button", className: "link", title, onClick }, text);

// Column is a table's column: its id in the sort param, heading, value to sort by and text.
interface Column {
  readonly id: string;
  readonly label: string;
  readonly value: (u: ResourceUse) => number | null;
  readonly text: (u: ResourceUse) => string;
}

const column = (
  id: string,
  label: string,
  value: (u: ResourceUse) => number | null | undefined,
  format: (n: number | null) => string,
): Column => ({
  id,
  label,
  value: (u) => value(u) ?? null,
  text: (u) => format(value(u) ?? null),
});

const cpu = [
  column("cpu", "CPU", (u) => u.summary.cpu_cores_mean, cores),
  column("cpu_peak", "CPU peak", (u) => u.summary.cpu_cores_peak, cores),
];
const memory = [
  column(
    "memory",
    "Memory",
    (u) => u.summary.memory_bytes_mean,
    (n) => bytes(n),
  ),
  column(
    "memory_peak",
    "Memory peak",
    (u) => u.summary.memory_bytes_peak,
    (n) => bytes(n),
  ),
];
const network = [
  column(
    "in",
    "In",
    (u) => u.summary.rx_bytes_per_sec_mean,
    (n) => bytes(n, true),
  ),
  column(
    "out",
    "Out",
    (u) => u.summary.tx_bytes_per_sec_mean,
    (n) => bytes(n, true),
  ),
];
const disk = column(
  "disk",
  "Disk",
  (u) => u.summary.disk_bytes_last,
  (n) => bytes(n),
);

// columnsOf are the columns of the overview, or of a resource's page with each of its parts'
// means after its own: those of the parts rows carry, in their order.
export function columnsOf(
  detail: Detail | undefined,
  rows: readonly ResourceUse[],
  withDisk: boolean,
): Column[] {
  const parts = (rows[0]?.parts ?? []).map((part, i) =>
    column(
      `part:${part.name}`,
      partTitles[part.name] ?? part.name,
      (u) => u.parts?.[i]?.mean,
      details[detail ?? "cpu"].part,
    ),
  );
  switch (detail) {
    case "cpu":
      return [...cpu, ...parts];
    case "memory":
      return [...memory, ...parts];
    case "network":
      return [...network, ...parts];
    case "disk":
      return [disk];
    case undefined:
      return [...cpu, ...memory, ...network, ...(withDisk ? [disk] : [])];
  }
}

// Sort is a table's order: by a column's id, highest first, or by name ascending.
export interface Sort {
  readonly by: string;
  readonly desc: boolean;
}

// parseSort reads the sort param: a column's id, with a leading - for highest first.
export const parseSort = (s: string | undefined): Sort =>
  s?.startsWith("-") ? { by: s.slice(1), desc: true } : { by: s ?? "name", desc: false };

const sortParam = (s: Sort) => (s.desc ? `-${s.by}` : s.by);

// nextSort is the order a click on a column's heading gives: a value column highest first,
// then lowest; the name column ascending, then descending.
function nextSort(s: Sort, by: string): Sort {
  if (s.by !== by) {
    return { by, desc: by !== "name" };
  }
  return { by, desc: !s.desc };
}

// sorted is rows in s's order, a row without the value last, then by name.
function sorted(rows: readonly ResourceUse[], cols: readonly Column[], s: Sort): ResourceUse[] {
  const col = cols.find((c) => c.id === s.by);
  const sign = s.desc ? -1 : 1;
  return [...rows].sort((a, b) => {
    if (!col) {
      return sign * a.key.localeCompare(b.key);
    }
    const [x, y] = [col.value(a), col.value(b)];
    if (x === y) {
      return a.key.localeCompare(b.key);
    }
    return x === null ? 1 : y === null ? -1 : sign * (x - y);
  });
}

// Heading is a column's heading, a button sorting by it, marked with the order it sorts in.
function Heading(p: {
  id: string;
  label: string;
  sort: Sort;
  onSort: (s: string) => void;
}): ReactElement {
  const on = p.sort.by === p.id;
  return createElement(
    "th",
    { "aria-sort": on ? (p.sort.desc ? "descending" : "ascending") : "none" },
    createElement(
      "button",
      {
        type: "button",
        className: on ? "res-sort on" : "res-sort",
        title: `Sort by ${p.label.toLowerCase()}`,
        onClick: () => p.onSort(sortParam(nextSort(p.sort, p.id))),
      },
      p.label,
      on && createElement("span", { "aria-hidden": true }, p.sort.desc ? " ↓" : " ↑"),
    ),
  );
}

export interface TableProps {
  readonly label: string;
  readonly rows: readonly ResourceUse[];
  readonly columns: readonly Column[];
  readonly sort: Sort;
  readonly onSort: (s: string) => void;
  // pick opens a row: a service's pods, or a host's own use.
  readonly pick?: (key: string) => void;
  // logs is a row's Logs query, where it has one.
  readonly logs?: (key: string) => string;
}

// Table is a row per use, sorted by the column whose heading was clicked, each opening its
// logs over the window.
export function Table(p: TableProps): ReactElement {
  const cell = (u: ResourceUse): ReactNode =>
    p.pick ? button(u.key, () => p.pick?.(u.key)) : u.key;
  return createElement(
    "table",
    { className: "res-table", "aria-label": p.label },
    createElement(
      "thead",
      null,
      createElement(
        "tr",
        null,
        createElement(Heading, { id: "name", label: p.label, sort: p.sort, onSort: p.onSort }),
        p.columns.map((c) =>
          createElement(Heading, {
            key: c.id,
            id: c.id,
            label: c.label,
            sort: p.sort,
            onSort: p.onSort,
          }),
        ),
        createElement("th", null),
      ),
    ),
    createElement(
      "tbody",
      null,
      sorted(p.rows, p.columns, p.sort).map((u) =>
        createElement(
          "tr",
          { key: u.key },
          createElement("td", null, cell(u)),
          p.columns.map((c) => createElement("td", { key: c.id }, c.text(u))),
          createElement(
            "td",
            null,
            p.logs &&
              button(
                "Logs",
                () => navigate({ view: "logs", query: p.logs?.(u.key) ?? "" }),
                "Open in Logs",
              ),
          ),
        ),
      ),
    ),
  );
}
