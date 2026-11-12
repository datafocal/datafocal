import type { FieldStat, LogLine } from "@datafocal/api";
import {
  around,
  type Destination,
  pivotsOf,
  type Subject,
  traceable,
  traceOf,
} from "@datafocal/shell";
import { unwrap } from "@datafocal/ui";
import { marked, part, ranked } from "./row";

// Item is one row of the line table: a line, or a row of an open line's detail under it,
// its actions or a field. A field's value wraps over as many rows as it needs; first marks
// the row that names it, and more says the value went on past the rows shown.
export type Item = LogLine | Detail;

type Detail =
  | { readonly kind: "acts"; readonly line: LogLine }
  | {
      readonly kind: "field";
      readonly line: LogLine;
      readonly field: string;
      // raw is the value as stored, which a filter matches.
      readonly raw: string;
      readonly text: string;
      readonly first: boolean;
      readonly more: boolean;
    };

// Acts are what a line's detail can do: narrow the query, open the line's context, open
// another view on it (the pivots every view shares), or close it.
export interface Acts {
  readonly filter: (field: string, value: string, exclude: boolean) => void;
  readonly context: (l: LogLine) => void;
  readonly open: (to: Destination) => void;
  readonly close: (l: LogLine) => void;
}

// subjectOf is a line as the other views read it: its place, the request its id gathers,
// the line itself, and the ten minutes around it.
function subjectOf(l: LogLine): Subject {
  const by = traceOf(l);
  const attrs = l.attributes ?? {};
  return {
    service: l.service,
    pod: attrs["pod"],
    host: attrs["host"],
    id: by && { field: by[0], value: by[1] },
    line: { segment: l.segment ?? "", row: l.row },
    time: around(Date.parse(l.timestamp)),
  };
}

// keyChars is the width of the field name column, in characters; maxRows is the most rows
// one field's value takes before it is cut.
const keyChars = 30;
const maxRows = 40;

// wrap is v split at its newlines and then into pieces width characters long.
function wrap(v: string, width: number): string[] {
  return v.split("\n").flatMap((line) => {
    const pieces: string[] = [];
    for (let at = 0; at < line.length; at += width) {
      pieces.push(line.slice(at, at + width));
    }
    return pieces.length === 0 ? [""] : pieces;
  });
}

// pairs are a line's fields, best ranked first, after its message, level and service.
function pairs(l: LogLine, fields: readonly FieldStat[]): [string, string][] {
  const own: [string, string][] = [
    ["message", l.message],
    ["level", l.level],
    ["service", l.service ?? ""],
  ];
  const rest = ranked(Object.entries(l.attributes ?? {}), fields);
  return [...own.filter(([k, v]) => v !== "" || k !== "message"), ...rest];
}

// detail is the rows an open line adds under it, cols characters wide.
function detail(l: LogLine, cols: number, fields: readonly FieldStat[]): Item[] {
  const out: Item[] = [{ kind: "acts", line: l }];
  const width = Math.max(20, cols - keyChars);
  for (const [field, raw] of pairs(l, fields)) {
    const pieces = wrap(unwrap(raw), width);
    pieces.slice(0, maxRows).forEach((text, i) => {
      const more = i === maxRows - 1 && pieces.length > maxRows;
      out.push({ kind: "field", line: l, field, raw, text, first: i === 0, more });
    });
  }
  return out;
}

// isLine reports whether an item is a line rather than a row of one's detail.
export const isLine = (item: Item): item is LogLine => !("kind" in item);

// itemsOf is the table's rows: each line, and under each open one its detail. With none
// open they are the lines themselves, so a stream of many lines is not copied each change.
export function itemsOf(
  lines: readonly LogLine[],
  open: ReadonlySet<LogLine>,
  cols: number,
  fields: readonly FieldStat[],
): readonly Item[] {
  if (open.size === 0) {
    return lines;
  }
  return lines.flatMap((line): Item[] => [
    line,
    ...(open.has(line) ? detail(line, cols, fields) : []),
  ]);
}

const button = (parent: HTMLElement, label: string, title: string, run: () => void) => {
  const b = parent.appendChild(document.createElement("button"));
  b.type = "button";
  b.textContent = label;
  b.title = title;
  b.onclick = (e) => {
    e.stopPropagation();
    run();
  };
  return b;
};

// actsNode is an open line's actions: its context, a copy of it, and closing it.
export function actsNode(l: LogLine, acts: Acts): HTMLElement {
  const row = document.createElement("div");
  row.className = "ldet acts";
  button(row, "View in context", "This producer's lines around this one, unfiltered", () =>
    acts.context(l),
  );
  button(row, "Copy", "Copy the line as JSON", () => {
    void navigator.clipboard?.writeText(JSON.stringify(l, null, 2));
  });
  for (const p of pivotsOf(subjectOf(l)).filter((x) => x.view !== "logs")) {
    button(row, p.label, `Open ${p.label} on this line`, () => acts.open(p.to));
  }
  if (l.service) {
    const service = l.service;
    button(row, `Only ${service}`, "Filter to this line's service", () =>
      acts.filter("service", service, false),
    );
  }
  button(row, "Close", "Collapse this line", () => acts.close(l));
  return row;
}

// fieldNode is one row of a field's value, with filtering in and out on the row naming it.
export function fieldNode(
  item: Extract<Item, { kind: "field" }>,
  marks: RegExp | null,
  acts: Acts,
): HTMLElement {
  const row = document.createElement("div");
  row.className = `ldet${item.first ? " first" : ""}`;
  part(row, "dk", item.first ? item.field : "");
  const value = part(row, "dv", "");
  marked(value, item.text, item.field === "message" ? marks : null);
  if (item.more) {
    part(value, "dmore", " … cut; Copy has the rest");
  }
  if (item.first && item.field !== "message") {
    const tools = part(row, "dt", "");
    button(tools, "+", `Only lines with ${item.field} = this`, () =>
      acts.filter(item.field, item.raw, false),
    );
    button(tools, "−", `Lines without ${item.field} = this`, () =>
      acts.filter(item.field, item.raw, true),
    );
    if (traceable(item.field, item.raw)) {
      button(tools, "⇢", `The trace ${item.field} = this gathers`, () =>
        acts.open({ view: "trace", field: item.field, traceId: item.raw }),
      ).className = "trace";
    }
  }
  return row;
}
