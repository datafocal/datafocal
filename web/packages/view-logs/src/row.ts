import type { FieldStat, LogLine } from "@datafocal/api";
import { lineText, unwrap } from "@datafocal/ui";

// ranked sorts a line's key=value pairs by their field's signal, unranked fields last.
export function ranked(
  pairs: [string, string][],
  fields: readonly FieldStat[],
): [string, string][] {
  const at = (k: string) => {
    const i = fields.findIndex((f) => f.name === k);
    return i < 0 ? fields.length : i;
  };
  return pairs.sort(([a], [b]) => at(a) - at(b));
}

// clock is a line's time of day to the millisecond, the same width on every line.
export function clock(ts: string): string {
  const t = Date.parse(ts);
  return Number.isNaN(t) ? ts.slice(11, 23) : new Date(t).toISOString().slice(11, 23);
}

// severity is the colour class of a level, whatever its producer spells it.
export function severity(level: string): "error" | "warn" | "info" {
  const l = level.toLowerCase();
  if (/^(err|fatal|crit|panic|emerg|alert)/.test(l)) {
    return "error";
  }
  return l.startsWith("warn") ? "warn" : "info";
}

const maxValue = 48;
const maxFields = 4;

// part appends a span of text, with a class, to parent.
export function part(parent: HTMLElement, className: string, content: string): HTMLElement {
  const span = parent.appendChild(document.createElement("span"));
  span.className = className;
  span.textContent = content;
  return span;
}

// marked appends content to parent with each match of marks wrapped in a <mark>.
export function marked(parent: HTMLElement, content: string, marks: RegExp | null): void {
  if (!marks) {
    parent.append(content);
    return;
  }
  content.split(marks).forEach((piece, i) => {
    if (i % 2 === 1) {
      parent.appendChild(document.createElement("mark")).textContent = piece;
    } else if (piece !== "") {
      parent.append(piece);
    }
  });
}

// markRegex matches any of words, ignoring case, capturing the match; null for none.
export function markRegex(words: readonly string[]): RegExp | null {
  const escaped = words
    .filter((w) => w !== "")
    .map((w) => w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  return escaped.length === 0 ? null : new RegExp(`(${escaped.join("|")})`, "gi");
}

// lineNode is a line as a table row: time, level, service, then what it says, its matches
// marked, with its most telling fields after it, muted. The format it was read in is the
// row's tooltip.
export function lineNode(
  l: LogLine,
  fields: readonly FieldStat[],
  marks: RegExp | null,
  open: boolean,
): HTMLElement {
  const row = document.createElement("div");
  row.className = `lrow ${severity(l.level)}${open ? " open" : ""}`;
  row.setAttribute("role", "button");
  row.setAttribute("aria-expanded", String(open));
  row.tabIndex = 0;
  const format = l.attributes?.["format"];
  if (format) {
    row.title = `read as ${format}`;
  }
  part(row, "lt", clock(l.timestamp));
  part(row, "llv", l.level);
  part(row, "lsvc", l.service ?? "");
  const msg = part(row, "lmsg", "");
  marked(msg, lineText(l), marks);
  const high = (k: string) => fields.some((f) => f.name === k && f.high_signal);
  for (const [k, v] of ranked(Object.entries(l.attributes ?? {}), fields)
    .filter(([k]) => high(k))
    .slice(0, maxFields)) {
    const value = unwrap(v);
    part(
      part(msg, "lkv", `${k}=`),
      "",
      value.length > maxValue ? `${value.slice(0, maxValue)}…` : value,
    );
  }
  return row;
}
