import type { Predicate } from "@datafocal/api";
import { createElement, type ReactElement, useId, useState } from "react";

// A filter is the Logs search's field terms: field:value matches a field, a leading -
// excludes, a value with * matches as a glob, and a quoted value may hold spaces.
const term = /\s*(-)?([A-Za-z_][\w.-]*):("(?:[^"\\]|\\.)*"|\S+)|\s*(\S+)/gy;

// fields are what a sample carries to filter on, for autocomplete.
const fields = ["host", "service", "pod", "namespace", "container", "container.id"];

const unquote = (s: string): string => {
  const v: unknown = s.startsWith('"') && s.length > 1 ? JSON.parse(s) : s;
  return typeof v === "string" ? v : s;
};

// glob is a value with * as an anchored regex, each other character literal.
const glob = (v: string) =>
  `^${v
    .split("*")
    .map((s) => s.replace(/[.+?^${}()|[\]\\]/g, "\\$&"))
    .join(".*")}$`;

// predicateOf is one field term as a predicate.
function predicateOf(not: boolean, field: string, raw: string): Predicate {
  const value = unquote(raw);
  if (value.includes("*")) {
    return { field, op: not ? "noregex" : "regex", value: glob(value) };
  }
  return { field, op: not ? "ne" : "eq", value };
}

// parseFilter is q's terms as predicates, or what in it is no field term.
export function parseFilter(q: string): { where: Predicate[]; error?: string } {
  const where: Predicate[] = [];
  term.lastIndex = 0;
  for (let m = term.exec(q); m && m[0].trim() !== ""; m = term.exec(q)) {
    const [, not, field, value, word] = m;
    if (word !== undefined || !field || value === undefined) {
      return { where, error: `"${word}" is no field:value, as host:framework or -service:etcd` };
    }
    where.push(predicateOf(not === "-", field, value));
  }
  return { where };
}

// quoted is a value as a filter term writes it.
const quoted = (v: string) => (/^[^\s"*]+$/.test(v) ? v : JSON.stringify(v));

// withTerm is q with field:value added, unless q already holds it.
export function withTerm(q: string, field: string, value: string): string {
  const t = `${field}:${quoted(value)}`;
  return q.split(/\s+/).includes(t) ? q : `${q} ${t}`.trim();
}

// suggestions complete the word before the end of q to a field name and ":".
function suggestions(q: string): readonly string[] {
  const partial = /(?:^|\s)-?([A-Za-z_][\w.]*)$/.exec(q)?.[1] ?? "";
  const head = q.slice(0, q.length - partial.length);
  return partial === ""
    ? []
    : fields.filter((f) => f.startsWith(partial) && f !== partial).map((f) => `${head}${f}:`);
}

// FilterBar edits the page's filter, run on Enter, and says what in it is no field term.
// Keyed by the filter, so one set elsewhere, as by picking a host, shows here too.
export function FilterBar(p: {
  readonly q: string;
  readonly onRun: (q: string) => void;
}): ReactElement {
  const [q, setQ] = useState(p.q);
  const list = useId();
  const { error } = parseFilter(q);
  return createElement(
    "form",
    {
      className: "res-filter",
      role: "search",
      onSubmit: (e: { preventDefault: () => void }) => {
        e.preventDefault();
        p.onRun(q.trim());
      },
    },
    createElement("input", {
      value: q,
      list,
      spellCheck: false,
      "aria-label": "Filter",
      placeholder: "Filter: host:framework service:querier -namespace:kube-system pod:checkout-*",
      onChange: (e: { currentTarget: HTMLInputElement }) => setQ(e.currentTarget.value),
    }),
    createElement(
      "datalist",
      { id: list },
      suggestions(q).map((s) => createElement("option", { key: s, value: s })),
    ),
    error && createElement("p", { className: "res-note res-fault", role: "alert" }, error),
  );
}
