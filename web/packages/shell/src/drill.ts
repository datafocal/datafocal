import type { Predicate } from "@datafocal/api";
import { Chip } from "@datafocal/ui";
import { createElement, type ReactElement } from "react";
import { stepTo, useParams } from "./url";

// A view's drill-down, the same in every view: the predicates it is filtered to, shown as
// chips, the one thing picked, which the view opens in its inspector, and the range narrowed
// to a span. Each is in the URL, and each step is a page of the browser's history, so a link
// reopens the drill-down where it was and Back retraces it.

export interface Drill {
  readonly filter: readonly Predicate[];
  // picked is what the view picked, as the view names it, such as a line's "segment/row".
  readonly picked: string | undefined;
  // toggle adds a predicate to the filter, or takes it off; either drops the pick.
  readonly toggle: (p: Predicate) => void;
  readonly pick: (id: string | undefined) => void;
  // narrow sets the range to [from, to), in epoch ms, and drops the pick.
  readonly narrow: (from: number, to: number) => void;
}

const same = (a: Predicate, b: Predicate) => JSON.stringify(a) === JSON.stringify(b);
const isPredicates = (v: unknown): v is Predicate[] => Array.isArray(v);

const filterOf = (v: string | undefined): Predicate[] => {
  try {
    const parsed: unknown = v ? JSON.parse(v) : [];
    return isPredicates(parsed) ? parsed : [];
  } catch {
    return [];
  }
};

const iso = (ms: number) => new Date(ms).toISOString();

// said is a predicate as the query language writes it.
const opText: Readonly<Record<string, string>> = {
  eq: "=",
  ne: "!=",
  lt: "<",
  le: "<=",
  gt: ">",
  ge: ">=",
};
const said = (p: Predicate) => `${p.field}${opText[p.op ?? "eq"] ?? "="}${p.value ?? ""}`;

// useDrill is the page's drill-down, read from its URL.
export function useDrill(): Drill {
  const params = useParams();
  const filter = filterOf(params["filter"]);
  const written = (f: readonly Predicate[]) => (f.length > 0 ? JSON.stringify(f) : null);
  return {
    filter,
    picked: params["pick"],
    toggle: (p) =>
      stepTo({
        filter: written(
          filter.some((g) => same(p, g)) ? filter.filter((g) => !same(p, g)) : [...filter, p],
        ),
        pick: null,
      }),
    pick: (id) => stepTo({ pick: id ?? null }),
    narrow: (from, to) => stepTo({ time: `${iso(from)},${iso(to)}`, pick: null }),
  };
}

// DrillChips are the drill-down's filter, each taken off by a click.
export function DrillChips({ drill }: { drill: Drill }): ReactElement | null {
  return drill.filter.length === 0
    ? null
    : createElement(
        "div",
        { className: "drill-chips", role: "list", "aria-label": "Filter" },
        drill.filter.map((p) =>
          createElement(
            Chip,
            { key: said(p), title: `Stop filtering on ${said(p)}`, onClick: () => drill.toggle(p) },
            `${said(p)} ×`,
          ),
        ),
      );
}
