import type { FieldStat, DataFocalClient, Selector, Series } from "@datafocal/api";
import { type FocusTarget, navigate, useFocus, useParam } from "@datafocal/shell";
import { Show } from "@datafocal/ui";
import { createElement, type ReactElement, useCallback, useState } from "react";
import { useAsync } from "./lines";
import { compile, withBound, withFilter } from "./search";

export interface FieldRailProps {
  readonly client: DataFocalClient;
  readonly query: string;
  readonly selector: Selector;
  readonly snapshot: string;
  // fields are the query's fields, ranked by signal, while rank_fields answers.
  readonly fields: readonly FieldStat[] | Error | undefined;
}

// Facet is a field's most common values in the query's lines, with their counts.
interface Facet {
  readonly field: string;
  readonly values: readonly { readonly value: string; readonly count: number }[];
  readonly cardinality: number;
}

const maxFacets = 6;
const maxValues = 5;
// maxCardinality is the most values a field can have and still be worth listing: past it,
// the top five are a sliver of the lines.
const maxCardinality = 100;
const always = ["level", "service"];
const skip = new Set(["ts", "timestamp", "message", "pattern_id", ...always]);

// offered is whether a field is worth listing at all: one with more than one value that is
// not the line's time, text or pattern.
const offered = (f: FieldStat) =>
  !skip.has(f.name) && !/^param_\d+$|_pattern$/.test(f.name) && f.cardinality > 1;

// faceted is the fields to list: level and service, then the best ranked numbers and fields
// with few enough values to choose between.
function faceted(fields: readonly FieldStat[]): FieldStat[] {
  const listable = fields.filter(
    (f) =>
      offered(f) &&
      (f.cardinality <= maxCardinality || f.type === "num") &&
      // An id, which nearly every line has its own value of, says nothing they share.
      f.type !== "id",
  );
  const first = always.map(
    (name): FieldStat =>
      fields.find((f) => f.name === name) ?? {
        name,
        type: "",
        cardinality: 0,
        present: 1,
        lines: 0,
        signal: 0,
        high_signal: false,
      },
  );
  return [...first, ...listable].slice(0, maxFacets);
}

// count is how many lines a facet value covers, short.
function count(n: number): string {
  return n >= 1e6
    ? `${(n / 1e6).toFixed(1)}M`
    : n >= 1e4
      ? `${Math.round(n / 1e3)}k`
      : n >= 1e3
        ? `${(n / 1e3).toFixed(1)}k`
        : String(n);
}

// FacetHeading is a field's name, which stacks the volume by its values, and how many it has.
function FacetHeading({ facet }: { facet: Facet }): ReactElement {
  const [, setBy] = useParam("by");
  return createElement(
    "h3",
    null,
    createElement(
      "button",
      {
        type: "button",
        title: `Stack the volume by ${facet.field}`,
        onClick: () => setBy(facet.field === "level" ? null : facet.field),
      },
      facet.field,
    ),
    createElement("span", null, facet.cardinality > 0 ? facet.cardinality : ""),
  );
}

// FacetList is one field's values, each a bar as long as its share of the top value, and
// a click away from focusing on the query's lines with it.
function FacetList({ facet, rail }: { facet: Facet; rail: FieldRailProps }): ReactElement {
  const focus = useFocus();
  const { query } = rail;
  const on = (value: string): FocusTarget => ({
    title: `${facet.field}=${value}`,
    query: withFilter(compile(query), facet.field, value),
    selector: rail.selector,
    slice: [{ field: facet.field, op: "eq", value }],
    snapshot: rail.snapshot,
  });
  const top = Math.max(1, ...facet.values.map((v) => v.count));
  return createElement(
    "div",
    { className: "fac" },
    createElement(FacetHeading, { facet }),
    facet.values.map(({ value, count: n }) =>
      createElement(
        "div",
        { key: value, className: "fac-v" },
        createElement(
          "button",
          {
            type: "button",
            title: `Focus on lines with ${facet.field}=${JSON.stringify(value)}`,
            onClick: () => focus(on(value)),
          },
          createElement("i", {
            "aria-hidden": true,
            ref: (el: HTMLElement | null) => el?.style.setProperty("width", `${(n / top) * 100}%`),
          }),
          createElement("span", null, value === "" ? "(none)" : value),
          createElement("span", null, count(n)),
        ),
        createElement(
          "button",
          {
            type: "button",
            className: "fac-x",
            title: `Lines without ${facet.field}=${JSON.stringify(value)}`,
            "aria-label": `Exclude ${facet.field} ${value}`,
            onClick: () =>
              navigate({
                view: "logs",
                query: withFilter(query, facet.field, value, true),
                mode: "lines",
              }),
          },
          "−",
        ),
      ),
    ),
  );
}

// FacetCard loads a field's top values in the query's lines and lists them, or nothing when
// the lines hold none.
function FacetCard(p: { name: string; cardinality: number; rail: FieldRailProps }): ReactElement {
  const { name, cardinality, rail } = p;
  const { client, selector, snapshot } = rail;
  const load = useCallback(
    async (signal: AbortSignal): Promise<Facet> => {
      const req = {
        selector,
        measure: "count",
        group_by: name,
        bins: "all",
        limit: maxValues,
        snapshot,
      };
      const { series } = (await client.call("aggregate", req, { signal })).data;
      const values = series.map((s) => ({ value: s.key, count: s.values?.[0] ?? 0 }));
      return { field: name, values, cardinality };
    },
    [client, selector, snapshot, name, cardinality],
  );
  return createElement(Show<Facet>, {
    answer: useAsync(load),
    render: (f) => f.values.length > 0 && createElement(FacetList, { facet: f, rail }),
  });
}

// spreads are the measures a number's facet shows.
const spreads = ["min", "p50", "p95", "max"];

// Bound adds field:<op>n to the query on Enter.
function Bound(p: { field: string; op: ">=" | "<="; query: string }): ReactElement {
  return createElement(
    "form",
    {
      onSubmit: (e: { preventDefault: () => void; currentTarget: HTMLFormElement }) => {
        e.preventDefault();
        const n = e.currentTarget.querySelector("input")?.valueAsNumber ?? Number.NaN;
        if (!Number.isNaN(n)) {
          navigate({ view: "logs", query: withBound(p.query, p.field, p.op, n), mode: "lines" });
        }
      },
    },
    createElement(
      "label",
      null,
      p.op === ">=" ? "≥" : "≤",
      createElement("input", {
        type: "number",
        step: "any",
        placeholder: p.op === ">=" ? "min" : "max",
        "aria-label": `${p.field} ${p.op}`,
      }),
    ),
  );
}

// SpreadTable is a number's spread for each service, a row each under one heading.
function SpreadTable({ series }: { series: readonly Series[] }): ReactElement {
  const cell = (s: Series, i: number) => {
    const v = s.measure_values?.[i]?.[0];
    return typeof v === "number" ? count(v) : "–";
  };
  return createElement(
    "table",
    { className: "fac-n" },
    createElement(
      "thead",
      null,
      createElement(
        "tr",
        null,
        createElement("th", null, "service"),
        spreads.map((m) => createElement("th", { key: m }, m)),
      ),
    ),
    createElement(
      "tbody",
      null,
      series.map((s) =>
        createElement(
          "tr",
          { key: s.key },
          createElement("th", { title: s.key }, s.key || "(none)"),
          spreads.map((m, i) => createElement("td", { key: m }, cell(s, i))),
        ),
      ),
    ),
  );
}

// NumberCard is a number's spread in the query's lines, by service since quantiles do not add
// across groups, from one aggregate, and inputs that bound it.
function NumberCard({ name, rail }: { name: string; rail: FieldRailProps }): ReactElement {
  const { client, selector, snapshot } = rail;
  const load = useCallback(
    async (signal: AbortSignal) => {
      const measures = spreads.map((m) => ({ measure: `${m}:${name}` }));
      const req = { selector, measure: "", measures, group_by: "service", bins: "all", limit: 5 };
      return (await client.call("aggregate", { ...req, snapshot }, { signal })).data.series;
    },
    [client, selector, snapshot, name],
  );
  return createElement(
    "div",
    { className: "fac" },
    createElement(FacetHeading, { facet: { field: name, values: [], cardinality: 0 } }),
    createElement(Show<Series[]>, {
      answer: useAsync(load),
      render: (series) => createElement(SpreadTable, { series }),
    }),
    createElement(
      "div",
      { className: "fac-b" },
      createElement(Bound, { field: name, op: ">=", query: rail.query }),
      createElement(Bound, { field: name, op: "<=", query: rail.query }),
    ),
  );
}

// MoreFields names the fields past the listed ones, each a click from listing its values.
function MoreFields(p: {
  fields: readonly FieldStat[];
  onAdd: (name: string) => void;
}): ReactElement | null {
  if (p.fields.length === 0) {
    return null;
  }
  return createElement(
    "details",
    { className: "fac-more" },
    createElement(
      "summary",
      null,
      `${p.fields.length} more field${p.fields.length === 1 ? "" : "s"}`,
    ),
    p.fields.map((f) =>
      createElement(
        "button",
        {
          key: f.name,
          type: "button",
          title: `List ${f.name}'s values`,
          onClick: () => p.onAdd(f.name),
        },
        createElement("span", null, f.name),
        createElement("span", null, f.cardinality),
      ),
    ),
  );
}

// FieldRail lists the fields that tell the query's lines apart, each with its top values,
// and names the rest to add. A field added stays listed for this query only.
export function FieldRail(p: FieldRailProps): ReactElement {
  const [state, setState] = useState<{ query: string; names: readonly string[] }>();
  const added = state?.query === p.query ? state.names : [];
  // ranked is the fields once rank_fields answers them, as Array.isArray reads a readonly
  // array as any[].
  const ranked = p.fields instanceof Error ? undefined : p.fields;
  const fields = ranked ?? [];
  const shown = [...faceted(fields), ...fields.filter((f) => added.includes(f.name))];
  const more = fields.filter((f) => offered(f) && !shown.some((s) => s.name === f.name));
  return createElement(
    "aside",
    { className: "frail", "aria-label": "Fields" },
    createElement("h2", null, "Fields"),
    ranked === undefined && createElement(Show, { answer: p.fields, render: () => null }),
    ranked !== undefined &&
      shown.map((f) =>
        f.type === "num"
          ? createElement(NumberCard, { key: f.name, name: f.name, rail: p })
          : createElement(FacetCard, {
              key: f.name,
              name: f.name,
              cardinality: f.cardinality,
              rail: p,
            }),
      ),
    createElement(MoreFields, {
      fields: more,
      onAdd: (name) => setState({ query: p.query, names: [...added, name] }),
    }),
  );
}
