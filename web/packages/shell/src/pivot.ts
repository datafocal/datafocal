import type { LineRef, LogLine, Predicate } from "@datafocal/api";
import { logql } from "@datafocal/query";
import { createElement, type ReactElement } from "react";
import { type Destination, navigate, type TimeRange, type View } from "./url";

// A subject is what a view has picked, a line, a span, a pod, a service, a slice of time,
// told in the terms every view reads; pivots are the same links from it into every other view,
// so going from resources to logs reads the same as going from a trace or the graph to them.
// Each is a navigation, a page of the browser's history.

export interface Subject {
  // time is the span the subject covers, which every pivot opens on.
  readonly time?: TimeRange | undefined;
  readonly service?: string | undefined;
  readonly pod?: string | undefined;
  readonly host?: string | undefined;
  // id is the request or trace the subject belongs to, by the field that names it.
  readonly id?: { readonly field: string; readonly value: string } | undefined;
  // where selects the subject's own lines, where the view knows them better than its place.
  readonly where?: readonly Predicate[] | undefined;
  // line is the one line the subject is, which Logs opens on.
  readonly line?: LineRef | undefined;
  readonly title?: string | undefined;
}

// A field names a unit of work, whose value gathers the lines of one trace, when its name
// ends in an id word or names a unit; a parent's id, or a line's own, links within a trace.
const idName = /(^|[._-])id$|[a-z0-9]Id$/;
const unitName =
  /^(trace|session|transaction|correlation|conversation|task|job|run|request|prompt)(_?id|Id)?$/i;
const leaf = (k: string) => k.slice(k.lastIndexOf(".") + 1);

// traceable reports whether the line's field k, valued v, gathers a trace.
export function traceable(k: string, v: string): boolean {
  const l = leaf(k);
  return (
    v !== "" &&
    !v.startsWith("[") &&
    k !== "pattern_id" &&
    !/parent|^uuid$|^span_?id$|^id$/i.test(l) &&
    (idName.test(l) || unitName.test(l))
  );
}

// traceOf is the field the line's trace is best gathered by: trace_id, else a unit of
// work's field, else any id field; undefined when it has none.
export function traceOf(l: LogLine): [string, string] | undefined {
  const fields = Object.entries(l.attributes ?? {}).filter(([k, v]) => traceable(k, v));
  const rank = ([k]: [string, string]) => (k === "trace_id" ? 0 : unitName.test(leaf(k)) ? 1 : 2);
  return fields.sort((a, b) => rank(a) - rank(b))[0];
}

// around is the ten minutes centred on at, epoch ms: the span a pivot from one moment, as a
// line's, opens on.
export const around = (at: number, half: number = 5 * 60_000): TimeRange => ({
  from: new Date(at - half).toISOString(),
  to: new Date(at + half).toISOString(),
});

export interface Pivot {
  readonly view: View;
  readonly label: string;
  readonly to: Destination;
}

const eq = (field: string, value: string): Predicate => ({ field, op: "eq", value });

// linesOf are the predicates selecting the subject's lines: its own, else its request's, else
// its place's.
function linesOf(s: Subject): readonly Predicate[] {
  if (s.where && s.where.length > 0) {
    return s.where;
  }
  if (s.id) {
    return [eq(s.id.field, s.id.value)];
  }
  const place = [
    s.service && eq("service", s.service),
    s.pod && eq("pod", s.pod),
    !s.pod && s.host && eq("host", s.host),
  ];
  return place.filter((p): p is Predicate => Boolean(p));
}

const span = (s: Subject) => (s.time ? { time: `${s.time.from},${s.time.to}` } : {});

// The pivot into each view, where the subject says enough for it to show.
const toLogs = (s: Subject): Pivot | undefined => {
  const lines = linesOf(s);
  if (lines.length === 0) {
    return undefined;
  }
  // A request's lines are in every service, so its query names none.
  const query =
    s.id && !s.where
      ? `{service=~".+"} | ${s.id.field}=${JSON.stringify(s.id.value)}`
      : logql(lines);
  const to: Destination = {
    view: "logs",
    query,
    ...span(s),
    ...(s.line && { focus: s.line }),
    ...(s.title && { title: s.title }),
  };
  return { view: "logs", label: "Logs", to };
};

const toTrace = (s: Subject): Pivot | undefined =>
  s.id && {
    view: "trace",
    label: "Trace",
    to: { view: "trace", field: s.id.field, traceId: s.id.value },
  };

const toGraph = (s: Subject): Pivot | undefined =>
  s.service
    ? { view: "graph", label: "Graph", to: { view: "graph", focus: s.service } }
    : undefined;

const toResources = (s: Subject): Pivot | undefined =>
  s.service || s.pod
    ? {
        view: "resources",
        label: "Resources",
        to: {
          view: "resources",
          ...(s.service && { service: s.service }),
          ...(s.pod && { pod: s.pod }),
          ...span(s),
        },
      }
    : undefined;

// pivotsOf are the links from s into each view that can show it.
export const pivotsOf = (s: Subject): Pivot[] =>
  [toLogs(s), toTrace(s), toGraph(s), toResources(s)].filter((x): x is Pivot => x !== undefined);

// Pivots are the links from a subject into every view but the one it is in and those omit
// names, which would show nothing of it, as toolbar buttons.
export function Pivots(p: { subject: Subject; from?: View; omit?: readonly View[] }): ReactElement {
  return createElement(
    "div",
    { className: "pivots", role: "group", "aria-label": "Open in" },
    pivotsOf(p.subject)
      .filter((x) => x.view !== p.from && !p.omit?.includes(x.view))
      .map((x) =>
        createElement(
          "button",
          { key: x.view, type: "button", className: "tbtn", onClick: () => navigate(x.to) },
          x.label,
        ),
      ),
  );
}
