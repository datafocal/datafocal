import type { DataFocalClient, Predicate, SLOStatusResponse, TimeSpec } from "@datafocal/api";
import { at } from "./doc";
import { same } from "./parts";

// What an SLO's page reads from its definition, never guessed: its
// valid events, its bad ones, what makes one bad, and for a time-slice SLO how a slice is
// measured and judged.

type Detail = SLOStatusResponse["slo"];

// Slices are how a time-slice SLO measures each slice and the objective that judges it: a
// slice is good when its value op value holds. counted marks a count, whose slice is zero, not
// missing, when nothing matched, so long as anything at all was collected then.
export interface Slices {
  readonly measure: string;
  readonly bins: string;
  readonly where: readonly Predicate[];
  readonly op: string;
  readonly value: number;
  readonly counted: boolean;
  // resetsAt names the field holding when the metric's window resets, as a quota's does, from
  // the definition's datafocal/resets-at annotation.
  readonly resetsAt: string | undefined;
}

export interface Scopes {
  readonly valid: readonly Predicate[];
  readonly bad: readonly Predicate[];
  readonly badness: readonly Predicate[];
  readonly slices: Slices | undefined;
  // distinctBy names the field an event is one of, from the definition's datafocal/distinct-by,
  // so lines repeating one event show it once.
  readonly distinctBy: string | undefined;
}

const spec = ["spec", "indicator", "spec"];
const metric = ["metricSource", "spec", "query"];
const text = (v: unknown) => (typeof v === "string" ? v : undefined);
const isPredicates = (v: unknown): v is Predicate[] => Array.isArray(v);

// queryOf is the definition's valid query: an event ratio's total, a time-slice SLO's metric.
const queryOf = (doc: unknown) =>
  text(at(doc, [...spec, "ratioMetric", "total", ...metric])) ??
  text(at(doc, [...spec, "thresholdMetric", ...metric]));

// slicesOf is a time-slice SLO's measure and bins, as the query tool compiled its metric, and
// its first objective's comparison.
function slicesOf(doc: unknown, args: unknown, where: readonly Predicate[]): Slices | undefined {
  const measure = text(at(args, ["measure"]));
  const bins = text(at(args, ["bins"]));
  const objective = at(doc, ["spec", "objectives", 0]);
  const op = text(at(objective, ["op"]));
  const value = Number(at(objective, ["value"]));
  const timeslices = at(doc, ["spec", "budgetingMethod"]) === "Timeslices";
  if (!timeslices || !measure || !bins || !op || !Number.isFinite(value)) {
    return undefined;
  }
  const counted = ["count", "rate", "sum"].includes(measure.split(":")[0] ?? "");
  const resetsAt = text(at(doc, ["metadata", "annotations", "datafocal/resets-at"]));
  return { measure, bins, where, op, value, counted, resetsAt };
}

// scopesOf compiles the definition's query with the query tool, which returns the selector and
// measure it compiled, and takes the bad events from slo_status. A query that compiles to no
// selector fails, rather than scope the page to every service.
export async function scopesOf(
  client: DataFocalClient,
  doc: unknown,
  d: Detail,
  time: TimeSpec,
): Promise<Scopes> {
  const q = queryOf(doc);
  // Only the call the query compiles to is read, so a log query returns one line of its result.
  const args = q ? (await client.call("query", { q, time, limit: 1 })).data.call.args : undefined;
  const where = at(args, ["selector", "where"]);
  if (!isPredicates(where) || where.length === 0) {
    throw new Error(`${d.name}'s valid query compiles to no selector`);
  }
  const bad = d.bad_events ?? [];
  return {
    valid: where,
    bad,
    badness: bad.filter((p) => !where.some((v) => same(v, p))),
    slices: slicesOf(doc, args, where),
    distinctBy: text(at(doc, ["metadata", "annotations", "datafocal/distinct-by"])),
  };
}

// meets reports whether a slice's value meets the objective's comparison.
export function meets(op: string, v: number, target: number): boolean {
  switch (op) {
    case "lt":
      return v < target;
    case "lte":
    case "le":
      return v <= target;
    case "gt":
      return v > target;
    case "gte":
    case "ge":
      return v >= target;
    default:
      return v === target;
  }
}
