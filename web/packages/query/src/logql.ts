import type { Predicate } from "@datafocal/api";

// logql is the Logs query for exactly the lines where selects: its equalities the stream
// selector, each other predicate a filter after it, and a group's alternatives or'd.

const opText: Readonly<Record<string, string>> = {
  eq: "=",
  ne: "!=",
  lt: "<",
  le: "<=",
  gt: ">",
  ge: ">=",
  match: "|=",
  nomatch: "!~",
  regex: "=~",
  noregex: "!~",
};

const term = (p: Predicate) => {
  const op = opText[p.op ?? "eq"] ?? "=";
  const v = p.value ?? "";
  // A comparison of a number reads as one; every other value is quoted.
  return `${p.field}${op}${/^[<>]/.test(op) && /^-?[\d.]+$/.test(v) ? v : JSON.stringify(v)}`;
};

const isEquality = (p: Predicate) => !p.any && (p.op ?? "eq") === "eq";

export function logql(where: readonly Predicate[]): string {
  const stream = where.filter(isEquality).map(term);
  const filters = where
    .filter((p) => !isEquality(p))
    .map((p) => ` | ${(p.any ?? [p]).map(term).join(" or ")}`);
  return `{${stream.join(", ")}}${filters.join("")}`;
}
