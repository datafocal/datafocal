import type { LogLine, Predicate } from "@datafocal/api";
import { traceOf } from "@datafocal/shell";
import { duration } from "@datafocal/ui";

// The small pieces the drill-down's parts share: how a value reads, and a line's fields.

export const same = (a: Predicate, b: Predicate): boolean =>
  JSON.stringify(a) === JSON.stringify(b);

export const rate = (v: number | null | undefined): string =>
  v == null ? "—" : `${v.toFixed(1)}×`;

export const count = (n: number): string =>
  n >= 1e6
    ? `${(n / 1e6).toFixed(1)}M`
    : n >= 1e4
      ? `${(n / 1e3).toFixed(1)}K`
      : String(Math.round(n));

const bytes = (n: number) => {
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(units.length - 1, Math.max(0, Math.floor(Math.log10(Math.max(n, 1)) / 3)));
  return `${(n / 1000 ** i).toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
};

// isDuration and isBytes read a field's unit from its name, as its producer spelled it.
export const isDuration = (field: string): boolean => /(_ms|duration)$/.test(field);
const isBytes = (field: string) => /bytes/.test(field);

// measured is a number of field as it reads in the field's unit.
// plain is a number as logged when whole, as an id or a count is, else to four significant
// digits, as 2.31 for 2.3096520000000003.
const plain = (n: number) => (Number.isInteger(n) ? String(n) : String(Number(n.toPrecision(4))));

export const measured = (field: string, n: number): string =>
  isDuration(field) ? duration(n) : isBytes(field) ? bytes(n) : plain(n);

// shown is a field's value as a row shows it: a number in its unit, else as logged.
export const shown = (field: string, v: string): string =>
  v !== "" && Number.isFinite(Number(v)) ? measured(field, Number(v)) : v;

// heading is a column's name as its heading: a duration field's without its unit, since its
// values are shown in theirs.
export const heading = (field: string): string => field.replace(/_ms$/, "");

export const valueOf = (l: LogLine, k: string): string =>
  k === "service" ? (l.service ?? "") : (l.attributes?.[k] ?? "");

// numbers are a line's numeric fields, by name.
export const numbers = (l: LogLine): [string, number][] =>
  Object.entries(l.attributes ?? {})
    .filter(([, v]) => v !== "" && Number.isFinite(Number(v)))
    .map(([k, v]) => [k, Number(v)]);

export const clock = (at: string | number): string =>
  new Date(typeof at === "number" ? at : Date.parse(at)).toLocaleTimeString([], { hour12: false });

export const iso = (ms: number): string => new Date(ms).toISOString();

// idField is the field a line's request or trace is gathered by, as every view reads it.
export const idField = (l: LogLine): string | undefined => traceOf(l)?.[0];

// placeOf is where a line ran, the finest unit the resource view reads: its pod, else its host.
export const placeOf = (l: LogLine): { pod?: string; host?: string } => {
  const pod = valueOf(l, "pod");
  const host = valueOf(l, "host");
  return pod ? { pod } : host ? { host } : {};
};
