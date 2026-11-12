import type { AggregateResponse, DataFocalClient, Predicate, TimeSpec } from "@datafocal/api";
import { resolve } from "@datafocal/shell";
import { BarRow, Panel, Show, useAnswer } from "@datafocal/ui";
import { createElement as h, type ReactElement } from "react";
import { count, iso } from "./parts";

// What changed in an SLO's valid events over the range against as long before it, by the
// fields that tell them apart: for an SLO whose bad slices hold no events to list, as one
// counting a service's lines, which of its sources went quiet or grew.

interface Change {
  readonly value: string;
  readonly now: number;
  readonly before: number;
}

const totals = (r: AggregateResponse) =>
  new Map(r.series.map((s) => [s.key, (s.values ?? []).reduce<number>((n, v) => n + (v ?? 0), 0)]));

// before is the range as long as time ending where it starts.
function before(time: TimeSpec): { from: string; to: string } {
  const now = Date.now();
  const [from, to] = [resolve(time.from ?? "now-1h", now), resolve(time.to ?? "now", now)];
  return { from: iso(from - (to - from)), to: iso(from) };
}

// isTime reports whether a value is a moment, as a reset time is: no source that goes quiet.
const isTime = (v: string) => /^\d{4}-\d\d-\d\dT\d\d:/.test(v);

// changesOf are each value's counts now and before, the largest drop first.
function changesOf(now: Map<string, number>, then: Map<string, number>): Change[] {
  return [...new Set([...now.keys(), ...then.keys()])]
    .filter((v) => v !== "")
    .map((value) => ({ value, now: now.get(value) ?? 0, before: then.get(value) ?? 0 }))
    .sort((a, b) => b.before - b.now - (a.before - a.now))
    .slice(0, 6);
}

// useChanges reads the valid events' telling enum fields and each one's counts now and before.
function useChanges(client: DataFocalClient, time: TimeSpec, valid: readonly Predicate[]) {
  return useAnswer(JSON.stringify(["changed", time, valid]), async () => {
    const was = before(time);
    const selector = { time: { from: was.from, to: time.to ?? "now" }, where: [...valid] };
    const { fields } = (await client.call("rank_fields", { selector })).data;
    const enums = fields
      .filter(
        // One value is enough: a single source going quiet is the change an absence shows.
        (f) => f.type === "enum" && !valid.some((p) => p.field === f.name),
      )
      .slice(0, 4);
    const by = (field: string, t: TimeSpec) =>
      client.call("aggregate", {
        selector: { time: t, where: [...valid] },
        measure: "count",
        group_by: field,
        bins: "all",
      });
    return Promise.all(
      enums.map(async (f) => {
        const [n, b] = await Promise.all([by(f.name, time), by(f.name, was)]);
        const pair: [string, Change[]] = [f.name, changesOf(totals(n.data), totals(b.data))];
        return pair;
      }),
    ).then((fields) => fields.filter(([, rows]) => !rows.every((r) => isTime(r.value))));
  });
}

// Changed is a panel per telling field of the valid events, each value's count over the range
// against as long before it, the largest drop first.
export function Changed(p: {
  client: DataFocalClient;
  time: TimeSpec;
  valid: readonly Predicate[];
}): ReactElement {
  const changes = useChanges(p.client, p.time, p.valid);
  return h(Show<[string, Change[]][]>, {
    answer: changes,
    render: (fields) =>
      fields.length === 0
        ? h(Panel, { title: "What changed" }, h("p", null, "No field tells these events apart."))
        : h(
            "div",
            { className: "g3" },
            fields.map(([field, rows]) => {
              const most = Math.max(1, ...rows.flatMap((r) => [r.now, r.before]));
              return h(
                Panel,
                { key: field, title: `${field} · against as long before` },
                rows.map((r) =>
                  h(BarRow, {
                    key: r.value,
                    label: r.value,
                    share: r.now / most,
                    figure: `${count(r.now)} · ${count(r.before)} before`,
                  }),
                ),
              );
            }),
          ),
  });
}
