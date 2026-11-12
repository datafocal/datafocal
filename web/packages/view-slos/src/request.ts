import type { DataFocalClient, LogLine, Predicate, TimeSpec } from "@datafocal/api";
import { around, Pivots } from "@datafocal/shell";
import {
  duration,
  Inspector,
  type Kpi,
  Kpis,
  lineText,
  Meter,
  Show,
  useAnswer,
} from "@datafocal/ui";
import { createElement as h, type ReactElement } from "react";
import {
  heading,
  idField,
  isDuration,
  iso,
  measured,
  numbers,
  placeOf,
  shown,
  valueOf,
} from "./parts";
import { type Mark, Resources } from "./resources";

// A bad event in the inspector: what it read, its request's steps with what each read, and
// the pod that served it while it ran, each part built of the fields the lines carry.

// spanOf is when the event ran: a reported duration ends at its time, else it is an instant.
function spanOf(l: LogLine): Mark {
  const to = Date.parse(l.timestamp);
  const took = numbers(l).find(([k]) => isDuration(k))?.[1] ?? 0;
  return { from: to - took, to };
}

// read are the event's numeric fields but its duration, as tiles in their units.
const read = (l: LogLine): Kpi[] =>
  numbers(l)
    .filter(([k]) => !isDuration(k))
    .slice(0, 8)
    .map(([k, v]) => ({ label: heading(k), value: measured(k, v) }));

// startOf is when a line's work began: its time less the duration it reports.
const startOf = (l: LogLine) =>
  Date.parse(l.timestamp) - (numbers(l).find(([k]) => isDuration(k))?.[1] ?? 0);

// shared are the numeric fields most of the lines carry, but durations, the commonest first.
const shared = (lines: readonly LogLine[]) => {
  const seen = new Map<string, number>();
  for (const [k] of lines.flatMap(numbers)) {
    seen.set(k, (seen.get(k) ?? 0) + 1);
  }
  return [...seen]
    .filter(([k]) => !isDuration(k))
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, 2)
    .map(([k]) => k);
};

// Steps are the request's lines in the order their work began: each one's message, its start
// from the request's, how long it took, and the numbers most of them carry.
function Steps({ lines }: { lines: readonly LogLine[] }): ReactElement {
  const sorted = [...lines].sort((a, b) => startOf(a) - startOf(b));
  const t0 = Math.min(...sorted.map(startOf));
  const took = (l: LogLine) => numbers(l).find(([k]) => isDuration(k));
  const longest = Math.max(1, ...sorted.map((l) => took(l)?.[1] ?? 0));
  const fields = shared(sorted);
  return h(
    "table",
    { "aria-label": "The request's steps" },
    h(
      "thead",
      null,
      h(
        "tr",
        null,
        ["Step", "Start", "Took", ...fields.map(heading)].map((n) => h("th", { key: n }, n)),
      ),
    ),
    h(
      "tbody",
      null,
      sorted.map((l) => {
        const t = took(l);
        return h(
          "tr",
          { key: `${l.segment ?? ""}/${l.row}` },
          h("td", null, lineText(l)),
          h("td", null, `+${duration(startOf(l) - t0)}`),
          h("td", null, t ? h(Meter, { text: measured(t[0], t[1]), share: t[1] / longest }) : ""),
          ...fields.map((k) => h("td", { key: k }, shown(k, valueOf(l, k)) || "")),
        );
      }),
    ),
  );
}

// useSteps reads every line carrying the event's request id, around its time.
function useSteps(client: DataFocalClient, l: LogLine, field: string | undefined) {
  const id = field ? valueOf(l, field) : "";
  return useAnswer(`steps ${field}=${id}`, async () => {
    if (!field) {
      return [l];
    }
    const m = spanOf(l);
    const time = { from: iso(m.from - 60_000), to: iso(m.to + 60_000) };
    const where: Predicate[] = [{ field, op: "eq", value: id }];
    return (
      (await client.call("search_logs", { selector: { time, where }, limit: 200 })).data.lines ?? []
    );
  });
}

const tbtn = (label: string, onClick: () => void) =>
  h("button", { type: "button", className: "tbtn", onClick }, label);

// RequestInspector is a bad event: its numbers, its request's steps, and the pod that served it.
export function RequestInspector(p: {
  client: DataFocalClient;
  l: LogLine;
  title: string;
  close: () => void;
}): ReactElement {
  const field = idField(p.l);
  const id = field ? valueOf(p.l, field) : "";
  const steps = useSteps(p.client, p.l, field);
  const mark = spanOf(p.l);
  const place = placeOf(p.l);
  const nearby: TimeSpec = around(mark.to);
  const where = Object.entries(place).map(
    ([k, v]): Predicate => ({ field: k, op: "eq", value: v ?? "" }),
  );
  const actions = h(
    "div",
    { className: "slo-acts" },
    h(Pivots, {
      from: "slos",
      subject: {
        service: p.l.service,
        ...place,
        id: field ? { field, value: id } : undefined,
        line: { segment: p.l.segment ?? "", row: p.l.row },
        time: around(mark.to),
      },
    }),
    tbtn("Close", p.close),
  );
  const body = () => [
    h(Kpis, { key: "r", tiles: read(p.l) }),
    h("h3", { key: "sh" }, "Steps"),
    h(Show<readonly LogLine[]>, {
      key: "s",
      answer: steps,
      render: (ls) => h(Steps, { lines: ls }),
    }),
    where.length > 0 &&
      h(Resources, {
        key: "u",
        client: p.client,
        time: nearby,
        where,
        service: p.l.service,
        pod: place.pod,
        mark,
      }),
  ];
  return h(Inspector, {
    title: p.title,
    actions,
    tabs: [{ id: "request", label: "Request", render: body }],
  });
}
