import {
  type AggregateRequest,
  type Anomaly,
  binStarts,
  type RCAHop,
  type RCARejection,
  type RejectReason,
  type Series,
  type TimeSpec,
} from "@datafocal/api";
import { navigate } from "@datafocal/shell";
import { Annotation, Chart } from "@datafocal/ui";
import { createElement, type ReactElement, useEffect, useMemo, useState } from "react";

// onset is when hop's cause began: the incident's time plus the hop's lag, negative when
// the cause leads.
export const onset = (incident: Anomaly, h: RCAHop): number =>
  Date.parse(incident.at) + h.lag_seconds * 1e3;

// Replay sweeps the lanes in replayMs, a step at a time.
const [replayMs, replaySteps] = [5000, 100];

const hopLogs = (h: RCAHop, n: number) =>
  navigate({
    view: "logs",
    query: `{service=${JSON.stringify(h.to)}} | pattern_id=${JSON.stringify(h.pattern_to)}`,
    mode: "lines",
    title: `Hop ${n} · ${h.to}`,
  });

const hopTrace = (h: RCAHop, n: number) =>
  navigate({
    view: "trace",
    where: `pattern_id=${JSON.stringify(h.pattern_to)}`,
    title: `Hop ${n} · ${h.to}`,
  });

// lanesRequest asks for each hop's cause pattern counted over time.
export function lanesRequest(hops: readonly RCAHop[], time: TimeSpec): AggregateRequest {
  const ids = hops.map((h) => h.pattern_to).join("|");
  const where = [{ field: "pattern_id", op: "regex", value: `^(${ids})$` }];
  return { selector: { time, where }, measure: "count", group_by: "pattern_id", bins: "auto" };
}

// align puts every series on the bins they share, a missing bin counting 0, so the lanes
// share their time axis.
function align(series: readonly Series[]) {
  const xs = series[0] ? binStarts(series[0]) : [];
  const ys = new Map(series.map((s) => [s.key, xs.map((_, bin) => s.values?.[bin] ?? 0)]));
  return { xs, ys };
}

const button = (label: string, onClick: () => void) =>
  createElement("button", { type: "button", key: label, className: "tbtn", onClick }, label);

interface LanesProps {
  readonly hops: readonly RCAHop[];
  readonly lanes: readonly Series[];
  readonly playhead: number | undefined;
  readonly onPlayhead: (t: number | undefined) => void;
}

// Lanes are each hop's lines over time on a shared axis, which replay sweeps a playhead
// across.
function Lanes({ hops, lanes, playhead, onPlayhead }: LanesProps): ReactElement {
  const { xs, ys } = useMemo(() => align(lanes), [lanes]);
  const [t0 = 0, t1 = 0] = [xs[0], xs.at(-1)];
  const [replaying, setReplaying] = useState(false);
  const [el, setEl] = useState<HTMLDivElement | null>(null);
  useEffect(() => {
    if (!replaying) {
      return;
    }
    let f = 0;
    const tick = setInterval(() => {
      f += 1 / replaySteps;
      onPlayhead(f < 1 ? t0 + f * (t1 - t0) : undefined);
      setReplaying(f < 1);
    }, replayMs / replaySteps);
    return () => clearInterval(tick);
  }, [replaying, t0, t1, onPlayhead]);
  return createElement(
    "div",
    { ref: setEl, className: "g-lanes" },
    xs.length > 0 && button("Replay", () => setReplaying(true)),
    el &&
      hops.map((h, i) =>
        createElement(Chart, {
          key: `${i}`,
          kind: "sparkline",
          data: [xs, ys.get(h.pattern_to) ?? xs.map(() => 0)],
          width: el.clientWidth,
          height: 24,
          label: `Hop ${i + 1} · ${h.to} lines over time`,
        }),
      ),
    playhead !== undefined &&
      createElement("div", {
        className: "g-playhead",
        style: { left: `${((playhead - t0) / (t1 - t0)) * 100}%` },
      }),
  );
}

// because says why n candidates were rejected, by reason.
const because: Readonly<Record<RejectReason, (n: number) => string>> = {
  below_rho: (n) => `${n} moved with it too weakly to count.`,
  same_node: (n) => `${n} are on its own service, so they restate the symptom.`,
  no_attribution: (n) =>
    `${n} moved with it but share no trace, correlation value or host with it.`,
  entity_indeterminate: (n) =>
    `${n} touch a shared resource, but whose load moved could not be told.`,
};

// noChain says that the walk from incident found no chain, and why.
function noChain(incident: Anomaly, rejected: readonly RCARejection[]): string {
  const why = Object.entries(because).flatMap(([reason, say]) => {
    const n = rejected.filter((r) => r.rejected_by === reason).length;
    return n > 0 ? [say(n)] : [];
  });
  return [
    `No causal chain found for ${incident.service}'s ${incident.kind} at ${incident.at}.`,
    ...(why.length > 0 ? why : ["Nothing moved with it."]),
  ].join(" ");
}

export interface InvestigationProps extends LanesProps {
  readonly incident: Anomaly;
  readonly rejected: readonly RCARejection[];
}

// Investigation is the root-cause chain under the graph: the fix sentence, then each hop
// with its onset and drill-downs, then the lanes. With no chain it says why.
export function Investigation(p: InvestigationProps): ReactElement {
  const { incident, hops } = p;
  const first = hops[0];
  if (!first) {
    return createElement(
      "section",
      { className: "g-inv", "aria-label": "Root cause" },
      createElement("p", { role: "status" }, noChain(incident, p.rejected)),
    );
  }
  return createElement(
    "section",
    { className: "g-inv", "aria-label": "Root cause" },
    createElement(Annotation, {
      parts: [
        "Start with ",
        { text: first.to, onClick: () => hopLogs(first, 1) },
        `: it moves with ${incident.service}'s ${incident.kind} (ρ ${first.rho.toFixed(2)}, lag ${first.lag_seconds} s), admitted by ${first.admitted_by}.`,
      ],
      source: "rca_walk",
    }),
    createElement(
      "ol",
      { "aria-label": "Root-cause hops" },
      hops.map((h, i) =>
        createElement(
          "li",
          { key: `${i}` },
          createElement(
            "span",
            null,
            `${h.to} · ${new Date(onset(incident, h)).toISOString()} · lag ${h.lag_seconds} s · ${h.admitted_by}`,
          ),
          button("Logs", () => hopLogs(h, i + 1)),
          button("Trace", () => hopTrace(h, i + 1)),
        ),
      ),
    ),
    createElement(Lanes, p),
  );
}
