import type { DataFocalClient, Predicate, ResourceUsageResponse, TimeSpec } from "@datafocal/api";
import { navigate, pivotsOf } from "@datafocal/shell";
import { Chart, Kpis, Panel, Show, useAnswer } from "@datafocal/ui";
import { createElement as h, type ReactElement } from "react";
import { measured } from "./parts";

// The CPU and memory of the pods that served an SLO's events, from the cluster's own metrics,
// leading to the resource view on the same pods and range.

type Use = ResourceUsageResponse["groups"][number];
type Series = "cpu_cores" | "memory_bytes";

// Mark is a span within the range, as a request's, whose use is set against the time before it.
export interface Mark {
  readonly from: number;
  readonly to: number;
}

const startsOf = (r: ResourceUsageResponse, n: number) =>
  Array.from({ length: n }, (_, i) => r.start + i * r.step);

// over is the summed use of every group in the bins that start in [from, to).
function over(r: ResourceUsageResponse, key: Series, from: number, to: number) {
  const xs = startsOf(r, r.groups[0]?.[key]?.length ?? 0);
  const at = xs.flatMap((x, i) => (x + r.step > from && x < to ? [i] : []));
  const sums = at.map((i) => r.groups.reduce((n, g) => n + (g[key]?.[i] ?? 0), 0));
  return {
    mean: sums.length ? sums.reduce((a, b) => a + b, 0) / sums.length : null,
    peak: sums.length ? Math.max(...sums) : null,
  };
}

// cores are a CPU use in cores, a small one in millicores.
const cores = (v: number | null) =>
  v == null ? "—" : v < 1 ? `${Math.round(v * 1000)}m cores` : `${v.toFixed(2)} cores`;
const mem = (v: number | null) => (v == null ? "—" : measured("bytes", v));

// marked are the mark's use against the as long before it.
const marked = (r: ResourceUsageResponse, m: Mark) => {
  const span = Math.max(m.to - m.from, r.step);
  const [during, before] = [
    over(r, "cpu_cores", m.from, m.from + span),
    over(r, "cpu_cores", m.from - 5 * span, m.from),
  ];
  const memory = over(r, "memory_bytes", m.from, m.from + span);
  return [
    {
      label: "CPU while it ran",
      value: cores(during.mean),
      detail: `${cores(before.mean)} before`,
    },
    { label: "Memory while it ran", value: mem(memory.peak), detail: "peak" },
  ];
};

const chart = (
  r: ResourceUsageResponse,
  key: Series,
  label: string,
  format: (v: number) => string,
) => {
  const groups = r.groups.slice(0, 6);
  const xs = startsOf(r, groups[0]?.[key]?.length ?? 0);
  return h(
    "div",
    { key, className: "slo-chart" },
    h("h4", null, label),
    h(Chart, {
      kind: "line",
      data: [xs, ...groups.map((g: Use) => (g[key] ?? []).map((v) => v ?? null))],
      width: 360,
      height: 140,
      label,
      x: { time: true },
      y: { format },
      series: groups.map((g) => g.key),
    }),
  );
};

// Resources are the CPU and memory of the pods where picks, over time, and with a mark its use
// against the time before it, opening the resource view on them.
export function Resources(p: {
  client: DataFocalClient;
  time: TimeSpec;
  where: readonly Predicate[];
  service: string | undefined;
  pod?: string | undefined;
  mark?: Mark | undefined;
}): ReactElement | null {
  const use = useAnswer(JSON.stringify(["resources", p.time, p.where]), async () => {
    const selector = { time: p.time, where: [...p.where] };
    return (await p.client.call("resource_usage", { selector, group_by: "pod", bins: "auto" }))
      .data;
  });
  // The resource view on the same pods and range, as every view's Resources link opens it.
  const to = pivotsOf({
    service: p.service,
    pod: p.pod,
    time: { from: p.time.from ?? "now-1h", to: p.time.to ?? "now" },
  }).find((x) => x.view === "resources")?.to;
  const open = () => to && navigate(to);
  // Pods that report no samples, as a process outside the cluster, have no panel.
  const none = use !== undefined && !(use instanceof Error) && use.groups.length === 0;
  if (none) {
    return null;
  }
  return h(
    Panel,
    {
      title: p.pod ? `Resources · ${p.pod}` : "Resources",
      action: { label: "Open resources ›", onClick: open },
    },
    h(Show<ResourceUsageResponse>, {
      answer: use,
      render: (r) =>
        r.groups.length === 0
          ? h("p", null, "No resource samples for these pods in range.")
          : [
              p.mark && h(Kpis, { key: "k", tiles: marked(r, p.mark) }),
              h(
                "div",
                { key: "c", className: "g2" },
                chart(r, "cpu_cores", "CPU", (v) => cores(v).replace(" cores", "")),
                chart(r, "memory_bytes", "Memory", (v) => mem(v)),
              ),
            ],
    }),
  );
}
