import type { Anomaly, DataFocalClient, Selector, Series } from "@datafocal/api";
import { binStarts } from "@datafocal/api";
import { useFocus } from "@datafocal/shell";
import { Chart, Chip, Show } from "@datafocal/ui";
import { createElement, type ReactElement, useCallback } from "react";
import { useAsync } from "./lines";
import { Template, trendsOf } from "./patterns";

export interface InsightsModeProps {
  readonly client: DataFocalClient;
  readonly selector: Selector;
  readonly snapshot: string;
}

interface InsightRowProps extends Pick<InsightsModeProps, "selector" | "snapshot"> {
  readonly a: Anomaly;
  readonly trend: Series | undefined;
}

// why is the one sentence saying why find_anomalies flagged a: a pattern first seen in the
// range, or one logging well past what its baseline expects.
function why(a: Anomaly): string {
  const at = `${a.at.slice(11, 16)} UTC`;
  const lines = `${a.observed.toLocaleString("en")} lines`;
  if (a.kind === "new_pattern") {
    return `Flagged as new: ${a.service} first logged this pattern at ${at}, ${lines} since.`;
  }
  const times = a.expected > 0 ? `${(a.observed / a.expected).toFixed(1)}× ` : "";
  return `Flagged as a spike: ${a.service} logged this ${times}as often as its baseline at ${at}, ${lines} against ${a.expected.toFixed(1)} expected.`;
}

// InsightRow is a group's pattern, which focuses on its lines, its service, its lines over
// time, what it observed against what was expected, and its level.
function InsightRow({ a, trend, selector, snapshot }: InsightRowProps) {
  const focus = useFocus();
  const members = a.members?.length ?? 0;
  return createElement(
    "li",
    null,
    createElement(
      "button",
      {
        type: "button",
        title: a.pattern_id,
        onClick: () =>
          focus({
            title: a.pattern || a.pattern_id,
            query: `{service=~".+"} | pattern_id=${JSON.stringify(a.pattern_id)}`,
            selector,
            slice: [{ field: "pattern_id", op: "eq", value: a.pattern_id }],
            snapshot,
            why: why(a),
          }),
      },
      createElement(Template, { text: a.pattern || a.pattern_id }),
      members > 0 && ` +${members} ${a.kind === "new_pattern" ? "new patterns" : "more"}`,
    ),
    createElement("span", { className: "pat-svc" }, a.service),
    trend
      ? createElement(Chart, {
          kind: "sparkline",
          data: [binStarts(trend), (trend.values ?? []).map((v) => v ?? 0)],
          width: 72,
          height: 20,
          label: `${a.pattern} lines over time`,
        })
      : createElement("span"),
    createElement("span", { className: "pat-n" }, `${a.observed} vs ${a.expected.toFixed(1)}`),
    createElement("span", null, a.level && createElement(Chip, null, a.level)),
  );
}

// InsightsMode is what stands out in the lines, find_anomalies' groups by score.
export function InsightsMode({ client, selector, snapshot }: InsightsModeProps): ReactElement {
  const load = useCallback(
    async (signal: AbortSignal) => {
      const req = { selector, snapshot };
      const { anomalies } = (await client.call("find_anomalies", req, { signal })).data;
      const ids = anomalies.map((a) => a.pattern_id);
      return {
        anomalies,
        trends: await trendsOf(client, { selector, snapshot, ids, bins: "auto" }, signal),
      };
    },
    [client, selector, snapshot],
  );
  const found = useAsync(load);
  if (found instanceof Error) {
    return createElement(Show, { answer: found, render: () => null });
  }
  return createElement(
    "div",
    { className: "pats" },
    found && createElement("p", null, `${found.anomalies.length.toLocaleString("en")} anomalies`),
    createElement(
      "ul",
      null,
      found?.anomalies.map((a) =>
        createElement(InsightRow, {
          key: a.pattern_id,
          selector,
          snapshot,
          a,
          trend: found.trends.get(a.pattern_id),
        }),
      ),
    ),
  );
}
