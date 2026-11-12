import type { FieldStat, DataFocalClient, LineRef, LogLine } from "@datafocal/api";
import {
  navigate,
  useParams,
  type TimeRange,
  useSnapshot,
  useQueryRange,
  useTimeRange,
  type ViewProps,
} from "@datafocal/shell";
import { Chip, Menu, type MenuSection, Splitter, useLast } from "@datafocal/ui";
import { createElement, type ReactElement, useCallback, useMemo, useState } from "react";
import { QueryBar } from "./bar";
import { FieldRail } from "./fields";
import { Histogram } from "./histogram";
import { InsightsMode } from "./insights";
import { useAsync } from "./lines";
import { PatternsMode } from "./patterns";
import { clock, markRegex } from "./row";
import { compile, highlights, isPlain, withFilter } from "./search";
import { StatusBar } from "./status";
import { openStream, type Stream } from "./stream";
import { type StreamStatus, StreamTable } from "./table";
import "./logs.css";

const isLineRef = (v: unknown): v is LineRef =>
  typeof v === "object" && v !== null && "segment" in v && "row" in v;

const stored = (key: string): string[] => {
  const v: unknown = JSON.parse(localStorage.getItem(key) ?? "[]");
  return Array.isArray(v) ? v.filter((q) => typeof q === "string") : [];
};

// remember puts q first in the list kept under key, ten long.
function remember(key: string, q: string): void {
  localStorage.setItem(
    key,
    JSON.stringify([q, ...stored(key).filter((x) => x !== q)].slice(0, 10)),
  );
}

// actions are the view's secondary actions, for the "⋯" menu.
function actions(query: string): MenuSection[] {
  const open = (q: string) => ({ label: q, run: () => navigate({ view: "logs", query: q }) });
  const sections = [
    {
      items: [
        { label: "Save view", run: () => remember("datafocal.logs.saved", query) },
        {
          label: "Create SLO from here",
          run: () => navigate({ view: "slos", mode: "new", query: compile(query) }),
        },
        { label: "Add to view", run: () => navigate({ view: "views", query: compile(query) }) },
      ],
    },
    { title: "Saved views", items: stored("datafocal.logs.saved").map(open) },
    { title: "History", items: stored("datafocal.logs.history").map(open) },
  ];
  return sections;
}

type Mode = "insights" | "lines" | "patterns";

// useFound opens the query's stream at the pinned snapshot, if any, read back to the focus
// line when one is named.
function useFound(client: DataFocalClient, query: string, time: TimeRange, focus?: LineRef) {
  const [pin] = useSnapshot();
  const load = useCallback(
    (signal: AbortSignal) =>
      openStream(client, compile(query), time, { focus, snapshot: pin ?? "", signal }),
    [client, query, time, focus, pin],
  );
  return useAsync(load);
}

// useFields ranks the fields of the lines stream matches, at its selector and snapshot.
function useFields(client: DataFocalClient, stream: Stream | undefined) {
  const load = useCallback(
    async (signal: AbortSignal): Promise<readonly FieldStat[]> => {
      if (!stream) {
        return [];
      }
      const req = { selector: stream.selector, snapshot: stream.snapshot };
      return (await client.call("rank_fields", req, { signal })).data.fields;
    },
    [client, stream],
  );
  return useAsync(load);
}

// noFields are a line's ranked fields until rank_fields answers.
const noFields: readonly FieldStat[] = [];

interface ToolbarProps {
  readonly query: string;
  readonly mode: Mode;
  readonly result: Stream | Error | undefined;
  readonly fields: readonly FieldStat[] | Error | undefined;
  readonly sections: readonly MenuSection[];
}

function Toolbar({ query, mode, result, fields, sections }: ToolbarProps): ReactElement {
  const found = result instanceof Error ? undefined : result;
  const modeButton = (m: Mode, label: string) =>
    createElement(
      "button",
      {
        type: "button",
        className: "tbtn",
        "aria-pressed": mode === m,
        onClick: () => navigate({ view: "logs", query, mode: m }),
      },
      label,
    );
  return createElement(
    "div",
    { className: "logs-q" },
    createElement(QueryBar, {
      key: query,
      query,
      fields: fields instanceof Error ? [] : (fields?.map((f) => f.name) ?? []),
      call: found?.call,
      error: result instanceof Error ? result : undefined,
      onRun: (q: string) => {
        remember("datafocal.logs.history", q);
        navigate({ view: "logs", query: q, mode: mode === "patterns" ? "patterns" : "lines" });
      },
    }),
    createElement(
      "div",
      { className: "seg", role: "group", "aria-label": "Mode" },
      modeButton("insights", "Insights"),
      modeButton("lines", "Lines"),
      modeButton("patterns", "Patterns"),
    ),
    serviceOf(query) &&
      createElement(
        "button",
        {
          type: "button",
          className: "tbtn",
          title: "This service's CPU, memory, network and disk over the range",
          onClick: () => navigate({ view: "resources", service: serviceOf(query) ?? "" }),
        },
        "Resources",
      ),
    createElement(Menu, { label: "More actions", sections }),
  );
}

const minute = 60_000;

// contextOf opens the lines of l's producer around it, unfiltered, with l picked out.
function contextOf(query: string, l: LogLine): void {
  const t = Date.parse(l.timestamp);
  const svc = l.service ?? "";
  navigate({
    view: "logs",
    query: isPlain(query) ? `service:${svc}` : `{service=${JSON.stringify(svc)}}`,
    mode: "lines",
    focus: { segment: l.segment ?? "", row: l.row },
    title: `Context · ${svc} at ${clock(l.timestamp)}`,
    time: `${new Date(t - 10 * minute).toISOString()},${new Date(t + 2 * minute).toISOString()}`,
  });
}

// serviceOf is the one service a LogQL query's selector names, as {service="db-3"} does.
const serviceOf = (query: string): string | undefined =>
  /[{,]\s*service\s*=\s*"([^"]+)"/.exec(query)?.[1];

interface LinesModeProps {
  readonly client: DataFocalClient;
  readonly found: Stream;
  readonly fields: readonly FieldStat[];
  readonly query: string;
  readonly time: TimeRange;
  readonly focus: LineRef | undefined;
  readonly setTime: (r: TimeRange) => void;
}

// LinesMode is the query's lines as one stream under its status: oldest at the top, newest
// at the bottom, following new lines while the range runs to now.
function LinesMode(p: LinesModeProps): ReactElement {
  const { found, query, time } = p;
  const [paused, setPaused] = useState(false);
  const [status, setStatus] = useState<StreamStatus>();
  const tailing = time.to === "now";
  const marks = useMemo(() => markRegex(highlights(query)), [query]);
  const acts = useMemo(
    () => ({
      filter: (field: string, value: string, exclude: boolean) =>
        navigate({ view: "logs", query: withFilter(query, field, value, exclude), mode: "lines" }),
      context: (l: LogLine) => contextOf(query, l),
      open: navigate,
    }),
    [query],
  );
  const onLive = () =>
    tailing ? setPaused(!paused) : (setPaused(false), p.setTime({ from: time.from, to: "now" }));
  return createElement(
    "div",
    { className: "live" },
    createElement(StatusBar, { status, tailing, paused, onLive }),
    createElement(StreamTable, {
      key: found.snapshot + query,
      client: p.client,
      stream: found,
      fields: p.fields,
      marks,
      live: tailing && !paused,
      focus: p.focus,
      acts,
      onStatus: setStatus,
    }),
  );
}

interface ResultsProps extends ViewProps {
  readonly found: Stream;
  readonly fields: readonly FieldStat[] | Error | undefined;
  // pending is whether found is the last result, kept while the next loads.
  readonly pending: boolean;
  readonly mode: Mode;
  readonly query: string;
  readonly time: TimeRange;
  readonly focus: LineRef | undefined;
  readonly setTime: (r: TimeRange) => void;
}

// showing is what the results show under the volume: the mode's, or that nothing matched. A
// range running to now may yet tail lines in, so its lines are never empty.
const showing = ({ found, mode, time }: ResultsProps): Mode | "empty" =>
  found.lines().length === 0 && (mode === "patterns" || (mode === "lines" && time.to !== "now"))
    ? "empty"
    : mode;

// Results are the fields that tell the lines apart beside the volume over the lines, their
// patterns or their anomalies.
function Results(p: ResultsProps): ReactElement {
  const { client, found, query, time, setTime } = p;
  const { selector, snapshot } = found;
  const fields = Array.isArray(p.fields) ? p.fields : noFields;
  const shows = showing(p);
  return createElement(
    "div",
    { className: p.pending ? "logs-b stale" : "logs-b", "aria-busy": p.pending },
    createElement(FieldRail, { client, query, selector, snapshot, fields: p.fields }),
    createElement(Splitter, {
      label: "fields",
      name: "rail",
      initial: 280,
      min: 180,
      max: 640,
      edge: "left",
    }),
    createElement(
      "div",
      { className: "logs-m" },
      createElement(Histogram, {
        client,
        selector,
        snapshot,
        onBrush: setTime,
        onJump: (at: string) => setTime({ from: time.from, to: at }),
      }),
      shows === "empty" && createElement(Empty, { query, time, setTime }),
      shows === "insights" && createElement(InsightsMode, { client, selector, snapshot }),
      shows === "patterns" && createElement(PatternsMode, { client, query, selector, snapshot }),
      shows === "lines" &&
        createElement(LinesMode, { client, found, fields, query, time, focus: p.focus, setTime }),
    ),
  );
}

// Empty says the query and range that matched nothing, and offers to widen the range.
function Empty({ query, time, setTime }: Pick<ResultsProps, "query" | "time" | "setTime">) {
  return createElement(
    "p",
    { className: "logs-empty", role: "status" },
    `No lines match ${query} from ${time.from} to ${time.to}. `,
    createElement(
      "button",
      { type: "button", className: "tbtn", onClick: () => setTime({ from: "now-7d", to: "now" }) },
      "Widen to the last 7d",
    ),
  );
}

// useFocus is the line navigate asked to show, from the focus param.
function useFocus(param: string | undefined): LineRef | undefined {
  return useMemo(() => {
    const at: unknown = param === undefined ? null : JSON.parse(param);
    return isLineRef(at) ? at : undefined;
  }, [param]);
}

// LogsView is the query bar over the fields, the volume and the lines, which follow new
// ones as they arrive while the range runs to now; with no query, every service's. Its
// other modes are the lines' patterns and what stands out in them. A drill-down lands on
// its line.
export function LogsView({ client }: ViewProps): ReactElement {
  const params = useParams();
  const [time, setTime] = useTimeRange();
  const bounds = useQueryRange();
  const query = params["query"] ?? "";
  const mode: Mode =
    params["mode"] === "patterns" || params["mode"] === "insights" ? params["mode"] : "lines";
  const focus = useFocus(params["focus"]);
  const result = useFound(client, query, bounds, focus);
  const found = useLast(result);
  const fields = useFields(client, found);
  const pending = result === undefined;
  const sections = actions(query);
  return createElement(
    "div",
    { className: "logs" },
    createElement(Toolbar, { query, mode, result, fields, sections }),
    params["title"] &&
      createElement(
        "div",
        { className: "logs-ctx" },
        createElement(Chip, { title: query }, params["title"]),
        createElement(
          "button",
          { type: "button", className: "tbtn", onClick: () => history.back() },
          "Back",
        ),
      ),
    pending && createElement("progress", { className: "busy", "aria-label": "Loading" }),
    found &&
      createElement(Results, { client, found, fields, pending, mode, query, time, focus, setTime }),
  );
}
