import {
  binStarts,
  type ExplainSpikeResponse,
  type FocusResponse,
  type DataFocalClient,
  type LogLine,
  type Predicate,
  type Selector,
  type Shift,
} from "@datafocal/api";
import {
  type Answer,
  Chart,
  Failure,
  lineText,
  Show,
  unwrap,
  useAnswer,
  useLast,
} from "@datafocal/ui";
import { createElement, Fragment, type ReactElement, type ReactNode } from "react";
import { type Destination, navigate } from "./url";

// FocusTarget is what a click focuses on: a slice of a scope, as the LogQL query Logs opens,
// and, when what opened it flagged the slice, why in one sentence.
export interface FocusTarget {
  readonly title: string;
  readonly query: string;
  readonly selector: Selector;
  readonly slice: readonly Predicate[];
  readonly snapshot?: string;
  readonly why?: string;
}

interface Focused {
  readonly target: FocusTarget;
  readonly res: FocusResponse;
  readonly snapshot: string;
}

// deeper is f's target narrowed to field=value, at the snapshot f was answered at.
const deeper = (f: Focused, field: string, value: string): FocusTarget => ({
  title: `${field}=${value}`,
  query: `${f.target.query} | ${field}=${JSON.stringify(value)}`,
  selector: f.target.selector,
  slice: [...f.target.slice, { field, op: "eq", value }],
  snapshot: f.snapshot,
});

// useFocused is focus's answer for target, aborting the call a newer target supersedes.
function useFocused(client: DataFocalClient, target: FocusTarget): Answer<Focused> {
  const { selector, slice, snapshot } = target;
  const req = { selector, slice: [...slice], ...(snapshot && { snapshot }) };
  return useAnswer(JSON.stringify(target), async (signal) => {
    const env = await client.call("focus", req, { signal });
    return { target, res: env.data, snapshot: env.meta.snapshot };
  });
}

const button = (label: string, onClick: () => void, title?: string) =>
  createElement("button", { key: label, type: "button", className: "tbtn", title, onClick }, label);

const section = (title: string, ...body: ReactNode[]) =>
  createElement("section", null, createElement("h3", null, title), ...body);

const pct = (v: number) => `${Math.round(v * 100)}%`;

const count = (n: number) => n.toLocaleString("en");

// sectionNames say what each section of a focus call is, for its failure.
const sectionNames: Readonly<Record<string, string>> = {
  trend: "the lines over time",
  lift: "what these lines have in common",
  related: "the patterns",
  examples: "the example lines",
};

// Bar is one share as a bar on a 0–100% scale, labelled with where and how much.
function Bar({ where, share, note }: { where: string; share: number; note: string }) {
  return createElement(
    "span",
    { className: `common-bar ${where}` },
    createElement("span", null, where),
    createElement(
      "span",
      { className: "common-track" },
      createElement("i", {
        ref: (el: HTMLElement | null) => el?.style.setProperty("width", pct(share)),
      }),
    ),
    createElement("span", null, note),
  );
}

// Common is what the focused lines have in common: each value as its share here against
// its share elsewhere, two bars on one scale so the gap reads at a glance.
function Common({ lift, onPick }: { lift: ExplainSpikeResponse; onPick: (s: Shift) => void }) {
  return section(
    "What these lines have in common",
    createElement("p", null, `Compared with the other ${count(lift.rest)} lines in this range.`),
    lift.shifts.map((s) =>
      createElement(
        "button",
        {
          key: `${s.field}=${s.value}`,
          type: "button",
          className: "common-r",
          title: `${count(s.slice_count)} of these ${count(lift.lines)} lines; ${count(s.baseline_count)} of the other ${count(lift.rest)}`,
          onClick: () => onPick(s),
        },
        createElement("span", { className: "common-k" }, `${s.field} = ${s.value || "(none)"}`),
        createElement(Bar, {
          where: "here",
          share: s.slice_share,
          note: `${pct(s.slice_share)} · ${count(s.slice_count)}`,
        }),
        createElement(Bar, {
          where: "elsewhere",
          share: s.baseline_share,
          note: pct(s.baseline_share),
        }),
      ),
    ),
  );
}

// Trend is how many lines the focus holds and how they fall over the range.
function Trend({ f }: { f: Focused }) {
  const series = f.res.trend?.series[0];
  if (!series) {
    return null;
  }
  const values = (series.values ?? []).map((v) => v ?? 0);
  return section(
    `${count(values.reduce((a, v) => a + v, 0))} lines`,
    createElement(Chart, {
      kind: "sparkline",
      data: [binStarts(series), values],
      width: 360,
      height: 48,
      label: `${f.target.title} lines over time`,
    }),
  );
}

// shownFields are the fields an example line shows beside its message: those the focus
// selects on, then those its lines have in common, so a line whose message is the same for
// every event still says why it is here.
// The service is the line's chip already.
const shownFields = (f: Focused): string[] =>
  [
    ...new Set([
      ...f.target.slice.map((p) => p.field ?? ""),
      ...(f.res.lift?.shifts ?? []).map((s) => s.field),
    ]),
  ].filter((k) => k !== "" && k !== "service");

// fieldsOf is l's value of each of fields it carries, as field=value.
const fieldsOf = (l: LogLine, fields: readonly string[]): string =>
  fields
    .flatMap((k) => {
      const v = l.attributes?.[k];
      return v === undefined ? [] : [`${k}=${unwrap(v)}`];
    })
    .join("  ");

// Sections are what failed, the focus's trend, what its lines have in common, its patterns
// when there are more than one, and examples with the fields they are focused on, each
// value focusing deeper.
function Sections({ f, onFocus }: { f: Focused; onFocus: (t: FocusTarget) => void }) {
  const { lift, related, examples, errors = {} } = f.res;
  const fields = shownFields(f);
  return createElement(
    Fragment,
    null,
    Object.entries(errors).map(([name, e]) =>
      createElement(Failure, {
        key: name,
        what: `Couldn't load ${sectionNames[name] ?? name}`,
        message: e,
      }),
    ),
    createElement(Trend, { f }),
    lift &&
      lift.shifts.length > 0 &&
      createElement(Common, { lift, onPick: (s) => onFocus(deeper(f, s.field, s.value)) }),
    related &&
      related.patterns.length > 1 &&
      section(
        "Patterns",
        related.patterns.map((p) =>
          button(
            `${p.pattern || p.pattern_id} · ${count(p.count)}`,
            () => onFocus(deeper(f, "pattern_id", p.pattern_id)),
            p.pattern_id,
          ),
        ),
      ),
    examples &&
      section(
        "Examples",
        (examples.lines ?? []).map((l, i) =>
          createElement(
            "div",
            { key: i, className: "ln" },
            createElement("span", null, l.timestamp.slice(11, 19)),
            button(l.service ?? "", () => onFocus(deeper(f, "service", l.service ?? ""))),
            createElement(
              "span",
              null,
              lineText(l),
              fieldsOf(l, fields) &&
                createElement("code", { className: "ln-kv" }, fieldsOf(l, fields)),
            ),
          ),
        ),
      ),
  );
}

export interface FocusPanelProps {
  readonly client: DataFocalClient;
  readonly target: FocusTarget;
  readonly onFocus: (t: FocusTarget) => void;
  readonly onClose: () => void;
}

// Actions open the target's lines in Logs, as patterns or lines, or a trace among them.
function Actions(p: { target: FocusTarget; traceId: string | undefined; onClose: () => void }) {
  const { target, traceId, onClose } = p;
  const go = (to: Destination) => () => {
    onClose();
    navigate(to);
  };
  const logs = { view: "logs", query: target.query, title: target.title } as const;
  return createElement(
    "div",
    { className: "focus-a" },
    button("Narrow to this", go({ ...logs, mode: "patterns" })),
    button("Open lines", go({ ...logs, mode: "lines" })),
    traceId && button("Open trace", go({ view: "trace", field: "trace_id", traceId })),
  );
}

// Head is what the panel focuses on and the query that stands for it, with the panel's close.
function Head(p: { target: FocusTarget; traceId: string | undefined; onClose: () => void }) {
  const { target, onClose } = p;
  return createElement(
    "header",
    null,
    createElement(
      "div",
      { className: "focus-t" },
      createElement("span", { className: "focus-k" }, "Focus"),
      createElement(
        "button",
        {
          type: "button",
          className: "focus-x",
          "aria-label": "Close",
          title: "Close (Esc)",
          onClick: onClose,
        },
        "×",
      ),
    ),
    createElement("h2", { title: target.title }, target.title),
    createElement("code", null, target.query),
    createElement(Actions, p),
  );
}

interface BodyProps {
  readonly target: FocusTarget;
  readonly answer: Answer<Focused>;
  readonly shown: Focused | undefined;
  readonly onFocus: (t: FocusTarget) => void;
}

// Body is why the target was flagged, then the call's failure or its sections.
function Body({ target, answer, shown, onFocus }: BodyProps) {
  return createElement(
    "div",
    { className: "focus-b" },
    target.why && createElement("p", { className: "focus-why" }, target.why),
    answer instanceof Error
      ? createElement(Show, { answer, render: () => null })
      : shown && createElement(Sections, { f: shown, onFocus }),
  );
}

// FocusPanel is what one focus call says about a target, beside the view, under the query
// it stands for and the ways on to its lines, which stay put while its body scrolls; while
// a deeper focus loads, the last one stays, dimmed.
export function FocusPanel({ client, target, onFocus, onClose }: FocusPanelProps): ReactElement {
  const answer = useFocused(client, target);
  const shown = useLast(answer);
  const pending = answer === undefined;
  const traced = shown?.res.examples?.lines?.find((l) => l.attributes?.["trace_id"]);
  const traceId = traced?.attributes?.["trace_id"];
  return createElement(
    "section",
    { className: pending ? "focus stale" : "focus", "aria-label": "Focus" },
    pending && createElement("progress", { className: "busy", "aria-label": "Loading" }),
    createElement(Head, { target, traceId, onClose }),
    createElement(Body, { target, answer, shown, onFocus }),
  );
}
