import {
  binStarts,
  type DataFocalClient,
  type Pattern,
  type Selector,
  type Series,
} from "@datafocal/api";
import { baselineRange, type FocusTarget, useCompare, useFocus } from "@datafocal/shell";
import { Chart, Chip, Show, useLast } from "@datafocal/ui";
import { createElement, Fragment, type ReactElement, useCallback, useMemo, useState } from "react";
import { useAsync } from "./lines";
import { compile, withFilter } from "./search";

export interface PatternsModeProps {
  readonly client: DataFocalClient;
  readonly query: string;
  readonly selector: Selector;
  readonly snapshot: string;
}

// TrendsRequest asks for the lines over bins of each of ids' patterns in selector.
interface TrendsRequest {
  readonly selector: Selector;
  readonly snapshot: string;
  readonly ids: readonly string[];
  readonly bins: string;
}

// trendsOf counts each pattern the request names over its bins, keyed by pattern_id.
export async function trendsOf(
  client: DataFocalClient,
  { selector, snapshot, ids, bins }: TrendsRequest,
  signal: AbortSignal,
): Promise<Map<string, Series>> {
  if (ids.length === 0) {
    return new Map();
  }
  const where = [
    ...(selector.where ?? []),
    { field: "pattern_id", op: "regex", value: `^(${ids.join("|")})$` },
  ];
  const req = { selector: { ...selector, where }, snapshot, measure: "count", bins };
  const opts = { signal };
  const { series } = (
    await client.call("aggregate", { ...req, group_by: "pattern_id", limit: ids.length }, opts)
  ).data;
  return new Map(series.map((s) => [s.key, s]));
}

// patternFocus is the focus on pattern id's lines in the query, or on those with also.
function patternFocus(
  p: PatternsModeProps,
  id: string,
  title: string,
  also?: { field: string; value: string },
): FocusTarget {
  const slice = [{ field: "pattern_id", op: "eq", value: id }];
  if (also) {
    slice.push({ field: also.field, op: "eq", value: also.value });
  }
  return {
    title,
    query: slice.reduce((q, s) => withFilter(q, s.field, s.value), compile(p.query)),
    selector: p.selector,
    slice,
    snapshot: p.snapshot,
  };
}

// Shifts are explain_spike's fields over-represented in a pattern's lines, the top three,
// with the lines behind each share, each focusing on the pattern's lines with it.
function Shifts(p: PatternsModeProps & { id: string }) {
  const { client, selector, snapshot, id } = p;
  const focus = useFocus();
  const load = useCallback(
    async (signal: AbortSignal) => {
      const req = { selector, slice: [{ field: "pattern_id", op: "eq", value: id }], snapshot };
      return (await client.call("explain_spike", req, { signal })).data.shifts.slice(0, 3);
    },
    [client, selector, snapshot, id],
  );
  const shifts = useAsync(load);
  if (shifts instanceof Error) {
    return createElement(Show, { answer: shifts, render: () => null });
  }
  return createElement(
    "p",
    null,
    shifts === undefined &&
      createElement("progress", { className: "busy", "aria-label": "Loading" }),
    shifts?.map((s) =>
      createElement(
        Chip,
        {
          key: `${s.field}=${s.value}`,
          onClick: () => focus(patternFocus(p, id, `${s.field}=${s.value}`, s)),
        },
        `${s.field}=${s.value} ${Math.round(s.slice_share * 100)}% (${s.slice_count}) vs ${Math.round(s.baseline_share * 100)}% (${s.baseline_count})`,
      ),
    ),
  );
}

// Template is a pattern's text with its masked parts, <*> and the like, muted.
export function Template({ text }: { text: string }): ReactElement {
  return createElement(
    Fragment,
    null,
    text
      .split(/(<[^<>\s]*>)/)
      .map((part, i) =>
        i % 2 === 1 ? createElement("span", { key: i, className: "ph" }, part) : part,
      ),
  );
}

// Trend is a pattern's lines over the window, and its count in the baseline window.
interface Trend {
  readonly now: Series | undefined;
  readonly was: number;
}

interface PatternRowProps extends PatternsModeProps {
  readonly pat: Pattern;
  readonly trend: Trend | undefined;
  // open is whether the row shows what sets its lines apart.
  readonly open: boolean;
  readonly onExplain: () => void;
}

// delta is how a count moved from the baseline's: by a percentage, or new from none.
function delta(n: number, was: number): string {
  if (was === 0) {
    return n > 0 ? "new" : "";
  }
  return `${n >= was ? "+" : "−"}${Math.round((Math.abs(n - was) / was) * 100)}%`;
}

// sparkline is a trend's lines over time.
const sparkline = (label: string, s: Series) =>
  createElement(Chart, {
    kind: "sparkline",
    data: [binStarts(s), (s.values ?? []).map((v) => v ?? 0)],
    width: 72,
    height: 20,
    label,
  });

// PatternRow is a pattern's text, which focuses on its lines, its producer, its lines over
// time, count and change since the baseline, and status.
function PatternRow(p: PatternRowProps): ReactElement {
  const { pat, trend } = p;
  const focus = useFocus();
  const text = pat.pattern || pat.pattern_id;
  return createElement(
    "li",
    null,
    createElement(
      "button",
      {
        type: "button",
        title: pat.pattern_id,
        onClick: () => focus(patternFocus(p, pat.pattern_id, text)),
      },
      pat.pattern ? createElement(Template, { text: pat.pattern }) : pat.pattern_id,
    ),
    createElement("span", { className: "pat-svc" }, pat.service),
    trend?.now ? sparkline(`${text} lines over time`, trend.now) : createElement("span"),
    createElement(
      "span",
      { className: "pat-n" },
      pat.count.toLocaleString("en"),
      trend && createElement("small", null, ` ${delta(pat.count, trend.was)}`),
    ),
    createElement("span", null, pat.status && createElement(Chip, null, pat.status)),
    createElement(
      "button",
      { type: "button", className: "pat-x", "aria-expanded": p.open, onClick: p.onExplain },
      "Explain",
    ),
    p.open && createElement(Shifts, { ...p, id: pat.pattern_id }),
  );
}

// useTrends is each listed pattern's lines over time and its count in the baseline window:
// the range compare offsets to, or else the window just before.
function useTrends({ client, selector, snapshot }: PatternsModeProps, ids: readonly string[]) {
  const [compare] = useCompare();
  const load = useCallback(
    async (signal: AbortSignal): Promise<ReadonlyMap<string, Trend>> => {
      const before = baselineRange(selector.time ?? {}, compare ?? { offset: "period" });
      const [cur, was] = await Promise.all([
        trendsOf(client, { selector, snapshot, ids, bins: "auto" }, signal),
        trendsOf(
          client,
          { selector: { ...selector, time: before }, snapshot, ids, bins: "all" },
          signal,
        ),
      ]);
      return new Map(
        ids.map((id) => [id, { now: cur.get(id), was: was.get(id)?.values?.[0] ?? 0 }]),
      );
    },
    [client, selector, snapshot, ids, compare],
  );
  return useAsync(load);
}

interface Page {
  readonly patterns: readonly Pattern[];
  readonly total: number;
  readonly cursor: string | undefined;
}

// Pages are the patterns read so far: the first page, then each next one More reads by the
// cursor the last ended with, or why reading it failed.
interface Pages extends Page {
  readonly first: Page;
  readonly error?: Error;
}

// usePages is the query's patterns a page at a time, the last read kept while the next loads.
function usePages({ client, selector, snapshot }: PatternsModeProps) {
  const load = useCallback(
    async (signal: AbortSignal): Promise<Page> => {
      const env = await client.call("list_patterns", { selector, snapshot }, { signal });
      return { ...env.data, cursor: env.meta.cursor };
    },
    [client, selector, snapshot],
  );
  const answer = useAsync(load);
  const first = useLast(answer);
  const [more, setMore] = useState<Pages>();
  const pages = first && (more?.first === first ? more : { ...first, first });
  const readMore = (h: Pages, cursor: string) =>
    void client.call("list_patterns", { selector, snapshot, cursor }).then(
      (env) =>
        setMore({ ...h, patterns: [...h.patterns, ...env.data.patterns], cursor: env.meta.cursor }),
      (e: unknown) => setMore({ ...h, error: e instanceof Error ? e : new Error(String(e)) }),
    );
  return { answer, pages, readMore };
}

// PatternsMode is the query's patterns by count, a page at a time, each focusing on its lines
// or explaining what sets its lines apart.
export function PatternsMode(p: PatternsModeProps): ReactElement {
  const { answer, pages, readMore } = usePages(p);
  const [explained, setExplained] = useState<string>();
  const listed = pages?.patterns;
  // The last selector's patterns, kept while the next loads, have no trends in this one.
  const ids = useMemo(
    () => (answer === undefined ? [] : (listed?.map((x) => x.pattern_id) ?? [])),
    [answer, listed],
  );
  const trends = useTrends(p, ids);
  if (answer instanceof Error) {
    return createElement(Show, { answer, render: () => null });
  }
  return createElement(
    "div",
    { className: answer === undefined ? "pats trend stale" : "pats trend" },
    answer === undefined &&
      createElement("progress", { className: "busy", "aria-label": "Loading" }),
    pages &&
      createElement(
        "p",
        null,
        `${pages.patterns.length.toLocaleString("en")} of ${pages.total.toLocaleString("en")} patterns`,
      ),
    createElement(
      "ul",
      null,
      createElement(
        "li",
        { className: "pat-h", "aria-hidden": true },
        ["Pattern", "Producer", "Trend", "Lines", "", ""].map((h, i) =>
          createElement("span", { key: i }, h),
        ),
      ),
      listed?.map((pat) =>
        createElement(PatternRow, {
          key: pat.pattern_id,
          ...p,
          pat,
          trend: trends instanceof Error ? undefined : trends?.get(pat.pattern_id),
          open: explained === pat.pattern_id,
          onExplain: () => setExplained(explained === pat.pattern_id ? undefined : pat.pattern_id),
        }),
      ),
    ),
    pages && createElement(More, { pages, readMore }),
  );
}

// More reads the page after those listed, while there is one, or says why it failed.
function More({ pages, readMore }: { pages: Pages; readMore: (p: Pages, c: string) => void }) {
  const next = pages.cursor;
  return createElement(
    Fragment,
    null,
    pages.error && createElement(Show, { answer: pages.error, render: () => null }),
    next &&
      createElement(
        "button",
        { type: "button", className: "tbtn", onClick: () => readMore(pages, next) },
        "More patterns",
      ),
  );
}
