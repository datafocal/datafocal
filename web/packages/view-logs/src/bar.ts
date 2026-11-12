import type { ToolCall } from "@datafocal/api";
import { suggest, tokenize } from "@datafocal/query";
import { Show } from "@datafocal/ui";
import { compile, isPlain, terms } from "./search";
import { createElement, type ReactElement, type ReactNode, useId, useState } from "react";

export interface QueryBarProps {
  readonly query: string;
  // fields are the field names autocomplete offers, best first.
  readonly fields: readonly string[];
  readonly call: ToolCall | undefined;
  readonly error: Error | undefined;
  readonly onRun: (q: string) => void;
}

// faultAt is the offset the querier's parse error points at, which it writes as "at offset N".
function faultAt(e: Error | undefined): number | undefined {
  const m = e && /at offset (\d+)/.exec(e.message);
  return m ? Number(m[1]) : undefined;
}

// plainKinds are the highlight classes of a plain search's terms.
const plainKinds = { text: "q-text", field: "q-label", not: "q-not", or: "q-op", error: "q-fault" };

// PlainHighlight is a plain search's terms by kind, an unclosed phrase underlined.
function PlainHighlight({ q }: { q: string }): ReactElement {
  const parts: ReactNode[] = [];
  let at = 0;
  for (const t of terms(q)) {
    parts.push(
      q.slice(at, t.start),
      createElement(
        "span",
        { key: t.start, className: plainKinds[t.kind] },
        q.slice(t.start, t.end),
      ),
    );
    at = t.end;
  }
  parts.push(q.slice(at));
  return createElement("pre", { "aria-hidden": true }, parts);
}

// Highlight is q's tokens by kind, underlining the token at fault, or the end of q past it.
function Highlight({ q, fault }: { q: string; fault: number | undefined }): ReactElement {
  if (isPlain(q)) {
    return createElement(PlainHighlight, { q });
  }
  const parts: ReactNode[] = [];
  let at = 0;
  let marked = fault === undefined;
  for (const t of tokenize(q)) {
    const faulty = !marked && t.end > (fault ?? 0);
    marked ||= faulty;
    parts.push(
      q.slice(at, t.start),
      createElement(
        "span",
        { key: t.start, className: faulty ? `q-${t.kind} q-fault` : `q-${t.kind}` },
        q.slice(t.start, t.end),
      ),
    );
    at = t.end;
  }
  parts.push(
    q.slice(at),
    !marked && createElement("span", { key: "end", className: "q-fault" }, " "),
  );
  return createElement("pre", { "aria-hidden": true }, parts);
}

// plainSuggest completes the word before cursor in a plain search to a field name and ":".
function plainSuggest(q: string, cursor: number, fields: readonly string[]): readonly string[] {
  const partial = /(?:^|\s)-?([A-Za-z_][\w.]*)$/.exec(q.slice(0, cursor))?.[1] ?? "";
  return partial === ""
    ? []
    : fields.filter((f) => f.startsWith(partial) && f !== partial).map((f) => `${f}:`);
}

// Ran is what the query ran as: a plain search's LogQL, or LogQL's tool call.
function Ran({ query, call }: { query: string; call: ToolCall | undefined }): ReactElement | null {
  if (isPlain(query)) {
    return createElement("p", { className: "q-call" }, `LogQL ${compile(query)}`);
  }
  return call
    ? createElement("p", { className: "q-call" }, `${call.tool} ${JSON.stringify(call.args)}`)
    : null;
}

// QueryBar edits and runs a LogQL query, with the tool call it compiled to, the error it
// failed with, or what a mistyped comparison meant, as one line underneath.
export function QueryBar({ query, fields, call, error, onRun }: QueryBarProps): ReactElement {
  const [q, setQ] = useState(query);
  const [cursor, setCursor] = useState(query.length);
  const list = useId();
  const head = q.slice(0, cursor).replace(/[A-Za-z_][\w.]*$/, "");
  const words = isPlain(q) ? plainSuggest(q, cursor, fields) : suggest(q, cursor, fields);
  const hint = isPlain(q) ? terms(q).find((t) => t.hint)?.hint : undefined;
  const edit = (e: { currentTarget: HTMLInputElement }) => {
    setQ(e.currentTarget.value);
    setCursor(e.currentTarget.selectionStart ?? e.currentTarget.value.length);
  };
  return createElement(
    "form",
    {
      className: "qbar",
      onSubmit: (e: { preventDefault: () => void }) => {
        e.preventDefault();
        onRun(q);
      },
    },
    createElement(
      "div",
      { className: "q-in" },
      createElement(Highlight, { q, fault: q === query ? faultAt(error) : undefined }),
      createElement("input", {
        value: q,
        list,
        spellCheck: false,
        "aria-label": "Query",
        placeholder:
          'Search: timeout -healthz service:querier level:error "pool exhausted" · or LogQL {…}',
        onChange: edit,
        onSelect: edit,
      }),
    ),
    createElement(
      "datalist",
      { id: list },
      words.map((w) => createElement("option", { key: w, value: head + w + q.slice(cursor) })),
    ),
    hint
      ? createElement("p", { className: "q-call q-hint" }, hint)
      : error
        ? createElement(
            "div",
            { className: "q-call" },
            createElement(Show, { answer: error, render: () => null }),
          )
        : createElement(Ran, { query, call }),
  );
}
