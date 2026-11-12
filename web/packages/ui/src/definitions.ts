import type { ImportDefinitionsResponse, DataFocalClient, ListSLOsResponse } from "@datafocal/api";
import { createElement, type ReactElement, type ReactNode, useId, useState } from "react";
import { type Answer, Failure, Show, useAnswer } from "./answer";
import { Chart } from "./chart";

// The SLOs' and saved views' shared parts: an SLO's card, and the form saving documents.

type Status = ListSLOsResponse["slos"][number];
type Imported = ImportDefinitionsResponse["documents"][number];

export const pct = (v: number | null | undefined, digits = 2): string =>
  v == null ? "—" : `${(v * 100).toFixed(digits)}%`;

// button is a toolbar button; primary marks the one action a page leads with.
export const button = (text: string, onClick: () => void, primary = false): ReactElement =>
  createElement(
    "button",
    { type: "button", className: primary ? "tbtn slo-primary" : "tbtn", onClick },
    text,
  );

// download saves content as a file called name.
export function download(name: string, content: string): void {
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([content], { type: "application/yaml" }));
  a.download = name;
  a.click();
  URL.revokeObjectURL(a.href);
}

const readOnly =
  "This querier's definitions are read-only (mounted image). Export the file and commit it to your definitions.";

// useWritable is whether saving into the definitions directory is possible.
const useWritable = (client: DataFocalClient): Answer<boolean> =>
  useAnswer("writable", async () => (await client.call("list_slos", {})).data.writable);

const stateOf = (s: Status) => (s.pending && s.state === "ok" ? "pending" : s.state);
const when = (t: string | undefined) => (t ? ` since ${t.slice(0, 16).replace("T", " ")}` : "");
const rate = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(1)}×`);

// health is what an SLO leads with: the server's state where it alerts or lacks data, else
// what the budget says, since partial only means the window isn't fully counted yet.
export function health(s: Status): { label: string; tone: string; rank: number } {
  const b = s.budget_remaining;
  const state = stateOf(s);
  return state === "firing"
    ? { label: "Firing", tone: "err", rank: 0 }
    : state === "pending"
      ? { label: "Pending", tone: "warn", rank: 1 }
      : state === "unbound"
        ? { label: "Unbound", tone: "mute", rank: 5 }
        : b == null
          ? { label: "No data", tone: "mute", rank: 4 }
          : b <= 0
            ? { label: "Exhausted", tone: "err", rank: 2 }
            : b < 0.25
              ? { label: "At risk", tone: "warn", rank: 3 }
              : { label: "Healthy", tone: "ok", rank: 4 };
}

// toneOfBurn colours a burn rate against the standard page (14.4×) and ticket (1×) thresholds.
const toneOfBurn = (v: number | null | undefined) =>
  v == null ? "mute" : v >= 14.4 ? "err" : v >= 1 ? "warn" : "mute";

const cell = (className: string, ...children: ReactNode[]) =>
  createElement("div", { className }, ...children);

const link = (text: ReactNode, onClick: () => void) =>
  createElement("button", { type: "button", className: "slo-link", onClick }, text);

// sliCell is the SLI, coloured by whether it meets the objective, over the objective.
export const sliCell = (s: Status): ReactNode[] => [
  cell(`slo-num slo-${s.sli == null ? "mute" : s.sli >= s.objective ? "ok" : "err"}`, pct(s.sli)),
  cell("slo-sub", `target ${pct(s.objective)}`),
];

export const burnCell = (s: Status): ReactNode =>
  cell(
    "slo-burn",
    ...(
      [
        ["1h", s.burn_rate_1h],
        ["6h", s.burn_rate_6h],
      ] as const
    ).map(([w, v]) =>
      createElement(
        "span",
        { key: w, className: `slo-${toneOfBurn(v)}` },
        rate(v),
        createElement("small", null, w),
      ),
    ),
  );

// Burndown is an SLO's budget left across its window, from the list's trend, as a sparkline
// beside where the window started and where it is now. A budget overspent past -100% is
// drawn at -100%.
export function Burndown({ s }: { s: Status }): ReactElement {
  const trend = s.budget_trend ?? [];
  const counted = trend.flatMap((v, i) => (v == null ? [] : [[i, Math.max(-1, v)] as const]));
  if (counted.length < 2) {
    return cell("slo-spark slo-sub", "no data");
  }
  return cell(
    "slo-spark",
    createElement(Chart, {
      kind: "sparkline",
      data: [counted.map(([i]) => i), counted.map(([, v]) => v)],
      width: 340,
      height: 48,
      label: `${s.name} error budget left across its window`,
    }),
    cell(
      "slo-sub slo-spark-axis",
      createElement("span", null, `start ${pct(counted[0]?.[1], 0)}`),
      createElement("span", null, `now ${pct(s.budget_remaining, 0)}`),
    ),
  );
}

const stat = (label: string, ...value: ReactNode[]) =>
  cell("slo-stat", createElement("small", null, label), ...value);

// Card is an SLO's health, SLI, budget and burn rates, over its budget burning down; open
// opens the named SLO's page.
export function Card(p: {
  s: Status;
  pick?: ReactNode;
  open: (slo: string) => void;
}): ReactElement {
  const { s } = p;
  const h = health(s);
  const b = s.budget_remaining;
  return createElement(
    "article",
    { className: `slo-card slo-card-${h.tone}`, "aria-label": s.name },
    createElement(
      "header",
      null,
      cell(`slo-state slo-${h.tone}`, createElement("i"), h.label),
      cell(
        "slo-sub",
        s.state === "partial" ? `coverage ${pct(s.coverage, 0)}` : when(s.since).trim(),
      ),
      p.pick,
    ),
    cell(
      "slo-title",
      link(s.name, () => p.open(s.name)),
      createElement("span", { className: "slo-sub" }, s.service ?? ""),
    ),
    cell(
      "slo-stats",
      stat("SLI", ...sliCell(s)),
      stat(
        "Error budget",
        cell(`slo-num slo-${h.tone}`, b == null ? "—" : b > 0 ? pct(b, 0) : "0%"),
        cell("slo-sub", b == null || b > 0 ? "left" : `${rate(1 - b)} the budget spent`),
      ),
      stat("Burn rate", burnCell(s)),
    ),
    createElement(Burndown, { s }),
    s.groups &&
      cell(
        "slo-sub",
        link(`${s.groups.length} groups`, () => p.open(s.name)),
      ),
  );
}

// classified is an imported document's class, why, and its preview: an SLO's status over the
// last 24 hours, a view's panels.
const classified = (d: Imported): ReactNode =>
  createElement(
    "li",
    { key: `${d.kind}/${d.name}` },
    `${d.kind ?? "?"} ${d.name ?? ""}: ${d.class}`,
    d.error && ` — ${d.error}`,
    d.preview &&
      ` · ${d.preview.state}, SLI ${pct(d.preview.sli)}, budget left ${pct(d.preview.budget_remaining, 1)} over 24h`,
    d.panels && ` · ${d.panels.length} panels: ${d.panels.join(", ")}`,
    d.notes?.map((n) => createElement("small", { key: n }, n)),
  );

// Checked is content's documents classified, with their previews, and its normalized YAML,
// to save, or to export where the directory is read-only.
export function Checked(p: {
  client: DataFocalClient;
  content: string;
  file: string;
  onSaved: () => void;
}): ReactElement {
  const { client, content, file, onSaved } = p;
  const dry = useAnswer(
    content,
    async () => (await client.call("import_definitions", { content, dry_run: true })).data,
  );
  const writable = useWritable(client);
  const [failed, setFailed] = useState<Error>();
  const save = () =>
    void client.call("import_definitions", { content, dry_run: false }).then(onSaved, setFailed);
  return createElement(
    "div",
    null,
    createElement(Show<ImportDefinitionsResponse>, {
      answer: dry,
      render: (d) => [
        createElement("ul", { key: "l" }, d.documents.map(classified)),
        d.content === ""
          ? null
          : writable === true
            ? button("Save", save, true)
            : [
                button("Export", () => download(file, d.content), true),
                createElement("p", { key: "r", role: "note" }, readOnly),
              ],
        failed &&
          createElement(Failure, { key: "f", what: "Save failed", message: failed.message }),
        createElement("pre", { key: "y", "aria-label": "YAML" }, d.content),
      ],
    }),
  );
}

// ImportForm classifies a dropped or chosen file's documents, with their previews, before
// saving.
export function ImportForm(p: { client: DataFocalClient; onSaved: () => void }): ReactElement {
  const [content, setContent] = useState("");
  const read = (f: File | undefined) => void f?.text().then(setContent);
  return createElement(
    "div",
    { className: "slos" },
    createElement("header", null, createElement("h2", null, "Import")),
    createElement(
      "label",
      {
        className: "slo-drop",
        onDragOver: (e: DragEvent) => e.preventDefault(),
        onDrop: (e: DragEvent) => (e.preventDefault(), read(e.dataTransfer?.files[0])),
      },
      "Drop an OpenSLO or View file here, or choose one ",
      createElement("input", {
        type: "file",
        accept: ".yaml,.yml,.json",
        onChange: (e: { target: HTMLInputElement }) => read(e.target.files?.[0]),
      }),
    ),
    content &&
      createElement(Checked, {
        client: p.client,
        content,
        file: "definitions.yaml",
        onSaved: p.onSaved,
      }),
  );
}

// Choice is a suggestion an input offers: the value it fills in, and what it says about it.
export interface Choice {
  readonly value: string;
  readonly note?: string;
}

// Choices is an input's suggestions, as the datalist id names.
export function Choices({ id, choices }: { id: string; choices: readonly Choice[] }): ReactElement {
  return createElement(
    "datalist",
    { id },
    choices.map((c) => createElement("option", { key: c.value, value: c.value }, c.note)),
  );
}

// QueryField is a log query's input, completing the word before the cursor to what words
// suggests there, as the Logs query bar does.
export function QueryField(p: {
  value: string;
  words: (q: string, cursor: number) => readonly string[];
  set: (q: string) => void;
}): ReactNode {
  const [cursor, setCursor] = useState(p.value.length);
  const id = useId();
  const head = p.value.slice(0, cursor).replace(/[A-Za-z_][\w.]*$/, "");
  const tail = p.value.slice(cursor);
  const edit = (e: { currentTarget: HTMLInputElement }) => {
    setCursor(e.currentTarget.selectionStart ?? e.currentTarget.value.length);
    if (e.currentTarget.value !== p.value) {
      p.set(e.currentTarget.value);
    }
  };
  return [
    createElement("input", {
      key: "i",
      type: "text",
      className: "slo-query",
      spellCheck: false,
      value: p.value,
      list: id,
      onChange: edit,
      onSelect: edit,
    }),
    createElement(Choices, {
      key: "c",
      id,
      choices: p.words(p.value, cursor).map((w) => ({ value: head + w + tail })),
    }),
  ];
}
