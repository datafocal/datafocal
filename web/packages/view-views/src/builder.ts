import type { DataFocalClient, TimeSpec } from "@datafocal/api";
import { suggest } from "@datafocal/query";
import { navigate, useParams, useQueryRange } from "@datafocal/shell";
import { button, Checked, Choices, QueryField, Show, useAnswer } from "@datafocal/ui";
import { createElement, type ReactElement, type ReactNode, useId, useState } from "react";
import { ViewPanel } from "./panel";

interface Panel {
  title?: string | undefined;
  kind: string;
  query?: string;
  slo?: string;
  grid: { x: number; y: number; w: number; h: number };
}
interface Variable {
  name: string;
  column: string;
  default?: string | undefined;
  allowAll?: boolean;
}
// Doc is a View document as the builder edits it.
interface Doc {
  apiVersion: string;
  kind: string;
  metadata: { name: string; displayName?: string };
  spec: { time?: TimeSpec; variables?: Variable[]; panels: Panel[] };
}

type Set = (v: string) => void;

const kinds = ["timeseries", "stat", "table", "lines", "patterns", "slo"];
const widths = ["3", "4", "6", "8", "12"];
const yesNo = ["no", "yes"];
const heights: Readonly<Record<string, number>> = { small: 2, medium: 4, tall: 6 };

const isDoc = (v: unknown): v is Doc => typeof v === "object" && v !== null && "spec" in v;
const slug = (t: string) =>
  t
    .toLowerCase()
    .match(/[a-z0-9]+/g)
    ?.join("-") ?? "";

// kindOf is how a query added from elsewhere draws: its patterns, a metric over time, or lines.
const kindOf = (q: string) =>
  /\|\s*patterns\b/.test(q) ? "patterns" : /^\s*\w/.test(q) ? "timeseries" : "lines";

// packed is panels laid out in rows of their widths, in order.
function packed(panels: readonly Panel[]): Panel[] {
  let [x, y, tallest] = [0, 0, 0];
  return panels.map((p) => {
    const { w, h } = p.grid;
    if (x + w > 12) {
      [x, y, tallest] = [0, y + tallest, 0];
    }
    const grid = { x, y, w, h };
    [x, tallest] = [x + w, Math.max(tallest, h)];
    return { ...p, grid };
  });
}

// added is doc with q appended as a panel below the others, keeping their layout.
const added = (doc: Doc, q: string): Doc => {
  const y = Math.max(0, ...doc.spec.panels.map((p) => p.grid.y + p.grid.h));
  const panel = { kind: kindOf(q), query: q, grid: { x: 0, y, w: 6, h: 4 } };
  return { ...doc, spec: { ...doc.spec, panels: [...doc.spec.panels, panel] } };
};

const fresh = (time: TimeSpec): Doc => ({
  apiVersion: "datafocal/v1",
  kind: "View",
  metadata: { name: "" },
  spec: { time, variables: [], panels: [] },
});

// input is a labelled text input, offering the datalist named list, or with options a select.
const input = (label: string, value: string, set: Set, options?: string[] | string) => {
  const onChange = (e: { target: { value: string } }) => set(e.target.value);
  const opts = Array.isArray(options) && options.map((o) => createElement("option", { key: o }, o));
  const field = opts
    ? createElement("select", { value, onChange }, opts)
    : createElement("input", { type: "text", value, onChange, list: options });
  return createElement("label", { key: label }, `${label} `, field);
};

const row = (label: string, ...children: ReactNode[]) =>
  createElement(
    "div",
    { key: label, className: "vw-row", role: "group", "aria-label": label },
    ...children,
  );

// replaced is items with the one at i changed by v, or with a null v left out.
const replaced = <T>(items: readonly T[], i: number, v: Partial<T> | null): T[] =>
  items.flatMap((x, j) => (j !== i ? [x] : v ? [{ ...x, ...v }] : []));

// Form is the document, its setter, the fields to suggest and the range panels preview over.
interface Form {
  readonly client: DataFocalClient;
  readonly doc: Doc;
  readonly set: (d: Doc) => void;
  readonly fields: readonly string[];
  readonly time: TimeSpec;
}

const setSpec = (f: Form, spec: Partial<Doc["spec"]>) =>
  f.set({ ...f.doc, spec: { ...f.doc.spec, ...spec } });

// variables are each variable's name, column (from the fields columns lists), default and All.
function variables(f: Form, columns: string): ReactNode[] {
  const vars = f.doc.spec.variables ?? [];
  const put = (i: number, v: Partial<Variable> | null) =>
    setSpec(f, { variables: replaced(vars, i, v) });
  return [
    ...vars.map((v, i) =>
      row(
        `Variable ${i + 1}`,
        input("Name", v.name, (name) => put(i, { name })),
        input("Column", v.column, (column) => put(i, { column }), columns),
        input("Default", v.default ?? "", (d) => put(i, { default: d || undefined })),
        input("All", v.allowAll ? "yes" : "no", (a) => put(i, { allowAll: a === "yes" }), yesNo),
        button("Remove", () => put(i, null)),
      ),
    ),
    button("Add variable", () => setSpec(f, { variables: [...vars, { name: "", column: "" }] })),
  ];
}

// preview is panel i read with each variable at its default, else All.
const preview = (f: Form, panel: Panel, index: number) =>
  createElement(ViewPanel, {
    client: f.client,
    view: f.doc.metadata.name,
    index,
    panel: { ...panel, title: panel.title ?? panel.slo ?? panel.query ?? "" },
    time: f.time,
    now: undefined,
    vars: Object.fromEntries((f.doc.spec.variables ?? []).map((v) => [v.name, v.default ?? "*"])),
  });

// PanelForm is a panel's kind, title, query, size and place, over its preview. A change of
// size or order lays every panel out again; until then a file's own layout is kept.
function PanelForm({ f, i }: { f: Form; i: number }): ReactNode {
  const panels = f.doc.spec.panels;
  const panel = panels[i];
  if (!panel) {
    return null;
  }
  const vars = f.doc.spec.variables ?? [];
  const put = (next: Panel[], pack = false) => setSpec(f, { panels: pack ? packed(next) : next });
  const edit = (v: Partial<Panel>, pack = false) => put(replaced(panels, i, v), pack);
  const grid = (g: Partial<Panel["grid"]>) => edit({ grid: { ...panel.grid, ...g } }, true);
  const words = (q: string, cursor: number) =>
    /!?=\s*$/.test(q.slice(0, cursor))
      ? vars.map((v) => `"$${v.name}"`)
      : suggest(q, cursor, f.fields);
  const query = createElement(QueryField, {
    value: panel.query ?? "",
    words,
    set: (q) => edit({ query: q }),
  });
  const height = Object.keys(heights).find((k) => heights[k] === panel.grid.h) ?? "";
  const up = panels.map((x, j) => (j === i - 1 ? panel : j === i ? (panels[j - 1] ?? x) : x));
  return row(
    `Panel ${i + 1}`,
    input("Kind", panel.kind, (kind) => edit({ kind }), kinds),
    input("Title", panel.title ?? "", (t) => edit({ title: t || undefined })),
    panel.kind === "slo"
      ? input("SLO", panel.slo ?? "", (slo) => edit({ slo }))
      : createElement("label", { key: "q" }, "Query ", query),
    input("Width", String(panel.grid.w), (w) => grid({ w: Number(w) }), widths),
    input("Height", height, (h) => grid({ h: heights[h] ?? 4 }), Object.keys(heights)),
    i > 0 && button("Up", () => put(up, true)),
    button("Remove", () => put(replaced(panels, i, null), true)),
    preview(f, panel, i),
  );
}

// Body is the form beside the YAML and Save or Export; the name follows the title until typed.
function Body({ f, saved }: { f: Form; saved: boolean }): ReactElement {
  const { doc, set } = f;
  const columns = useId();
  const [named, setNamed] = useState(saved);
  const meta = (m: Partial<Doc["metadata"]>) =>
    set({ ...doc, metadata: { ...doc.metadata, ...m } });
  const section = (title: string, ...children: ReactNode[]) =>
    createElement(
      "fieldset",
      { key: title, className: "panel" },
      createElement("legend", null, title),
      ...children,
    );
  return createElement(
    "div",
    { className: "vw-builder" },
    createElement(
      "form",
      { onSubmit: (e: { preventDefault: () => void }) => e.preventDefault() },
      createElement(Choices, { id: columns, choices: f.fields.map((value) => ({ value })) }),
      section(
        "View",
        input("Title", doc.metadata.displayName ?? "", (t) =>
          meta({ displayName: t, ...(!named && { name: slug(t) }) }),
        ),
        saved
          ? createElement("span", { key: "n", className: "vw-hint" }, `Name ${doc.metadata.name}`)
          : input("Name", doc.metadata.name, (name) => (setNamed(true), meta({ name }))),
      ),
      section("Variables", ...variables(f, columns)),
      section(
        "Panels",
        ...doc.spec.panels.map((_, i) => createElement(PanelForm, { key: i, f, i })),
        button("Add panel", () => set(added(doc, ""))),
      ),
    ),
    doc.metadata.name
      ? createElement(Checked, {
          client: f.client,
          content: JSON.stringify(doc),
          file: `${doc.metadata.name}.yaml`,
          onSaved: () => navigate({ view: "views" }),
        })
      : createElement("p", { className: "vw-hint" }, "Title the view to check and save it."),
  );
}

// Builder is a view's form beside its YAML: a new view without name, the
// named one's otherwise, with the query param appended as a panel when given.
export function Builder(p: { client: DataFocalClient; name: string | undefined }): ReactElement {
  const { client, name } = p;
  const { query } = useParams();
  const time = useQueryRange();
  const loaded = useAnswer(name ?? "", async () => {
    const got: unknown = name ? (await client.call("get_view", { name })).data.document : undefined;
    const doc = isDoc(got) ? got : fresh(time);
    return query ? added(doc, query) : doc;
  });
  const fields = useAnswer(JSON.stringify(time), async () =>
    (await client.call("rank_fields", { selector: { time } })).data.fields.map((f) => f.name),
  );
  const [edited, set] = useState<Doc>();
  const doc = edited ?? (loaded instanceof Error ? undefined : loaded);
  const suggested = Array.isArray(fields) ? fields : [];
  return createElement(
    "div",
    { className: "views" },
    createElement("header", null, createElement("h2", null, name ? `Edit ${name}` : "New view")),
    doc === undefined
      ? createElement(Show<Doc>, { answer: loaded, render: () => null })
      : createElement(Body, { f: { client, doc, set, fields: suggested, time }, saved: !!name }),
  );
}
