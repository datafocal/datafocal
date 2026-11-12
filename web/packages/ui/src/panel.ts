import { createElement, type ReactElement, type ReactNode } from "react";

// The pieces a view's page is built of: a framed panel with its title and an action, the
// tiles over a page, rows of a label with a bar and a figure, and a number with a bar beside
// it.

// width sets an element's width to a share, as inline styles are not allowed.
const width = (share: number) => (el: HTMLElement | null) =>
  el?.style.setProperty("width", `${Math.max(0, Math.min(1, share)) * 100}%`);

// Meter is a number with a bar beside it showing its share of the largest in its column.
export const Meter = (p: { text: string; share: number }): ReactElement =>
  createElement(
    "span",
    { className: "meter" },
    p.text,
    createElement("span", null, createElement("i", { ref: width(p.share) })),
  );

export interface PanelProps {
  readonly title: string;
  // action is a link at the title's end, as "Open ›".
  readonly action?: { readonly label: string; readonly onClick: () => void } | undefined;
  // big is the panel's headline number and what it counts.
  readonly big?: readonly [value: string, unit: string] | undefined;
  readonly children?: ReactNode;
}

// Panel is a framed section with its title, an action at the title's end and a headline number.
export const Panel = (p: PanelProps): ReactElement =>
  createElement(
    "section",
    { className: "panel card", "aria-label": p.title },
    createElement(
      "h3",
      null,
      p.title,
      p.action &&
        createElement(
          "button",
          { type: "button", className: "link", onClick: p.action.onClick },
          p.action.label,
        ),
    ),
    p.big &&
      createElement("div", { className: "big" }, p.big[0], createElement("small", null, p.big[1])),
    p.children,
  );

export interface Kpi {
  readonly label: string;
  readonly value: string;
  // detail says what the value is out of or against.
  readonly detail?: string | undefined;
  // tone marks a value past its threshold.
  readonly tone?: "bad" | undefined;
}

// Kpis are the tiles over a page: each a label, a value and what it is against.
export const Kpis = (p: { tiles: readonly Kpi[] }): ReactElement =>
  createElement(
    "div",
    { className: "kpis" },
    p.tiles.map((t) =>
      createElement(
        "div",
        { key: t.label, className: t.tone ? `kpi ${t.tone}` : "kpi" },
        createElement("small", null, t.label),
        createElement("strong", null, t.value),
        t.detail && createElement("span", null, t.detail),
      ),
    ),
  );

export interface BarRowProps {
  readonly label: string;
  // share is the row's bar, of the largest in its panel.
  readonly share: number;
  readonly figure: string;
  readonly pressed?: boolean | undefined;
  readonly onClick?: (() => void) | undefined;
}

// BarRow is a panel's row: its label with a bar beside it and a figure at its end.
export const BarRow = (p: BarRowProps): ReactElement =>
  createElement(
    "button",
    { type: "button", className: "prow", "aria-pressed": p.pressed, onClick: p.onClick },
    createElement(Meter, { text: p.label, share: p.share }),
    createElement("span", null, p.figure),
  );
