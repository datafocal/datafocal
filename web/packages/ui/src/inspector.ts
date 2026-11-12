import { createElement, type ReactElement, type ReactNode, useState } from "react";

export interface Tab {
  readonly id: string;
  readonly label: string;
  // count, when set, is how many things the tab lists, shown beside its label.
  readonly count?: number;
  readonly render: () => ReactNode;
}

// Hero is a selection's headline figure: its value, what it means, and where it sits in the
// whole it is part of, as a share from start, both 0 to 1.
interface Hero {
  readonly value: string;
  readonly caption?: string;
  readonly start?: number;
  readonly share?: number;
  // color tints the share, as the selection is drawn elsewhere.
  readonly color?: string;
}

export interface InspectorProps {
  readonly title: string;
  // kicker says what kind of thing is selected, above its title; meta is a line under it.
  readonly kicker?: string;
  readonly meta?: ReactNode;
  readonly hero?: Hero;
  readonly actions?: ReactNode;
  readonly tabs: readonly Tab[];
  // tabKey names the kind of selection, so the next one of that kind reopens the same tab.
  readonly tabKey?: string;
  // onClose, when set, closes the panel from a button at its title's end.
  readonly onClose?: () => void;
}

const remembered = new Map<string, string>();

// HeroFigure is a selection's headline figure over a bar placing it in its whole.
function HeroFigure({ hero }: { hero: Hero }): ReactElement {
  const placed = hero.share !== undefined;
  return createElement(
    "div",
    { className: "insp-hero" },
    createElement(
      "p",
      null,
      createElement("strong", null, hero.value),
      hero.caption && createElement("span", null, hero.caption),
    ),
    placed &&
      createElement(
        "div",
        { className: "insp-place", "aria-hidden": true },
        createElement("span", {
          ref: (el: HTMLElement | null) => {
            el?.style.setProperty("left", `${(hero.start ?? 0) * 100}%`);
            el?.style.setProperty("width", `${Math.max(0.5, (hero.share ?? 0) * 100)}%`);
            if (hero.color) {
              el?.style.setProperty("--c", hero.color);
            }
          },
        }),
      ),
  );
}

// Head is the panel's head: what is selected over its title and a close button, a line about
// it, its headline figure, and what can be done with it.
function Head(p: InspectorProps): ReactElement {
  const close =
    p.onClose &&
    createElement(
      "button",
      {
        type: "button",
        className: "insp-x",
        "aria-label": `Close ${p.title}`,
        title: "Close (Esc)",
        onClick: p.onClose,
      },
      "×",
    );
  return createElement(
    "header",
    null,
    (p.kicker || close) &&
      createElement("div", { className: "insp-k" }, createElement("span", null, p.kicker), close),
    createElement("h2", null, p.title),
    p.meta && createElement("div", { className: "insp-meta" }, p.meta),
    p.hero && createElement(HeroFigure, { hero: p.hero }),
    p.actions && createElement("div", { className: "insp-acts" }, p.actions),
  );
}

// Inspector is the detail panel every view uses for its selection: what it is, its title and
// a line about it, its headline figure, what can be done with it, then its tabs.
export function Inspector(p: InspectorProps): ReactElement {
  const [chosen, setChosen] = useState<string>();
  const id = (p.tabKey !== undefined && remembered.get(p.tabKey)) || chosen;
  const tab = p.tabs.find((t) => t.id === id) ?? p.tabs[0];
  const select = (t: Tab) => {
    if (p.tabKey !== undefined) {
      remembered.set(p.tabKey, t.id);
    }
    setChosen(t.id);
  };
  return createElement(
    "section",
    { className: "insp", "aria-label": p.title },
    createElement(Head, p),
    p.tabs.length > 1 &&
      createElement(
        "div",
        { role: "tablist" },
        p.tabs.map((t) =>
          createElement(
            "button",
            {
              key: t.id,
              type: "button",
              role: "tab",
              "aria-selected": t === tab,
              onClick: () => select(t),
            },
            t.label,
            t.count !== undefined && createElement("span", { className: "insp-n" }, t.count),
          ),
        ),
      ),
    // The body scrolls, so it takes focus for keyboard scrolling whatever the tab holds.
    createElement("div", { className: "insp-b", tabIndex: 0 }, tab?.render()),
  );
}
