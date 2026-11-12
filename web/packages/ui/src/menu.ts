import { createElement, Fragment, type ReactElement, useId } from "react";

export interface MenuItem {
  readonly label: string;
  readonly run: () => void;
  readonly shortcut?: string;
  // checked makes the item a toggle, ticked when true.
  readonly checked?: boolean;
}

export interface MenuSection {
  readonly title?: string;
  readonly items: readonly MenuItem[];
}

export interface MenuProps {
  readonly label: string;
  readonly sections: readonly MenuSection[];
}

// Menu is the "⋯" button and the popover of secondary controls it opens.
export function Menu({ label, sections }: MenuProps): ReactElement {
  const id = useId();
  const item = (it: MenuItem) =>
    createElement(
      "button",
      {
        key: it.label,
        type: "button",
        role: it.checked === undefined ? "menuitem" : "menuitemcheckbox",
        "aria-checked": it.checked,
        popoverTarget: id,
        popoverTargetAction: "hide",
        onClick: it.run,
      },
      it.label,
      it.shortcut && createElement("span", { className: "kbd" }, it.shortcut),
    );
  return createElement(
    Fragment,
    null,
    createElement(
      "button",
      { type: "button", className: "tbtn", popoverTarget: id, "aria-label": label, title: label },
      "⋯",
    ),
    createElement(
      "div",
      { id, popover: "auto", role: "menu", "aria-label": label, className: "menu" },
      sections.map((s, i) =>
        createElement(
          "div",
          { key: i, role: "group", "aria-label": s.title },
          s.title && createElement("div", { "aria-hidden": true }, s.title),
          s.items.map(item),
        ),
      ),
    ),
  );
}
