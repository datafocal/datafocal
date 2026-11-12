import { createElement, type KeyboardEvent, type PointerEvent, type ReactElement } from "react";

export interface SplitterProps {
  // label names the pane the splitter sizes, for assistive tech.
  readonly label: string;
  // name is the CSS variable on the splitter's parent that holds the pane's width, and the key
  // the width is kept under between visits.
  readonly name: string;
  readonly initial: number;
  readonly min: number;
  readonly max: number;
  // edge is the side of the splitter the pane is on.
  readonly edge: "left" | "right";
}

const storeKey = (name: string) => `datafocal.split.${name}`;

// kept is the width last left for name, or initial; 0 is a collapsed pane.
function kept(p: SplitterProps): number {
  const v = Number(localStorage.getItem(storeKey(p.name)) ?? Number.NaN);
  return Number.isFinite(v) && (v === 0 || (v >= p.min && v <= p.max)) ? v : p.initial;
}

// apply sets the pane's width on the splitter's parent, collapsing it under min.
function apply(el: HTMLElement, p: SplitterProps, width: number): number {
  const w = width < p.min / 2 ? 0 : Math.min(Math.max(width, p.min), p.max);
  el.parentElement?.style.setProperty(`--${p.name}`, `${w}px`);
  el.parentElement?.toggleAttribute(`data-${p.name}-collapsed`, w === 0);
  el.setAttribute("aria-valuenow", String(w));
  return w;
}

const store = (p: SplitterProps, w: number) => localStorage.setItem(storeKey(p.name), String(w));

// Splitter is a bar a pane's width is dragged by, written straight to the page so dragging
// renders nothing. Dragged under half its least width the pane collapses; arrows nudge it,
// and Enter or a double-click collapses or restores it.
export function Splitter(p: SplitterProps): ReactElement {
  const sign = p.edge === "left" ? 1 : -1;
  const width = (el: HTMLElement) => Number(el.getAttribute("aria-valuenow") ?? p.initial);
  const toggle = (el: HTMLElement) => store(p, apply(el, p, width(el) === 0 ? p.initial : 0));
  return createElement("div", {
    role: "separator",
    tabIndex: 0,
    className: "split",
    "aria-label": `Resize ${p.label}`,
    "aria-orientation": "vertical",
    "aria-valuemin": 0,
    "aria-valuemax": p.max,
    title: "Drag to resize, double-click to collapse",
    ref: (el: HTMLDivElement | null) => {
      if (el && !el.hasAttribute("aria-valuenow")) {
        apply(el, p, kept(p));
      }
    },
    onPointerDown: (e: PointerEvent<HTMLDivElement>) => {
      const el = e.currentTarget;
      const [x0, w0] = [e.clientX, width(el)];
      el.setPointerCapture(e.pointerId);
      const move = (m: globalThis.PointerEvent) => apply(el, p, w0 + sign * (m.clientX - x0));
      const up = () => {
        el.removeEventListener("pointermove", move);
        el.removeEventListener("pointerup", up);
        store(p, width(el));
      };
      el.addEventListener("pointermove", move);
      el.addEventListener("pointerup", up);
    },
    onDoubleClick: (e: { currentTarget: HTMLDivElement }) => toggle(e.currentTarget),
    onKeyDown: (e: KeyboardEvent<HTMLDivElement>) => {
      const el = e.currentTarget;
      const step = { ArrowLeft: -sign * 20, ArrowRight: sign * 20 }[e.key];
      if (step !== undefined) {
        e.preventDefault();
        store(p, apply(el, p, (width(el) || p.min) + step));
      } else if (e.key === "Enter") {
        toggle(el);
      }
    },
  });
}
