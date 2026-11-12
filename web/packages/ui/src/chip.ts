import { createElement, type ReactElement, type ReactNode } from "react";

export interface ChipProps {
  readonly tone?: "hot" | "warm";
  readonly title?: string;
  readonly onClick?: () => void;
  readonly children?: ReactNode;
}

// Chip is a small rounded label; it is a button when it does something.
export function Chip({ tone, title, onClick, children }: ChipProps): ReactElement {
  const className = tone ? `chip ${tone}` : "chip";
  return onClick
    ? createElement("button", { type: "button", className, title, onClick }, children)
    : createElement("span", { className, title }, children);
}

export interface NormalityChipProps {
  // percentile is where the entity sits among its peers, 0–100.
  readonly percentile: number;
  readonly peers: number;
  readonly window: string;
  readonly onClick?: () => void;
}

const compact = new Intl.NumberFormat("en", { notation: "compact" });

// NormalityChip says where an entity sits among similar ones: "p97 of 1.2K · 6h".
export function NormalityChip(p: NormalityChipProps): ReactElement {
  const tone = p.percentile >= 95 ? "hot" : p.percentile >= 80 ? "warm" : undefined;
  return createElement(
    Chip,
    {
      ...(tone && { tone }),
      ...(p.onClick && { onClick: p.onClick }),
      title: "Percentile among similar entities",
    },
    `p${p.percentile} of ${compact.format(p.peers)} · ${p.window}`,
  );
}

export interface Clause {
  readonly text: string;
  readonly onClick: () => void;
}

export interface AnnotationProps {
  // parts is the sentence, in order; a Clause is a link.
  readonly parts: readonly (string | Clause)[];
  readonly source?: string;
}

// Annotation is a one-sentence finding whose clauses link to their evidence.
export function Annotation({ parts, source }: AnnotationProps): ReactElement {
  return createElement(
    "p",
    { className: "annot" },
    createElement(
      "span",
      null,
      parts.map((p, i) =>
        typeof p === "string"
          ? p
          : createElement(
              "button",
              { key: i, type: "button", className: "link", onClick: p.onClick },
              p.text,
            ),
      ),
    ),
    source && createElement("small", null, source),
  );
}
