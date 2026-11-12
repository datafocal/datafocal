import { createElement, type ReactElement, type ReactNode } from "react";

export interface EvidenceLine {
  readonly time: string;
  readonly service: string;
  readonly message: string;
  readonly error?: boolean;
}

export interface Evidence {
  readonly title: string;
  // lines are the log lines backing the result.
  readonly lines: readonly EvidenceLine[];
  // computed says how the result was computed.
  readonly computed: ReactNode;
  // query, when set, finds the lines in Logs.
  readonly query?: string;
}

export interface EvidenceDrawerProps {
  readonly evidence: Evidence;
  readonly onClose: () => void;
  readonly onOpenLogs: (query: string) => void;
}

// EvidenceDrawer shows what backs a result below the view, so the view stays visible.
export function EvidenceDrawer({
  evidence,
  onClose,
  onOpenLogs,
}: EvidenceDrawerProps): ReactElement {
  const { query } = evidence;
  return createElement(
    "section",
    { className: "drawer", "aria-label": "Evidence" },
    createElement(
      "header",
      null,
      createElement("h2", null, evidence.title),
      createElement(
        "div",
        { className: "drawer-a" },
        query !== undefined &&
          createElement(
            "button",
            { type: "button", className: "tbtn", onClick: () => onOpenLogs(query) },
            "Open in Logs",
          ),
        createElement(
          "button",
          { type: "button", className: "tbtn", onClick: onClose },
          "Close ",
          createElement("span", { className: "kbd" }, "Esc"),
        ),
      ),
    ),
    createElement(
      "div",
      { className: "drawer-b" },
      createElement(
        "div",
        { className: "drawer-l" },
        createElement("h3", null, `Lines behind it · ${evidence.lines.length}`),
        evidence.lines.length === 0
          ? "No backing lines."
          : evidence.lines.map((l, i) =>
              createElement(
                "div",
                { key: i, className: l.error ? "ln err" : "ln" },
                createElement("span", null, l.time),
                createElement("span", null, l.service),
                createElement("span", null, l.message),
              ),
            ),
      ),
      createElement("div", { className: "drawer-c" }, evidence.computed),
    ),
  );
}
