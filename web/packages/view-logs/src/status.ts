import type { TailStats } from "@datafocal/data";
import { Show } from "@datafocal/ui";
import { createElement, type ReactElement } from "react";
import type { StreamStatus } from "./table";

export interface StatusBarProps {
  readonly status: StreamStatus | undefined;
  // tailing is whether the range runs to now; paused, whether the reader stopped the tail.
  readonly tailing: boolean;
  readonly paused: boolean;
  readonly onLive: () => void;
}

const count = (n: number) => n.toLocaleString("en-US");

// rateLine is how many lines a second the tail brings.
const rateLine = ({ linesPerSec }: TailStats) => `${Math.round(linesPerSec)} lines/s`;

// held says how many lines the stream holds and whether older ones are still to read.
function held(s: StreamStatus | undefined): string {
  if (!s) {
    return "";
  }
  return `${count(s.held)} lines ${s.more ? "· scroll up for older" : "· start of range"}`;
}

// Live is the tail's switch: on, the stream follows new lines; off, it holds still.
function Live({ tailing, paused, onLive }: Omit<StatusBarProps, "status">): ReactElement {
  const on = tailing && !paused;
  return createElement(
    "button",
    {
      type: "button",
      className: "tbtn live-btn",
      "aria-pressed": on,
      title: on ? "Stop following new lines" : "Follow new lines as they arrive",
      onClick: onLive,
    },
    createElement("i", { className: on ? "live-dot" : "live-dot off", "aria-hidden": true }),
    on ? "Live" : tailing ? "Paused" : "Go live",
  );
}

// StatusBar is the stream's state: whether it is live and how fast lines arrive, how many
// it holds, and a way back to the newest when scrolled away from new ones.
export function StatusBar(p: StatusBarProps): ReactElement {
  const s = p.status;
  const tail = s?.tail;
  return createElement(
    "div",
    { className: "live-bar" },
    createElement(Live, p),
    p.tailing &&
      !p.paused &&
      tail &&
      createElement(Show<TailStats>, {
        answer: tail.error ?? tail,
        render: (t) => createElement("span", { className: "live-rate" }, rateLine(t)),
      }),
    createElement("span", { className: "live-held" }, held(s)),
    createElement("span", { className: "sp" }),
    s &&
      !s.following &&
      createElement(
        "button",
        { type: "button", className: "tbtn jump", onClick: s.jump },
        s.unseen > 0 ? `↓ ${count(s.unseen)} new lines` : "↓ Newest",
      ),
  );
}
