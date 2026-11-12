import type { FieldStat, DataFocalClient, LineRef, LogLine } from "@datafocal/api";
import { createTail, type Tail, type TailStats } from "@datafocal/data";
import { createTableScroller, type TableScroller } from "@datafocal/render-table";
import { createElement, type ReactElement, useEffect, useRef, useState } from "react";
import { type Acts, actsNode, fieldNode, type Item, isLine, itemsOf } from "./detail";
import { lineNode } from "./row";
import type { Stream } from "./stream";

const rowHeight = 24;
// nearTop is how many rows from the top scrolling back reads the page before.
const nearTop = 30;
const charWidth = 7.3;
const capacity = 10_000;
const tick = 500;

// StreamStatus is what the status line says about the stream.
export interface StreamStatus {
  readonly held: number;
  readonly more: boolean;
  readonly following: boolean;
  // unseen is the lines appended since the table was last at the newest.
  readonly unseen: number;
  readonly tail: TailStats | undefined;
  readonly jump: () => void;
}

// Shown is how the table draws lines: their fields ranked, the words to mark, the line to
// pick out, and what an open line's detail does.
interface Shown {
  readonly fields: readonly FieldStat[];
  readonly marks: RegExp | null;
  readonly focus: LineRef | undefined;
  readonly acts: Omit<Acts, "close">;
}

// LineTable shows a stream oldest first in el: following the newest line while scrolled to
// it, reading older lines as it nears the top, appending a tail's lines while live, and
// opening a line's detail under it on a click.
class LineTable {
  private readonly scroller: TableScroller<Item> = createTableScroller({ rowHeight, overscan: 10 });
  private readonly table: HTMLElement | null;
  private readonly tail: Tail;
  private readonly resize: ResizeObserver;
  private readonly acts: Acts;
  private readonly onStatus: (s: StreamStatus) => void;
  private items: readonly Item[] = [];
  private open = new Set<LogLine>();
  private cols: number;
  private following = true;
  private unseen = 0;
  private loading = false;
  private timer = 0;

  constructor(
    private readonly el: HTMLElement,
    private readonly stream: Stream,
    private readonly shown: Shown,
    out: { readonly client: DataFocalClient; readonly onStatus: (s: StreamStatus) => void },
  ) {
    this.onStatus = out.onStatus;
    this.tail = createTail(out.client, { capacity });
    this.cols = Math.floor(el.clientWidth / charWidth) || 120;
    this.acts = { ...shown.acts, close: (l) => this.toggle(l) };
    this.items = this.build();
    const src = {
      total: () => this.items.length,
      rows: (a: number, b: number) => Promise.resolve(this.items.slice(a, b)),
    };
    this.scroller.mount(el, src, (item) => this.render(item));
    this.table = el.querySelector<HTMLElement>("[role=table]");
    this.table?.addEventListener("scroll", this.scrolled, { passive: true });
    this.resize = new ResizeObserver(this.resized);
    this.resize.observe(el);
    this.arrive();
  }

  // live starts or stops appending the tail's lines, followed from the newest line held.
  live(on: boolean): void {
    this.tail.stop();
    clearInterval(this.timer);
    if (!on) {
      return;
    }
    const newest = this.stream.lines().at(-1);
    const { selector } = this.stream;
    const time = { ...selector.time, ...(newest && { from: newest.timestamp }), to: "now" };
    this.tail.start({ ...selector, time });
    this.timer = window.setInterval(() => void this.take(), tick);
  }

  dispose(): void {
    this.tail.stop();
    clearInterval(this.timer);
    this.resize.disconnect();
    this.table?.removeEventListener("scroll", this.scrolled);
    this.scroller.dispose();
  }

  // arrive scrolls to the focus line, a few lines below the top, or else to the newest.
  private arrive(): void {
    const at = this.shown.focus ? this.stream.indexOf(this.shown.focus) : -1;
    this.following = at < 0;
    this.scroller.scrollToIndex(at < 0 ? this.items.length - 1 : Math.max(0, at - 5));
    this.report();
  }

  private build(): readonly Item[] {
    return itemsOf(this.stream.lines(), this.open, this.cols, this.shown.fields);
  }

  // redraw rebuilds the rows, keeping the view on the newest line when following, or else
  // where it was once shift rows went before it.
  private redraw(shift = 0): void {
    this.items = this.build();
    this.scroller.refresh();
    if (this.following) {
      this.scroller.scrollToIndex(this.items.length - 1);
    } else if (shift !== 0) {
      this.scroller.shift(shift);
    }
    this.report();
  }

  private report(): void {
    this.onStatus({
      held: this.stream.lines().length,
      more: this.stream.more(),
      following: this.following,
      unseen: this.unseen,
      tail: this.tail.stats(),
      jump: () => {
        this.following = true;
        this.scroller.scrollToIndex(this.items.length - 1);
      },
    });
  }

  // take appends what the tail holds that the stream does not.
  private async take(): Promise<void> {
    const { added, dropped } = this.stream.append(await this.tail.window(0, capacity));
    if (added === 0) {
      this.report();
      return;
    }
    this.unseen += this.following ? 0 : added;
    this.redraw(this.following ? 0 : -dropped);
  }

  private toggle(l: LogLine): void {
    if (!this.open.delete(l)) {
      this.open.add(l);
    }
    this.redraw();
  }

  private render(item: Item): Node {
    if (!isLine(item)) {
      return item.kind === "acts"
        ? actsNode(item.line, this.acts)
        : fieldNode(item, this.shown.marks, this.acts);
    }
    const { fields, marks, focus } = this.shown;
    const node = lineNode(item, fields, marks, this.open.has(item));
    node.classList.toggle(
      "sel",
      focus !== undefined && focus.segment === item.segment && focus.row === item.row,
    );
    node.onclick = () => this.toggle(item);
    node.onkeydown = (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        this.toggle(item);
      }
    };
    return node;
  }

  // scrolled notes whether the table is at the newest line, and near the top reads the
  // page of older lines.
  private readonly scrolled = () => {
    const t = this.table;
    if (!t) {
      return;
    }
    const following = t.scrollTop + t.clientHeight >= t.scrollHeight - rowHeight;
    if (following !== this.following) {
      this.following = following;
      this.unseen = 0;
      this.report();
    }
    if (t.scrollTop < nearTop * rowHeight && this.stream.more() && !this.loading) {
      this.loading = true;
      void this.stream.older().then((added) => {
        this.loading = false;
        this.redraw(added);
      });
    }
  };

  private readonly resized = () => {
    const cols = Math.floor(this.el.clientWidth / charWidth);
    if (cols !== this.cols && cols > 0) {
      this.cols = cols;
      if (this.open.size > 0) {
        this.redraw();
      }
    }
  };
}

export interface StreamTableProps extends Shown {
  readonly client: DataFocalClient;
  readonly stream: Stream;
  // live is whether the tail appends to the stream: the range runs to now and isn't paused.
  readonly live: boolean;
  readonly onStatus: (s: StreamStatus) => void;
}

// StreamTable mounts a LineTable for the stream, live while live is.
export function StreamTable(p: StreamTableProps): ReactElement {
  const [el, setEl] = useState<HTMLDivElement | null>(null);
  const table = useRef<LineTable | null>(null);
  const { stream, fields, marks, focus, acts, onStatus, client, live } = p;
  useEffect(() => {
    if (!el) {
      return;
    }
    const t = new LineTable(el, stream, { fields, marks, focus, acts }, { client, onStatus });
    table.current = t;
    return () => {
      t.dispose();
      table.current = null;
    };
  }, [el, stream, fields, marks, focus, acts, onStatus, client]);
  useEffect(() => {
    table.current?.live(live);
  }, [el, stream, fields, marks, focus, acts, onStatus, client, live]);
  return createElement("div", { ref: setEl, className: "logs-t" });
}
