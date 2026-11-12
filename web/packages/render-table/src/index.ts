// RowSource is the rows a table shows, fetched a window at a time.
export interface RowSource<RowView> {
  total(): number;
  rows(from: number, to: number): Promise<readonly RowView[]>;
}

// TableScroller shows a source's visible rows, plus overscan, as DOM rows in el.
export interface TableScroller<RowView> {
  mount(el: HTMLElement, src: RowSource<RowView>, render: (row: RowView) => Node): void;
  scrollToIndex(i: number): void;
  // refresh redraws every row, for a source whose rows changed.
  refresh(): void;
  // shift scrolls by rows, so rows put before the visible ones leave the view where it was.
  shift(rows: number): void;
  dispose(): void;
}

// maxHeight keeps the scrolled element under Chrome's 33.5M px cap; past it, a pixel of
// scroll moves more than a pixel of rows.
const maxHeight = 1e7;

class Scroller<RowView> implements TableScroller<RowView> {
  private readonly scroller = document.createElement("div");
  private readonly spacer = this.scroller.appendChild(document.createElement("div"));
  private readonly view = this.spacer.appendChild(document.createElement("div"));
  private readonly shown = new Map<number, HTMLElement>();
  private readonly resize = new ResizeObserver(() => this.schedule());
  private src: RowSource<RowView> = { total: () => 0, rows: async () => [] };
  private render: (row: RowView) => Node = () => document.createTextNode("");
  private frame = 0;
  // generation counts refreshes, so rows read before one are not shown after it.
  private generation = 0;
  // range is the rows to show, first to last, and the offset of the scrolled-to pixel.
  private range = { first: 0, last: 0, top: 0 };

  constructor(
    private readonly rowHeight: number,
    private readonly overscan: number,
  ) {
    this.scroller.setAttribute("role", "table");
    // Focusable, so a keyboard can scroll it when its rows hold nothing that takes focus.
    this.scroller.tabIndex = 0;
    Object.assign(this.scroller.style, { height: "100%", overflowY: "auto" });
    Object.assign(this.view.style, { position: "sticky", top: "0", overflow: "hidden" });
  }

  mount(el: HTMLElement, src: RowSource<RowView>, render: (row: RowView) => Node): void {
    this.src = src;
    this.render = render;
    el.replaceChildren(this.scroller);
    this.scroller.addEventListener("scroll", this.schedule, { passive: true });
    this.resize.observe(this.scroller);
    this.schedule();
  }

  scrollToIndex(i: number): void {
    this.size(this.src.total());
    this.scroller.scrollTop = (i * this.rowHeight) / this.scale();
    this.schedule();
  }

  refresh(): void {
    for (const node of this.shown.values()) {
      node.remove();
    }
    this.shown.clear();
    this.generation++;
    this.schedule();
  }

  shift(rows: number): void {
    this.size(this.src.total());
    this.scroller.scrollTop += (rows * this.rowHeight) / this.scale();
    this.schedule();
  }

  dispose(): void {
    cancelAnimationFrame(this.frame);
    this.resize.disconnect();
    this.scroller.removeEventListener("scroll", this.schedule);
    this.scroller.remove();
  }

  private readonly schedule = () => {
    this.frame ||= requestAnimationFrame(() => this.update());
  };

  // scale is how many pixels of rows one pixel of scroll moves.
  private scale(): number {
    const height = this.src.total() * this.rowHeight;
    const range = Math.min(height, maxHeight) - this.scroller.clientHeight;
    return range > 0 ? (height - this.scroller.clientHeight) / range : 1;
  }

  private size(total: number): void {
    this.spacer.style.height = `${Math.min(total * this.rowHeight, maxHeight)}px`;
    this.view.style.height = `${this.scroller.clientHeight}px`;
    this.scroller.setAttribute("aria-rowcount", String(total));
  }

  private update(): void {
    this.frame = 0;
    const total = this.src.total();
    this.size(total);
    const top = this.scroller.scrollTop * this.scale();
    const first = Math.max(0, Math.floor(top / this.rowHeight) - this.overscan);
    const visible = Math.ceil((top + this.scroller.clientHeight) / this.rowHeight);
    const last = Math.min(total, visible + this.overscan);
    this.range = { first, last, top };
    for (const [i, node] of this.shown) {
      if (i < first || i >= last) {
        node.remove();
        this.shown.delete(i);
      } else {
        this.place(i, node);
      }
    }
    if (this.shown.size < last - first) {
      const generation = this.generation;
      void this.src.rows(first, last).then((rows) => {
        if (generation !== this.generation) {
          return;
        }
        this.size(this.src.total());
        rows.forEach((row, k) => this.add(first + k, row));
      });
    }
  }

  // add shows row i, unless it is shown already or has scrolled out of range meanwhile.
  private add(i: number, row: RowView): void {
    const { first, last } = this.range;
    if (this.shown.has(i) || i < first || i >= last) {
      return;
    }
    const node = this.view.appendChild(document.createElement("div"));
    node.setAttribute("role", "row");
    node.setAttribute("aria-rowindex", String(i + 1));
    Object.assign(node.style, { position: "absolute", left: "0", right: "0", contain: "strict" });
    node.style.height = `${this.rowHeight}px`;
    const cell = node.appendChild(document.createElement("div"));
    cell.setAttribute("role", "cell");
    cell.style.height = "100%";
    cell.append(this.render(row));
    this.shown.set(i, node);
    this.place(i, node);
  }

  private place(i: number, node: HTMLElement): void {
    node.style.transform = `translateY(${i * this.rowHeight - this.range.top}px)`;
  }
}

// createTableScroller returns a scroller of rows rowHeight CSS pixels tall.
export function createTableScroller<RowView>(opts: {
  rowHeight: number;
  overscan: number;
}): TableScroller<RowView> {
  return new Scroller(opts.rowHeight, opts.overscan);
}
