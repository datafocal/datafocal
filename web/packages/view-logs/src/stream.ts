import type {
  DataFocalClient,
  LineRef,
  LogLine,
  SearchResponse,
  Selector,
  TimeSpec,
  ToolCall,
} from "@datafocal/api";

const pageSize = 500;
// maxHeld is the most lines a stream keeps; a live tail past it lets its oldest go, and
// scrolling back reads them again.
const maxHeld = 50_000;
// maxSeek is the most lines opening reads back looking for the line it was asked to show.
const maxSeek = 5_000;

const key = (l: LogLine) => `${l.segment}/${l.row}`;
const ms = (l: LogLine) => Date.parse(l.timestamp);

const isSearch = (r: unknown): r is SearchResponse =>
  typeof r === "object" && r !== null && "mode" in r;
const hasSelector = (a: unknown): a is { selector: Selector } =>
  typeof a === "object" && a !== null && "selector" in a;

// Stream is a query's lines in time order, oldest first: a page of the newest in the range,
// older pages as the reader scrolls back, and a tail's lines appended as they arrive. It
// pins the snapshot its first page was read at, so scrolling back reads the same data.
export interface Stream {
  readonly lines: () => readonly LogLine[];
  // more is whether older lines in the range are still to read.
  readonly more: () => boolean;
  // older reads the page before the oldest line held, resolving to how many it put first.
  readonly older: () => Promise<number>;
  // append adds a tail's lines that are new, oldest first, resolving to how many it added
  // and how many of the oldest it let go to stay under maxHeld.
  readonly append: (ls: readonly LogLine[]) => { added: number; dropped: number };
  // indexOf is where the line ref names is held, or -1.
  readonly indexOf: (ref: LineRef) => number;
  readonly snapshot: string;
  readonly selector: Selector;
  readonly call: ToolCall;
}

interface Page {
  readonly lines: readonly LogLine[];
  readonly snapshot: string;
  readonly call: ToolCall;
}

// Pin is the snapshot a read is pinned to, if any, and what aborts it.
interface Pin {
  readonly snapshot: string;
  readonly signal?: AbortSignal;
}

// read runs q over time, newest lines first, at the pinned snapshot.
async function read(client: DataFocalClient, q: string, time: TimeSpec, pin: Pin): Promise<Page> {
  const { snapshot, signal } = pin;
  const req = { q, time, limit: pageSize, ...(snapshot && { snapshot }) };
  const env = await client.call("query", req, { ...(signal && { signal }) });
  const { result, call } = env.data;
  return { lines: (isSearch(result) && result.lines) || [], snapshot: env.meta.snapshot, call };
}

// openStream reads q's newest page over time at the pinned snapshot. Given focus, it reads
// back until it holds that line, so the line can be shown with what led up to it.
export async function openStream(
  client: DataFocalClient,
  q: string,
  time: TimeSpec,
  at: Pin & { readonly focus: LineRef | undefined },
): Promise<Stream> {
  const { focus } = at;
  const first = await read(client, q, time, at);
  const held: LogLine[] = [];
  const seen = new Set<string>();
  let more = first.lines.length >= pageSize;
  const put = (ls: readonly LogLine[]) => ls.filter((l) => !seen.has(key(l)) && seen.add(key(l)));
  for (let i = first.lines.length - 1; i >= 0; i--) {
    const l = first.lines[i];
    if (l && !seen.has(key(l))) {
      seen.add(key(l));
      held.push(l);
    }
  }
  const stream: Stream = {
    lines: () => held,
    more: () => more,
    async older() {
      const oldest = held[0];
      if (!more || !oldest) {
        return 0;
      }
      const page = await read(
        client,
        q,
        { ...time, to: oldest.timestamp },
        {
          snapshot: first.snapshot,
        },
      );
      const fresh = put([...page.lines].reverse());
      more = page.lines.length >= pageSize && fresh.length > 0;
      held.unshift(...fresh);
      return fresh.length;
    },
    append(ls) {
      const fresh = put(ls).sort((a, b) => ms(a) - ms(b));
      held.push(...fresh);
      const dropped = Math.max(0, held.length - maxHeld);
      for (const l of held.splice(0, dropped)) {
        seen.delete(key(l));
      }
      more ||= dropped > 0;
      return { added: fresh.length, dropped };
    },
    indexOf: (ref) => held.findIndex((l) => l.segment === ref.segment && l.row === ref.row),
    snapshot: first.snapshot,
    selector: hasSelector(first.call.args) ? first.call.args.selector : { time },
    call: first.call,
  };
  while (focus && stream.indexOf(focus) < 0 && stream.more() && held.length < maxSeek) {
    await stream.older();
  }
  return stream;
}
