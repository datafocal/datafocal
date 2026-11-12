import type { Envelope, DataFocalClient, LogLine, SearchResponse, Selector } from "@datafocal/api";

// RowWindow is a slice of the lines a tail holds, oldest first.
export type RowWindow = readonly LogLine[];

// TailStats are the lines the backend sent per second over the last second or more, and why
// the last follow failed, until the next batch arrives.
export interface TailStats {
  linesPerSec: number;
  error: Error | undefined;
}

// Tail follows a selector's new lines into a ring buffer of fixed capacity.
export interface Tail {
  start(selector: Selector): void;
  pause(): void;
  resume(): void;
  window(from: number, to: number): Promise<RowWindow>;
  stats(): TailStats;
  stop(): void;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// createRing returns a ring buffer of the last capacity lines pushed.
function createRing(capacity: number) {
  const lines: LogLine[] = [];
  let pushed = 0;
  return {
    push(l: LogLine) {
      lines[pushed++ % capacity] = l;
    },
    // all is the lines held, oldest first.
    all(): LogLine[] {
      const oldest = pushed > capacity ? pushed % capacity : 0;
      return [...lines.slice(oldest), ...lines.slice(0, oldest)];
    },
  };
}

// meterSpan is how far back the rate looks. A follow brings its lines in bursts seconds
// apart, so a rate over the last second reads 0 between them.
const meterSpan = 10_000;

// createMeter counts the lines received, by the second, and reads their rate over the last
// meterSpan or since it started.
function createMeter() {
  const started = performance.now();
  const seconds = new Map<number, number>();
  return {
    count() {
      const s = Math.floor(performance.now() / 1000);
      seconds.set(s, (seconds.get(s) ?? 0) + 1);
    },
    read() {
      const now = performance.now();
      const from = Math.floor((now - meterSpan) / 1000);
      let sent = 0;
      for (const [s, n] of seconds) {
        if (s < from) {
          seconds.delete(s);
        } else {
          sent += n;
        }
      }
      return { linesPerSec: (sent * 1000) / Math.max(Math.min(now - started, meterSpan), 1000) };
    },
  };
}

// createTail returns a tail over client holding at most capacity lines. The querier sends each
// line once, as it lands, so a follow that ends or fails is followed again a second later.
export function createTail(client: DataFocalClient, opts: { capacity: number }): Tail {
  const ring = createRing(opts.capacity);
  const meter = createMeter();
  let selector: Selector = {};
  let following: AbortController | undefined;
  let error: Error | undefined;

  const take = (b: Envelope<SearchResponse>) => {
    error = undefined;
    for (const l of b.data.lines ?? []) {
      meter.count();
      ring.push(l);
    }
  };
  const follow = async (signal: AbortSignal) => {
    while (!signal.aborted) {
      await client.follow({ selector }, take, signal).catch((e: unknown) => {
        if (!signal.aborted) {
          error = e instanceof Error ? e : new Error(String(e));
        }
      });
      await sleep(1000);
    }
  };
  const tail: Tail = {
    start(s) {
      selector = s;
      tail.resume();
    },
    pause() {
      following?.abort();
    },
    resume() {
      following?.abort();
      following = new AbortController();
      void follow(following.signal);
    },
    window: (from, to) => Promise.resolve(ring.all().slice(from, to)),
    stats: () => ({ ...meter.read(), error }),
    stop() {
      tail.pause();
    },
  };
  return tail;
}
