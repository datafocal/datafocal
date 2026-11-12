import { ToolError } from "@datafocal/api";
import {
  createElement,
  Fragment,
  type ReactElement,
  type ReactNode,
  useEffect,
  useEffectEvent,
  useState,
} from "react";

// Answer is a load's state: undefined while loading, the value, or why there is none.
export type Answer<T> = T | Error | undefined;

// Absent is an answer the logs can't give because they lack what it reads.
export class Absent extends Error {}

// isAbsent reports whether e means the logs lack what was asked: an Absent, or a
// ToolError coded unknown_field.
export const isAbsent = (e: Error): boolean =>
  e instanceof Absent || (e instanceof ToolError && e.code === "unknown_field");

// Show renders an answer once it arrives, or says whether the logs lack it or the call failed.
export function Show<T>(p: {
  readonly answer: Answer<T>;
  readonly render: (v: T) => ReactNode;
}): ReactElement {
  const a = p.answer;
  if (!(a instanceof Error)) {
    return createElement(Fragment, null, a === undefined ? "Loading…" : p.render(a));
  }
  if (isAbsent(a)) {
    const reason = a instanceof ToolError ? a.reason : a.message;
    return createElement("p", { role: "status" }, `Not in these logs: ${reason}`);
  }
  return createElement(Failure, { what: "Failed", message: a.message });
}

// Failure says what failed and the whole of why, wrapped and selectable however narrow its
// column, with a button copying the message to paste elsewhere.
export function Failure({ what, message }: { what: string; message: string }): ReactElement {
  return createElement(
    "div",
    { className: "failure", role: "alert" },
    createElement("p", null, createElement("b", null, `${what}: `), message),
    createElement(
      "button",
      {
        type: "button",
        className: "tbtn",
        title: "Copy the error",
        onClick: () => void navigator.clipboard.writeText(message),
      },
      "Copy",
    ),
  );
}

// useAnswer is what load resolves to, loaded again whenever key changes, which aborts signal;
// a null key loads nothing. Its calls go through the client's loader, so an answer drawn twice
// is asked once.
export function useAnswer<T>(
  key: string | null,
  load: (signal: AbortSignal) => Promise<T>,
): Answer<T> {
  const [answer, setAnswer] = useState<{ key: string; value: T | Error }>();
  const onLoad = useEffectEvent(load);
  useEffect(() => {
    if (key === null) {
      return;
    }
    const ctl = new AbortController();
    const set = (value: T | Error) => ctl.signal.aborted || setAnswer({ key, value });
    onLoad(ctl.signal).then(set, (e: unknown) =>
      set(e instanceof Error ? e : new Error(String(e))),
    );
    return () => ctl.abort();
  }, [key]);
  return answer?.key === key ? answer.value : undefined;
}

// useLast is answer's value, or while the next loads the last value it had, so a view keeps
// showing it dimmed instead of going blank.
export function useLast<T>(answer: Answer<T>): T | undefined {
  const [last, setLast] = useState<Answer<T>>();
  if (answer !== undefined && answer !== last) {
    setLast(answer);
  }
  const shown = answer ?? last;
  return shown instanceof Error ? undefined : shown;
}

// tileText is a tile's non-value: "…" loading, "—" absent, "failed" failed.
export const tileText = (a: Error | undefined): string =>
  a === undefined ? "…" : isAbsent(a) ? "—" : "failed";
