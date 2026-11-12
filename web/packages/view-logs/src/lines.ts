import { ToolError } from "@datafocal/api";
import { useEffect, useState } from "react";

// failure is what a run failed with, a too_large refusal as just its message, which says to
// narrow the query.
function failure(e: unknown): Error {
  if (e instanceof ToolError && e.code === "too_large") {
    return new Error(e.reason);
  }
  return e instanceof Error ? e : new Error(String(e));
}

// useAsync is what run resolves to, or the error it failed with; a new run aborts the one it
// supersedes and starts over.
export function useAsync<T>(run: (signal: AbortSignal) => Promise<T>): T | Error | undefined {
  const [state, setState] = useState<{ run: typeof run; value: T | Error }>();
  useEffect(() => {
    const ctl = new AbortController();
    const settle = (value: T | Error) => ctl.signal.aborted || setState({ run, value });
    void run(ctl.signal).then(settle, (e: unknown) => settle(failure(e)));
    return () => ctl.abort();
  }, [run]);
  return state?.run === run ? state.value : undefined;
}
