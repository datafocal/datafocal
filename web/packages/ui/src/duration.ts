// duration is a span of milliseconds in the largest units that keep it short.
export function duration(ms: number): string {
  if (ms < 10) {
    return `${Number(ms.toFixed(2))} ms`;
  }
  if (ms < 1000) {
    return `${Math.round(ms)} ms`;
  }
  if (ms < 60_000) {
    return `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)} s`;
  }
  const m = Math.floor(ms / 60_000);
  return m < 60
    ? `${m}m ${Math.round((ms % 60_000) / 1000)}s`
    : `${Math.floor(m / 60)}h ${m % 60}m`;
}
