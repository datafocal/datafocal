import { budget } from "../harness";

// The app shell, left open for ten seconds. A 60 Hz frame measures 16.7 ms give or take
// requestAnimationFrame's jitter, so the p95 ceiling is 17 ms.
budget({
  name: "app shell idle",
  path: "./",
  run: (page) => page.waitForTimeout(10_000),
  max: { frameP95: 17, frameP99: 33, heapBytes: 150e6 },
});
