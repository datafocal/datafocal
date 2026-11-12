import { timelinePan } from "../pr/timeline-pan";

// A 60 Hz frame measures 16.7 ms give or take requestAnimationFrame's jitter, so the p95
// ceiling is 17 ms.
timelinePan({ frameP95: 17 });
