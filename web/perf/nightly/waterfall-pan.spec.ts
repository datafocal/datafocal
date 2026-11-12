import { waterfallPan } from "../pr/waterfall-pan";

// A 60 Hz frame measures 16.7 ms give or take requestAnimationFrame's jitter, so the p95
// ceiling is 17 ms.
waterfallPan({ frameP95: 17 }, 300);
