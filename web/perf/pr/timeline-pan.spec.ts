import { timelinePan } from "./timeline-pan";

// The pan draws at most 600 frames at 60 Hz, each now about 63 draw calls, 470 bars and
// lines, and 60 labels; the ceilings allow a quarter more.
const frames = 600;

timelinePan({
  longFrames: 0,
  drawCalls: frames * 80,
  primitives: frames * 600,
  labels: frames * 75,
});
