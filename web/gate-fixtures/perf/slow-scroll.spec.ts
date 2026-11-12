import { budget } from "../../perf/harness";

// The app shell scrolled with a 100 ms busy loop on each scroll event, which the long-frame
// budget must fail.
budget({
  name: "slow scroll",
  path: "./",
  run: async (page) => {
    await page.evaluate(() => {
      document.body.style.height = "10000px";
      addEventListener("scroll", () => {
        const end = performance.now() + 100;
        while (performance.now() < end) {}
      });
    });
    for (let i = 0; i < 20; i++) {
      await page.mouse.wheel(0, 200);
      await page.waitForTimeout(100);
    }
  },
  max: { longFrames: 0 },
});
