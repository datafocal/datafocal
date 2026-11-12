import { budget } from "../harness";

// The app shell, scrolled for two seconds.
budget({
  name: "app shell scroll",
  path: "./",
  run: async (page) => {
    for (let i = 0; i < 20; i++) {
      await page.mouse.wheel(0, 200);
      await page.waitForTimeout(100);
    }
  },
  max: { longFrames: 0 },
  delta: { frameP95: 0.1 },
  packages: ["app", "shell", "ui", "api"],
});
