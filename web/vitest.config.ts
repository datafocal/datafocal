import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

// unit runs in node; browser runs *.browser.test.ts in Chromium.
export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "unit",
          include: ["packages/*/src/**/*.test.ts"],
          exclude: ["**/*.browser.test.ts"],
          globalSetup: "./vitest.setup.ts",
        },
      },
      {
        test: {
          name: "browser",
          include: ["packages/*/src/**/*.browser.test.ts"],
          browser: {
            enabled: true,
            headless: true,
            provider: playwright(),
            instances: [{ browser: "chromium" }],
          },
        },
      },
    ],
  },
});
