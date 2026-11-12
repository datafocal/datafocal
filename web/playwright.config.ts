import { defineConfig, type PlaywrightTestConfig } from "@playwright/test";

const preview = (port: number, outDir: string) => ({
  command: `corepack pnpm exec vite preview --mode fixtures --strictPort --port ${port} --outDir ${outDir}`,
  port,
});

// The perf scenarios run in Chromium against the fixtures build
// (make web-preview) and, for their deltas, against main's fixtures build at PERF_MAIN_DIST.
const mainDist = process.env["PERF_MAIN_DIST"];
if (mainDist) {
  process.env["PERF_MAIN_URL"] = "http://localhost:4174/";
}

const config: PlaywrightTestConfig = defineConfig({
  testDir: ".",
  testMatch: "**/perf/**/*.spec.ts",
  workers: 1,
  timeout: 10 * 60_000,
  use: { browserName: "chromium", baseURL: "http://localhost:4173/" },
  webServer: [preview(4173, "../../dist"), ...(mainDist ? [preview(4174, mainDist)] : [])],
});

export default config;
