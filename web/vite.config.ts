import { defineConfig } from "vite";

const querier = "http://localhost:8080";

export default defineConfig(({ mode }) => ({
  root: "packages/app",
  // The fixtures build is a preview served from wherever it is published.
  base: mode === "fixtures" ? "./" : "/ui/",
  build: {
    outDir: "../../dist",
    emptyOutDir: true,
    // Name each view's chunk after its package, for its budget in .size-limit.json.
    rolldownOptions: {
      output: {
        chunkFileNames: (chunk) =>
          `assets/${/packages\/(view-[^/]+)\//.exec(chunk.facadeModuleId ?? "")?.[1] ?? "[name]"}-[hash].js`,
      },
    },
  },
  server: { proxy: { "/v1": querier, "/mcp": querier } },
}));
