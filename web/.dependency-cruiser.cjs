// Package boundaries and the third-party allowlist. Paths are
// unanchored so the copies of packages under gate-fixtures/ are held to the same rules.
const internal = "packages/[^/]+/src/";

// imports lets the packages matching from import only themselves and the packages in allowed.
const imports = (name, from, allowed) => ({
  name,
  severity: "error",
  from: { path: `packages/(${from})/src/` },
  to: { path: internal, pathNot: `packages/($1|${allowed})/src/` },
});

// thirdParty lets the packages matching from import only the external modules in allowed,
// and their tests anything.
const thirdParty = (name, from, allowed) => ({
  name,
  severity: "error",
  from: { path: `packages/(${from})/src/`, pathNot: "\\.test\\.ts$" },
  to: { pathNot: [internal, `(^|/node_modules/)(${allowed})(/|$)`] },
});

module.exports = {
  forbidden: [
    { name: "no-circular", severity: "error", from: {}, to: { circular: true } },
    imports("api-is-a-leaf", "api", "api"),
    imports("base-imports-api", "query|data|ui|render-core", "api"),
    imports("renderer-imports-core", "render-[^/]+", "render-core|api"),
    imports("shell-imports-ui", "shell", "ui|query|api"),
    imports("view-imports-below", "view-[^/]+", "shell|ui|render-[^/]+|data|query|api"),
    thirdParty("no-third-party", "api|query|data|render-(?!core/)[^/]+", "(?!)"),
    thirdParty("no-third-party", "render-core", "flatbush"),
    thirdParty("react-only", "shell|view-(?!graph/)[^/]+|app", "react|react-dom"),
    thirdParty("react-and-dagre-only", "view-graph", "react|react-dom|@dagrejs/dagre"),
    thirdParty("react-and-uplot-only", "ui", "react|react-dom|uplot"),
  ],
  options: {
    doNotFollow: { path: "node_modules" },
    // TypeScript 7 has no compiler API for dependency-cruiser to parse with yet.
    parser: "swc",
    tsPreCompilationDeps: true,
    enhancedResolveOptions: {
      exportsFields: ["exports"],
      conditionNames: ["import", "default"],
      extensions: [".ts", ".tsx", ".js"],
    },
  },
};
