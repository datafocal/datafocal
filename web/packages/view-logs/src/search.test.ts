import { expect, test } from "vitest";
import { compile, highlights, terms, withBound, withFilter } from "./search";

test.each([
  ["", '{service=~".+"}'],
  ["timeout", '{service=~".+"} |~ "(?i)timeout"'],
  ["pool timeout", '{service=~".+"} |~ "(?i)pool" |~ "(?i)timeout"'],
  ['"pool timeout"', '{service=~".+"} |~ "(?i)pool timeout"'],
  ["-healthz", '{service=~".+"} !~ "(?i)healthz"'],
  ["service:querier", '{service=~".+"} | service="querier"'],
  ['-message.role:"user"', '{service=~".+"} | message.role!="user"'],
  ["timeout OR refused", '{service=~".+"} |~ "(?i)(timeout|refused)"'],
  ["GET /v1/query?x=1", '{service=~".+"} |~ "(?i)GET" |~ "(?i)/v1/query\\\\?x=1"'],
  ['{service="db"} |= "x"', '{service="db"} |= "x"'],
  ["duration_ms:>500", '{service=~".+"} | duration_ms > 500'],
  ["duration_ms:>=0.5", '{service=~".+"} | duration_ms >= 0.5'],
  ["status:<500", '{service=~".+"} | status < 500'],
  ["status:<=-1", '{service=~".+"} | status <= -1'],
])("compiles %j to LogQL", (q, want) => {
  expect(compile(q)).toBe(want);
});

test("an unclosed phrase is an error, not a filter", () => {
  expect(terms('level:error "pool tim').map((t) => t.kind)).toEqual(["field", "error"]);
  expect(compile('level:error "pool tim')).toBe('{service=~".+"} | level="error"');
});

test("a field compared with a number as a bare word is flagged, not searched", () => {
  expect(terms("timeout duration_ms>500")[1]).toMatchObject({
    kind: "error",
    hint: "did you mean duration_ms:>500",
  });
  expect(compile("timeout duration_ms>500")).toBe('{service=~".+"} |~ "(?i)timeout"');
});

test("highlights are the words a query looks for", () => {
  expect(highlights('pool "conn refused" -healthz service:db')).toEqual(["pool", "conn refused"]);
  expect(highlights('{service="db"} |= "timeout" |~ "(?i)pool"')).toEqual(["timeout", "pool"]);
});

test("withFilter adds a field term in the query's own language", () => {
  expect(withFilter("timeout", "service", "db-3")).toBe("timeout service:db-3");
  expect(withFilter("", "path", "/v1 x", true)).toBe('-path:"/v1 x"');
  expect(withFilter('{service="db"}', "level", "error", true)).toBe(
    '{service="db"} | level!="error"',
  );
  expect(withBound("timeout", "duration_ms", ">=", 500)).toBe("timeout duration_ms:>=500");
  expect(withBound('{service="db"}', "duration_ms", "<=", 9)).toBe(
    '{service="db"} | duration_ms <= 9',
  );
});
