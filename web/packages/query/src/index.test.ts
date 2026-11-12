import { expect, test } from "vitest";
import { suggest, tokenize } from "./index";

const read = (q: string) => tokenize(q).map((t) => `${t.kind} ${q.slice(t.start, t.end)}`);

// Each construct of the querier's LogQL subset, with the kind of each token it reads as.
test.each([
  [
    '{service="checkout", host=~"host-.*"}',
    'selector {|label service|op =|string "checkout"|op ,|label host|op =~|string "host-.*"|selector }',
  ],
  [
    '{a!="x", b!~`y`}',
    'selector {|label a|op !=|string "x"|op ,|label b|op !~|string `y`|selector }',
  ],
  [
    '{a="x"} |= "x" != "y" |~ "re" !~ "re"',
    'selector {|label a|op =|string "x"|selector }|op |=|string "x"|op !=|string "y"|op |~|string "re"|op !~|string "re"',
  ],
  [
    '{a="x"} | json | logfmt | unpack',
    'selector {|label a|op =|string "x"|selector }|pipe ||stage json|pipe ||stage logfmt|pipe ||stage unpack',
  ],
  [
    '{a="x"} | status >= 500',
    'selector {|label a|op =|string "x"|selector }|pipe ||label status|op >=|number 500',
  ],
  [
    '{a="x"} | level="error" or level="warn"',
    'selector {|label a|op =|string "x"|selector }|pipe ||label level|op =|string "error"|op or|label level|op =|string "warn"',
  ],
  [
    'count_over_time({a="x"}[5m])',
    'stage count_over_time|op (|selector {|label a|op =|string "x"|selector }|op [|number 5m|op ]|op )',
  ],
  [
    'sum by (route) (rate({a="x"}[1h30m]))',
    'stage sum|op by|op (|label route|op )|op (|stage rate|op (|selector {|label a|op =|string "x"|selector }|op [|number 1h30m|op ]|op )|op )',
  ],
  [
    'quantile_over_time(0.95, {a="x"} | unwrap duration_ms [5m]) by (route)',
    'stage quantile_over_time|op (|number 0.95|op ,|selector {|label a|op =|string "x"|selector }|pipe ||stage unwrap|label duration_ms|op [|number 5m|op ]|op )|op by|op (|label route|op )',
  ],
  [
    'topk(10, count_over_time({a="x"}[5m]))',
    'stage topk|op (|number 10|op ,|stage count_over_time|op (|selector {|label a|op =|string "x"|selector }|op [|number 5m|op ]|op )|op )',
  ],
  ['{a="x"} | patterns', 'selector {|label a|op =|string "x"|selector }|pipe ||stage patterns'],
  ['{a="x"} | explain', 'selector {|label a|op =|string "x"|selector }|pipe ||stage explain'],
  [
    '{a="x"} | line_format "{{.a}}"',
    'selector {|label a|op =|string "x"|selector }|pipe ||label line_format|string "{{.a}}"',
  ],
  ['{a="x} + 1', 'selector {|label a|op =|error "x} + 1'],
  ["{a=@}", "selector {|label a|op =|error @|selector }"],
])("%s", (q, want) => {
  expect(read(q)).toEqual(want.split(/\|(?=[a-z]+ )/));
});

test.each([
  ["", 0, ["count_over_time", "rate", "sum", "quantile_over_time", "topk"]],
  ["{se", 3, ["service"]],
  ['{a="x", ', 8, ["service", "status"]],
  ['{a="x"} | j', 11, ["json"]],
  [
    '{a="x"} | ',
    10,
    ["json", "logfmt", "unpack", "patterns", "explain", "trace", "unwrap", "service", "status"],
  ],
  ["sum by (s", 9, ["service", "status"]],
  ['{a="se', 6, []],
])("suggest(%j, %i)", (q, cursor, want) => {
  expect(suggest(q, cursor, ["service", "status"])).toEqual(want);
});
