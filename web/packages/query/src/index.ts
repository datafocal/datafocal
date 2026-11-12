// Package query reads LogQL for highlighting and autocomplete only: the querier parses and
// validates it, and its parse errors carry the offset the query bar underlines; and writes the
// Logs query for a selector, which every view's link to Logs opens.

export { logql } from "./logql";

export interface Token {
  start: number;
  end: number;
  kind: "selector" | "label" | "op" | "string" | "pipe" | "stage" | "number" | "error";
}

// pipeStages follow a "|"; functions start a metric query.
const pipeStages = ["json", "logfmt", "unpack", "patterns", "explain", "trace", "unwrap"];
const functions = ["count_over_time", "rate", "sum", "quantile_over_time", "topk"];
const keywords = ["and", "or", "by"];

// lexemes are tried in order at each offset; a word is classified once it is read.
const lexemes: readonly [RegExp, Token["kind"] | "space" | "word"][] = [
  [/\s+/y, "space"],
  [/"(?:[^"\\]|\\.)*"|`[^`]*`/y, "string"],
  [/["`][^]*/y, "error"],
  [/[{}]/y, "selector"],
  [/\|[=~]|[=!]~|[!=<>]=|[=<>,()[\]]/y, "op"],
  [/\|/y, "pipe"],
  [/\d+(?:\.\d+)?(?:[a-z]+\d*)*/y, "number"],
  [/[A-Za-z_][\w.]*/y, "word"],
  [/./y, "error"],
];

function word(text: string, prev: Token | undefined): Token["kind"] {
  if (keywords.includes(text)) {
    return "op";
  }
  const stages = prev?.kind === "pipe" ? pipeStages : functions;
  return stages.includes(text) ? "stage" : "label";
}

// tokenize splits q into its tokens, leaving out whitespace.
export function tokenize(q: string): readonly Token[] {
  const tokens: Token[] = [];
  for (let at = 0; at < q.length; ) {
    for (const [re, kind] of lexemes) {
      re.lastIndex = at;
      const m = re.exec(q);
      if (!m) {
        continue;
      }
      const end = at + m[0].length;
      if (kind === "word") {
        tokens.push({ start: at, end, kind: word(m[0], tokens.at(-1)) });
      } else if (kind !== "space") {
        tokens.push({ start: at, end, kind });
      }
      at = end;
      break;
    }
  }
  return tokens;
}

// suggest completes the word before cursor: a stage or field after "|", a field where a
// label goes, and a metric function at the start.
export function suggest(q: string, cursor: number, fields: readonly string[]): readonly string[] {
  const before = q.slice(0, cursor);
  const partial = /[A-Za-z_][\w.]*$/.exec(before)?.[0] ?? "";
  const rest = before.slice(0, before.length - partial.length);
  const tokens = tokenize(rest);
  const text = (i: number) => {
    const t = tokens.at(i);
    return t && rest.slice(t.start, t.end);
  };
  let words: readonly string[] = [];
  if (tokens.length === 0) {
    words = functions;
  } else if (tokens.at(-1)?.kind === "pipe") {
    words = [...pipeStages, ...fields];
  } else if (["{", ",", "and", "or"].includes(text(-1) ?? "") || text(-2) === "by") {
    words = fields;
  }
  return words.filter((w) => w.startsWith(partial) && w !== partial);
}
