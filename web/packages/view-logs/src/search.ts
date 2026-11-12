// Search is the query box's plain language: words and quoted phrases match a line's message
// ignoring case, field:value matches a field, field:>N (or >=, <, <=) bounds a number, a
// leading - excludes, and OR between two words or phrases matches either. A query that starts
// with "{" is LogQL and passes through.

const anyLine = '{service=~".+"}';

// Term is one piece of a plain search, by the offsets it spans in the query.
export interface Term {
  readonly start: number;
  readonly end: number;
  readonly kind: "text" | "field" | "not" | "or" | "error";
  readonly field?: string;
  readonly value: string;
  // op is a field's comparison with value, a number; hint says what an error meant.
  readonly op?: string;
  readonly hint?: string;
}

const isLogQL = (q: string) => q.trimStart().startsWith("{");

// lexeme is a term after any space: an optional "-", then field:value, a quoted phrase or a
// word. A phrase runs to its closing quote, or to the end when it has none.
const lexeme =
  /\s*(-)?(?:([A-Za-z_][\w.-]*):("(?:[^"\\]|\\.)*"?|\S+)|("(?:[^"\\]|\\.)*"?)|(\S+))/gy;

const closed = (s: string) => !s.startsWith('"') || (s.length > 1 && /[^\\]"$|^""$/.test(s));
// unquote is a quoted phrase's text, or s itself when it isn't quoted.
function unquote(s: string): string {
  const v: unknown = s.startsWith('"') ? JSON.parse(s) : s;
  return typeof v === "string" ? v : s;
}

// bound is a field's value comparing it with a number; comparison is a word doing so, which
// reads as text by mistake.
const bound = /^(>=|<=|>|<)(-?\d+(?:\.\d+)?)$/;
const comparison = /^([A-Za-z_][\w.]*)(>=|<=|>|<)(-?\d+(?:\.\d+)?)$/;

// kindOf is a term's kind: excluded, a field's value, or text to find.
const kindOf = (not: string | undefined, field: string | undefined): Term["kind"] =>
  not ? "not" : field ? "field" : "text";

// unsearchable is what a lexeme, all, reads as when it is no field or text to find: an
// unclosed phrase, OR, or a comparison missing its colon; undefined otherwise.
function unsearchable(
  all: string,
  not: string | undefined,
  raw: string,
  word: string | undefined,
): Omit<Term, "start" | "end"> | undefined {
  if (!closed(raw)) {
    return { kind: "error", value: all.trim() };
  }
  if (word === "OR" && !not) {
    return { kind: "or", value: "" };
  }
  const [, name, op, n] = comparison.exec(word ?? "") ?? [];
  return name && n
    ? { kind: "error", value: all.trim(), hint: `did you mean ${name}:${op}${n}` }
    : undefined;
}

// searched is what a lexeme reads as when it is a field's value or text to find.
function searched(
  not: string | undefined,
  field: string | undefined,
  raw: string,
): Omit<Term, "start" | "end"> {
  const [, op, limit] = (field && !not && bound.exec(raw)) || [];
  if (field && op && limit) {
    return { kind: "field", field, op, value: limit };
  }
  return { kind: kindOf(not, field), value: unquote(raw), ...(field && { field }) };
}

// term reads one lexeme match, at offset start, as a term.
function term(m: RegExpExecArray, start: number): Term {
  const [all, not, field, fieldValue, phrase, word] = m;
  const raw = fieldValue ?? phrase ?? word ?? "";
  const read = unsearchable(all, not, raw, word) ?? searched(not, field, raw);
  return { start, end: m.index + all.length, ...read };
}

// terms splits a plain search into its terms; a phrase with no closing quote is an error.
export function terms(q: string): Term[] {
  const out: Term[] = [];
  lexeme.lastIndex = 0;
  for (let m = lexeme.exec(q); m && m[0].trim() !== ""; m = lexeme.exec(q)) {
    out.push(term(m, m.index + (m[0].length - m[0].trimStart().length)));
  }
  return out;
}

const escapeRegex = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const quote = (s: string) => JSON.stringify(s);

// stage is the LogQL a term or OR group of text terms filters by.
function stage(group: readonly Term[]): string {
  const [t] = group;
  if (!t) {
    return "";
  }
  if (group.length > 1) {
    return ` |~ ${quote(`(?i)(${group.map((g) => escapeRegex(g.value)).join("|")})`)}`;
  }
  if (t.op) {
    return ` | ${t.field} ${t.op} ${t.value}`;
  }
  if (t.field) {
    return ` | ${t.field}${t.kind === "not" ? "!=" : "="}${quote(t.value)}`;
  }
  const re = quote(`(?i)${escapeRegex(t.value)}`);
  return t.kind === "not" ? ` !~ ${re}` : ` |~ ${re}`;
}

// groups joins the text terms an OR links, so each group is one stage.
function groups(ts: readonly Term[]): Term[][] {
  const out: Term[][] = [];
  ts.forEach((t, i) => {
    if (t.kind === "or" || t.kind === "error") {
      return;
    }
    const prev = out.at(-1);
    if (ts[i - 1]?.kind === "or" && prev && t.kind === "text" && !t.field) {
      prev.push(t);
    } else {
      out.push([t]);
    }
  });
  return out;
}

// compile is the LogQL a query runs as: LogQL itself, or a plain search's terms as stages
// after a selector matching every line.
export function compile(q: string): string {
  if (isLogQL(q)) {
    return q;
  }
  return anyLine + groups(terms(q)).map(stage).join("");
}

// highlights are the words a query looks for in a line's text, to mark where they match.
export function highlights(q: string): string[] {
  if (isLogQL(q)) {
    return [...q.matchAll(/\|[=~]\s*"((?:[^"\\]|\\.)*)"/g)]
      .map((m) => unquote(`"${m[1] ?? ""}"`).replace(/^\(\?i\)/, ""))
      .filter((s) => s !== "" && !/[\\()[\]|*+?]/.test(s));
  }
  return terms(q)
    .filter((t) => t.kind === "text" && !t.field)
    .map((t) => t.value);
}

// withFilter is q narrowed to lines whose field is, or with exclude is not, value: a
// field:value term in a plain search, a label filter stage in LogQL.
export function withFilter(q: string, field: string, value: string, exclude = false): string {
  if (isLogQL(q)) {
    return `${q} | ${field}${exclude ? "!=" : "="}${quote(value)}`;
  }
  const v = /^[^\s"]+$/.test(value) ? value : quote(value);
  return `${q.trim()} ${exclude ? "-" : ""}${field}:${v}`.trim();
}

// withBound is q narrowed to lines whose field is op n: a field:<op>n term in a plain search,
// a comparison stage in LogQL.
export function withBound(q: string, field: string, op: ">=" | "<=", n: number): string {
  return isLogQL(q) ? `${q} | ${field} ${op} ${n}` : `${q.trim()} ${field}:${op}${n}`.trim();
}

// isPlain reports whether q is a plain search rather than LogQL.
export const isPlain = (q: string): boolean => !isLogQL(q);
