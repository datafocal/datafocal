import type { LogLine } from "@datafocal/api";

// unwrap reads a value holding a JSON array of strings, as nested fields are flattened, as
// its strings.
export function unwrap(v: string): string {
  if (!v.startsWith('["')) {
    return v;
  }
  try {
    const a: unknown = JSON.parse(v);
    return Array.isArray(a) ? a.join(" ") : v;
  } catch {
    return v;
  }
}

// lineText is what a line says: its message, or for a record without one its longest field
// holding words, which is where a structured record keeps its prose.
export function lineText(l: LogLine): string {
  if (l.message !== "") {
    return l.message;
  }
  const prose = Object.entries(l.attributes ?? {})
    .map(([k, v]) => [k, unwrap(v)] as const)
    .filter(([k, v]) => !k.endsWith("_pattern") && /\s/.test(v.trim()))
    .sort((a, b) => b[1].length - a[1].length)[0];
  const [key, value] = prose ?? ["", ""];
  return prose
    ? `${key.slice(key.lastIndexOf(".") + 1)}: ${value.replace(/\s+/g, " ").trim()}`
    : "";
}

// firstValue is a field's value as one: the first non-empty element of a list it holds.
export function firstValue(v: string | undefined): string {
  if (!v?.startsWith("[")) {
    return v ?? "";
  }
  try {
    const list: unknown = JSON.parse(v);
    return Array.isArray(list) ? String(list.find((x) => x !== "") ?? "") : v;
  } catch {
    return v;
  }
}

const oneLine = (s: string, n = 160) => {
  const one = s.replace(/\s+/g, " ").trim();
  return one.length > n ? `${one.slice(0, n - 1)}…` : one;
};

// attr is the first of a line's values whose field's name ends in one of ends, in their order.
function attr(l: LogLine, ...ends: string[]): string {
  const a = Object.entries(l.attributes ?? {});
  for (const end of ends) {
    const v = a.find(([k, x]) => k.endsWith(end) && firstValue(x) !== "")?.[1];
    if (v !== undefined) {
      return firstValue(v);
    }
  }
  return "";
}

// args are the arguments a tool call is told by, the one naming what it acts on first.
const args = [
  ".input.command",
  ".input.file_path",
  ".input.path",
  ".input.pattern",
  ".input.query",
  ".input.url",
  ".input.skill",
  ".input.description",
  ".input.prompt",
  ".arguments",
  ".args",
];

// AgentStep is what an agent's line does, read from the columns ingest discovers: a tool
// call, its result, thinking, or a message, by whom, with which tool, and as a row's label.
export interface AgentStep {
  readonly kind: "call" | "result" | "thinking" | "message";
  readonly role: string;
  readonly tool: string;
  readonly label: string;
}

// stepKind is what a line with a message role does.
function stepKind(l: LogLine): AgentStep["kind"] {
  const a = l.attributes ?? {};
  if (a["tool_call_id"] !== undefined) {
    return "result";
  }
  if (firstValue(a["tool_name"]) !== "") {
    return "call";
  }
  return a["context_thinking_chars"] === undefined ? "message" : "thinking";
}

// stepLabel says what step does: the call and on what, the result it hands back, the
// thinking, or the message. A command's leading cd only says where it runs.
function stepLabel(l: LogLine, kind: AgentStep["kind"], role: string, tool: string): string {
  const said = () =>
    oneLine(
      attr(l, ".text", "message.content", ".content") || lineText(l).replace(/^[\w.]+: /, ""),
    );
  switch (kind) {
    case "call":
      return `${tool} · ${oneLine(attr(l, ...args).replace(/^cd \S+\s*(;|&&)\s*/, ""), 120)}`;
    case "result":
      return `${tool || "tool"} result · ${oneLine(attr(l, ".content.content", ".stdout", ".output", ".result") || said(), 120)}`;
    case "thinking": {
      const thought = oneLine(attr(l, ".thinking"), 120);
      return thought ? `thinking · ${thought}` : "thinking";
    }
    case "message":
      return `${role}: ${said()}`;
  }
}

// agentStep is what an agent's line does, or undefined for a line carrying no message role.
export function agentStep(l: LogLine): AgentStep | undefined {
  const role = l.attributes?.["content_role"];
  if (role === undefined) {
    return undefined;
  }
  const kind = stepKind(l);
  const tool = firstValue(l.attributes?.["tool_name"]);
  return { kind, role, tool, label: stepLabel(l, kind, role, tool) };
}
