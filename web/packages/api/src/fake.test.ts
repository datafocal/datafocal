import { expect, test } from "vitest";
import rules from "../fixtures/request_rules.json";
import type { ToolName } from "./client";
import { checkRequest, createFakeClient } from "./fake";

const tools: readonly string[] = ["aggregate", "get_trace"];
const isTool = (t: string): t is ToolName => tools.includes(t);

test.each(rules)("the fake refuses $tool as the engine does: $message", async (rule) => {
  const refusal = { code: rule.code, message: `${rule.code}: ${rule.message}` };
  expect(isTool(rule.tool) && checkRequest(rule.tool, rule.req)).toMatchObject(refusal);
  const client = createFakeClient({});
  const call = isTool(rule.tool) && client.call(rule.tool, rule.req);
  await expect(call).rejects.toMatchObject(refusal);
});
