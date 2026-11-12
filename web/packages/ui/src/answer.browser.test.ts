import { ToolError } from "@datafocal/api";
import { createElement } from "react";
import { flushSync } from "react-dom";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, expect, test } from "vitest";
import { Absent, type Answer, Show } from "./index";

let root: Root | undefined;
afterEach(() => root?.unmount());

// shown renders answer and returns its role and text, a failure's without its Copy button.
function shown(answer: Answer<string>): [string | null | undefined, string | null] {
  const el = document.body.appendChild(document.createElement("div"));
  root = createRoot(el);
  flushSync(() => root?.render(createElement(Show<string>, { answer, render: (v) => v })));
  return [el.firstElementChild?.getAttribute("role"), (el.querySelector("p") ?? el).textContent];
}

const refusal = (code: string) =>
  new ToolError({ code, message: "no field foo", retryable: false });

test("an unknown field reads as absence, without its code", () => {
  expect(shown(refusal("unknown_field"))).toEqual(["status", "Not in these logs: no field foo"]);
  expect(shown(new Absent("no cost"))).toEqual(["status", "Not in these logs: no cost"]);
});

test.each(["invalid_request", "scan_too_large", "unbounded_scan", "unusable_series", "internal"])(
  "%s reads as a failure, with its code",
  (code) => {
    expect(shown(refusal(code))).toEqual(["alert", `Failed: ${code}: no field foo`]);
  },
);

test("a plain error fails with its message; loading and a value show as themselves", () => {
  expect(shown(new Error("network down"))).toEqual(["alert", "Failed: network down"]);
  expect(shown(undefined)[1]).toBe("Loading…");
  expect(shown("42")[1]).toBe("42");
});
