import { afterEach, expect, test } from "vitest";
import { createTableScroller } from "./index";

const el = document.body.appendChild(document.createElement("div"));
el.style.height = "480px";
const scroller = createTableScroller<number>({ rowHeight: 24, overscan: 10 });
afterEach(() => scroller.dispose());

// rows is a source of n rows, each its own index.
const rows = (n: number) => ({
  total: () => n,
  rows: async (from: number, to: number) => Array.from({ length: to - from }, (_, i) => from + i),
});
const shown = () => [...el.querySelectorAll("[role=row]")].map((r) => Number(r.textContent));

test("scrollToIndex shows the last of 5M rows, past the element height cap", async () => {
  scroller.mount(el, rows(5_000_000), (i) => document.createTextNode(String(i)));
  scroller.scrollToIndex(4_999_999);
  await expect.poll(shown).toContain(4_999_999);
  const last = el.querySelector("[aria-rowindex='5000000']")?.getBoundingClientRect();
  expect(last && last.bottom <= el.getBoundingClientRect().bottom + 0.5).toBe(true);
});

test("only the visible rows and the overscan are in the DOM", async () => {
  scroller.mount(el, rows(1_000_000), (i) => document.createTextNode(String(i)));
  scroller.scrollToIndex(500_000);
  await expect.poll(shown).toContain(500_000);
  expect(shown().length).toBeLessThanOrEqual(480 / 24 + 1 + 2 * 10);
});
