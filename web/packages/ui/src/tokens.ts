// The colour tokens tokens.css defines, for code that names one (a canvas or chart colour).
export type ColorToken =
  | "--bg"
  | "--panel"
  | "--panel-2"
  | "--raise"
  | "--line"
  | "--line-2"
  | "--ink"
  | "--ink-2"
  | "--ink-3"
  | "--accent"
  | "--accent-soft"
  | "--insight"
  | "--insight-soft"
  | "--ok"
  | "--warn"
  | "--err"
  | "--err-soft"
  | "--cat-1"
  | "--cat-2"
  | "--cat-3"
  | "--cat-4"
  | "--cat-5";

// cssColor resolves token to the colour it has at el, in el's theme.
export function cssColor(token: ColorToken, el: Element): string {
  const probe = el.appendChild(document.createElement("span"));
  probe.style.color = `var(${token})`;
  const color = getComputedStyle(probe).color;
  probe.remove();
  return color;
}
