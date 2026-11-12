import type { DataFocalClient } from "@datafocal/api";

export type Path = readonly (string | number)[];

// at is v's value along path, or undefined where v has none.
export const at = (v: unknown, path: Path): unknown =>
  path.reduce<unknown>(
    (o, k) => (typeof o === "object" && o !== null ? Reflect.get(o, k) : undefined),
    v,
  );

// list is v when it is an array, else empty.
export const list = (v: unknown): readonly unknown[] => (Array.isArray(v) ? v : []);

// put is v with value at path, every object along it copied.
export function put(v: unknown, [k, ...rest]: Path, value: unknown): unknown {
  if (k === undefined) {
    return value;
  }
  const copy: object = Object.assign(Array.isArray(v) ? [] : {}, v);
  Reflect.set(copy, k, put(Reflect.get(copy, k), rest, value));
  return copy;
}

// exported is the named SLO's document, normalized.
export const exported = async (client: DataFocalClient, name: string): Promise<unknown> => {
  const e = await client.call("export_definitions", { names: [name], format: "json" });
  return at(JSON.parse(e.data.content), [0]);
};

export { button, download, pct } from "@datafocal/ui";
