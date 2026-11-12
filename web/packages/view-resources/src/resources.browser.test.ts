import { type FixtureClient, fixtureClient } from "@datafocal/api";
import { Shell } from "@datafocal/shell";
import { createElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, expect, test, vi } from "vitest";
import { userEvent } from "vitest/browser";
import engine from "../fixtures/engine.json";
import { ResourcesView } from "./index";

// The view on two nodes' and two edge devices' cAdvisor samples over the incident's ten
// minutes (internal/e2e/resources_test.go): gateway and checkout on node-1, db on node-2,
// kube-proxy on both, each at a fixed rate, sampled every 20s, and checkout's working set
// 900 MiB higher from 14:05; device-1's systemd units sshd and gnome-firefox, and device-2,
// running no containers, its root alone.
const time = "2026-07-18T14:00:00Z,2026-07-18T14:10:00Z";
let root: Root | undefined;
let fixtures: FixtureClient | undefined;
afterEach(async () => {
  expect(await fixtures?.faults(), "each call sent once, a frame's at once").toEqual([]);
  root?.unmount();
  expect(fixtures?.missed, "every request the view sent is in requests.json").toEqual([]);
});

function render(path = "") {
  history.replaceState(null, "", `/ui/resources/time=${encodeURIComponent(time)}${path}`);
  const client = fixtureClient(engine);
  fixtures = client;
  root = createRoot(document.body.appendChild(document.createElement("div")));
  root.render(
    createElement(Shell, { routes: [{ path: "/ui/resources", view: ResourcesView }], client }),
  );
}

const table = async (label: string) => {
  await expect
    .poll(() => document.querySelector(`table[aria-label="${label}"] tbody tr`))
    .toBeTruthy();
  return [...document.querySelectorAll(`table[aria-label="${label}"] tbody tr`)].map((r) =>
    [...r.children].slice(0, -1).map((c) => c.textContent),
  );
};

test("each service's use is its containers' on every host, rates from running totals", async () => {
  render();
  expect(await table("Services")).toEqual([
    [
      "checkout",
      "1.25 cores",
      "1.25 cores",
      "950 MiB",
      "1.4 GiB",
      "195 KiB/s",
      "98 KiB/s",
      "80 MiB",
    ],
    ["db", "1.00 cores", "1.00 cores", "2.0 GiB", "2.0 GiB", "293 KiB/s", "195 KiB/s", "4.0 GiB"],
    [
      "gateway",
      "0.50 cores",
      "0.50 cores",
      "256 MiB",
      "256 MiB",
      "391 KiB/s",
      "293 KiB/s",
      "120 MiB",
    ],
    ["gnome-firefox", "0.60 cores", "0.60 cores", "1.0 GiB", "1.0 GiB", "0 B/s", "0 B/s", "0 B"],
    [
      "kube-proxy",
      "0.10 cores",
      "0.10 cores",
      "64 MiB",
      "64 MiB",
      "2.0 KiB/s",
      "2.0 KiB/s",
      "20 MiB",
    ],
    ["sshd", "0.20 cores", "0.20 cores", "200 MiB", "200 MiB", "0 B/s", "0 B/s", "0 B"],
  ]);
});

// Each table a path shows, sorted by the sort its path names: every host's own use; a host
// filter, which leaves the other hosts' containers out; a host sampling every cgroup, whose
// rows are its units and not the slices holding them; a host running no containers, whose
// row is itself; a resource's page with its parts; and an order by another column.
test.each([
  {
    path: "",
    label: "Hosts",
    rows: [
      [
        "device-1",
        "1.00 cores",
        "1.00 cores",
        "2.0 GiB",
        "2.0 GiB",
        "2.0 KiB/s",
        "1000 B/s",
        "8.0 GiB",
      ],
      [
        "device-2",
        "0.25 cores",
        "0.25 cores",
        "512 MiB",
        "512 MiB",
        "1000 B/s",
        "1000 B/s",
        "1.0 GiB",
      ],
      [
        "node-1",
        "3.50 cores",
        "3.50 cores",
        "6.0 GiB",
        "6.0 GiB",
        "879 KiB/s",
        "586 KiB/s",
        "40 GiB",
      ],
      [
        "node-2",
        "2.00 cores",
        "2.00 cores",
        "4.0 GiB",
        "4.0 GiB",
        "488 KiB/s",
        "391 KiB/s",
        "20 GiB",
      ],
    ],
  },
  {
    path: "/q=host%3Anode-2",
    label: "Services",
    rows: [
      ["db", "1.00 cores", "1.00 cores", "2.0 GiB", "2.0 GiB", "293 KiB/s", "195 KiB/s", "4.0 GiB"],
      [
        "kube-proxy",
        "0.050 cores",
        "0.050 cores",
        "32 MiB",
        "32 MiB",
        "1000 B/s",
        "1000 B/s",
        "10 MiB",
      ],
    ],
  },
  {
    path: "/q=host%3Adevice-1",
    label: "Services",
    rows: [
      ["gnome-firefox", "0.60 cores", "0.60 cores", "1.0 GiB", "1.0 GiB", "0 B/s", "0 B/s", "0 B"],
      ["sshd", "0.20 cores", "0.20 cores", "200 MiB", "200 MiB", "0 B/s", "0 B/s", "0 B"],
    ],
  },
  {
    path: "/q=host%3Adevice-2",
    label: "Hosts",
    rows: [
      [
        "device-2",
        "0.25 cores",
        "0.25 cores",
        "512 MiB",
        "512 MiB",
        "1000 B/s",
        "1000 B/s",
        "1.0 GiB",
      ],
    ],
  },
  {
    path: "/stat=cpu",
    label: "Services",
    rows: [
      ["checkout", "1.25 cores", "1.25 cores", "1.00 cores", "0.25 cores"],
      ["db", "1.00 cores", "1.00 cores", "0.80 cores", "0.20 cores"],
      ["gateway", "0.50 cores", "0.50 cores", "0.40 cores", "0.10 cores"],
      ["gnome-firefox", "0.60 cores", "0.60 cores", "0.48 cores", "0.12 cores"],
      ["kube-proxy", "0.10 cores", "0.10 cores", "0.080 cores", "0.020 cores"],
      ["sshd", "0.20 cores", "0.20 cores", "0.16 cores", "0.040 cores"],
    ],
  },
  {
    path: "/sort=-in",
    label: "Services",
    rows: [["gateway"], ["db"], ["checkout"], ["kube-proxy"], ["gnome-firefox"], ["sshd"]],
  },
])("$label at '$path'", async ({ path, label, rows }) => {
  render(path);
  const got = await table(label);
  expect(got.map((r) => r.slice(0, rows[0]?.length))).toEqual(rows);
});

test("a host running no containers is its own row, in one table", async () => {
  render("/q=host%3Adevice-2");
  await table("Hosts");
  expect(
    [...document.querySelectorAll("table.res-table")].map((t) => t.getAttribute("aria-label")),
  ).toEqual(["Hosts"]);
  expect(document.querySelector(".res-facts")?.textContent).toBe(
    "Hosts1sampledCPU0.25 coresmean · peak 0.25 coresMemory512 MiBmean · peak 512 MiBNetwork in1000 B/smeanNetwork out1000 B/smeanDisk used1.0 GiBlatest",
  );
});

test("a resource's page heads its parts' columns, and a heading sorts by its column", async () => {
  render("/stat=cpu");
  await table("Services");
  const heads = () =>
    [...document.querySelectorAll('table[aria-label="Services"] th')].map((h) => h.textContent);
  expect(heads()).toEqual(["Services ↑", "CPU", "CPU peak", "user", "system", ""]);
  const sortBy = (label: string) =>
    [...document.querySelectorAll<HTMLButtonElement>('table[aria-label="Services"] th button')]
      .find((b) => b.textContent?.startsWith(label))
      ?.click();
  const order = ["checkout", "db", "gnome-firefox", "gateway", "sshd", "kube-proxy"];
  sortBy("system");
  await expect.poll(async () => (await table("Services")).map((r) => r[0])).toEqual(order);
  expect(location.pathname).toContain("/sort=-part%3Asystem");
  sortBy("system");
  await expect
    .poll(async () => (await table("Services")).map((r) => r[0]))
    .toEqual([...order].reverse());
});

test("picking a host narrows the page to it", async () => {
  render();
  await table("Hosts");
  [...document.querySelectorAll<HTMLButtonElement>('table[aria-label="Hosts"] button.link')]
    .find((b) => b.textContent === "node-2")
    ?.click();
  await expect
    .poll(() => document.querySelector<HTMLInputElement>('input[aria-label="Filter"]')?.value)
    .toBe("host:node-2");
  await expect
    .poll(async () => (await table("Services")).map((r) => r[0]))
    .toEqual(["db", "kube-proxy"]);
});

test("a filter that is no field term says so and sends nothing", async () => {
  render();
  await table("Services");
  const input = document.querySelector<HTMLInputElement>('input[aria-label="Filter"]');
  await userEvent.type(input ?? document.body, "host:node-2 oops");
  await expect
    .poll(() => document.querySelector(".res-fault")?.textContent)
    .toBe('"oops" is no field:value, as host:framework or -service:etcd');
});

test("clicking a line opens its logs around the bin clicked, less the sampler's own lines", async () => {
  history.replaceState(null, "", `/ui/resources/time=${encodeURIComponent(time)}`);
  fixtures = fixtureClient(engine);
  root = createRoot(document.body.appendChild(document.createElement("div")));
  root.render(
    createElement(Shell, {
      routes: [
        { path: "/ui/resources", view: ResourcesView },
        { path: "/ui/logs", view: () => null },
      ],
      client: fixtures,
    }),
  );
  await expect
    .poll(() => document.querySelector('[aria-label="CPU"] .u-legend')?.textContent)
    .toContain("checkout");
  const over = document.querySelector('[aria-label="CPU"] .u-over');
  const box = over?.getBoundingClientRect() ?? new DOMRect();
  // Halfway along, at the top of the plot, where checkout's 1.25 cores is the line nearest.
  const at = { clientX: box.left + box.width / 2, clientY: box.top + 2, bubbles: true };
  // A press and release in one place, as uPlot stops a click that ends a drag.
  for (const type of ["mousemove", "mousedown", "mouseup", "click"]) {
    over?.dispatchEvent(new MouseEvent(type, { ...at, movementX: 1 }));
  }
  await expect.poll(() => decodeURIComponent(location.pathname)).toContain("/ui/logs/");
  const path = decodeURIComponent(location.pathname);
  expect(path).toContain('query={service="checkout"} |~ "."');
  const [from = "", to = ""] = /time=([^/]+)/.exec(path)?.[1]?.split(",") ?? [];
  expect(Date.parse(to) - Date.parse(from)).toBe(3 * 15_000);
});

test("a container living within one bin is a point, not nothing", async () => {
  history.replaceState(null, "", `/ui/resources/time=${encodeURIComponent(time)}/service=brief`);
  const values = (v: number) => Array.from({ length: 40 }, (_, i) => (i === 20 ? v : null));
  const brief = {
    ...use("brief", { cpu_cores_mean: 0.5, memory_bytes_mean: 2 ** 27 }),
    cpu_cores: values(0.5),
    memory_bytes: values(2 ** 27),
  };
  fixtures = fixtureClient([
    {
      tool: "resource_usage",
      answer: {
        data: {
          start: 1784383200000,
          step: 15_000,
          hosts: [],
          groups: [brief],
          total: { ...brief, key: "total" },
          columns: ["service", "cpu_usage_total", "memory_working_set"],
        },
        meta: { ...meta, token_estimate: 0 },
      },
    },
  ]);
  const drawn = vi.spyOn(Path2D.prototype, "arc");
  root = createRoot(document.body.appendChild(document.createElement("div")));
  root.render(
    createElement(Shell, {
      routes: [{ path: "/ui/resources", view: ResourcesView }],
      client: fixtures,
    }),
  );
  await expect.poll(() => drawn.mock.calls.length).toBeGreaterThan(0);
  drawn.mockRestore();
});

test("dragging across a chart narrows the range to the span dragged", async () => {
  history.replaceState(null, "", `/ui/resources/time=${encodeURIComponent(time)}`);
  // Any range answers with the window's use, so the narrowed one draws too.
  const [overview] = engine;
  fixtures = fixtureClient([{ tool: "resource_usage", answer: overview?.answer }]);
  root = createRoot(document.body.appendChild(document.createElement("div")));
  root.render(
    createElement(Shell, {
      routes: [{ path: "/ui/resources", view: ResourcesView }],
      client: fixtures,
    }),
  );
  await expect.poll(() => document.querySelector(".u-over")).toBeTruthy();
  const over = document.querySelector(".u-over");
  const box = over?.getBoundingClientRect() ?? new DOMRect();
  // uPlot takes a move of no distance for a stray one, as a real drag never makes.
  const at = (type: string, f: number) =>
    over?.dispatchEvent(
      new MouseEvent(type, {
        clientX: box.left + f * box.width,
        clientY: box.top + 10,
        movementX: 1,
        bubbles: true,
      }),
    );
  at("mousedown", 0.25);
  at("mousemove", 0.75);
  at("mouseup", 0.75);
  await expect
    .poll(() => decodeURIComponent(location.pathname))
    .toMatch(/\/time=2026-07-18T14:0[23]:\d\d\.\d{3}Z,2026-07-18T14:0[78]:\d\d\.\d{3}Z/);
});

test("the facts are every container's and unit's use summed, apart from the hosts' own", async () => {
  render();
  await expect
    .poll(() => document.querySelector(".res-facts")?.textContent)
    .toBe(
      "Hosts4sampledCPU3.65 coresmean · peak 3.65 coresMemory4.4 GiBmean · peak 4.9 GiBNetwork in881 KiB/smeanNetwork out588 KiB/smeanDisk used4.2 GiBlatest",
    );
});

test("picking a service off Kubernetes shows its containers and the way back", async () => {
  render();
  await table("Services");
  [...document.querySelectorAll<HTMLButtonElement>("button.link")]
    .find((b) => b.textContent === "checkout")
    ?.click();
  expect(await table("Containers")).toEqual([
    [
      "checkout",
      "1.25 cores",
      "1.25 cores",
      "950 MiB",
      "1.4 GiB",
      "195 KiB/s",
      "98 KiB/s",
      "80 MiB",
    ],
  ]);
  expect(location.pathname).toContain("/service=checkout");
  expect(document.querySelector(".res-crumbs")?.textContent).toBe("All services›checkoutLogsGraph");
});

// A run's sandbox pod: the agent's container and the sandbox, which alone carries the pod's
// network, as cAdvisor samples them under containerd.
const meta = { snapshot: "s", coverage: 1, approx: false, cost: { gets: 0, bytes_read: 0 } };
const use = (key: string, summary: Record<string, number | null>) => ({ key, summary });
const pod = {
  start: 0,
  step: 15_000,
  hosts: [],
  groups: [
    use("agent", {
      cpu_cores_mean: 1,
      cpu_cores_peak: 1,
      memory_bytes_mean: 2 ** 30,
      memory_bytes_peak: 2 ** 30,
    }),
    use("sandbox", {
      cpu_cores_mean: 0,
      cpu_cores_peak: 0,
      memory_bytes_mean: 2 ** 20,
      memory_bytes_peak: 2 ** 20,
      rx_bytes_per_sec_mean: 1e6,
      tx_bytes_per_sec_mean: 1e5,
    }),
  ],
  total: use("total", { cpu_cores_mean: 1 }),
  columns: ["service", "pod", "container", "cpu_usage_total", "memory_working_set", "rx_bytes"],
};

test("a pod is its containers, its sandbox carrying its network", async () => {
  history.replaceState(null, "", `/ui/resources/time=${encodeURIComponent(time)}/pod=sandbox-s-1`);
  fixtures = fixtureClient([
    {
      tool: "resource_usage",
      req: {
        selector: {
          time: { from: "2026-07-18T14:00:00Z", to: "2026-07-18T14:10:00Z" },
          where: [{ field: "pod", op: "eq", value: "sandbox-s-1" }],
        },
        group_by: "container",
      },
      answer: { data: pod, meta: { ...meta, token_estimate: 0 } },
    },
  ]);
  root = createRoot(document.body.appendChild(document.createElement("div")));
  root.render(
    createElement(Shell, {
      routes: [{ path: "/ui/resources", view: ResourcesView }],
      client: fixtures,
    }),
  );
  expect(await table("Containers")).toEqual([
    ["agent", "1.00 cores", "1.00 cores", "1.0 GiB", "1.0 GiB", "—", "—"],
    ["sandbox", "0.00 cores", "0.00 cores", "1.0 MiB", "1.0 MiB", "977 KiB/s", "98 KiB/s"],
  ]);
  expect(document.querySelector(".res-crumbs")?.textContent).toBe("All services›sandbox-s-1Logs");
});
