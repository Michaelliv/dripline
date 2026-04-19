import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { startServer } from "../src/server.js";

let app: Awaited<ReturnType<typeof startServer>>;
let baseUrl: string;

beforeEach(async () => {
  app = await startServer({
    port: 0, // random port
    dbPath: ":memory:",
    orchestratorOptions: {
      resolvePlugin: () =>
        ({ name: "mock", version: "1.0.0", tables: [] }) as any,
    },
  });
  baseUrl = `http://localhost:${app.server.port}`;
});

afterEach(async () => {
  await app.close();
});

async function query(name: string, args: Record<string, any> = {}) {
  const res = await fetch(`${baseUrl}/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, args }),
  });
  return res.json();
}

async function mutate(name: string, args: Record<string, any> = {}) {
  const res = await fetch(`${baseUrl}/mutate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, args }),
  });
  return res.json();
}

describe("server", () => {
  test("health check", async () => {
    const res = await fetch(`${baseUrl}/health`);
    expect(res.status).toBe(200);
    expect(await res.text()).toBe("ok");
  });

  test("create lane via HTTP and query it back", async () => {
    const createResult = await mutate("lanes.create", {
      name: "http-test",
      sourcePlugin: "github",
      sourceConfig: { org: "acme" },
      sinkType: "s3",
      sinkConfig: { bucket: "data" },
      schedule: "every 1h",
    });

    expect(createResult.data).toBeTruthy();
    const id = createResult.data;

    const getResult = await query("lanes.get", { id });
    expect(getResult.data.name).toBe("http-test");
    expect(getResult.data.sourcePlugin).toBe("github");
    expect(getResult.data.sourceConfig).toEqual({ org: "acme" });
  });

  test("list lanes via HTTP", async () => {
    await mutate("lanes.create", {
      name: "pipe-a",
      sourcePlugin: "github",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });
    await mutate("lanes.create", {
      name: "pipe-b",
      sourcePlugin: "cloudflare",
      sourceConfig: {},
      sinkType: "r2",
      sinkConfig: {},
      schedule: "every 30m",
    });

    const result = await query("lanes.list");
    expect(result.data).toHaveLength(2);
  });

  test("worker is registered on startup", async () => {
    const result = await query("workers.list");
    expect(result.data.length).toBeGreaterThanOrEqual(1);
    expect(result.data[0].status).toBe("idle");
  });
});

// Separate describe block because these tests own their own server
// lifecycle — they restart dripyard against a persisted db file to
// simulate a container reboot (Render, docker, systemd). The ghost-row
// crash this guards against was invisible with :memory:, so we need
// real disk.
describe("server (persisted db)", () => {
  let dir: string;
  let dbPath: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "dripyard-test-"));
    dbPath = join(dir, "dripyard.db");
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  test("second boot drops the ghost worker row instead of crashing", async () => {
    // First boot: registers the embedded worker, row persists to disk.
    const first = await startServer({
      port: 0,
      dbPath,
      log: false,
      orchestratorOptions: {
        resolvePlugin: () =>
          ({ name: "mock", version: "1.0.0", tables: [] }) as any,
      },
    });
    const firstId = first.workerId;
    await first.close();

    // Second boot against the same file — the row from the first boot
    // is still there. Without ghost-cleanup this throws
    // UNIQUE constraint failed: workers.name and kills the process.
    const second = await startServer({
      port: 0,
      dbPath,
      log: false,
      orchestratorOptions: {
        resolvePlugin: () =>
          ({ name: "mock", version: "1.0.0", tables: [] }) as any,
      },
    });
    try {
      expect(second.workerId).toBeTruthy();
      expect(second.workerId).not.toBe(firstId);
      const res = await fetch(
        `http://localhost:${second.server.port}/query`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: "workers.list", args: {} }),
        },
      );
      const json = (await res.json()) as { data: Array<{ _id: string }> };
      // Exactly one row: the ghost got deregistered, the live worker
      // took its place.
      expect(json.data).toHaveLength(1);
      expect(json.data[0]._id).toBe(second.workerId!);
    } finally {
      await second.close();
    }
  });

  test("boot deregisters ALL older ghost rows, not just same-name", async () => {
    // Kubernetes (Render, GKE, EKS) assigns a fresh pod-name suffix
    // on every redeploy, so successive boots register workers with
    // DIFFERENT names — the unique constraint never fires, but ghost
    // rows accumulate. This test simulates three prior pods by varying
    // DRIPYARD_WORKER between boots, then checks the final boot keeps
    // ONLY its own row.
    const boot = async (name: string) => {
      process.env.DRIPYARD_WORKER = name;
      try {
        const srv = await startServer({
          port: 0,
          dbPath,
          log: false,
          orchestratorOptions: {
            resolvePlugin: () =>
              ({ name: "mock", version: "1.0.0", tables: [] }) as any,
          },
        });
        const id = srv.workerId;
        await srv.close();
        return id;
      } finally {
        delete process.env.DRIPYARD_WORKER;
      }
    };

    await boot("worker-pod-aaa-1");
    await boot("worker-pod-bbb-2");
    await boot("worker-pod-ccc-3");

    // Fourth boot — the live one we'll inspect.
    process.env.DRIPYARD_WORKER = "worker-pod-ddd-4";
    const live = await startServer({
      port: 0,
      dbPath,
      log: false,
      orchestratorOptions: {
        resolvePlugin: () =>
          ({ name: "mock", version: "1.0.0", tables: [] }) as any,
      },
    });
    delete process.env.DRIPYARD_WORKER;

    try {
      const res = await fetch(
        `http://localhost:${live.server.port}/query`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: "workers.list", args: {} }),
        },
      );
      const json = (await res.json()) as {
        data: Array<{ _id: string; name: string }>;
      };
      expect(json.data).toHaveLength(1);
      expect(json.data[0]._id).toBe(live.workerId!);
      expect(json.data[0].name).toBe("worker-pod-ddd-4");
    } finally {
      await live.close();
    }
  });
});
