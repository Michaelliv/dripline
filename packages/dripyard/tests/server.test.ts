import { afterEach, beforeEach, describe, expect, test } from "bun:test";
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
  const res = await fetch(`${baseUrl}/vex/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, args }),
  });
  return res.json();
}

async function mutate(name: string, args: Record<string, any> = {}) {
  const res = await fetch(`${baseUrl}/vex/mutate`, {
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
