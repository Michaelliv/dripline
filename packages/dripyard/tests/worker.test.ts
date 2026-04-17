import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, unlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SocketVexClient } from "../src/core/client.js";
import { startServer } from "../src/server.js";
import { startWorker } from "../src/worker.js";

let app: Awaited<ReturnType<typeof startServer>>;
let socketPath: string;

beforeEach(async () => {
  socketPath = join(tmpdir(), `dripyard-test-${Date.now()}-${Math.random()}.sock`);
  app = await startServer({
    port: 0,
    socketPath,
    dbPath: ":memory:",
    embeddedWorker: false, // no scheduler, no auto worker — we test remote workers
    orchestratorOptions: {
      resolvePlugin: () =>
        ({ name: "mock", version: "1.0.0", tables: [] }) as any,
    },
  });
});

afterEach(async () => {
  await app.close();
  if (existsSync(socketPath)) unlinkSync(socketPath);
});

describe("unix socket transport", () => {
  test("SocketVexClient can query and mutate over the socket", async () => {
    const client = new SocketVexClient(socketPath);

    // List via socket — should be empty
    const empty = await client.query<any[]>("lanes.list");
    expect(empty).toEqual([]);

    // Create via socket
    const id = await client.mutate<string>("lanes.create", {
      name: "sock-test",
      sourcePlugin: "mock",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });
    expect(typeof id).toBe("string");

    // Read back via socket
    const list = await client.query<Array<{ name: string }>>("lanes.list");
    expect(list).toHaveLength(1);
    expect(list[0].name).toBe("sock-test");
  });

  test("stop() waits for in-flight runs before deregistering", async () => {
    // Create a lane that will be picked up by the worker's poller.
    // We use 'every 100ms' so the poller fires quickly, and a plugin
    // resolver that blocks forever — the run will be in-flight when
    // we call stop().
    const id = await new SocketVexClient(socketPath).mutate<string>(
      "lanes.create",
      {
        name: "slow-lane",
        sourcePlugin: "slow",
        sourceConfig: {},
        sinkType: "local",
        sinkConfig: {}, // no endpoint → local mode, no R2 lease needed
        schedule: "every 1s",
        tables: [{ name: "t", params: {} }],
      },
    );
    expect(id).toBeTruthy();

    // A plugin whose list() never yields until the signal aborts.
    let aborted = false;
    const slowPlugin = {
      name: "slow",
      version: "1.0.0",
      tables: [
        {
          name: "t",
          columns: [{ name: "id", type: "string" as const }],
          async *list(ctx: any) {
            await new Promise<void>((resolve, reject) => {
              const onAbort = () => {
                aborted = true;
                reject(new DOMException("aborted", "AbortError"));
              };
              if (ctx.signal?.aborted) return onAbort();
              ctx.signal?.addEventListener?.("abort", onAbort);
            });
          },
        },
      ],
    };

    const handle = await startWorker({
      socketPath,
      name: "drain-test",
      resolvePlugin: () => slowPlugin as any,
      lanePollIntervalMs: 60_000,
      heartbeatIntervalMs: 60_000,
      telemetryIntervalMs: 60_000,
      shutdownGraceMs: 50, // tiny grace so we force the abort path
    });

    // Give the scheduler a beat to fire the first run.
    await new Promise((r) => setTimeout(r, 1200));

    const stopStart = Date.now();
    await handle.stop();
    const stopDuration = Date.now() - stopStart;

    // Grace window was 50ms; abort should kick in and unblock the
    // plugin immediately. Total stop() should be well under 2s even
    // counting the deregister round-trip.
    expect(stopDuration).toBeLessThan(2000);
    expect(aborted).toBe(true);
  });

  test("startWorker registers over socket and deregisters on stop", async () => {
    const handle = await startWorker({
      socketPath,
      name: "test-worker",
      resolvePlugin: () =>
        ({ name: "mock", version: "1.0.0", tables: [] }) as any,
      // Short cadence for the test, but not so short we hammer the socket
      lanePollIntervalMs: 60_000,
      heartbeatIntervalMs: 60_000,
      telemetryIntervalMs: 60_000,
    });

    expect(handle.workerId).toBeTruthy();

    // Worker should appear in the dashboard's workers list
    const client = new SocketVexClient(socketPath);
    const workers = await client.query<Array<{ _id: string; name: string }>>(
      "workers.list",
    );
    expect(workers.map((w) => w.name)).toContain("test-worker");

    await handle.stop();

    // After stop, the worker is deregistered
    const after = await client.query<Array<{ name: string }>>("workers.list");
    expect(after.map((w) => w.name)).not.toContain("test-worker");
  });
});
