import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { Vex, sqliteAdapter } from "vex-core";
import { LocalVexClient } from "../src/core/client.js";
import { Orchestrator } from "../src/core/orchestrator.js";
import {
  lanesPlugin,
  setLanesOrchestrator,
} from "../src/plugins/lanes.js";
import { runsPlugin } from "../src/plugins/runs.js";
import { workersPlugin } from "../src/plugins/workers.js";

let vex: Vex;

beforeEach(async () => {
  vex = await Vex.create({
    plugins: [lanesPlugin, runsPlugin, workersPlugin],
    transactional: sqliteAdapter(":memory:"),
    analytical: sqliteAdapter(":memory:"),
  });
});

afterEach(async () => {
  await vex.close();
});

describe("lanes", () => {
  const sampleLane = {
    name: "github-sync",
    sourcePlugin: "github",
    sourceConfig: { token: "ghp_xxx", org: "acme" },
    sinkType: "r2",
    sinkConfig: { bucket: "data", endpoint: "https://r2.example.com" },
    schedule: "every 15m",
  };

  test("create and get", async () => {
    const id = await vex.mutate("lanes.create", sampleLane);
    expect(typeof id).toBe("string");

    const lane = await vex.query("lanes.get", { id });
    expect(lane.name).toBe("github-sync");
    expect(lane.sourcePlugin).toBe("github");
    expect(lane.sourceConfig).toEqual({ token: "ghp_xxx", org: "acme" });
    expect(lane.enabled).toBe(true);
    expect(lane.proxyEnabled).toBe(false);
    expect(lane.createdAt).toBeGreaterThan(0);
  });

  test("list", async () => {
    await vex.mutate("lanes.create", sampleLane);
    await vex.mutate("lanes.create", {
      ...sampleLane,
      name: "cloudflare-sync",
      sourcePlugin: "cloudflare",
    });

    const list = await vex.query("lanes.list");
    expect(list).toHaveLength(2);
    expect(list[0].name).toBe("cloudflare-sync"); // sorted asc
    expect(list[1].name).toBe("github-sync");
  });

  test("toggle", async () => {
    const id = await vex.mutate("lanes.create", sampleLane);

    const result = await vex.mutate("lanes.toggle", { id });
    expect(result.enabled).toBe(false);

    const lane = await vex.query("lanes.get", { id });
    expect(lane.enabled).toBe(false);

    await vex.mutate("lanes.toggle", { id });
    const toggled = await vex.query("lanes.get", { id });
    expect(toggled.enabled).toBe(true);
  });

  test("update", async () => {
    const id = await vex.mutate("lanes.create", sampleLane);
    await vex.mutate("lanes.update", { id, schedule: "every 1h" });

    const lane = await vex.query("lanes.get", { id });
    expect(lane.schedule).toBe("every 1h");
    expect(lane.updatedAt).toBeGreaterThanOrEqual(lane.createdAt);
  });

  test("delete", async () => {
    const id = await vex.mutate("lanes.create", sampleLane);
    await vex.mutate("lanes.delete", { id });

    const lane = await vex.query("lanes.get", { id });
    expect(lane).toBeNull();
  });

  test("create validates schedule", async () => {
    expect(
      vex.mutate("lanes.create", {
        ...sampleLane,
        name: "bad-schedule",
        schedule: "every moon",
      }),
    ).rejects.toThrow(/invalid interval/);
  });

  test("create rejects maxRuntime >= schedule", async () => {
    expect(
      vex.mutate("lanes.create", {
        ...sampleLane,
        name: "bad-runtime",
        schedule: "every 15m",
        maxRuntime: "30m",
      }),
    ).rejects.toThrow(/must be less than schedule/);
  });

  test("create accepts tables array", async () => {
    const id = await vex.mutate("lanes.create", {
      ...sampleLane,
      name: "with-tables",
      tables: [
        { name: "github_issues", params: { org: "acme" } },
        { name: "github_repos", params: { org: "acme" } },
      ],
    });
    const lane = await vex.query("lanes.get", { id });
    expect(lane.tables).toHaveLength(2);
    expect(lane.tables[0].name).toBe("github_issues");
    expect(lane.tables[0].params).toEqual({ org: "acme" });
  });

  test("runNow dispatches through the configured orchestrator", async () => {
    // Use local mode (no endpoint) — orchestrator skips LeaseStore and
    // runs inline without real R2. Credentials absent with endpoint set
    // would throw synchronously before writing a run row.
    const id = await vex.mutate("lanes.create", {
      ...sampleLane,
      name: "runnow-sync",
      sinkConfig: {},
    });

    let resolvedPluginName: string | null = null;
    const orchestrator = new Orchestrator(new LocalVexClient(vex), {
      resolvePlugin: (name) => {
        resolvedPluginName = name;
        return { name: "mock", version: "1.0.0", tables: [] } as any;
      },
    });
    setLanesOrchestrator(orchestrator);

    const result = await vex.mutate("lanes.runNow", { id });
    expect(result).toEqual({ triggered: true });

    // runNow is fire-and-forget; wait briefly for the orchestrator to
    // write the runs row and resolve the plugin.
    await new Promise((r) => setTimeout(r, 150));

    expect(resolvedPluginName).toBe("github");
    const runs = await vex.query("runs.list", { laneId: id, limit: 10 });
    expect(runs.length).toBeGreaterThan(0);
    expect(runs[0].laneId).toBe(id);

    setLanesOrchestrator(null);
  });

  test("runNow rejects when orchestrator is not configured", async () => {
    const id = await vex.mutate("lanes.create", {
      ...sampleLane,
      name: "runnow-noorch",
      sinkConfig: {},
    });
    setLanesOrchestrator(null);

    expect(vex.mutate("lanes.runNow", { id })).rejects.toThrow(
      /No orchestrator configured/,
    );
  });

  test("runNow rejects disabled lanes", async () => {
    const id = await vex.mutate("lanes.create", {
      ...sampleLane,
      name: "runnow-disabled",
      sinkConfig: {},
    });
    await vex.mutate("lanes.toggle", { id }); // now disabled

    const orchestrator = new Orchestrator(new LocalVexClient(vex), {
      resolvePlugin: () =>
        ({ name: "mock", version: "1.0.0", tables: [] }) as any,
    });
    setLanesOrchestrator(orchestrator);

    expect(vex.mutate("lanes.runNow", { id })).rejects.toThrow(
      /disabled/,
    );
    setLanesOrchestrator(null);
  });
});

describe("runs", () => {
  test("start and finish", async () => {
    const laneId = await vex.mutate("lanes.create", {
      name: "test",
      sourcePlugin: "github",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });

    const runId = await vex.mutate("runs.start", { laneId });
    expect(typeof runId).toBe("string");

    const run = await vex.query("runs.get", { id: runId });
    expect(run.status).toBe("running");
    expect(run.laneId).toBe(laneId);

    await vex.mutate("runs.finish", {
      id: runId,
      status: "ok",
      rowsSynced: 100,
      filesPublished: 2,
      tableResults: [
        { tableName: "github_issues", rowsInserted: 80, durationMs: 500 },
        { tableName: "github_repos", rowsInserted: 20, durationMs: 200 },
      ],
    });

    const finished = await vex.query("runs.get", { id: runId });
    expect(finished.status).toBe("ok");
    expect(finished.rowsSynced).toBe(100);
    expect(finished.durationMs).toBeGreaterThanOrEqual(0);
    expect(finished.tableResults).toHaveLength(2);
  });

  test("list by lane", async () => {
    const p1 = await vex.mutate("lanes.create", {
      name: "p1",
      sourcePlugin: "github",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });
    const p2 = await vex.mutate("lanes.create", {
      name: "p2",
      sourcePlugin: "github",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });

    await vex.mutate("runs.start", { laneId: p1 });
    await vex.mutate("runs.start", { laneId: p1 });
    await vex.mutate("runs.start", { laneId: p2 });

    const allRuns = await vex.query("runs.list");
    expect(allRuns).toHaveLength(3);

    const p1Runs = await vex.query("runs.list", { laneId: p1 });
    expect(p1Runs).toHaveLength(2);
  });

  test("stats", async () => {
    const pId = await vex.mutate("lanes.create", {
      name: "stats-test",
      sourcePlugin: "github",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });

    const r1 = await vex.mutate("runs.start", { laneId: pId });
    await vex.mutate("runs.finish", { id: r1, status: "ok", rowsSynced: 50 });

    const r2 = await vex.mutate("runs.start", { laneId: pId });
    await vex.mutate("runs.finish", { id: r2, status: "error", error: "timeout" });

    const stats = await vex.query("runs.stats", { laneId: pId });
    expect(stats.total).toBe(2);
    expect(stats.ok).toBe(1);
    expect(stats.errors).toBe(1);
    expect(stats.lastRun).not.toBeNull();
  });
});

describe("workers", () => {
  test("register and list", async () => {
    const id = await vex.mutate("workers.register", {
      name: "worker-1",
      host: "localhost",
    });

    const workers = await vex.query("workers.list");
    expect(workers).toHaveLength(1);
    expect(workers[0].name).toBe("worker-1");
    expect(workers[0].status).toBe("idle");

    const worker = await vex.query("workers.get", { id });
    expect(worker.host).toBe("localhost");
  });

  test("heartbeat updates timestamp", async () => {
    const id = await vex.mutate("workers.register", {
      name: "worker-1",
      host: "localhost",
    });

    const before = await vex.query("workers.get", { id });
    await new Promise((r) => setTimeout(r, 10));
    await vex.mutate("workers.heartbeat", { id });

    const after = await vex.query("workers.get", { id });
    expect(after.lastHeartbeat).toBeGreaterThan(before.lastHeartbeat);
  });

  test("set running and idle", async () => {
    const id = await vex.mutate("workers.register", {
      name: "worker-1",
      host: "localhost",
    });

    await vex.mutate("workers.setRunning", {
      id,
      laneId: "pipe-1",
      runId: "run-1",
    });

    const running = await vex.query("workers.get", { id });
    expect(running.status).toBe("running");
    expect(running.currentLaneId).toBe("pipe-1");

    await vex.mutate("workers.setIdle", { id });
    const idle = await vex.query("workers.get", { id });
    expect(idle.status).toBe("idle");
    expect(idle.currentLaneId).toBeNull();
  });

  test("deregister", async () => {
    const id = await vex.mutate("workers.register", {
      name: "worker-1",
      host: "localhost",
    });
    await vex.mutate("workers.deregister", { id });

    const workers = await vex.query("workers.list");
    expect(workers).toHaveLength(0);
  });

  test("setDraining flips status and bumps heartbeat", async () => {
    const id = await vex.mutate("workers.register", {
      name: "worker-1",
      host: "localhost",
    });
    const before = await vex.query("workers.get", { id });
    expect(before.status).toBe("idle");

    await new Promise((r) => setTimeout(r, 5));
    await vex.mutate("workers.setDraining", { id });

    const after = await vex.query("workers.get", { id });
    expect(after.status).toBe("draining");
    // Heartbeat must advance so the UI doesn't flip the row to offline
    // mid-drain during the grace window.
    expect(after.lastHeartbeat).toBeGreaterThan(before.lastHeartbeat);
  });
});
