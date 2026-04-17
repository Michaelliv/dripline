import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { Vex, sqliteAdapter } from "vex-core";
import { lanesPlugin } from "../src/plugins/lanes.js";
import { runsPlugin } from "../src/plugins/runs.js";
import { workersPlugin } from "../src/plugins/workers.js";
import { LocalVexClient } from "../src/core/client.js";
import { Orchestrator } from "../src/core/orchestrator.js";

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

describe("orchestrator", () => {
  test("executeLane reads lane and records run", async () => {
    // Create a lane
    const laneId = await vex.mutate("lanes.create", {
      name: "test-sync",
      sourcePlugin: "mock",
      sourceConfig: { key: "value" },
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });

    // Create orchestrator with a mock plugin resolver
    let resolvedPluginName: string | null = null;
    const orchestrator = new Orchestrator(new LocalVexClient(vex), {
      resolvePlugin: (name) => {
        resolvedPluginName = name;
        // Return a minimal dripline plugin that produces no data
        return {
          name: "mock",
          version: "1.0.0",
          tables: [],
        } as any;
      },
    });

    const result = await orchestrator.executeLane(laneId);

    // The plugin was resolved
    expect(resolvedPluginName).toBe("mock");

    // A run was recorded
    expect(result.runId).toBeTruthy();
    expect(result.status).toBe("ok");

    // The run is in the database
    const run = await vex.query("runs.get", { id: result.runId });
    expect(run.status).toBe("ok");
    expect(run.laneId).toBe(laneId);
  });

  test("executeLane records error on failure", async () => {
    const laneId = await vex.mutate("lanes.create", {
      name: "failing-sync",
      sourcePlugin: "broken",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });

    const orchestrator = new Orchestrator(new LocalVexClient(vex), {
      resolvePlugin: () => {
        throw new Error("Plugin not found: broken");
      },
    });

    const result = await orchestrator.executeLane(laneId);
    expect(result.status).toBe("error");
    expect(result.error).toContain("Plugin not found");

    const run = await vex.query("runs.get", { id: result.runId });
    expect(run.status).toBe("error");
    expect(run.error).toContain("Plugin not found");
  });

  test("executeLane updates worker status", async () => {
    const laneId = await vex.mutate("lanes.create", {
      name: "worker-test",
      sourcePlugin: "mock",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });

    const workerId = await vex.mutate("workers.register", {
      name: "worker-1",
      host: "localhost",
    });

    const orchestrator = new Orchestrator(new LocalVexClient(vex), {
      resolvePlugin: () =>
        ({ name: "mock", version: "1.0.0", tables: [] }) as any,
    });

    const result = await orchestrator.executeLane(laneId, workerId);
    expect(result.status).toBe("ok");

    // Worker should be back to idle after run
    const worker = await vex.query("workers.get", { id: workerId });
    expect(worker.status).toBe("idle");
    expect(worker.currentLaneId).toBeNull();
  });

  test("getStatus returns summary", async () => {
    await vex.mutate("lanes.create", {
      name: "p1",
      sourcePlugin: "github",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });
    await vex.mutate("lanes.create", {
      name: "p2",
      sourcePlugin: "github",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });
    await vex.mutate("workers.register", {
      name: "worker-1",
      host: "localhost",
    });

    const orchestrator = new Orchestrator(new LocalVexClient(vex));
    const status = await orchestrator.getStatus();

    expect(status.lanes.total).toBe(2);
    expect(status.lanes.enabled).toBe(2);
    expect(status.runs.active).toBe(0);
    expect(status.workers).toHaveLength(1);
    expect(status.workers[0].name).toBe("worker-1");
  });

  test("missing lane throws", async () => {
    const orchestrator = new Orchestrator(new LocalVexClient(vex));
    expect(orchestrator.executeLane("nonexistent")).rejects.toThrow(
      "Lane not found",
    );
  });
});
