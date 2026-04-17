import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { Vex, sqliteAdapter } from "vex-core";
import { lanesPlugin } from "../src/plugins/lanes.js";
import { runsPlugin } from "../src/plugins/runs.js";
import { workersPlugin } from "../src/plugins/workers.js";
import { LocalVexClient } from "../src/core/client.js";
import { Orchestrator } from "../src/core/orchestrator.js";
import { Scheduler } from "../src/core/scheduler.js";

let vex: Vex;
let orchestrator: Orchestrator;
let scheduler: Scheduler;

const mockResolvePlugin = () =>
  ({ name: "mock", version: "1.0.0", tables: [] }) as any;

beforeEach(async () => {
  vex = await Vex.create({
    plugins: [lanesPlugin, runsPlugin, workersPlugin],
    transactional: sqliteAdapter(":memory:"),
    analytical: sqliteAdapter(":memory:"),
  });
  orchestrator = new Orchestrator(new LocalVexClient(vex), {
    resolvePlugin: mockResolvePlugin,
  });
  scheduler = new Scheduler(vex, orchestrator);
});

afterEach(async () => {
  await scheduler.stop();
  await vex.close();
});

describe("scheduler", () => {
  test("start registers jobs for enabled lanes", async () => {
    await vex.mutate("lanes.create", {
      name: "github-sync",
      sourcePlugin: "github",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 15m",
    });
    await vex.mutate("lanes.create", {
      name: "cf-sync",
      sourcePlugin: "cloudflare",
      sourceConfig: {},
      sinkType: "r2",
      sinkConfig: {},
      schedule: "every 1h",
    });

    await scheduler.start();

    const jobs = scheduler.listJobs();
    expect(jobs).toContain("sync.github_sync");
    expect(jobs).toContain("sync.cf_sync");
  });

  test("disabled lanes do not get jobs", async () => {
    const id = await vex.mutate("lanes.create", {
      name: "disabled-pipe",
      sourcePlugin: "github",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });
    await vex.mutate("lanes.toggle", { id }); // disable it

    await scheduler.start();

    const jobs = scheduler.listJobs();
    expect(jobs).not.toContain("sync.disabled_pipe");
  });

  test("refresh removes jobs for disabled lanes", async () => {
    const id = await vex.mutate("lanes.create", {
      name: "togglable",
      sourcePlugin: "github",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });

    await scheduler.start();
    expect(scheduler.listJobs()).toContain("sync.togglable");

    // Disable the lane
    await vex.mutate("lanes.toggle", { id });
    await scheduler.refresh();

    expect(scheduler.listJobs()).not.toContain("sync.togglable");
  });

  test("refresh picks up new lanes", async () => {
    await scheduler.start();
    expect(scheduler.listJobs()).toHaveLength(0);

    await vex.mutate("lanes.create", {
      name: "new-pipe",
      sourcePlugin: "github",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 30m",
    });

    await scheduler.refresh();
    expect(scheduler.listJobs()).toContain("sync.new_pipe");
  });

  test("stop removes all managed jobs", async () => {
    await vex.mutate("lanes.create", {
      name: "ephemeral",
      sourcePlugin: "github",
      sourceConfig: {},
      sinkType: "s3",
      sinkConfig: {},
      schedule: "every 1h",
    });

    await scheduler.start();
    expect(scheduler.listJobs().length).toBeGreaterThan(0);

    await scheduler.stop();
    expect(scheduler.listJobs()).toHaveLength(0);
  });

  test("scheduler does not own heartbeat — that's the worker's job", async () => {
    const workerId = await vex.mutate("workers.register", {
      name: "worker-1",
      host: "localhost",
    });

    scheduler.setWorkerId(workerId);
    await scheduler.start();

    // Heartbeat lives on whoever owns the worker row (dashboard's
    // telemetry job, or the standalone worker binary's own timer) —
    // not in the scheduler, so dashboard-only mode stays clean.
    expect(scheduler.listJobs()).not.toContain("scheduler.heartbeat");
  });
});
