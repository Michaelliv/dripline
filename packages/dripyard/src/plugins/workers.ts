import type { VexPluginAPI } from "vex-core";
import type { Spawner } from "../core/spawner.js";

/**
 * Default: throw. The dashboard passes a real spawner when it
 * registers this plugin via `workersPluginWithSpawner()`.
 */
let activeSpawner: Spawner | null = null;

/** Dashboard wires its ForkSpawner (or a cloud spawner) in here. */
export function setWorkersSpawner(spawner: Spawner | null): void {
  activeSpawner = spawner;
}

export function workersPlugin(api: VexPluginAPI) {
  api.setName("workers");

  api.registerTable("workers", {
    columns: {
      name: { type: "string", index: true },
      status: { type: "string", index: true }, // idle | running | offline
      lastHeartbeat: { type: "number" },
      currentLaneId: { type: "string", optional: true },
      currentRunId: { type: "string", optional: true },
      host: { type: "string" },
      startedAt: { type: "number" },
      /**
       * Set when this worker was started by the dashboard's spawner.
       * Links the workers row back to the spawner handle so the UI's
       * "stop" button can kill the correct child. Null for externally
       * started workers (systemd, k8s, the embedded one).
       */
      spawnHandle: { type: "string", optional: true },
    },
    unique: [["name"]],
  });

  /**
   * Per-worker telemetry samples — mirrors vex's metrics.samples but
   * scoped to a worker so multi-worker deployments (or single-machine
   * multi-process) can render per-worker sparklines. Sampled every 10s
   * by the server's telemetry job.
   */
  api.registerTable("worker_samples", {
    columns: {
      workerId: { type: "string", index: true },
      timestamp: { type: "number", index: true },
      heapUsedMb: { type: "number" },
      rssMb: { type: "number" },
      externalMb: { type: "number" },
      loadAvg1m: { type: "number" },
      loadAvg5m: { type: "number" },
      uptimeS: { type: "number" },
    },
  });

  api.registerQuery("list", {
    args: {},
    async handler(ctx) {
      return ctx.db.table("workers").order("name", "asc").all();
    },
  });

  api.registerQuery("get", {
    args: { id: "string" },
    async handler(ctx, args) {
      return ctx.db.table("workers").where("_id", "=", args.id).first();
    },
  });

  api.registerMutation("register", {
    args: { name: "string", host: "string" },
    async handler(ctx, args) {
      return ctx.db.table("workers").insert({
        name: args.name,
        status: "idle",
        lastHeartbeat: Date.now(),
        host: args.host,
        startedAt: Date.now(),
        spawnHandle: args.spawnHandle ?? null,
      });
    },
  });

  api.registerMutation("heartbeat", {
    args: { id: "string" },
    async handler(ctx, args) {
      await ctx.db.table("workers").update(args.id, {
        lastHeartbeat: Date.now(),
      });
    },
  });

  api.registerMutation("setRunning", {
    args: { id: "string", laneId: "string", runId: "string" },
    async handler(ctx, args) {
      await ctx.db.table("workers").update(args.id, {
        status: "running",
        currentLaneId: args.laneId,
        currentRunId: args.runId,
        lastHeartbeat: Date.now(),
      });
    },
  });

  /**
   * Mark a worker as draining — it's about to shut down but is still
   * awaiting in-flight runs. Heartbeat continues while in this state
   * so the dashboard doesn't flip it to "offline" mid-drain. The row
   * is removed via workers.deregister once the drain completes.
   */
  api.registerMutation("setDraining", {
    args: { id: "string" },
    async handler(ctx, args) {
      await ctx.db.table("workers").update(args.id, {
        status: "draining",
        lastHeartbeat: Date.now(),
      });
    },
  });

  api.registerMutation("setIdle", {
    args: { id: "string" },
    async handler(ctx, args) {
      await ctx.db.table("workers").update(args.id, {
        status: "idle",
        currentLaneId: null,
        currentRunId: null,
        lastHeartbeat: Date.now(),
      });
    },
  });

  api.registerMutation("deregister", {
    args: { id: "string" },
    async handler(ctx, args) {
      await ctx.db.table("workers").delete(args.id);
    },
  });

  /**
   * UI +1 button — spawn another worker process. The spawner is
   * configured at server startup. No spawner = mutation errors, which
   * is the right behavior (don't silently do nothing).
   */
  api.registerMutation("spawn", {
    args: {},
    async handler() {
      if (!activeSpawner) {
        throw new Error(
          "No spawner configured. Pass a Spawner when starting the server.",
        );
      }
      return activeSpawner.spawn();
    },
  });

  /**
   * Stop a worker we spawned. Looks up the worker's spawnHandle and
   * asks the spawner to terminate the child. The worker's SIGTERM
   * handler deregisters it on shutdown, so the row disappears naturally.
   *
   * Refuses to kill externally-started workers (no spawnHandle) — those
   * are managed by systemd/k8s/whatever, not by us.
   */
  api.registerMutation("kill", {
    args: { id: "string" },
    async handler(ctx, args) {
      const worker = await ctx.db
        .table("workers")
        .where("_id", "=", args.id)
        .first<{ spawnHandle: string | null; name: string }>();
      if (!worker) throw new Error(`Worker not found: ${args.id}`);
      if (!worker.spawnHandle) {
        throw new Error(
          `Worker ${worker.name} was not spawned by this dashboard — stop it through its own process manager.`,
        );
      }
      if (!activeSpawner?.stop) {
        throw new Error("Configured spawner does not support stop()");
      }
      await activeSpawner.stop(worker.spawnHandle);
    },
  });

  api.registerQuery("samplesRecent", {
    args: {},
    async handler(ctx, args) {
      const since = args.since ?? Date.now() - 30 * 60 * 1000; // 30m
      let q = ctx.db
        .table("worker_samples")
        .where("timestamp", ">=", since)
        .order("timestamp", "asc");
      if (args.workerId) q = q.where("workerId", "=", args.workerId);
      return q.limit(args.limit ?? 500).all();
    },
  });

  api.registerMutation("sampleRecord", {
    args: {
      workerId: "string",
      heapUsedMb: "number",
      rssMb: "number",
      externalMb: "number",
      loadAvg1m: "number",
      loadAvg5m: "number",
      uptimeS: "number",
    },
    async handler(ctx, args) {
      await ctx.db.table("worker_samples").insert({
        workerId: args.workerId,
        timestamp: Date.now(),
        heapUsedMb: args.heapUsedMb,
        rssMb: args.rssMb,
        externalMb: args.externalMb,
        loadAvg1m: args.loadAvg1m,
        loadAvg5m: args.loadAvg5m,
        uptimeS: args.uptimeS,
      });
    },
  });
}
