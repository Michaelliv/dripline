/**
 * Standalone worker — connects to a dashboard via unix socket, pulls
 * the current lane list, runs its own scheduler, and syncs.
 *
 * Stateless: no SQLite, no UI, no vex-core job system. Just a setInterval
 * loop per enabled lane that calls the shared Orchestrator (now
 * pointed at a SocketVexClient instead of a local Vex).
 *
 * The dashboard is the source of truth for lane definitions + run
 * history + worker registry. R2 remains the source of truth for
 * coordination (leases) and data (parquet), unchanged.
 */

import { hostname } from "node:os";
import { parseInterval } from "dripline";
import type { PluginDef, PluginFunction } from "dripline";
import { SocketVexClient } from "./core/client.js";
import { Orchestrator } from "./core/orchestrator.js";

export interface WorkerOptions {
  socketPath: string;
  name?: string;
  /** Map source plugin names to dripline plugin instances. */
  resolvePlugin: (name: string) => PluginDef | PluginFunction;
  /** How often to re-fetch the lane list from the dashboard. */
  lanePollIntervalMs?: number;
  /** Heartbeat cadence to the dashboard. */
  heartbeatIntervalMs?: number;
  /** Telemetry sample cadence. */
  telemetryIntervalMs?: number;
  /**
   * How long graceful shutdown waits for in-flight runs to complete
   * before aborting them. Set to 0 for immediate abort on SIGTERM.
   * Default: 30s, which is enough for most lanes to finish a batch
   * and renew their lease into cooldown.
   */
  shutdownGraceMs?: number;
}

/** Running worker handle — exposed for tests and graceful shutdown. */
export interface WorkerHandle {
  workerId: string;
  stop(): Promise<void>;
}

type Lane = {
  _id: string;
  enabled: boolean;
  schedule: string;
};

export async function startWorker(
  options: WorkerOptions,
): Promise<WorkerHandle> {
  const client = new SocketVexClient(options.socketPath);
  const orchestrator = new Orchestrator(client, {
    resolvePlugin: options.resolvePlugin,
  });

  const name = options.name ?? `worker-${hostname()}-${process.pid}`;
  // The parent spawner (if any) passes a handle via env so the dashboard
  // can correlate this workers row back to the spawned child process.
  const spawnHandle = process.env.DRIPYARD_SPAWN_HANDLE;
  const workerId = await client.mutate<string>("workers.register", {
    name,
    host: hostname(),
    spawnHandle,
  });
  console.log(`[worker] registered as ${name} (${workerId})`);

  // Schedule timers keyed by lane id so lane refresh can add/remove
  // them incrementally without tearing down the whole scheduler.
  const timers = new Map<string, ReturnType<typeof setInterval>>();

  // In-flight runs we need to drain cleanly on shutdown. Each entry
  // owns an AbortController so we can cancel the underlying dripline
  // sync if the grace window expires. The promise resolves when the
  // orchestrator's try/catch/finally has fully run — including lease
  // release — so awaiting it guarantees R2 state is consistent.
  type Inflight = { promise: Promise<unknown>; controller: AbortController };
  const inflight = new Set<Inflight>();
  let shuttingDown = false;

  function trackRun(laneId: string): void {
    if (shuttingDown) return;
    const controller = new AbortController();
    const promise = orchestrator
      .executeLane(laneId, workerId, controller.signal)
      .catch((err) => {
        // AbortError is expected during shutdown — don't noise up the logs.
        if ((err as Error)?.name === "AbortError") return;
        console.error(
          `[worker] executeLane ${laneId} failed:`,
          (err as Error)?.message ?? err,
        );
      })
      .finally(() => {
        inflight.delete(entry);
      });
    const entry: Inflight = { promise, controller };
    inflight.add(entry);
  }

  async function refreshLanes() {
    let lanes: Lane[];
    try {
      lanes = await client.query<Lane[]>("lanes.list");
    } catch (err) {
      console.error(`[worker] lane refresh failed:`, (err as Error).message);
      return;
    }

    const enabledIds = new Set(
      lanes.filter((p) => p.enabled).map((p) => p._id),
    );

    // Prune timers for disabled or deleted lanes.
    for (const [id, timer] of timers) {
      if (!enabledIds.has(id)) {
        clearInterval(timer);
        timers.delete(id);
      }
    }

    // Create timers for new or re-enabled lanes.
    for (const p of lanes) {
      if (!p.enabled || timers.has(p._id)) continue;
      const intervalMs = parseInterval(stripEvery(p.schedule));
      const laneId = p._id;
      const timer = setInterval(() => trackRun(laneId), intervalMs);
      timers.set(p._id, timer);
    }
  }

  await refreshLanes();

  const lanePollMs = options.lanePollIntervalMs ?? 30_000;
  const lanePoller = setInterval(refreshLanes, lanePollMs);

  // Heartbeat — fire-and-forget. If the dashboard is down we don't
  // care, leases keep coordinating sync work regardless.
  const hbMs = options.heartbeatIntervalMs ?? 30_000;
  const heartbeatTimer = setInterval(() => {
    client
      .mutate("workers.heartbeat", { id: workerId })
      .catch(() => {
        /* dashboard unreachable — ignore */
      });
  }, hbMs);

  // Telemetry — push RSS/heap/loadavg for the dashboard's sparklines.
  const telemetryMs = options.telemetryIntervalMs ?? 10_000;
  const telemetryTimer = setInterval(async () => {
    const mem = process.memoryUsage();
    const { loadavg } = await import("node:os");
    const [avg1, avg5] = loadavg();
    client
      .mutate("workers.sampleRecord", {
        workerId,
        heapUsedMb: Math.round((mem.heapUsed / 1024 / 1024) * 10) / 10,
        rssMb: Math.round((mem.rss / 1024 / 1024) * 10) / 10,
        externalMb: Math.round((mem.external / 1024 / 1024) * 10) / 10,
        loadAvg1m: Math.round(avg1 * 100) / 100,
        loadAvg5m: Math.round(avg5 * 100) / 100,
        uptimeS: Math.round(process.uptime()),
      })
      .catch(() => {
        /* ignore */
      });
  }, telemetryMs);

  /**
   * Graceful shutdown sequence:
   *   1. Stop scheduling new runs (clear lane timers + lane poller).
   *   2. Await in-flight runs with a grace window — they'll complete
   *      naturally, renew their lease into cooldown, and exit clean.
   *   3. If grace expires, abort the AbortController on each in-flight.
   *      Dripline throws AbortError at its next checkpoint; the
   *      orchestrator's catch block releases the lease explicitly
   *      (no TTL wait), records the run as errored, and returns.
   *   4. Await again — aborted promises should settle quickly.
   *   5. Stop heartbeat + telemetry, then deregister + exit.
   *
   * The heartbeat runs through the drain so the dashboard keeps
   * showing the worker as alive-but-draining rather than flipping it
   * to offline mid-shutdown.
   */
  const graceMs = options.shutdownGraceMs ?? 30_000;

  async function stop() {
    shuttingDown = true;

    // 1. No new runs.
    for (const t of timers.values()) clearInterval(t);
    timers.clear();
    clearInterval(lanePoller);

    // Flip the row to "draining" so the UI shows a distinct state
    // during the grace window. Best-effort — if the dashboard is down
    // there's nothing to display anyway.
    await client.mutate("workers.setDraining", { id: workerId }).catch(() => {
      /* dashboard unreachable */
    });

    // 2. Wait out the grace window for in-flight runs.
    if (inflight.size > 0) {
      console.log(
        `[worker] draining: waiting up to ${graceMs}ms for ${inflight.size} in-flight run(s)`,
      );
      const allDone = Promise.allSettled(
        [...inflight].map((e) => e.promise),
      );
      const timedOut = new Promise<"timeout">((resolve) =>
        setTimeout(() => resolve("timeout"), graceMs),
      );
      const outcome = await Promise.race([allDone, timedOut]);

      // 3. Force abort if the grace window blew. The orchestrator's
      //    catch path handles lease release for us; we just need to
      //    let those promises settle.
      if (outcome === "timeout" && inflight.size > 0) {
        console.log(
          `[worker] grace window expired, aborting ${inflight.size} run(s)`,
        );
        for (const entry of inflight) entry.controller.abort();
        await Promise.allSettled([...inflight].map((e) => e.promise));
      }
    }

    // 5. Tear down liveness timers + deregister.
    clearInterval(heartbeatTimer);
    clearInterval(telemetryTimer);
    await client
      .mutate("workers.deregister", { id: workerId })
      .catch(() => {
        /* dashboard unreachable — row will fall off via heartbeat TTL */
      });
  }

  // Graceful shutdown — deregister so the dashboard doesn't wait for
  // a heartbeat TTL to notice we're gone.
  const shutdown = () => {
    stop()
      .then(() => process.exit(0))
      .catch(() => process.exit(1));
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);

  return { workerId, stop };
}

function stripEvery(spec: string): string {
  return spec.replace(/^every\s+/, "").trim();
}
