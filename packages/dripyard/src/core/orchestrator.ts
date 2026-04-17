import {
  Database,
  Dripline,
  laneLeaseName,
  laneSchema,
  type Lease,
  LeaseStore,
  parseInterval,
  type PluginDef,
  type PluginFunction,
  Remote,
  type RemoteConfig,
  resolveRemote,
} from "dripline";
import type { FlareGun } from "@miclivs/flaregun";
import type { VexClient } from "./client.js";

/**
 * Result of one lane execution. Mirrors dripline's LaneRunResult.
 * `skipped` means the lease was held by another worker (or it's still
 * in cooldown from a recent successful run).
 */
export interface RunResult {
  runId: string;
  status: "ok" | "error" | "skipped";
  reason?: string;
  rowsSynced: number;
  filesPublished: number;
  durationMs: number;
  error?: string;
  tableResults: Array<{
    tableName: string;
    rowsInserted: number;
    cursor?: string;
    durationMs: number;
    error?: string;
  }>;
}

export interface OrchestratorStatus {
  lanes: { total: number; enabled: number };
  runs: { active: number; recentOk: number; recentErrors: number };
  workers: Array<{
    id: string;
    name: string;
    status: string;
    lastHeartbeat: number;
  }>;
}

export interface OrchestratorOptions {
  flaregun?: FlareGun;
  /** Resolve a dripline plugin by name (e.g. "github" → the plugin function). */
  resolvePlugin?: (name: string) => PluginDef | PluginFunction;
}

/** Stored lane row — the shape we read from vex. */
interface LaneRow {
  _id: string;
  name: string;
  sourcePlugin: string;
  sourceConfig: Record<string, any>;
  tables: Array<{ name: string; params?: Record<string, any> }>;
  sinkType: string;
  sinkConfig: Record<string, any>;
  proxyEnabled: boolean;
  schedule: string;
  maxRuntime: string | null;
  enabled: boolean;
}

export class Orchestrator {
  private client: VexClient;
  private flaregun?: FlareGun;
  private resolvePlugin: (name: string) => PluginDef | PluginFunction;

  /**
   * @param client VexClient — LocalVexClient in dashboard mode, SocketVexClient
   *               when running inside a standalone worker.
   */
  constructor(client: VexClient, options?: OrchestratorOptions) {
    this.client = client;
    this.flaregun = options?.flaregun;
    this.resolvePlugin = options?.resolvePlugin ?? defaultResolvePlugin;
  }

  /**
   * Execute one lane, 1:1 with dripline's `runLane`:
   *   1. Acquire R2 lease (TTL = maxRuntime). If null → record skipped.
   *   2. Create Dripline + in-memory DuckDB with lane's schema.
   *   3. Hydrate cursor state from R2 (_state/<lane>/_dripline_sync.parquet).
   *   4. For each table in the lane, call dl.sync with its params.
   *   5. Publish one parquet per table to raw/<table>/lane=<lane>/run=<id>.
   *   6. Push cursors back to R2.
   *   7. On success: renew lease to interval (cooldown marker).
   *   8. On error: release lease so another worker can retry immediately.
   *
   * When the lane's sinkConfig lacks an `endpoint`, we run in
   * "local mode" — no lease, no publish, no cursor hydration. Good for
   * local dev, tests, and dry-runs.
   */
  /**
   * @param signal Optional AbortSignal. Abort to cancel an in-flight
   *               sync — dripline throws at its next checkpoint,
   *               control falls into the catch block, the lease is
   *               released, and the run is recorded with the abort
   *               reason. Workers use this to drain cleanly on SIGTERM.
   */
  async executeLane(
    laneId: string,
    workerId?: string,
    signal?: AbortSignal,
  ): Promise<RunResult> {
    const lane = await this.client.query<LaneRow | null>(
      "lanes.get",
      { id: laneId },
    );
    if (!lane) throw new Error(`Lane not found: ${laneId}`);

    const intervalMs = parseInterval(stripEvery(lane.schedule));
    const maxRuntimeMs = lane.maxRuntime
      ? parseInterval(stripEvery(lane.maxRuntime))
      : Math.min(10 * 60 * 1000, Math.floor(intervalMs / 2));

    const hasRemote = !!lane.sinkConfig?.endpoint;
    const remote = hasRemote
      ? new Remote(lane.sinkConfig as RemoteConfig)
      : null;
    const leaseStore = hasRemote
      ? LeaseStore.fromRemote(resolveRemote(lane.sinkConfig as RemoteConfig))
      : null;

    // ── Phase 1: acquire lease ───────────────────────────────────────
    let lease: Lease | null = null;
    if (leaseStore) {
      try {
        lease = await leaseStore.acquire(
          laneLeaseName(lane.name),
          maxRuntimeMs,
        );
      } catch (e: any) {
        return this.recordRun(lane, workerId, {
          status: "error",
          error: `lease acquire failed: ${e.message ?? String(e)}`,
          rowsSynced: 0,
          filesPublished: 0,
          tableResults: [],
        });
      }

      if (lease == null) {
        return this.recordRun(lane, workerId, {
          status: "skipped",
          reason: "lease held",
          rowsSynced: 0,
          filesPublished: 0,
          tableResults: [],
        });
      }
    }

    // ── Start run record (running) ───────────────────────────────────
    const runId = await this.client.mutate<string>("runs.start", {
      laneId: lane._id,
      workerId: workerId ?? null,
    });

    if (workerId) {
      await this.client.mutate("workers.setRunning", {
        id: workerId,
        laneId: lane._id,
        runId,
      });
    }

    const start = Date.now();
    const db = await Database.create(":memory:");

    try {
      // ── Phase 2: run the sync ────────────────────────────────────────
      const result = await this.runSync(lane, db, remote, runId, signal);

      // ── Phase 3 (success): renew lease into cooldown ─────────────────
      if (leaseStore && lease) {
        // Best-effort — if we lost the lease mid-run the renew returns
        // null. Not fatal; the next run will start cleanly.
        await leaseStore.renew(lease, intervalMs).catch(() => null);
      }

      await this.client.mutate("runs.finish", {
        id: runId,
        status: "ok",
        rowsSynced: result.rowsSynced,
        filesPublished: result.filesPublished,
        tableResults: result.tableResults,
      });
      if (workerId)
        await this.client.mutate("workers.setIdle", { id: workerId });

      return {
        runId,
        status: "ok",
        ...result,
        durationMs: Date.now() - start,
      };
    } catch (err: any) {
      // ── Phase 3 (failure): release so another worker can retry ───────
      if (leaseStore && lease) {
        await leaseStore.release(lease).catch(() => {
          /* best effort */
        });
      }

      const errMsg = err?.message ?? String(err);
      await this.client.mutate("runs.finish", {
        id: runId,
        status: "error",
        error: errMsg,
      });
      if (workerId)
        await this.client.mutate("workers.setIdle", { id: workerId });

      return {
        runId,
        status: "error",
        rowsSynced: 0,
        filesPublished: 0,
        durationMs: Date.now() - start,
        error: errMsg,
        tableResults: [],
      };
    } finally {
      await db.close();
    }
  }

  /**
   * The inside of Phase 2 — isolated so the lease/run bookkeeping in
   * executeLane stays readable. Mirrors the inner try-block of
   * dripline's runLane.
   *
   * `runId` is passed through so dripline's SyncProgressEvent (fired
   * per 10k-row batch) can be streamed into the run_progress table
   * live. The UI subscribes to that and gets row counts climbing in
   * real time while the sync runs.
   */
  private async runSync(
    lane: LaneRow,
    db: Database,
    remote: Remote | null,
    runId: string,
    signal?: AbortSignal,
  ): Promise<{
    rowsSynced: number;
    filesPublished: number;
    tableResults: RunResult["tableResults"];
  }> {
    const plugin = this.resolvePlugin(lane.sourcePlugin);

    // Route HTTP through flaregun when the lane opts in. Dripline
    // threads `connection.fetch` to `ctx.fetch` — each plugin's
    // helpers call `f(...)` instead of `globalThis.fetch(...)`, so
    // every outbound request goes through the rotating worker pool
    // without the plugin knowing anything about proxies.
    //
    // When proxyEnabled is set but flaregun isn't available (missing
    // creds, Cloudflare unreachable, etc.) we refuse to run the lane
    // rather than silently hitting the upstream API from the server's
    // own IP. That would defeat the whole point of the flag.
    let fetchOverride: typeof globalThis.fetch | undefined;
    if (lane.proxyEnabled) {
      if (!this.flaregun) {
        throw new Error(
          `Lane "${lane.name}" has proxyEnabled=true but flaregun is not configured. ` +
            `Set CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID, or disable proxyEnabled on the lane.`,
        );
      }
      // Cast to the full fetch type. Flaregun's fetch is a reduced
      // callable surface — it implements (url, opts) => Promise<Response>
      // which is all plugins ever call. The `typeof globalThis.fetch`
      // contract formally includes static members like `preconnect` that
      // plugin code never touches, so this cast is safe in practice.
      fetchOverride = this.flaregun.fetch.bind(
        this.flaregun,
      ) as typeof globalThis.fetch;
    }

    const schema = laneSchema(lane.name);
    const dl = await Dripline.create({
      plugins: [plugin],
      connections: [
        {
          name: "default",
          plugin: lane.sourcePlugin,
          config: lane.sourceConfig,
          ...(fetchOverride ? { fetch: fetchOverride } : {}),
        },
      ],
      database: db,
      schema,
    });

    const tableResults: RunResult["tableResults"] = [];
    let rowsSynced = 0;
    let filesPublished = 0;

    try {
      // Pull previous cursor state from R2 so incremental lanes pick
      // up where the last run left off. No-op when remote is null.
      if (remote) await remote.hydrateCursors(db, lane.name);

      const tablesPublished = new Set<string>();
      const tablesToSync =
        lane.tables && lane.tables.length > 0
          ? lane.tables
          : // Fallback: no explicit table list → sync everything the
            // plugin exposes with no params. Matches dripline's behavior
            // when you call dl.sync() with no argument.
            [{ name: "__all__", params: {} as Record<string, any> }];

      for (const t of tablesToSync) {
        const syncArg =
          t.name === "__all__" ? undefined : { [t.name]: { ...(t.params ?? {}) } };

        // Fire-and-forget: vex mutations are serialized, so ordering is
        // preserved even without awaits. Blocking here would add a
        // round-trip per 10k rows and slow the sync down for no reason.
        const onProgress = (ev: {
          table: string;
          rowsInserted: number;
          cursor?: unknown;
          elapsedMs: number;
        }) => {
          this.client
            .mutate("runs.progressAppend", {
              runId,
              tableName: ev.table,
              rowsInserted: ev.rowsInserted,
              cursor: ev.cursor != null ? String(ev.cursor) : undefined,
              elapsedMs: ev.elapsedMs,
            })
            .catch((err) =>
              console.error(`[orchestrator] progress write failed:`, err),
            );
        };

        const result = await dl.sync(syncArg, { signal, onProgress });

        for (const r of result.tables) {
          rowsSynced += r.rowsInserted;
          if (r.rowsInserted > 0) tablesPublished.add(r.table);
          tableResults.push({
            tableName: r.table,
            rowsInserted: r.rowsInserted,
            cursor: r.cursor != null ? String(r.cursor) : undefined,
            durationMs: r.durationMs,
          });
        }
        // Per-table errors come back on result.errors — surface them as
        // tableResults entries so the UI can display them without
        // failing the whole run.
        for (const e of result.errors) {
          tableResults.push({
            tableName: e.table,
            rowsInserted: 0,
            durationMs: 0,
            error: e.error,
          });
        }
      }

      if (remote && tablesPublished.size > 0) {
        const published = await remote.publishRun(db, lane.name, [
          ...tablesPublished,
        ]);
        filesPublished = published.length;
        await remote.pushCursors(db, lane.name);
      }
    } finally {
      await dl.close();
    }

    return { rowsSynced, filesPublished, tableResults };
  }

  /**
   * Helper: record a short-circuited run (skipped before entering the
   * runs.start path, or failed during lease acquire). Writes a single
   * complete row with finishedAt set.
   */
  private async recordRun(
    lane: LaneRow,
    workerId: string | undefined,
    outcome: {
      status: "skipped" | "error";
      reason?: string;
      error?: string;
      rowsSynced: number;
      filesPublished: number;
      tableResults: RunResult["tableResults"];
    },
  ): Promise<RunResult> {
    const start = Date.now();
    const runId = await this.client.mutate<string>("runs.start", {
      laneId: lane._id,
      workerId: workerId ?? null,
    });
    await this.client.mutate("runs.finish", {
      id: runId,
      status: outcome.status,
      reason: outcome.reason,
      error: outcome.error,
      rowsSynced: outcome.rowsSynced,
      filesPublished: outcome.filesPublished,
      tableResults: outcome.tableResults,
    });
    return {
      runId,
      status: outcome.status,
      reason: outcome.reason,
      error: outcome.error,
      rowsSynced: outcome.rowsSynced,
      filesPublished: outcome.filesPublished,
      durationMs: Date.now() - start,
      tableResults: outcome.tableResults,
    };
  }

  async getStatus(): Promise<OrchestratorStatus> {
    const lanes = await this.client.query<Array<{ enabled: boolean }>>(
      "lanes.list",
    );
    const enabled = lanes.filter((p) => p.enabled).length;

    const activeRuns = await this.client.query<Array<{ status: string }>>(
      "runs.list",
      { limit: 100 },
    );
    const active = activeRuns.filter((r) => r.status === "running").length;
    const recentOk = activeRuns.filter((r) => r.status === "ok").length;
    const recentErrors = activeRuns.filter((r) => r.status === "error").length;

    const workers = (
      await this.client.query<
        Array<{
          _id: string;
          name: string;
          status: string;
          lastHeartbeat: number;
        }>
      >("workers.list")
    ).map((w) => ({
      id: w._id,
      name: w.name,
      status: w.status,
      lastHeartbeat: w.lastHeartbeat,
    }));

    return {
      lanes: { total: lanes.length, enabled },
      runs: { active, recentOk, recentErrors },
      workers,
    };
  }
}

function defaultResolvePlugin(_name: string): never {
  throw new Error(
    "No plugin resolver configured. Pass resolvePlugin to Orchestrator options.",
  );
}

/** Accept both "every 15m" (vex-core idiom) and "15m" (dripline idiom). */
function stripEvery(spec: string): string {
  return spec.replace(/^every\s+/, "").trim();
}
