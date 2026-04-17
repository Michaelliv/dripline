/**
 * `dripline run` — execute due lanes against the configured remote.
 *
 * Stateless: each invocation iterates configured lanes, tries to acquire
 * each lane's lease, runs the work if successful, and renews the lease
 * to the lane's interval on success (cooldown). Crashes release the
 * lease immediately so the next worker reclaims it.
 *
 * The lease is the only coordination primitive — no shared scheduler,
 * no leader election. Adding workers is a deployment-only change.
 */

import { Database } from "../core/db.js";
import { loadConfig } from "../config/loader.js";
import {
  laneLeaseName,
  laneSchema,
  type ValidatedLane,
  validateLanes,
} from "../core/lanes.js";
import { type Lease, LeaseStore } from "../core/lease.js";
import { Remote, resolveRemote } from "../core/remote.js";
import { loadAllPlugins } from "../plugin/loader.js";
import { registry } from "../plugin/registry.js";
import { Dripline } from "../sdk.js";
import type { SyncProgressEvent } from "../plugin/types.js";
import { error, info, jsonOutput, success, warn } from "../utils/output.js";

export interface RunOptions {
  /** Only run the named lane. Default: all lanes. */
  lane?: string;
  /** Output as JSON instead of human-readable. */
  json?: boolean;
  quiet?: boolean;
}

export interface LaneRunResult {
  lane: string;
  status: "ok" | "skipped" | "error";
  reason?: string;
  syncedTables: number;
  rowsInserted: number;
  publishedFiles: number;
  durationMs: number;
}

/**
 * Errors thrown by `run()` for misconfiguration. The CLI wrapper in
 * main.ts catches these and converts them to `process.exit(1)`; tests
 * catch them as ordinary exceptions.
 */
export class RunConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RunConfigError";
  }
}

export async function run(options: RunOptions = {}): Promise<LaneRunResult[]> {
  const config = loadConfig();

  if (!config.remote) {
    throw new RunConfigError(
      "no remote configured. Set `remote` in .dripline/config.json.",
    );
  }

  let lanes: ValidatedLane[] = validateLanes(config.lanes);
  if (lanes.length === 0) {
    throw new RunConfigError(
      "no lanes configured. Add a `lanes` block to .dripline/config.json.",
    );
  }
  if (options.lane) {
    lanes = lanes.filter((l) => l.name === options.lane);
    if (lanes.length === 0) {
      throw new RunConfigError(`lane "${options.lane}" not found`);
    }
  }

  await loadAllPlugins();

  const remote = new Remote(config.remote);
  const leaseStore = LeaseStore.fromRemote(resolveRemote(config.remote));

  const results: LaneRunResult[] = [];
  for (const lane of lanes) {
    results.push(
      await runLane(lane, {
        config,
        remote,
        leaseStore,
        json: options.json ?? false,
        quiet: options.quiet ?? false,
      }),
    );
  }

  if (options.json) {
    jsonOutput({ lanes: results });
  } else if (!options.quiet) {
    const ok = results.filter((r) => r.status === "ok").length;
    const skipped = results.filter((r) => r.status === "skipped").length;
    const failed = results.filter((r) => r.status === "error").length;
    console.log();
    info(
      `${ok} ran, ${skipped} skipped, ${failed} failed (${results.length} lanes total)`,
    );
  }

  return results;
}

interface LaneRunCtx {
  config: ReturnType<typeof loadConfig>;
  remote: Remote;
  leaseStore: LeaseStore;
  json: boolean;
  quiet: boolean;
}

async function runLane(
  lane: ValidatedLane,
  ctx: LaneRunCtx,
): Promise<LaneRunResult> {
  const start = Date.now();
  // Hoisted so the early-skip and error paths can build a complete
  // LaneRunResult before any work has been recorded.
  let syncedTables = 0;
  let rowsInserted = 0;
  let publishedFiles = 0;

  const log = (fn: (msg: string) => void, msg: string) => {
    if (!ctx.quiet && !ctx.json) fn(`[${lane.name}] ${msg}`);
  };
  const finish = (
    status: LaneRunResult["status"],
    reason?: string,
  ): LaneRunResult => ({
    lane: lane.name,
    status,
    reason,
    syncedTables,
    rowsInserted,
    publishedFiles,
    durationMs: Date.now() - start,
  });

  // Phase 1: try to acquire the work lease.
  let lease: Lease | null;
  try {
    lease = await ctx.leaseStore.acquire(
      laneLeaseName(lane.name),
      lane.maxRuntimeMs,
    );
  } catch (e) {
    log(error, `lease acquire failed: ${(e as Error).message}`);
    return finish("error", (e as Error).message);
  }

  if (lease == null) {
    log(warn, "skipped (held by another worker or in cooldown)");
    return finish("skipped", "lease held");
  }

  // Phase 2: run the lane.
  log(info, `acquired lease, running ${lane.tables.length} table sync(s)`);
  const db = await Database.create(":memory:");

  try {
    const dl = await Dripline.create({
      plugins: registry.listPlugins(),
      connections: ctx.config.connections,
      database: db,
      schema: laneSchema(lane.name),
    });

    try {
      await ctx.remote.hydrateCursors(db, lane.name);

      // Sync each (table, params) entry. Errors per table are captured
      // by engine.sync() and reported in result.errors — we surface
      // them as a lane-level "ok with errors" rather than failing the
      // whole lane, so one bad table doesn't block the others from
      // making progress.
      const tablesPublished = new Set<string>();
      for (const t of lane.tables) {
        const onProgress = ctx.quiet || ctx.json ? undefined : (ev: SyncProgressEvent) => {
          const rate = ev.elapsedMs > 0 ? (ev.rowsInserted / (ev.elapsedMs / 1000)).toFixed(1) : "?";
          const cursor = ev.cursor != null ? ` cursor=${ev.cursor}` : "";
          log(info, `${ev.table}: ${ev.rowsInserted.toLocaleString()} rows (${rate}/s${cursor})`);
        };
        const result = await dl.sync({ [t.name]: { ...t.params } }, onProgress);
        for (const r of result.tables) {
          syncedTables++;
          rowsInserted += r.rowsInserted;
          if (r.rowsInserted > 0) tablesPublished.add(r.table);
        }
        for (const e of result.errors) {
          log(warn, `${e.table}: ${e.error}`);
        }
      }

      const published = await ctx.remote.publishRun(db, lane.name, [
        ...tablesPublished,
      ]);
      publishedFiles = published.length;

      await ctx.remote.pushCursors(db, lane.name);
    } finally {
      await dl.close();
    }

    // Phase 3 (success): renew the lease to the lane's interval. This
    // serves as the cooldown — subsequent runs see the lease as held
    // and skip until it expires at the next scheduled tick.
    const cooldown = await ctx.leaseStore.renew(lease, lane.intervalMs);
    if (cooldown == null) {
      log(warn, "could not extend lease into cooldown (lost lease mid-run)");
    }

    log(
      success,
      `done in ${Date.now() - start}ms — ${publishedFiles} file(s) published, ${rowsInserted} row(s)`,
    );
    return finish("ok");
  } catch (e) {
    // Phase 3 (failure): release immediately so another worker can retry.
    try {
      await ctx.leaseStore.release(lease);
    } catch {
      /* best effort */
    }
    log(error, `failed: ${(e as Error).message}`);
    return finish("error", (e as Error).message);
  } finally {
    await db.close();
  }
}
