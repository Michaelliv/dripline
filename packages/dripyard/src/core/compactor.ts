/**
 * Per-table compaction scheduler.
 *
 * Mirrors `Scheduler` (which registers `sync.<lane>` Vex jobs) but for
 * the read side: one `compact.<table>` Vex job per compactable table.
 * Each tick acquires a per-table R2 lease, runs `Remote.compact()`,
 * releases the lease.
 *
 * Why per-table jobs instead of one global job:
 *   - Vex's job runner serializes a single job's handler — running 30
 *     compactions sequentially every 30m would block other jobs and
 *     produce one giant log line per cycle.
 *   - Lease contention already serializes a single table's work across
 *     multiple workers, so per-table jobs let dripyard parallelize
 *     across the workspace AND coordinate cleanly with sibling workers.
 *   - The UI gets per-table run rows it can show next to the existing
 *     warehouse view (this lands in a follow-up — for now we just log).
 *
 * A table is compactable iff its plugin declares a `primaryKey`. The
 * set is fixed at server boot — adding a plugin requires a restart, same
 * as lanes (workspace edits + restart is the documented flow).
 */

import {
  Database,
  type Lease,
  LeaseStore,
  Remote,
  type RemoteConfig,
  registry,
  resolveRemote,
  type TableDef,
} from "dripline";
import type { Vex } from "vex-core";

/** Default cadence for compaction. Mirrors the cron docs in dripline:
 *  "every 15-30m is plenty" — we pick the slower end so a workspace
 *  with 30+ tables doesn't spend its compute budget on dedupe. */
const DEFAULT_COMPACT_SCHEDULE = "every 30m";

/** Default cap on a single table's compaction wall-clock. */
const DEFAULT_MAX_RUNTIME_MS = 10 * 60 * 1000;

export interface CompactorOptions {
  /** Vex job schedule string (e.g. "every 30m"). Default: 30m. */
  schedule?: string;
  /** Per-table max wall-clock. Default: 10 minutes. */
  maxRuntimeMs?: number;
}

export class Compactor {
  private vex: Vex;
  private remote: Remote | null;
  private leaseStore: LeaseStore | null;
  private schedule: string;
  private maxRuntimeMs: number;
  private managedJobs: Set<string> = new Set();

  constructor(
    vex: Vex,
    remote: RemoteConfig | null,
    options: CompactorOptions = {},
  ) {
    this.vex = vex;
    this.remote = remote ? new Remote(remote) : null;
    this.leaseStore = remote
      ? LeaseStore.fromRemote(resolveRemote(remote))
      : null;
    this.schedule = options.schedule ?? DEFAULT_COMPACT_SCHEDULE;
    this.maxRuntimeMs = options.maxRuntimeMs ?? DEFAULT_MAX_RUNTIME_MS;
  }

  /**
   * Register a `compact.<table>` Vex job for every compactable table
   * in the registry. No-op when the workspace has no remote — there's
   * nothing to compact into.
   */
  async start(): Promise<void> {
    if (!this.remote || !this.leaseStore) return;

    for (const { table } of registry.getAllTables()) {
      if (!table.primaryKey || table.primaryKey.length === 0) continue;
      const jobName = `compact.${sanitize(table.name)}`;
      const remote = this.remote;
      const leaseStore = this.leaseStore;
      const maxRuntimeMs = this.maxRuntimeMs;

      await this.vex.addJob(jobName, {
        schedule: this.schedule,
        description: `Compact table: ${table.name}`,
        async handler() {
          await runCompactTable(table, remote, leaseStore, maxRuntimeMs);
        },
      });
      this.managedJobs.add(jobName);
    }
  }

  /** Tear down all managed jobs. Used by server `close()`. */
  async stop(): Promise<void> {
    for (const jobName of this.managedJobs) {
      await this.vex.removeJob(jobName);
    }
    this.managedJobs.clear();
  }

  /** Currently managed compact job names. Mirrors Scheduler.listJobs(). */
  listJobs(): string[] {
    return [...this.managedJobs];
  }
}

/**
 * Structured result of one table's compaction tick.
 *
 *   ok          compaction ran and rewrote curated + manifest
 *   skipped     lease held by another worker, or nothing to compact
 *   error       DuckDB or S3 failure — details in `error`
 */
export interface CompactTableResult {
  table: string;
  status: "ok" | "skipped" | "error";
  reason?: string;
  rows: number;
  files: number;
  rawCleaned: number;
  durationMs: number;
  error?: string;
}

/**
 * One table's compaction tick. Mirrors `compactTable` in dripline's
 * `commands/compact.ts` — same lease + DuckDB lifecycle, same
 * partitionBy fallback chain. Errors are caught and logged so a single
 * bad table can't kill the Vex job runner.
 *
 * Exported so an ad-hoc trigger (e.g. workspace.compactNow) can reuse
 * the exact same code path the scheduler runs. Returns a structured
 * result; the scheduled handler ignores it, the ad-hoc trigger reports
 * it to the caller.
 */
export async function runCompactTable(
  table: TableDef,
  remote: Remote,
  leaseStore: LeaseStore,
  maxRuntimeMs: number,
): Promise<CompactTableResult> {
  const start = Date.now();
  const base: CompactTableResult = {
    table: table.name,
    status: "skipped",
    rows: 0,
    files: 0,
    rawCleaned: 0,
    durationMs: 0,
  };
  const finish = (
    extra: Partial<CompactTableResult>,
  ): CompactTableResult => ({
    ...base,
    ...extra,
    durationMs: Date.now() - start,
  });

  const leaseKey = `compact-${table.name}`;
  let lease: Lease | null = null;
  try {
    lease = await leaseStore.acquire(leaseKey, maxRuntimeMs);
  } catch (err) {
    const msg = (err as Error).message;
    console.error(`[compact.${table.name}] lease acquire failed:`, msg);
    return finish({ status: "error", error: msg });
  }
  // Held by another worker, or recently compacted by a sibling. Quiet
  // skip — this is the expected path for most ticks under multi-worker.
  if (lease == null) return finish({ reason: "lease held" });

  const partitionBy =
    table.partitionBy ??
    (table.keyColumns && table.keyColumns.length > 0
      ? table.keyColumns.map((k) => k.name)
      : []);

  // createForContainer() applies a hard memory cap + a temp directory
  // so compaction spills hash aggregates / sorts / joins to disk
  // instead of OOMing on memory-constrained hosts (Render starter at
  // 512 MB, etc). Without this the compactor enters a permanent OOM
  // loop on any large table. Overrides via DRIPLINE_DUCKDB_* env vars.
  const db = await Database.createForContainer(":memory:");
  try {
    const result = await remote.compact(db, table.name, {
      primaryKey: table.primaryKey ?? [],
      cursor: table.cursor,
      partitionBy,
    });
    if (result.rows === 0 && result.files === 0) {
      return finish({ reason: "no raw files" });
    }
    console.log(
      `[compact.${table.name}] ${result.files} curated file(s), ${result.rows} row(s), ${result.rawCleaned} raw cleaned in ${Date.now() - start}ms`,
    );
    return finish({
      status: "ok",
      rows: result.rows,
      files: result.files,
      rawCleaned: result.rawCleaned,
    });
  } catch (err) {
    const msg = (err as Error).message;
    console.error(`[compact.${table.name}] failed:`, msg);
    return finish({ status: "error", error: msg });
  } finally {
    await db.close();
    // Compaction has no cooldown — release on every exit path so the
    // next tick can re-acquire on its own cadence.
    try {
      await leaseStore.release(lease);
    } catch {
      /* best effort */
    }
  }
}

function sanitize(name: string): string {
  return name.replace(/[^a-z0-9_]/gi, "_").toLowerCase();
}
