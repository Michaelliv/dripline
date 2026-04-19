/**
 * `dripline compact` — roll raw/ into curated/ and refresh manifests.
 *
 * The compactor is the single-writer per table that dripline's workers
 * rely on to turn append-only raw/ files into deduped, partitioned,
 * query-ready curated/ files.
 *
 * Design mirrors `dripline run`:
 *   - Stateless: each invocation enumerates compactable tables, tries
 *     to acquire a per-table lease, compacts, releases.
 *   - Safe under concurrency: two compactors running at once will pick
 *     disjoint tables via lease contention — no corruption possible.
 *   - No cooldown. Compaction is idempotent; the cron cadence controls
 *     frequency. Running it twice in a row just rewrites the same files.
 *
 * A table is "compactable" iff some registered plugin declares it with
 * a `primaryKey` — without a PK we can't dedupe, so the table is
 * silently skipped. Tables without raw files to process are also
 * skipped (a fast HEAD-like LIST avoids the expensive COPY).
 */

import { Database } from "../core/db.js";
import { loadConfig } from "../config/loader.js";
import { type Lease, LeaseStore } from "../core/lease.js";
import { Remote, resolveRemote } from "../core/remote.js";
import { loadAllPlugins } from "../plugin/loader.js";
import { registry } from "../plugin/registry.js";
import type { TableDef } from "../plugin/types.js";
import { error, info, jsonOutput, success, warn } from "../utils/output.js";

export interface CompactOptions {
  /** Only compact the named table(s). Default: all compactable tables. */
  tables?: string[];
  /** Output as JSON instead of human-readable. */
  json?: boolean;
  quiet?: boolean;
  /**
   * Max wall-clock for a single table's compaction. Default: 10 minutes.
   * On timeout the lease expires and another worker can retry.
   */
  maxRuntimeMs?: number;
}

export interface TableCompactResult {
  table: string;
  status: "ok" | "skipped" | "error";
  reason?: string;
  /** Row count in curated/ after compaction. */
  rows: number;
  /** Number of curated parquet files after compaction. */
  files: number;
  /** Number of raw files deleted during cleanup. */
  rawCleaned: number;
  durationMs: number;
}

/** Same pattern as RunConfigError — CLI wraps these into exit codes. */
export class CompactConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CompactConfigError";
  }
}

const DEFAULT_MAX_RUNTIME_MS = 10 * 60 * 1000;

/** Human-safe name for the per-table compaction lease. */
export function compactLeaseName(table: string): string {
  return `compact-${table}`;
}

export async function compact(
  options: CompactOptions = {},
): Promise<TableCompactResult[]> {
  const config = loadConfig();
  if (!config.remote) {
    throw new CompactConfigError(
      "no remote configured. Set `remote` in .dripline/config.json.",
    );
  }

  await loadAllPlugins();

  // Collect compactable tables from every registered plugin. A table is
  // compactable iff it declares a primaryKey. We keep the TableDef so
  // downstream code can read `cursor` (tiebreaker) without a second lookup.
  const compactable = new Map<string, TableDef>();
  for (const { table } of registry.getAllTables()) {
    if (table.primaryKey && table.primaryKey.length > 0) {
      compactable.set(table.name, table);
    }
  }

  if (options.tables && options.tables.length > 0) {
    // Respect the filter, but validate it — unknown names should error
    // loud, not silently produce empty output.
    for (const name of options.tables) {
      if (!compactable.has(name)) {
        throw new CompactConfigError(
          `table "${name}" is not compactable (not registered, or has no primaryKey)`,
        );
      }
    }
    for (const name of [...compactable.keys()]) {
      if (!options.tables.includes(name)) compactable.delete(name);
    }
  }

  if (compactable.size === 0) {
    throw new CompactConfigError(
      "no compactable tables. Declare `primaryKey` on at least one plugin table.",
    );
  }

  const remote = new Remote(config.remote);
  const leaseStore = LeaseStore.fromRemote(resolveRemote(config.remote));

  const maxRuntimeMs = options.maxRuntimeMs ?? DEFAULT_MAX_RUNTIME_MS;
  const json = options.json ?? false;
  const quiet = options.quiet ?? false;

  const results: TableCompactResult[] = [];
  for (const [name, table] of compactable) {
    results.push(
      await compactTable(name, table, {
        remote,
        leaseStore,
        maxRuntimeMs,
        json,
        quiet,
      }),
    );
  }

  if (json) {
    jsonOutput({ tables: results });
  } else if (!quiet) {
    const ok = results.filter((r) => r.status === "ok").length;
    const skipped = results.filter((r) => r.status === "skipped").length;
    const failed = results.filter((r) => r.status === "error").length;
    console.log();
    info(
      `${ok} compacted, ${skipped} skipped, ${failed} failed (${results.length} tables total)`,
    );
  }

  return results;
}

interface CompactTableCtx {
  remote: Remote;
  leaseStore: LeaseStore;
  maxRuntimeMs: number;
  json: boolean;
  quiet: boolean;
}

async function compactTable(
  name: string,
  table: TableDef,
  ctx: CompactTableCtx,
): Promise<TableCompactResult> {
  const start = Date.now();
  let rows = 0;
  let files = 0;
  let rawCleaned = 0;

  const log = (fn: (msg: string) => void, msg: string) => {
    if (!ctx.quiet && !ctx.json) fn(`[${name}] ${msg}`);
  };
  const finish = (
    status: TableCompactResult["status"],
    reason?: string,
  ): TableCompactResult => ({
    table: name,
    status,
    reason,
    rows,
    files,
    rawCleaned,
    durationMs: Date.now() - start,
  });

  // Phase 1: acquire the per-table lease.
  const leaseKey = compactLeaseName(name);
  let lease: Lease | null;
  try {
    lease = await ctx.leaseStore.acquire(leaseKey, ctx.maxRuntimeMs);
  } catch (e) {
    log(error, `lease acquire failed: ${(e as Error).message}`);
    return finish("error", (e as Error).message);
  }
  if (lease == null) {
    log(warn, "skipped (another compactor holds the lease)");
    return finish("skipped", "lease held");
  }

  // Phase 2: compact. Uses a fresh ephemeral DuckDB — we don't need
  // the dripline engine here because compact() only reads/writes
  // parquet via DuckDB's httpfs.
  //
  // createForContainer() applies a hard memory cap + a temp directory
  // so hash aggregates / sorts / joins spill to disk instead of OOMing
  // the worker. Without this, compacting a large partitioned table on
  // a 512 MB host (Render starter) enters a permanent OOM loop — the
  // first compact dies, the lease releases, the next cron tick
  // re-acquires, dies the same way, forever. Env overrides live in
  // db.ts: DRIPLINE_DUCKDB_MEMORY_LIMIT, _THREADS, _TEMP_DIR.
  const db = await Database.createForContainer(":memory:");
  try {
    // Partition curated/ by the table's declared partitionBy, falling
    // back to keyColumn names. Plugins can override to produce fewer,
    // larger files (e.g. partition by org_id only instead of org_id + date).
    const partitionBy =
      table.partitionBy ?? (
        table.keyColumns && table.keyColumns.length > 0
          ? table.keyColumns.map((k) => k.name)
          : []
      );

    const result = await ctx.remote.compact(db, name, {
      primaryKey: table.primaryKey ?? [],
      cursor: table.cursor,
      partitionBy,
    });
    rows = result.rows;
    files = result.files;
    rawCleaned = result.rawCleaned;

    // Remote.compact() returns (0, 0, 0) when neither raw/ nor curated/
    // has any objects yet — a fresh warehouse, nothing to do.
    if (rows === 0 && files === 0) {
      log(warn, "skipped (no raw files yet)");
      return finish("skipped", "no raw files");
    }

    log(
      success,
      `compacted in ${Date.now() - start}ms — ${files} curated file(s), ${rows} row(s), ${rawCleaned} raw file(s) cleaned`,
    );
    return finish("ok");
  } catch (e) {
    const msg = (e as Error).message;
    log(error, `failed: ${msg}`);
    return finish("error", msg);
  } finally {
    await db.close();
    // Compaction has no cooldown — release on every exit path (success,
    // skip-via-empty, error) so the next cron tick can re-acquire on
    // its own cadence. `release()` is idempotent and holder-checked.
    try {
      await ctx.leaseStore.release(lease);
    } catch {
      /* best effort */
    }
  }
}
