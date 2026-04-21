/**
 * Remote: S3-compatible warehouse layer for dripline.
 *
 * Three primitives — hydrate, publish, compact — that turn any dripline
 * engine into a worker writing into a shared remote bucket. Works
 * against Cloudflare R2, MinIO, AWS S3, or any S3-compatible store.
 *
 * Design constraints:
 *   1. Workers are stateless. They hydrate ONLY cursor metadata, never
 *      table data. Memory + runtime are O(1) regardless of warehouse size.
 *   2. Compactor writes a manifest per table so query-side cold starts
 *      don't pay LIST costs. Manifests carry partition min/max so DuckDB
 *      can prune without fetching footers.
 *   3. Each run writes ONE parquet file per (lane, table) into raw/,
 *      append-only. Cuts small-file overhead.
 *
 * Layout in the bucket:
 *   <prefix>/_state/<lane>/_dripline_sync.parquet     cursor metadata per lane
 *   <prefix>/raw/<table>/lane=<lane>/run=<id>.parquet append-only landing
 *   <prefix>/curated/<table>/<hive>/part-0.parquet    compacted, deduped
 *   <prefix>/_manifests/<table>.json                  file index + stats
 */

import { AwsClient } from "aws4fetch";
import type { Database } from "./db.js";
import type { RemoteConfig } from "../config/types.js";
import { laneSchema, laneStatePath } from "./lanes.js";
import { RemoteFS } from "./remote-fs.js";

export interface ResolvedRemote {
  endpoint: string;
  bucket: string;
  prefix: string;
  region: string;
  secretType: "R2" | "S3";
  accessKeyId: string;
  secretAccessKey: string;
}

/**
 * Resolve a RemoteConfig (with optional env-var indirection) into a
 * concrete credential set. Throws if anything's missing.
 */
export function resolveRemote(cfg: RemoteConfig): ResolvedRemote {
  const accessKeyId =
    cfg.accessKeyId ??
    (cfg.accessKeyEnv ? process.env[cfg.accessKeyEnv] : undefined);
  const secretAccessKey =
    cfg.secretAccessKey ??
    (cfg.secretKeyEnv ? process.env[cfg.secretKeyEnv] : undefined);
  if (!accessKeyId || !secretAccessKey) {
    throw new Error(
      "remote: missing credentials. Set accessKeyId/secretAccessKey or accessKeyEnv/secretKeyEnv.",
    );
  }
  if (!cfg.endpoint) throw new Error("remote: endpoint is required");
  if (!cfg.bucket) throw new Error("remote: bucket is required");
  return {
    endpoint: cfg.endpoint.replace(/\/$/, ""),
    bucket: cfg.bucket,
    prefix: (cfg.prefix ?? "").replace(/^\/|\/$/g, ""),
    region: cfg.region ?? "auto",
    secretType: cfg.secretType ?? "S3",
    accessKeyId,
    secretAccessKey,
  };
}

export class Remote {
  private readonly aws: AwsClient;
  private readonly r: ResolvedRemote;
  private readonly fs: RemoteFS;
  private attachedDbs = new WeakSet<Database>();

  constructor(cfg: RemoteConfig) {
    this.r = resolveRemote(cfg);
    this.aws = new AwsClient({
      accessKeyId: this.r.accessKeyId,
      secretAccessKey: this.r.secretAccessKey,
      service: "s3",
      region: this.r.region,
    });
    this.fs = new RemoteFS({
      aws: this.aws,
      endpoint: this.r.endpoint,
      bucket: this.r.bucket,
      prefix: this.r.prefix,
    });
  }

  // ── Path helpers ───────────────────────────────────────────────────

  /** S3 URL for a key inside the configured prefix. Used in DuckDB SQL.
   *  Public because tests need it to write raw parquet directly. */
  s3(key: string): string {
    const k = this.r.prefix ? `${this.r.prefix}/${key}` : key;
    return `s3://${this.r.bucket}/${k.replace(/^\//, "")}`;
  }

  /** DuckDB read_parquet() expression for curated data. Always uses
   *  `**\/*.parquet` with `hive_partitioning => true` — unpartitioned
   *  tables write into a `_/` subdirectory so the glob matches both.
   *
   *  `union_by_name => true` is essential for schema evolution: when a
   *  plugin adds a new column, curated parquet files written before
   *  the change lack it. Without union_by_name DuckDB unions by
   *  position and either errors (when column counts differ) or
   *  corrupts types (when they happen to match positionally). With
   *  it, missing columns become NULL and the new schema wins. */
  curatedRead(
    table: string,
    extra?: Record<string, string | boolean>,
  ): string {
    const url = this.s3(`curated/${table}/**/*.parquet`);
    const opts = { hive_partitioning: true, union_by_name: true, ...extra };
    const params = Object.entries(opts)
      .map(([k, v]) => `${k} => ${v}`)
      .join(", ");
    return `read_parquet('${url}', ${params})`;
  }

  // ── DuckDB attachment ──────────────────────────────────────────────

  /**
   * Install httpfs and create the remote secret on the given DuckDB.
   * Idempotent — safe to call repeatedly. Builds an R2 or S3 secret
   * based on `secretType` so the same code works against both.
   */
  async attach(db: Database): Promise<void> {
    if (this.attachedDbs.has(db)) return;
    await db.exec(`INSTALL httpfs; LOAD httpfs;`);
    await db.exec(`DROP SECRET IF EXISTS dripline_remote;`);

    if (this.r.secretType === "R2") {
      // R2 secret type lets DuckDB derive the endpoint from the account.
      // For R2 we expect endpoint to be "https://<account>.r2.cloudflarestorage.com",
      // from which we extract the account id.
      const accountMatch =
        /^https?:\/\/([^.]+)\.r2\.cloudflarestorage\.com/.exec(this.r.endpoint);
      if (!accountMatch) {
        throw new Error(
          `remote: secretType=R2 requires endpoint of the form https://<account>.r2.cloudflarestorage.com, got ${this.r.endpoint}`,
        );
      }
      await db.exec(`
        CREATE SECRET dripline_remote (
          TYPE R2,
          KEY_ID '${esc(this.r.accessKeyId)}',
          SECRET '${esc(this.r.secretAccessKey)}',
          ACCOUNT_ID '${esc(accountMatch[1])}'
        );
      `);
    } else {
      // Generic S3 — works against MinIO, AWS, and any other S3-compatible store.
      const useSsl = this.r.endpoint.startsWith("https://");
      const endpointHost = this.r.endpoint.replace(/^https?:\/\//, "");
      await db.exec(`
        CREATE SECRET dripline_remote (
          TYPE S3,
          KEY_ID '${esc(this.r.accessKeyId)}',
          SECRET '${esc(this.r.secretAccessKey)}',
          ENDPOINT '${esc(endpointHost)}',
          URL_STYLE 'path',
          USE_SSL ${useSsl},
          REGION '${esc(this.r.region)}'
        );
      `);
    }

    this.attachedDbs.add(db);
  }

  // ── Cursor state (per lane) ────────────────────────────────────────

  /**
   * Hydrate cursor metadata for one lane from the bucket into the local
   * `_dripline_sync` table. Worker runtime + memory are O(1) in
   * warehouse size — we never touch the actual data files.
   */
  async hydrateCursors(db: Database, lane: string): Promise<void> {
    await this.attach(db);
    const schema = laneSchema(lane);
    const qn = `"${schema}"."_dripline_sync"`;

    await db.exec(`CREATE SCHEMA IF NOT EXISTS "${schema}";`);
    await db.exec(`
      CREATE TABLE IF NOT EXISTS ${qn} (
        table_name VARCHAR, params_key VARCHAR, plugin VARCHAR,
        last_cursor VARCHAR, last_sync_at BIGINT, rows_synced BIGINT,
        status VARCHAR, error VARCHAR, duration_ms BIGINT,
        PRIMARY KEY (table_name, params_key)
      );
    `);

    const stateUrl = this.s3(laneStatePath(lane));
    try {
      await db.exec(`
        INSERT OR REPLACE INTO ${qn}
        SELECT * FROM read_parquet('${stateUrl}');
      `);
    } catch {
      // First run — no state file yet. Fine.
    }
  }

  /** Push the local `_dripline_sync` table back to the bucket. */
  async pushCursors(db: Database, lane: string): Promise<void> {
    await this.attach(db);
    const schema = laneSchema(lane);
    const qn = `"${schema}"."_dripline_sync"`;
    const stateUrl = this.s3(laneStatePath(lane));
    await db.exec(`COPY ${qn} TO '${stateUrl}' (FORMAT PARQUET);`);
  }

  // ── Publish (raw/, append-only) ────────────────────────────────────

  /**
   * Publish the current contents of each given table as ONE parquet file
   * into `raw/<table>/lane=<lane>/run=<runId>.parquet`. Append-only —
   * never rewrites. Skips empty tables.
   */
  async publishRun(
    db: Database,
    lane: string,
    tables: string[],
    runId: string = isoRunId(),
  ): Promise<{ table: string; rows: number; url: string }[]> {
    await this.attach(db);
    const schema = laneSchema(lane);
    const out: { table: string; rows: number; url: string }[] = [];

    for (const table of tables) {
      const qn = `"${schema}"."${table}"`;
      const cnt = await db.all(`SELECT COUNT(*) AS n FROM ${qn};`);
      const rows = Number((cnt[0] as { n: bigint | number })?.n ?? 0);
      if (rows === 0) continue;

      const key = `raw/${table}/lane=${lane}/run=${runId}.parquet`;
      const fileUrl = this.s3(key);
      await db.exec(`
        COPY (SELECT * FROM ${qn}) TO '${fileUrl}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000);
      `);
      out.push({ table, rows, url: fileUrl });
    }
    return out;
  }

  // ── Compaction (raw + curated → curated, deduped) ──────────────────

  countObjects(prefix: string): Promise<number> {
    return this.fs.countObjects(prefix);
  }

  listObjects(prefix: string): Promise<string[]> {
    return this.fs.listObjects(prefix);
  }

  deleteObjects(keys: string[]): Promise<void> {
    return this.fs.deleteObjects(keys);
  }

  /**
   * Compact one table: merge raw/ + curated/, dedupe by primary key
   * keeping the latest row per cursor, rewrite curated/ partitioned,
   * refresh the manifest, and delete the raw files we consumed.
   *
   * Returns `rows: 0, files: 0, rawCleaned: 0` if there's nothing in
   * raw/ to compact. Caller should treat this as "skipped".
   *
   * The raw cleanup is safe under concurrent writers: we snapshot the
   * exact set of raw file keys at the START of compaction and only
   * delete those keys after success. Any raw file written during the
   * compaction window survives and will be picked up by the next run.
   *
   * Set `opts.keepRaw: true` to leave raw/ untouched (for debugging or
   * when you want a hard audit trail).
   *
   * ## Memory story
   *
   * The classic way to express "dedup raw + curated, keep latest per pk"
   * is a single `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY cursor
   * DESC)` over `UNION ALL`, projecting `SELECT *`. It's correct, it's
   * short, and it OOMs on any meaningfully wide table under 300 MB of
   * DuckDB budget. The window operator pins the full payload (50 cols,
   * JSON blobs, etc) through the sort phase; the partitioned writer
   * pins one row-group buffer per open partition. Spill doesn't help
   * — those buffers are "pinned" in DuckDB's terms, meaning the engine
   * can't evict them to disk mid-operator.
   *
   * We avoid both costs by:
   *
   *   1. **Narrow decision**: the dedup window only reads (pk, cursor).
   *      Payload columns are projected in at the final COPY via a
   *      SEMI JOIN back against the winners set. The window never
   *      sees wide rows.
   *   2. **Per-partition writes**: one COPY per partition combo
   *      (observed in raw) writing a single file directly to its
   *      hive path. DuckDB's `PARTITION_BY` writer is bypassed, so
   *      there's no per-partition pinned buffer floor.
   *   3. **Anti-join curated slice**: after winners are picked from
   *      raw, curated rows survive only if no raw winner dominates
   *      them by cursor. Much cheaper than unioning the full curated
   *      partition just to drop most of it.
   *
   * The bench in `bench/compact-matrix-bench.ts` measured a 2-5x
   * reduction in peak RSS across 9 scenarios (single/multi PK,
   * cursor/no-cursor, wide/narrow, empty/existing curated, all-new/
   * all-update raw) — with bit-identical output to the baseline.
   */
  async compact(
    db: Database,
    table: string,
    opts: {
      primaryKey: string[];
      cursor?: string;
      partitionBy?: string[];
      keepRaw?: boolean;
    },
  ): Promise<{
    table: string;
    rows: number;
    files: number;
    rawCleaned: number;
  }> {
    await this.attach(db);

    // Snapshot the raw file list upfront. Anything written after this
    // point will survive compaction — no race condition with workers
    // writing concurrently. We only delete keys that actually exist
    // in this snapshot, and only after compact() has fully succeeded.
    const rawKeys = (await this.listObjects(`raw/${table}/`)).filter((k) =>
      k.endsWith(".parquet"),
    );
    const rawCount = rawKeys.length;
    if (rawCount === 0) {
      // Nothing new to compact — skip entirely. Curated data is
      // already deduped and partitioned from the last compact run.
      return { table, rows: 0, files: 0, rawCleaned: 0 };
    }
    const curatedCount = await this.countObjects(`curated/${table}/`);
    const hasCurated = curatedCount > 0;

    const raw = this.s3(`raw/${table}/**/*.parquet`);
    const pk = opts.primaryKey.map((c) => `"${c}"`).join(", ");
    const orderBy = opts.cursor
      ? `"${opts.cursor}" DESC NULLS LAST`
      : pk + " DESC";
    const partitionBy = opts.partitionBy ?? [];
    const parts = partitionBy.map((c) => `"${c}"`);
    // Build narrow cols. When the cursor is also one of the PK
    // columns — legitimate whenever the natural key includes the
    // progress field (e.g. daily aggregates keyed on (entity, date)
    // with cursor=date) — don't list it twice. UNION BY NAME rejects
    // duplicate column names in the SELECT list.
    const cursorInPk =
      opts.cursor != null && opts.primaryKey.includes(opts.cursor);
    const narrowCols =
      opts.cursor && !cursorInPk ? `${pk}, "${opts.cursor}"` : pk;

    const rawRead = `read_parquet('${raw}', union_by_name => true, hive_partitioning => false)`;
    const curatedReadBase = this.curatedRead(table);

    // Schema evolution: if the cursor column was added after curated
    // was first written, the existing parquet files don't have it
    // and union_by_name can't fabricate it (it only unifies columns
    // that exist in at least one file in the set). We probe curated's
    // schema once and synthesize `NULL AS <cursor>` for narrow reads
    // when it's missing. Wide reads are untouched — they keep
    // `SELECT *` and rely on union_by_name at the final SEMI JOIN.
    let curatedHasCursor = true;
    if (opts.cursor && !cursorInPk) {
      try {
        const schema = (await db.all(
          `DESCRIBE SELECT * FROM ${curatedReadBase} LIMIT 0`,
        )) as Array<{ column_name: string }>;
        curatedHasCursor = schema.some(
          (r) => r.column_name === opts.cursor,
        );
      } catch {
        // No curated yet, or transient error — assume present; the
        // SEMI JOIN will error loudly if it's really missing.
        curatedHasCursor = true;
      }
    }
    const curatedNarrowCols =
      opts.cursor && !cursorInPk && !curatedHasCursor
        ? `${pk}, NULL AS "${opts.cursor}"`
        : narrowCols;
    const curatedRead = curatedReadBase;

    // Discover distinct partition combos in raw — these are the only
    // curated partitions we need to read (and rewrite). Empty when
    // the table has no partitionBy; in that case we do a single pass
    // with no partition filter.
    type PartCombo = Record<string, unknown>;
    let partitionsInRaw: PartCombo[] = [{} as PartCombo];
    if (parts.length > 0) {
      const rows = (await db.all(
        `SELECT DISTINCT ${parts.join(", ")} FROM ${rawRead}`,
      )) as PartCombo[];
      partitionsInRaw = rows.length > 0 ? rows : [{} as PartCombo];
    }

    // The SEMI JOIN needs a tiebreaker for the case where raw and
    // curated contain rows with identical (pk, cursor). Without one,
    // both src rows match the single winner and the output doubles.
    // Add a synthetic `_src_order` column: 0 for raw, 1 for curated.
    // The window ordering prefers raw on ties (ASC on _src_order),
    // matching baseline's behaviour where raw's PK DESC would outrank
    // curated's. The EXCLUDE drops the helper before writing.
    const narrowColsWithOrder = `${narrowCols}, _src_order`;
    const semiJoinCols =
      opts.cursor && !cursorInPk
        ? `${pk}, "${opts.cursor}", _src_order`
        : `${pk}, _src_order`;
    const innerOrder = opts.cursor ? `${orderBy}, _src_order ASC` : orderBy;

    // Write one COPY per partition combo. This replaces DuckDB's
    // PARTITION_BY writer, whose per-partition row-group buffer was
    // the dominant pinned-memory cost on wide schemas.
    //
    // Shape (both with and without cursor):
    //   WITH src_tagged AS (raw tagged _src_order=0 UNION ALL
    //                       curated tagged _src_order=1 for this partition)
    //        winners    AS (narrow dedup over src_tagged by pk)
    //   SELECT src.* EXCLUDE (_src_order) FROM src_tagged
    //   SEMI JOIN winners USING (pk, cursor?, _src_order)
    //
    // Narrow projection flows through the window; wide payload only
    // materialises at the SEMI JOIN, right before COPY.
    for (const part of partitionsInRaw) {
      const partFilter = buildPartitionFilter(partitionBy, part);
      const targetPath = buildCuratedPath(
        this.s3(`curated/${table}`),
        partitionBy,
        part,
      );

      const srcTaggedNarrow = `
        SELECT ${narrowCols}, 0::INTEGER AS _src_order
        FROM ${rawRead}${partFilter}
        ${
          hasCurated
            ? `UNION ALL BY NAME
               SELECT ${curatedNarrowCols}, 1::INTEGER AS _src_order
               FROM ${curatedRead}${partFilter}`
            : ""
        }
      `;
      const srcTaggedWide = `
        SELECT *, 0::INTEGER AS _src_order FROM ${rawRead}${partFilter}
        ${
          hasCurated
            ? `UNION ALL BY NAME
               SELECT *, 1::INTEGER AS _src_order FROM ${curatedRead}${partFilter}`
            : ""
        }
      `;
      const body = `
        WITH winners AS (
          SELECT ${narrowColsWithOrder} FROM (
            SELECT ${narrowColsWithOrder},
                   ROW_NUMBER() OVER (
                     PARTITION BY ${pk} ORDER BY ${innerOrder}
                   ) AS _rn
            FROM (${srcTaggedNarrow})
          ) WHERE _rn = 1
        )
        SELECT src.* EXCLUDE (_src_order)
        FROM (${srcTaggedWide}) src
        SEMI JOIN winners USING (${semiJoinCols})
      `;

      // The path is a SQL string literal — escape single quotes so an
      // apostrophe-bearing partition value (e.g. "o'reilly") doesn't
      // break the COPY statement. Our buildCuratedPath keeps the raw
      // character (rather than URL-encoding) so curated paths remain
      // human-readable and consistent with dripline's existing layout.
      await db.exec(`
        COPY (${body})
        TO '${esc(targetPath)}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
      `);
    }

    // A partition present in curated but absent from raw has no new
    // data — we don't rewrite it, so it stays on disk as-is. Nothing
    // to do here beyond the above per-partition writes.

    const stats = await db.all(
      `SELECT COUNT(*) AS n FROM ${curatedRead};`,
    );
    const rows = Number((stats[0] as { n: bigint | number })?.n ?? 0);
    const files = await this.refreshManifest(db, table, partitionBy);

    // Finally — delete the raw files we consumed. This happens AFTER
    // the curated rewrite and manifest are durable, so a crash between
    // compact-rewrite and raw-cleanup simply leaves the system in a
    // "extra raw files" state on the next run; nothing is lost.
    let rawCleaned = 0;
    if (!opts.keepRaw && rawKeys.length > 0) {
      await this.deleteObjects(rawKeys);
      rawCleaned = rawKeys.length;
    }

    return { table, rows, files, rawCleaned };
  }

  // ── Manifest (per table, written via aws4fetch) ────────────────────

  /**
   * Walk `curated/<table>/` and write `_manifests/<table>.json` with
   * the file list, per-file row counts, and per-partition-column
   * min/max. Query side reads this single JSON instead of LISTing.
   */
  async refreshManifest(
    db: Database,
    table: string,
    partitionBy: string[],
  ): Promise<number> {
    const partCols = partitionBy.map((c) => `"${c}"`).join(", ");

    const filesRows = (await db.all(`
      SELECT
        filename,
        COUNT(*) AS row_count
        ${partitionBy
          .map((c) => `, MIN("${c}") AS "min_${c}", MAX("${c}") AS "max_${c}"`)
          .join("")}
      FROM ${this.curatedRead(table, { filename: true })}
      GROUP BY filename ${partCols.length > 0 ? `, ${partCols}` : ""}
      ORDER BY filename;
    `)) as Array<Record<string, unknown>>;

    const manifest = {
      table,
      version: 1,
      generated_at: new Date().toISOString(),
      partition_by: partitionBy,
      files: filesRows.map((r) => {
        const f: Record<string, unknown> = {
          path: r.filename,
          row_count: Number(r.row_count),
        };
        for (const c of partitionBy) {
          // DuckDB returns BIGINT columns as native bigint, which
          // JSON.stringify refuses to serialize. Coerce min/max via
          // jsonSafe so integer-typed partition keys (section_id,
          // org_id, etc.) round-trip cleanly.
          f[`min_${c}`] = jsonSafe(r[`min_${c}`]);
          f[`max_${c}`] = jsonSafe(r[`max_${c}`]);
        }
        return f;
      }),
    };

    // Write via aws4fetch — DuckDB's JSON writer is row-oriented and
    // doesn't cleanly produce a single-document file.
    const url = this.fs.http(`_manifests/${table}.json`);
    const res = await this.aws.fetch(url, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(manifest, null, 2),
    });
    if (res.status !== 200 && res.status !== 201) {
      throw new Error(`manifest PUT failed: ${res.status} ${await res.text()}`);
    }
    return filesRows.length;
  }

  /**
   * Read the manifest for a table. Returns null if no manifest exists.
   * Used by reader-side tooling to enumerate curated files without
   * paying LIST costs against the bucket.
   */
  async readManifest(table: string): Promise<{
    table: string;
    version: number;
    generated_at: string;
    partition_by: string[];
    files: Array<Record<string, unknown>>;
  } | null> {
    const url = this.fs.http(`_manifests/${table}.json`);
    const res = await this.aws.fetch(url, { method: "GET" });
    if (res.status === 404) return null;
    if (res.status !== 200) {
      throw new Error(`manifest GET failed: ${res.status} ${await res.text()}`);
    }
    return (await res.json()) as Awaited<ReturnType<Remote["readManifest"]>>;
  }

  // ── Reader-side helper ─────────────────────────────────────────────

  /**
   * Create or replace a view over `curated/<table>/` so query-mode SQL
   * can reference the table by name. DuckDB does its own predicate /
   * partition pruning via parquet stats.
   *
   * Assumes all curated files share an identical schema (guaranteed when
   * files are produced by `compact()`). No `union_by_name` — DuckDB
   * infers the schema from the first file.
   */
  async attachTable(
    db: Database,
    table: string,
    schema = "main",
  ): Promise<void> {
    await this.attach(db);
    const qn = `"${schema}"."${table}"`;

    // Use manifest file list when available — avoids the S3 LIST + per-file
    // schema scan that globs trigger. DuckDB gets an explicit array of URLs
    // and only reads one footer for the schema. Falls back to glob when
    // no manifest exists (before first compact).
    const manifest = await this.readManifest(table);
    if (manifest && manifest.files.length > 0) {
      const files = manifest.files.map((f) => f.path as string);
      const partitioned = (manifest.partition_by?.length ?? 0) > 0;
      const fileList = files.map((f) => `'${f}'`).join(", ");
      await db.exec(`
        CREATE OR REPLACE VIEW ${qn} AS
        SELECT * FROM read_parquet([${fileList}],
          hive_partitioning => ${partitioned});
      `);
    } else {
      // No manifest — fall back to glob discovery.
      await db.exec(`
        CREATE OR REPLACE VIEW ${qn} AS
        SELECT * FROM ${this.curatedRead(table)};
      `);
    }
  }
}

/** Single-quote escape for embedding strings in DuckDB DDL. */
function esc(s: string): string {
  return s.replace(/'/g, "''");
}

/**
 * Coerce a DuckDB-returned value into something `JSON.stringify` can
 * write. The only currently-troublesome type is `bigint`: BIGINT
 * columns surface as native bigint via `getRowObjectsJS`, and bigint
 * is a hard error for JSON.stringify. Numbers up to 2^53 round-trip
 * losslessly via Number(); larger ones we keep as strings so manifest
 * readers don't silently truncate. Everything else passes through.
 */
function jsonSafe(v: unknown): unknown {
  if (typeof v !== "bigint") return v;
  return v <= BigInt(Number.MAX_SAFE_INTEGER) &&
    v >= BigInt(Number.MIN_SAFE_INTEGER)
    ? Number(v)
    : v.toString();
}

/** Filesystem-safe ISO-8601 timestamp suitable for object keys. */
function isoRunId(): string {
  return new Date().toISOString().replace(/[:.]/g, "-");
}

/**
 * Build a `WHERE "part1" = 'v1' AND "part2" = 'v2'` filter clause for
 * one partition combo. Returns the leading space so callers can
 * concatenate directly after a `read_parquet(...)` call. Empty string
 * when the table has no partitioning or the combo is empty.
 *
 * NULL partition values get `IS NULL` rather than `= NULL`. Dripline
 * treats NULL partitions as second-class (hive paths can't represent
 * them cleanly) but we handle them correctly here for robustness.
 */
function buildPartitionFilter(
  partitionBy: string[],
  combo: Record<string, unknown>,
): string {
  if (partitionBy.length === 0) return "";
  const clauses = partitionBy.map((c) => {
    const v = combo[c];
    if (v == null) return `"${c}" IS NULL`;
    return `"${c}" = '${String(v).replace(/'/g, "''")}'`;
  });
  return ` WHERE ${clauses.join(" AND ")}`;
}

/**
 * Build the hive-partitioned target path for a COPY TO. For partitioned
 * tables we write to `<base>/col1=val1/col2=val2/data_0.parquet`; for
 * unpartitioned, `<base>/_/data_0.parquet` (the `_` keeps the glob
 * shape consistent with hive layouts so readers never special-case).
 *
 * Partition values are embedded as-is; the caller is responsible for
 * escaping the final path as a SQL string literal (compact() wraps
 * the path through `esc()` before the COPY statement). Values with
 * path separators would break the hive layout, but dripline's plugin
 * contract treats partition columns as low-cardinality identifiers,
 * so this isn't a realistic case.
 */
function buildCuratedPath(
  baseS3Url: string,
  partitionBy: string[],
  combo: Record<string, unknown>,
): string {
  if (partitionBy.length === 0) {
    return `${baseS3Url}/_/data_0.parquet`;
  }
  const segs = partitionBy.map((c) => {
    const v = combo[c];
    return `${c}=${v == null ? "__NULL__" : String(v)}`;
  });
  return `${baseS3Url}/${segs.join("/")}/data_0.parquet`;
}
