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
import type { Database } from "duckdb-async";
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

  /** S3 URL for a key inside the configured prefix. Used in DuckDB SQL. */
  private s3(key: string): string {
    const k = this.r.prefix ? `${this.r.prefix}/${key}` : key;
    return `s3://${this.r.bucket}/${k.replace(/^\//, "")}`;
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
   * raw/ or curated/ to compact. Caller should treat this as "skipped".
   *
   * The raw cleanup is safe under concurrent writers: we snapshot the
   * exact set of raw file keys at the START of compaction and only
   * delete those keys after success. Any raw file written during the
   * compaction window survives and will be picked up by the next run.
   *
   * Set `opts.keepRaw: true` to leave raw/ untouched (for debugging or
   * when you want a hard audit trail).
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
    const curatedCount = await this.countObjects(`curated/${table}/`);
    if (rawCount === 0 && curatedCount === 0) {
      return { table, rows: 0, files: 0, rawCleaned: 0 };
    }

    const rawGlob = this.s3(`raw/${table}/**/*.parquet`);
    const curatedGlob = this.s3(`curated/${table}/**/*.parquet`);
    const curatedDir = this.s3(`curated/${table}`);

    const pkList = opts.primaryKey.map((c) => `"${c}"`).join(", ");
    const orderBy = opts.cursor ? `"${opts.cursor}" DESC NULLS LAST` : pkList;

    // Raw and curated have DIFFERENT hive layouts:
    //   raw:     lane=<lane>/ — lane is metadata only, not a data column
    //   curated: <partitionBy>/ — these ARE data columns stripped by
    //            DuckDB's PARTITION_BY on write, so they're only
    //            recoverable via hive_partitioning=true.
    // We can't read both under one hive setting. Instead we read them
    // separately with the right setting each and UNION ALL BY NAME,
    // which aligns columns by name across the two schemas.
    const rawSelect =
      rawCount > 0
        ? `SELECT * FROM read_parquet('${rawGlob}',
             union_by_name => true,
             hive_partitioning => false)`
        : null;
    const curatedSelect =
      curatedCount > 0
        ? `SELECT * FROM read_parquet('${curatedGlob}',
             union_by_name => true,
             hive_partitioning => true)`
        : null;
    const selects = [rawSelect, curatedSelect].filter(
      (s): s is string => s !== null,
    );
    const src =
      selects.length === 1
        ? selects[0]
        : selects.join("\n      UNION ALL BY NAME\n      ");
    const deduped = `
      SELECT * EXCLUDE (_rn) FROM (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY ${pkList} ORDER BY ${orderBy}
        ) AS _rn FROM (${src})
      ) WHERE _rn = 1
    `;
    const partitionClause =
      opts.partitionBy && opts.partitionBy.length > 0
        ? `, PARTITION_BY (${opts.partitionBy.map((c) => `"${c}"`).join(", ")}), OVERWRITE_OR_IGNORE`
        : ", OVERWRITE_OR_IGNORE";

    await db.exec(`
      COPY (${deduped}) TO '${curatedDir}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000${partitionClause});
    `);

    const stats = await db.all(`
      SELECT COUNT(*) AS n FROM read_parquet('${curatedGlob}',
        hive_partitioning => true);
    `);
    const rows = Number((stats[0] as { n: bigint | number })?.n ?? 0);
    const files = await this.refreshManifest(db, table, opts.partitionBy ?? []);

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
    const curatedGlob = this.s3(`curated/${table}/**/*.parquet`);
    const partCols = partitionBy.map((c) => `"${c}"`).join(", ");

    const filesRows = (await db.all(`
      SELECT
        filename,
        COUNT(*) AS row_count
        ${partitionBy
          .map((c) => `, MIN("${c}") AS "min_${c}", MAX("${c}") AS "max_${c}"`)
          .join("")}
      FROM read_parquet('${curatedGlob}',
        hive_partitioning => true,
        filename => true)
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
          f[`min_${c}`] = r[`min_${c}`];
          f[`max_${c}`] = r[`max_${c}`];
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
   */
  async attachTable(
    db: Database,
    table: string,
    schema = "main",
  ): Promise<void> {
    await this.attach(db);
    const qn = `"${schema}"."${table}"`;

    // Prefer manifest file list over glob — avoids S3 LIST + footer
    // scan that can take 20-50s per table on large warehouses.
    const manifest = await this.readManifest(table);
    if (manifest && manifest.files.length > 0) {
      const paths = manifest.files.map((f) => f.path as string);
      const listLiteral = paths.map((p) => `'${p}'`).join(", ");
      await db.exec(`
        CREATE OR REPLACE VIEW ${qn} AS
        SELECT * FROM read_parquet([${listLiteral}],
          hive_partitioning => true, union_by_name => true);
      `);
      return;
    }

    // Fallback: no manifest — use glob (slower but works before first compact)
    const curatedGlob = this.s3(`curated/${table}/**/*.parquet`);
    await db.exec(`
      CREATE OR REPLACE VIEW ${qn} AS
      SELECT * FROM read_parquet('${curatedGlob}',
        hive_partitioning => true, union_by_name => true);
    `);
  }
}

/** Single-quote escape for embedding strings in DuckDB DDL. */
function esc(s: string): string {
  return s.replace(/'/g, "''");
}

/** Filesystem-safe ISO-8601 timestamp suitable for object keys. */
function isoRunId(): string {
  return new Date().toISOString().replace(/[:.]/g, "-");
}
