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
   *  tables write into a `_/` subdirectory so the glob matches both. */
  curatedRead(
    table: string,
    extra?: Record<string, string | boolean>,
  ): string {
    const url = this.s3(`curated/${table}/**/*.parquet`);
    const opts = { hive_partitioning: true, ...extra };
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
    if (rawCount === 0) {
      // Nothing new to compact — skip entirely. Curated data is
      // already deduped and partitioned from the last compact run.
      return { table, rows: 0, files: 0, rawCleaned: 0 };
    }
    const curatedCount = await this.countObjects(`curated/${table}/`);

    const raw = this.s3(`raw/${table}/**/*.parquet`);
    const pk = opts.primaryKey.map((c) => `"${c}"`).join(", ");
    const orderBy = opts.cursor ? `"${opts.cursor}" DESC NULLS LAST` : pk;
    const parts = (opts.partitionBy ?? []).map((c) => `"${c}"`);
    // Unpartitioned tables write a single file inside a `_/` subdirectory
    // so that `**/*.parquet` with `hive_partitioning => true` works
    // uniformly. DuckDB's COPY TO on S3 treats the path as a file key
    // (not a directory), so we must include the filename explicitly.
    const writeTarget = parts.length > 0
      ? `TO '${this.s3(`curated/${table}`)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000, PARTITION_BY (${parts.join(", ")}), OVERWRITE_OR_IGNORE)`
      : `TO '${this.s3(`curated/${table}/_/data.parquet`)}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)`;
    // rawRead is referenced twice: once to extract partition literals,
    // once inside the COPY. DuckDB downloads the raw files from S3
    // twice — acceptable since raw is small (one sync run's output).
    const rawRead = `read_parquet('${raw}', union_by_name => true, hive_partitioning => false)`;

    // Extract the distinct partition combos from raw as literals.
    // This is a lightweight S3 read (just the new data). Literal IN
    // values give DuckDB the best hive partition pruning — it can
    // skip curated files at the directory level without scanning.
    let curatedFilter = "";
    if (curatedCount > 0 && parts.length > 0) {
      const rows = await db.all(
        `SELECT DISTINCT ${parts.join(", ")} FROM ${rawRead}`,
      );
      if (rows.length > 0) {
        const literals = rows
          .map(
            (r) =>
              `(${parts
                .map((p) => {
                  const col = p.replace(/"/g, "");
                  const v = (r as Record<string, unknown>)[col];
                  // NULL partition values: WHERE (col) IN (NULL) never
                  // matches in SQL, so the curated filter will miss that
                  // partition. Acceptable — partition columns should be NOT NULL.
                  return v == null
                    ? "NULL"
                    : `'${String(v).replace(/'/g, "''")}'`;
                })
                .join(", ")})`,
          )
          .join(", ");
        curatedFilter = `WHERE (${parts.join(", ")}) IN (${literals})`;
      }
    }

    // Single COPY: raw + affected curated slice → deduped → partitioned parquet.
    const curatedUnion =
      curatedCount > 0
        ? `UNION ALL BY NAME
           SELECT * FROM ${this.curatedRead(table)}
           ${curatedFilter}`
        : "";

    await db.exec(`
      COPY (
        SELECT * EXCLUDE (_rn) FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY ${pk} ORDER BY ${orderBy}
          ) AS _rn
          FROM (SELECT * FROM ${rawRead} ${curatedUnion})
        ) WHERE _rn = 1
      ) ${writeTarget};
    `);

    const stats = await db.all(
      `SELECT COUNT(*) AS n FROM ${this.curatedRead(table)};`,
    );
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
