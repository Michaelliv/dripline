#!/usr/bin/env bun
/**
 * Compaction memory benchmark — empirical, not speculative.
 *
 * The problem this harness exists to study: the current `Remote.compact`
 * OOMs on production workloads inside a 512 MB container. The query
 * does a `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY cursor DESC)`
 * over a `UNION ALL` of raw + curated, which forces DuckDB to buffer
 * the entire window's sort state in memory. Wide schemas × many rows
 * × many window groups = the 128 MB allocation we saw fail.
 *
 * What this file does:
 *   1. Seeds local MinIO with a synthetic warehouse that mirrors the
 *      shape of the real failure — a "wide" schema (50 cols, a few
 *      JSON blobs), partitioned by org_id, with an existing curated
 *      baseline plus an incremental raw drop.
 *   2. Runs each candidate rewrite against a DuckDB with a 300 MB
 *      hard cap (matches Render starter's budget exactly).
 *   3. Measures: wall time, peak RSS, and output-row correctness
 *      (every strategy must produce the same deduped row set).
 *
 * Strategies measured:
 *   baseline   — current production query (ROW_NUMBER + filter)
 *   qualify    — QUALIFY ROW_NUMBER() = 1 (lexically smaller, same plan)
 *   argmax     — GROUP BY pk + JOIN on max(cursor) (streaming hash agg)
 *   per-part   — loop over affected partitions, one COPY each (tiny windows)
 *
 * Run:
 *   bun run bench/compact-bench.ts
 *
 * Requires MinIO at localhost:9100 with key=testkey / secret=testsecret123.
 * The existing compact test suite already uses this setup; we reuse its
 * env conventions so the benchmark slots in without new infra.
 */

import { AwsClient } from "aws4fetch";
import { Database } from "../src/core/db.js";
import { Remote } from "../src/core/remote.js";

const ENDPOINT =
  process.env.DRIPLINE_TEST_S3_ENDPOINT ?? "http://localhost:9100";
const BUCKET = process.env.DRIPLINE_TEST_S3_BUCKET ?? "dripline-test";
const KEY = process.env.DRIPLINE_TEST_S3_KEY ?? "testkey";
const SECRET = process.env.DRIPLINE_TEST_S3_SECRET ?? "testsecret123";

/**
 * Synthetic schema sized to echo a real POS order-detail table's shape.
 * 50 columns total, three of which are JSON blobs (real tables carry
 * nested arrays of objects — we flatten to stringified JSON since the
 * goal is to reproduce the width, not the nesting semantics).
 */
const NUM_COLS = 50;
const JSON_COL_IDXS = [5, 15, 30];
const ORG_IDS = ["org-a", "org-b", "org-c"];

/** How big the baseline + incremental are. Tuned so the default
 *  current query OOMs at 300 MB — that's the whole point. */
const CURATED_ROWS_PER_ORG = 120_000;
const RAW_INCREMENTAL_ROWS_PER_ORG = 10_000;

const MEMORY_LIMIT = process.env.BENCH_MEMORY_LIMIT ?? "300MB";
const TEMP_DIR = "/tmp/dripline-bench-spill";

const RUN_PREFIX = `bench/${process.pid}-${Date.now()}`;

const aws = new AwsClient({
  accessKeyId: KEY,
  secretAccessKey: SECRET,
  service: "s3",
  region: "auto",
});

function log(msg: string): void {
  const ts = new Date().toISOString().slice(11, 23);
  console.log(`[${ts}] ${msg}`);
}

async function ensureBucket(): Promise<void> {
  const r = await aws.fetch(`${ENDPOINT}/${BUCKET}/`, { method: "PUT" });
  if (r.status !== 200 && r.status !== 409) {
    throw new Error(`bucket PUT failed: ${r.status} ${await r.text()}`);
  }
}

async function cleanupPrefix(prefix: string): Promise<void> {
  const list = await aws.fetch(
    `${ENDPOINT}/${BUCKET}/?list-type=2&prefix=${encodeURIComponent(prefix)}`,
  );
  if (list.status !== 200) return;
  const xml = await list.text();
  const keys = [...xml.matchAll(/<Key>([^<]+)<\/Key>/g)].map((m) => m[1]);
  for (const k of keys) {
    await aws.fetch(`${ENDPOINT}/${BUCKET}/${k}`, { method: "DELETE" });
  }
}

/**
 * Build a SELECT-from-range expression that synthesises N rows of the
 * wide schema entirely inside DuckDB. 1000x faster than the INSERT
 * VALUES approach — seeding 120k rows drops from ~33s to <1s. Column
 * values are deterministic functions of the row index so every
 * strategy sees the same data.
 */
function seedSelect(
  rangeStart: number,
  rangeEnd: number,
  cursor: string,
  org: string,
): string {
  const cols: string[] = [];
  for (let i = 0; i < NUM_COLS; i++) {
    if (i === 0) cols.push(`(${rangeStart} + i)::BIGINT AS "id"`);
    else if (i === 1) cols.push(`'${cursor}' AS "updated_at"`);
    else if (i === 2) cols.push(`'${org}' AS "org_id"`);
    else if (JSON_COL_IDXS.includes(i)) {
      // Realistically-sized JSON blob per row (a few hundred bytes),
      // built via string concatenation so DuckDB evaluates it natively.
      cols.push(`
        '{"id":' || (${rangeStart} + i)::VARCHAR
        || ',"items":[{"sku":"sku-' || (${rangeStart} + i)::VARCHAR || '-0","qty":1,"price":12.5,"modifier":"mod-' || ((${rangeStart} + i) % 17)::VARCHAR || '"},'
        || '{"sku":"sku-' || (${rangeStart} + i)::VARCHAR || '-1","qty":2,"price":25.0,"modifier":"mod-' || ((${rangeStart} + i) % 17)::VARCHAR || '"},'
        || '{"sku":"sku-' || (${rangeStart} + i)::VARCHAR || '-2","qty":3,"price":37.5,"modifier":"mod-' || ((${rangeStart} + i) % 17)::VARCHAR || '"}]'
        || ',"notes":"row ' || (${rangeStart} + i)::VARCHAR || ' in ${org} generated at ${cursor}"}'
        AS "json_${i}"`);
    } else {
      cols.push(
        `'col${i}-' || (${rangeStart} + i)::VARCHAR || '-${org}' AS "col_${i}"`,
      );
    }
  }
  return `SELECT ${cols.join(", ")} FROM range(${rangeEnd - rangeStart}) t(i)`;
}

/**
 * Generate a parquet file on MinIO via a single DuckDB COPY from a
 * range-backed SELECT. The resulting file's encoding (zstd, row-group
 * size, column types) matches what real sync + compact would produce,
 * so measurements here transfer to production.
 */
async function writeSyntheticParquet(
  remote: Remote,
  url: string,
  selectSql: string,
): Promise<void> {
  const db = await Database.create(":memory:");
  try {
    await remote.attach(db);
    await db.exec(`
      COPY (${selectSql}) TO '${url}'
      (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
    `);
  } finally {
    await db.close();
  }
}

async function seed(remote: Remote, prefix: string): Promise<void> {
  log(`seeding baseline: ${CURATED_ROWS_PER_ORG} rows × ${ORG_IDS.length} orgs`);
  // Curated baseline — one file per org partition, cursor in the past.
  for (const org of ORG_IDS) {
    const base = ORG_IDS.indexOf(org) * 10_000_000;
    const url = `s3://${BUCKET}/${prefix}/curated/bench_table/org_id=${org}/data_0.parquet`;
    const sql = seedSelect(
      base,
      base + CURATED_ROWS_PER_ORG,
      "2026-01-01T00:00:00Z",
      org,
    );
    await writeSyntheticParquet(remote, url, sql);
    log(`  curated ${org}: ${CURATED_ROWS_PER_ORG} rows`);
  }

  log(
    `seeding incremental raw: ${RAW_INCREMENTAL_ROWS_PER_ORG} rows × ${ORG_IDS.length} orgs (half updates, half new PKs)`,
  );
  // Raw incremental — half update existing PKs (newer cursor), half
  // brand new. Modeled as UNION ALL of two range() scans per org so
  // DuckDB materialises both halves in a single COPY.
  for (const org of ORG_IDS) {
    const base = ORG_IDS.indexOf(org) * 10_000_000;
    const half = Math.floor(RAW_INCREMENTAL_ROWS_PER_ORG / 2);
    const updatesSql = seedSelect(
      base,
      base + half,
      "2026-04-19T00:00:00Z",
      org,
    );
    const newSql = seedSelect(
      base + CURATED_ROWS_PER_ORG,
      base + CURATED_ROWS_PER_ORG + half,
      "2026-04-19T00:00:00Z",
      org,
    );
    const combined = `${updatesSql} UNION ALL BY NAME ${newSql}`;
    const url = `s3://${BUCKET}/${prefix}/raw/bench_table/lane=bench/run=2026-04-19T00-00-00-000Z_${org}.parquet`;
    await writeSyntheticParquet(remote, url, combined);
    log(`  raw ${org}: ${half * 2} rows`);
  }
}

/**
 * Peak-memory probe. RSS via `process.memoryUsage()` at 25ms ticks.
 *
 * Deliberately NOT DuckDB's `duckdb_memory()` view — DuckDB connections
 * are single-threaded, so interleaving a probe SELECT against the
 * strategy's in-flight COPY would either serialise behind the COPY
 * (missing the entire query window) or stall the COPY itself. RSS is
 * the honest measurement of "does it fit in the container" anyway.
 */
class MemoryProbe {
  private peakRssMB = 0;
  private timer: ReturnType<typeof setInterval> | null = null;

  start(intervalMs = 25): void {
    this.timer = setInterval(() => {
      const rssMB = process.memoryUsage().rss / 1024 / 1024;
      if (rssMB > this.peakRssMB) this.peakRssMB = rssMB;
    }, intervalMs);
  }

  stop(): { rssMB: number } {
    if (this.timer) clearInterval(this.timer);
    return { rssMB: this.peakRssMB };
  }
}

interface Strategy {
  name: string;
  description: string;
  run: (ctx: StrategyCtx) => Promise<void>;
}

interface StrategyCtx {
  db: Database;
  prefix: string;
  rawUrl: string;
  curatedReadGlob: string;
  outDir: string;
  partitions: string[];
}

const strategies: Strategy[] = [
  {
    name: "baseline",
    description: "current prod: ROW_NUMBER() OVER (PARTITION BY pk)",
    async run(ctx) {
      const literals = ctx.partitions
        .map((p) => `('${p.replace(/'/g, "''")}')`)
        .join(", ");
      const curatedFilter =
        ctx.partitions.length > 0 ? `WHERE (org_id) IN (${literals})` : "";
      await ctx.db.exec(`
        COPY (
          SELECT * EXCLUDE (_rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY id ORDER BY updated_at DESC NULLS LAST
            ) AS _rn
            FROM (
              SELECT * FROM read_parquet('${ctx.rawUrl}', union_by_name => true, hive_partitioning => false)
              UNION ALL BY NAME
              SELECT * FROM ${ctx.curatedReadGlob}
              ${curatedFilter}
            )
          ) WHERE _rn = 1
        ) TO '${ctx.outDir}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000,
         PARTITION_BY (org_id), OVERWRITE_OR_IGNORE);
      `);
    },
  },
  {
    name: "qualify",
    description: "QUALIFY ROW_NUMBER() = 1 (lexically smaller, same plan)",
    async run(ctx) {
      const literals = ctx.partitions
        .map((p) => `('${p.replace(/'/g, "''")}')`)
        .join(", ");
      const curatedFilter =
        ctx.partitions.length > 0 ? `WHERE (org_id) IN (${literals})` : "";
      await ctx.db.exec(`
        COPY (
          SELECT * FROM (
            SELECT * FROM read_parquet('${ctx.rawUrl}', union_by_name => true, hive_partitioning => false)
            UNION ALL BY NAME
            SELECT * FROM ${ctx.curatedReadGlob}
            ${curatedFilter}
          )
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id ORDER BY updated_at DESC NULLS LAST
          ) = 1
        ) TO '${ctx.outDir}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000,
         PARTITION_BY (org_id), OVERWRITE_OR_IGNORE);
      `);
    },
  },
  {
    name: "argmax",
    description:
      "GROUP BY pk + JOIN on max(cursor). Streaming hash aggregate, small row-groups.",
    async run(ctx) {
      const literals = ctx.partitions
        .map((p) => `('${p.replace(/'/g, "''")}')`)
        .join(", ");
      const curatedFilter =
        ctx.partitions.length > 0 ? `WHERE (org_id) IN (${literals})` : "";
      // Two-phase: (1) find max(cursor) per pk via hash aggregate (tiny
      // state: pk + cursor), (2) inner-join the combined source to keep
      // only those rows. Avoids materialising (pk, cursor) → *all cols*
      // in a window sort buffer. ROW_GROUP_SIZE=10000 to keep the
      // PARTITION_BY writer's per-partition buffer bounded.
      await ctx.db.exec(`
        COPY (
          WITH src AS (
            SELECT * FROM read_parquet('${ctx.rawUrl}', union_by_name => true, hive_partitioning => false)
            UNION ALL BY NAME
            SELECT * FROM ${ctx.curatedReadGlob}
            ${curatedFilter}
          ),
          latest AS (
            SELECT id, MAX(updated_at) AS max_cursor
            FROM src
            GROUP BY id
          )
          SELECT src.* FROM src
          JOIN latest
            ON src.id = latest.id
           AND src.updated_at = latest.max_cursor
        ) TO '${ctx.outDir}'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 10000,
         PARTITION_BY (org_id), OVERWRITE_OR_IGNORE);
      `);
    },
  },
  {
    name: "per-part",
    description:
      "Per-partition COPY without PARTITION_BY writer. ROW_GROUP_SIZE=100000.",
    async run(ctx) {
      // DuckDB's PARTITION_BY writer keeps an open file handle + buffer
      // per distinct partition value, each ~ROW_GROUP_SIZE rows of
      // uncompressed row state. For wide schemas that's the real
      // memory hog, not the window. Skip it: one COPY per partition
      // that writes a single unpartitioned file. DuckDB only holds
      // ONE partition's worth of row-group state at a time.
      for (const org of ctx.partitions) {
        const safe = org.replace(/'/g, "''");
        await ctx.db.exec(`
          COPY (
            SELECT * EXCLUDE (_rn) FROM (
              SELECT *, ROW_NUMBER() OVER (
                PARTITION BY id ORDER BY updated_at DESC NULLS LAST
              ) AS _rn
              FROM (
                SELECT * FROM read_parquet('${ctx.rawUrl}', union_by_name => true, hive_partitioning => false)
                WHERE org_id = '${safe}'
                UNION ALL BY NAME
                SELECT * FROM ${ctx.curatedReadGlob}
                WHERE org_id = '${safe}'
              )
            ) WHERE _rn = 1
          ) TO '${ctx.outDir}/org_id=${encodeURIComponent(org)}/data_0.parquet'
          (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
        `);
      }
    },
  },
  {
    name: "per-part-small-rg",
    description:
      "Per-partition COPY + ROW_GROUP_SIZE=10000 to shrink writer buffer.",
    async run(ctx) {
      for (const org of ctx.partitions) {
        const safe = org.replace(/'/g, "''");
        await ctx.db.exec(`
          COPY (
            SELECT * EXCLUDE (_rn) FROM (
              SELECT *, ROW_NUMBER() OVER (
                PARTITION BY id ORDER BY updated_at DESC NULLS LAST
              ) AS _rn
              FROM (
                SELECT * FROM read_parquet('${ctx.rawUrl}', union_by_name => true, hive_partitioning => false)
                WHERE org_id = '${safe}'
                UNION ALL BY NAME
                SELECT * FROM ${ctx.curatedReadGlob}
                WHERE org_id = '${safe}'
              )
            ) WHERE _rn = 1
          ) TO '${ctx.outDir}/org_id=${encodeURIComponent(org)}/data_0.parquet'
          (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 10000);
        `);
      }
    },
  },
];

interface Result {
  strategy: string;
  description: string;
  status: "ok" | "error";
  error?: string;
  durationMs: number;
  peakRssMB: number;
  outputRows: number | null;
}

async function runStrategy(
  strategy: Strategy,
  prefix: string,
): Promise<Result> {
  const remote = new Remote({
    endpoint: ENDPOINT,
    bucket: BUCKET,
    prefix,
    accessKeyId: KEY,
    secretAccessKey: SECRET,
    secretType: "S3",
  });

  // Fresh DuckDB per strategy so caches and buffer pools don't bias
  // the measurement. 300 MB hard cap + spill — the production-host
  // configuration exactly.
  const db = await Database.create(":memory:", {
    memoryLimit: MEMORY_LIMIT,
    threads: 1,
    tempDirectory: TEMP_DIR,
    preserveInsertionOrder: false,
    objectCache: true,
  });

  const outDir = `s3://${BUCKET}/${prefix}/compacted_${strategy.name}`;
  await cleanupPrefix(`${prefix}/compacted_${strategy.name}/`);

  const rawUrl = `s3://${BUCKET}/${prefix}/raw/bench_table/**/*.parquet`;
  const curatedReadGlob = `read_parquet('s3://${BUCKET}/${prefix}/curated/bench_table/**/*.parquet', hive_partitioning => true)`;

  const ctx: StrategyCtx = {
    db,
    prefix,
    rawUrl,
    curatedReadGlob,
    outDir,
    partitions: [...ORG_IDS],
  };

  await remote.attach(db);

  const probe = new MemoryProbe();
  probe.start();
  const start = Date.now();
  const result: Result = {
    strategy: strategy.name,
    description: strategy.description,
    status: "ok",
    durationMs: 0,
    peakRssMB: 0,
    outputRows: null,
  };

  try {
    await strategy.run(ctx);
    result.durationMs = Date.now() - start;
    result.peakRssMB = probe.stop().rssMB;

    const cnt = await db.all(
      `SELECT COUNT(*) AS n FROM read_parquet('s3://${BUCKET}/${prefix}/compacted_${strategy.name}/**/*.parquet', hive_partitioning => true);`,
    );
    result.outputRows = Number((cnt[0] as { n: bigint | number }).n);
  } catch (err) {
    result.durationMs = Date.now() - start;
    result.peakRssMB = probe.stop().rssMB;
    result.status = "error";
    result.error = (err as Error).message;
  } finally {
    await db.close();
  }

  return result;
}

async function main(): Promise<void> {
  log(`bench config: memory=${MEMORY_LIMIT} prefix=${RUN_PREFIX}`);
  log(`MinIO: ${ENDPOINT} bucket=${BUCKET}`);

  await ensureBucket();

  const remote = new Remote({
    endpoint: ENDPOINT,
    bucket: BUCKET,
    prefix: RUN_PREFIX,
    accessKeyId: KEY,
    secretAccessKey: SECRET,
    secretType: "S3",
  });

  log("─── seed ───");
  const seedStart = Date.now();
  await seed(remote, RUN_PREFIX);
  log(`seed took ${Date.now() - seedStart}ms`);

  const results: Result[] = [];
  for (const strategy of strategies) {
    log(`─── strategy: ${strategy.name} ───`);
    log(`  ${strategy.description}`);
    const r = await runStrategy(strategy, RUN_PREFIX);
    if (r.status === "ok") {
      log(
        `  OK   dur=${r.durationMs}ms  peakRss=${r.peakRssMB.toFixed(1)}MB  rows=${r.outputRows}`,
      );
    } else {
      log(
        `  FAIL dur=${r.durationMs}ms  peakRss=${r.peakRssMB.toFixed(1)}MB  err=${r.error?.slice(0, 120)}`,
      );
    }
    results.push(r);
  }

  log("─── summary ───");
  console.log();
  console.log(
    "strategy   status  duration   peakRSS   rows      description",
  );
  console.log(
    "─────────  ──────  ─────────  ────────  ────────  ─────────────────────────────",
  );
  for (const r of results) {
    const dur = `${r.durationMs}ms`.padEnd(9);
    const rss = `${r.peakRssMB.toFixed(0)}MB`.padEnd(8);
    const rows = `${r.outputRows ?? "-"}`.padEnd(8);
    const name = r.strategy.padEnd(9);
    const status = r.status.padEnd(6);
    console.log(
      `${name}  ${status}  ${dur}  ${rss}  ${rows}  ${r.description}`,
    );
  }
  console.log();

  log("cleanup: deleting bench prefix from bucket");
  await cleanupPrefix(`${RUN_PREFIX}/`);
  log("done");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
