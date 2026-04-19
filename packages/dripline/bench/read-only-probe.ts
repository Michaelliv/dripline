#!/usr/bin/env bun
/**
 * Quick probe: how much memory does DuckDB use just to *read* the
 * curated + raw parquet without doing any compaction work?
 *
 * If the baseline bench OOMs at 282 MiB even for "per-part" (which is
 * basically a filtered read + write), the culprit might be parquet
 * reading itself, not the query. This script isolates that.
 */

import { Database } from "../src/core/db.js";
import { Remote } from "../src/core/remote.js";

const ENDPOINT = "http://localhost:9100";
const BUCKET = "dripline-test";
const KEY = "testkey";
const SECRET = "testsecret123";

async function main() {
  // Seed first — small dataset to mirror a typical bench state.
  const prefix = `probe/${process.pid}-${Date.now()}`;
  const remote = new Remote({
    endpoint: ENDPOINT,
    bucket: BUCKET,
    prefix,
    accessKeyId: KEY,
    secretAccessKey: SECRET,
    secretType: "S3",
  });

  // Generate 360k rows × 50 cols × 3 json blobs across 3 curated files
  // + 30k raw, matching the main bench dataset.
  const NUM_COLS = 50;
  const JSON_COL_IDXS = [5, 15, 30];
  const ORGS = ["org-a", "org-b", "org-c"];
  const PER_ORG_CURATED = 120_000;
  const PER_ORG_RAW = 10_000;

  function seedSelect(start: number, end: number, cursor: string, org: string) {
    const cols: string[] = [];
    for (let i = 0; i < NUM_COLS; i++) {
      if (i === 0) cols.push(`(${start} + i)::BIGINT AS "id"`);
      else if (i === 1) cols.push(`'${cursor}' AS "updated_at"`);
      else if (i === 2) cols.push(`'${org}' AS "org_id"`);
      else if (JSON_COL_IDXS.includes(i))
        cols.push(
          `'{"id":' || (${start} + i)::VARCHAR || ',"items":[{"sku":"sku-' || (${start} + i)::VARCHAR || '","qty":1,"price":12.5}]}' AS "json_${i}"`,
        );
      else cols.push(`'col${i}-' || (${start} + i)::VARCHAR AS "col_${i}"`);
    }
    return `SELECT ${cols.join(", ")} FROM range(${end - start}) t(i)`;
  }

  console.log("seeding...");
  for (const org of ORGS) {
    const base = ORGS.indexOf(org) * 10_000_000;
    const sql = seedSelect(base, base + PER_ORG_CURATED, "2026-01-01", org);
    const db = await Database.create(":memory:");
    try {
      await remote.attach(db);
      await db.exec(
        `COPY (${sql}) TO 's3://${BUCKET}/${prefix}/curated/bench_table/org_id=${org}/data_0.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);`,
      );
    } finally {
      await db.close();
    }
  }
  for (const org of ORGS) {
    const base = ORGS.indexOf(org) * 10_000_000;
    const sql = seedSelect(base, base + PER_ORG_RAW, "2026-04-19", org);
    const db = await Database.create(":memory:");
    try {
      await remote.attach(db);
      await db.exec(
        `COPY (${sql}) TO 's3://${BUCKET}/${prefix}/raw/bench_table/lane=probe/run=2026-04-19_${org}.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);`,
      );
    } finally {
      await db.close();
    }
  }
  console.log("seed done");

  // Probe: open a DuckDB with 300MB cap, measure peak RSS across
  // progressively more expensive reads.
  const db = await Database.create(":memory:", {
    memoryLimit: "300MB",
    threads: 1,
    tempDirectory: "/tmp/dripline-bench-spill",
    preserveInsertionOrder: false,
    objectCache: true,
  });
  await remote.attach(db);

  let peakRss = 0;
  const sampler = setInterval(() => {
    const rss = process.memoryUsage().rss / 1024 / 1024;
    if (rss > peakRss) peakRss = rss;
  }, 25);

  const probes = [
    {
      name: "COUNT raw",
      sql: `SELECT COUNT(*) AS n FROM read_parquet('s3://${BUCKET}/${prefix}/raw/bench_table/**/*.parquet', union_by_name => true, hive_partitioning => false)`,
    },
    {
      name: "COUNT curated (hive)",
      sql: `SELECT COUNT(*) AS n FROM read_parquet('s3://${BUCKET}/${prefix}/curated/bench_table/**/*.parquet', hive_partitioning => true)`,
    },
    {
      name: "COUNT DISTINCT org from raw",
      sql: `SELECT COUNT(DISTINCT org_id) AS n FROM read_parquet('s3://${BUCKET}/${prefix}/raw/bench_table/**/*.parquet', union_by_name => true, hive_partitioning => false)`,
    },
    {
      name: "COUNT from UNION raw+curated",
      sql: `SELECT COUNT(*) AS n FROM (
        SELECT * FROM read_parquet('s3://${BUCKET}/${prefix}/raw/bench_table/**/*.parquet', union_by_name => true, hive_partitioning => false)
        UNION ALL BY NAME
        SELECT * FROM read_parquet('s3://${BUCKET}/${prefix}/curated/bench_table/**/*.parquet', hive_partitioning => true)
      )`,
    },
    {
      name: "SELECT id,org_id from UNION raw+curated",
      sql: `SELECT id, org_id FROM (
        SELECT * FROM read_parquet('s3://${BUCKET}/${prefix}/raw/bench_table/**/*.parquet', union_by_name => true, hive_partitioning => false)
        UNION ALL BY NAME
        SELECT * FROM read_parquet('s3://${BUCKET}/${prefix}/curated/bench_table/**/*.parquet', hive_partitioning => true)
      ) LIMIT 1000`,
    },
    {
      name: "SELECT * filtered by org_id (narrow)",
      sql: `SELECT * FROM read_parquet('s3://${BUCKET}/${prefix}/curated/bench_table/**/*.parquet', hive_partitioning => true) WHERE org_id = 'org-a' LIMIT 1000`,
    },
  ];

  for (const p of probes) {
    const rssBefore = process.memoryUsage().rss / 1024 / 1024;
    const start = Date.now();
    try {
      const rows = await db.all(p.sql);
      const dur = Date.now() - start;
      const rssAfter = process.memoryUsage().rss / 1024 / 1024;
      console.log(
        `  ${p.name.padEnd(45)} dur=${dur}ms  rssBefore=${rssBefore.toFixed(0)}MB  rssAfter=${rssAfter.toFixed(0)}MB  peakSoFar=${peakRss.toFixed(0)}MB  rows=${rows.length}`,
      );
    } catch (e) {
      const dur = Date.now() - start;
      console.log(
        `  ${p.name.padEnd(45)} FAIL dur=${dur}ms  rss=${(process.memoryUsage().rss / 1024 / 1024).toFixed(0)}MB  err=${(e as Error).message.slice(0, 80)}`,
      );
    }
  }

  clearInterval(sampler);
  console.log(`\npeak RSS across all probes: ${peakRss.toFixed(0)}MB`);

  await db.close();

  // Cleanup
  const { AwsClient } = await import("aws4fetch");
  const aws = new AwsClient({
    accessKeyId: KEY,
    secretAccessKey: SECRET,
    service: "s3",
    region: "auto",
  });
  const list = await aws.fetch(
    `${ENDPOINT}/${BUCKET}/?list-type=2&prefix=${encodeURIComponent(prefix)}`,
  );
  const xml = await list.text();
  for (const m of xml.matchAll(/<Key>([^<]+)<\/Key>/g)) {
    await aws.fetch(`${ENDPOINT}/${BUCKET}/${m[1]}`, { method: "DELETE" });
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
