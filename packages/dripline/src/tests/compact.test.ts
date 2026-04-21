/**
 * End-to-end tests for `dripline compact` against MinIO.
 *
 * The compactor is the piece that makes the warehouse queryable —
 * without it, raw/ grows unboundedly and queries pay LIST costs against
 * thousands of tiny files. These tests prove:
 *
 *   1. Happy path: after `run`, `compact` moves raw → curated and
 *      writes a manifest.
 *   2. Dedupe correctness: two syncs producing the same primary key
 *      with different cursor values → compact keeps the latest row.
 *   3. Idempotency: running compact twice in a row produces the same
 *      curated state (modulo rewrites).
 *   4. Lease mutex: a second compactor running while the first holds
 *      the lease is skipped cleanly.
 *   5. Skip-empty: no raw files → skipped, not errored.
 *   6. Filter: --table picks out one table without touching others.
 *   7. Config errors: missing remote, no compactable tables, unknown
 *      filter target → CompactConfigError with clear messages.
 *
 * Auto-skips when MinIO is unreachable.
 */

import { strict as assert } from "node:assert";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, afterEach, before, beforeEach, describe, it } from "node:test";
import { AwsClient } from "aws4fetch";
import { compact, compactLeaseName } from "../commands/compact.js";
import { run } from "../commands/run.js";
import type { DriplineConfig } from "../config/types.js";
import { LeaseStore } from "../core/lease.js";
import { Remote } from "../core/remote.js";
import { registry } from "../plugin/registry.js";
import type { PluginDef } from "../plugin/types.js";

const ENDPOINT =
  process.env.DRIPLINE_TEST_S3_ENDPOINT ?? "http://localhost:9100";
const BUCKET = process.env.DRIPLINE_TEST_S3_BUCKET ?? "dripline-test";
const KEY = process.env.DRIPLINE_TEST_S3_KEY ?? "testkey";
const SECRET = process.env.DRIPLINE_TEST_S3_SECRET ?? "testsecret123";

const RUN_PREFIX = `compact-tests/${process.pid}-${Date.now()}`;

let backendUp = false;
async function probeBackend(): Promise<boolean> {
  try {
    const aws = new AwsClient({
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      service: "s3",
      region: "auto",
    });
    const r = await aws.fetch(`${ENDPOINT}/${BUCKET}/`, { method: "PUT" });
    return r.status === 200 || r.status === 409;
  } catch {
    return false;
  }
}

/**
 * Configurable in-process plugin. The rows it yields are held in a
 * module-level array so tests can mutate them between runs to simulate
 * an upstream that has changed — producing the same primary keys with
 * newer cursor values.
 */
let PLUGIN_ROWS: Array<{
  id: number;
  name: string;
  updated_at: string;
  org: string;
}> = [];

function registerTestPlugin(): void {
  const plugin: PluginDef = {
    name: "compact_test",
    version: "1.0.0",
    tables: [
      {
        name: "items",
        description: "Test items table for compaction",
        columns: [
          { name: "id", type: "number" },
          { name: "name", type: "string" },
          { name: "updated_at", type: "datetime" },
        ],
        keyColumns: [{ name: "org", required: "required" }],
        primaryKey: ["id"],
        cursor: "updated_at",
        async *list() {
          for (const row of PLUGIN_ROWS) yield row;
        },
      },
      {
        // Second table with NO primaryKey — proves it's filtered out
        // as non-compactable.
        name: "noprimarykey_items",
        description: "Test table without a primary key",
        columns: [{ name: "id", type: "number" }],
        keyColumns: [{ name: "org", required: "required" }],
        async *list() {
          yield { id: 1, org: "x" };
        },
      },
    ],
  };
  registry.register(plugin);
}

function setPluginRows(
  rows: Array<{ id: number; name: string; updated_at: string; org: string }>,
): void {
  PLUGIN_ROWS = rows;
}

/** Multi-column partition plugin for testing org + date partitioning. */
let MULTI_ROWS: Array<{
  id: number;
  name: string;
  updated_at: string;
  org: string;
  biz_date: string;
}> = [];

function registerMultiPartPlugin(): void {
  const plugin: PluginDef = {
    name: "multi_part_test",
    version: "1.0.0",
    tables: [
      {
        name: "sales",
        columns: [
          { name: "id", type: "number" },
          { name: "name", type: "string" },
          { name: "updated_at", type: "datetime" },
        ],
        keyColumns: [
          { name: "org", required: "required" },
          { name: "biz_date", required: "required" },
        ],
        primaryKey: ["id"],
        cursor: "updated_at",
        async *list() {
          for (const row of MULTI_ROWS) yield row;
        },
      },
    ],
  };
  registry.register(plugin);
}

function setMultiRows(
  rows: Array<{
    id: number;
    name: string;
    updated_at: string;
    org: string;
    biz_date: string;
  }>,
): void {
  MULTI_ROWS = rows;
}

function makeMultiProject(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), "dripline-compact-multi-"));
  mkdirSync(join(dir, ".dripline"), { recursive: true });
  const config: DriplineConfig = {
    connections: [{ name: "default", plugin: "multi_part_test", config: {} }],
    cache: { enabled: true, ttl: 300, maxSize: 1000 },
    rateLimits: {},
    lanes: {
      main: {
        tables: [
          { name: "sales", params: { org: "x", biz_date: "2024-01-01" } },
        ],
        interval: "60s",
        maxRuntime: "10s",
      },
    },
    remote: {
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      secretType: "S3",
    },
  };
  writeFileSync(
    join(dir, ".dripline", "config.json"),
    JSON.stringify(config, null, 2),
  );
  return dir;
}

function makeProject(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), "dripline-compact-"));
  mkdirSync(join(dir, ".dripline"), { recursive: true });
  const config: DriplineConfig = {
    connections: [{ name: "default", plugin: "compact_test", config: {} }],
    cache: { enabled: true, ttl: 300, maxSize: 1000 },
    rateLimits: {},
    lanes: {
      main: {
        tables: [{ name: "items", params: { org: "x" } }],
        interval: "60s",
        maxRuntime: "10s",
      },
    },
    remote: {
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      secretType: "S3",
    },
  };
  writeFileSync(
    join(dir, ".dripline", "config.json"),
    JSON.stringify(config, null, 2),
  );
  return dir;
}

let counter = 0;
const freshPrefix = (label: string) => `${RUN_PREFIX}/${label}-${++counter}`;

/** Count rows in curated/ via a direct DuckDB read. */
async function countCurated(prefix: string, table: string): Promise<number> {
  const { Database } = await import("../core/db.js");
  const db = await Database.create(":memory:");
  try {
    const remote = new Remote({
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      secretType: "S3",
    });
    await remote.attach(db);
    const url = `s3://${BUCKET}/${prefix}/curated/${table}/**/*.parquet`;
    const rows = await db.all(
      `SELECT COUNT(*) AS n FROM read_parquet('${url}', hive_partitioning => true);`,
    );
    return Number((rows[0] as { n: bigint | number }).n);
  } finally {
    await db.close();
  }
}

/** Read a specific row from curated/ to verify dedupe correctness. */
async function readCuratedById(
  prefix: string,
  table: string,
  id: number,
): Promise<{ id: number; name: string; updated_at: string } | null> {
  const { Database } = await import("../core/db.js");
  const db = await Database.create(":memory:");
  try {
    const remote = new Remote({
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      secretType: "S3",
    });
    await remote.attach(db);
    const url = `s3://${BUCKET}/${prefix}/curated/${table}/**/*.parquet`;
    const rows = await db.all(
      `SELECT id, name, updated_at FROM read_parquet('${url}', hive_partitioning => true) WHERE id = ${id};`,
    );
    if (rows.length === 0) return null;
    return rows[0] as { id: number; name: string; updated_at: string };
  } finally {
    await db.close();
  }
}

describe("dripline compact (end-to-end)", { concurrency: false }, () => {
  let originalCwd: string;

  before(async () => {
    originalCwd = process.cwd();
    backendUp = await probeBackend();
    if (!backendUp) {
      console.warn(`\n  ⚠ skipping compact tests: ${ENDPOINT} unreachable.\n`);
    }
    registerTestPlugin();
  });

  after(async () => {
    process.chdir(originalCwd);
    if (!backendUp) return;
    const aws = new AwsClient({
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      service: "s3",
      region: "auto",
    });
    const list = await aws.fetch(
      `${ENDPOINT}/${BUCKET}/?list-type=2&prefix=${encodeURIComponent(RUN_PREFIX)}`,
    );
    if (list.status !== 200) return;
    const xml = await list.text();
    const keys = [...xml.matchAll(/<Key>([^<]+)<\/Key>/g)].map((m) => m[1]);
    for (const k of keys) {
      await aws.fetch(`${ENDPOINT}/${BUCKET}/${k}`, { method: "DELETE" });
    }
  });

  beforeEach(() => {
    originalCwd = process.cwd();
    // Reset plugin rows between tests so each test controls its own data.
    setPluginRows([
      { id: 1, name: "a", updated_at: "2024-01-01T00:00:00Z", org: "x" },
      { id: 2, name: "b", updated_at: "2024-02-01T00:00:00Z", org: "x" },
      { id: 3, name: "c", updated_at: "2024-03-01T00:00:00Z", org: "x" },
    ]);
  });
  afterEach(() => process.chdir(originalCwd));

  const ift = (name: string, fn: () => Promise<void>) =>
    it(name, async () => {
      if (!backendUp) return;
      await fn();
    });

  // ─────────────────────────────────────────────────────────────────

  ift(
    "happy path: run then compact publishes curated/ and a manifest",
    async () => {
      const prefix = freshPrefix("happy");
      process.chdir(makeProject(prefix));

      const runResults = await run({ quiet: true });
      assert.equal(runResults[0].status, "ok");
      assert.equal(runResults[0].rowsInserted, 3);

      const compactResults = await compact({ quiet: true });
      // Only "items" is compactable (noprimarykey_items has no PK)
      assert.equal(compactResults.length, 1);
      assert.equal(compactResults[0].status, "ok");
      assert.equal(compactResults[0].rows, 3);
      assert.ok(compactResults[0].files >= 1);

      // Verify curated/ is readable
      const count = await countCurated(prefix, "items");
      assert.equal(count, 3);

      // Verify manifest was written
      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const manifest = await remote.readManifest("items");
      assert.ok(manifest, "manifest should exist");
      assert.equal(manifest.table, "items");
      assert.ok(manifest.files.length >= 1);
      assert.equal(
        manifest.files.reduce((sum, f) => sum + Number(f.row_count ?? 0), 0),
        3,
      );
    },
  );

  ift(
    "dedupe correctness: later cursor wins over earlier for same PK",
    async () => {
      const prefix = freshPrefix("dedupe");
      process.chdir(makeProject(prefix));

      // Run 1: id=1 at 2024-01-01
      setPluginRows([
        {
          id: 1,
          name: "original",
          updated_at: "2024-01-01T00:00:00Z",
          org: "x",
        },
      ]);
      await run({ quiet: true });

      // The run() call acquired+renewed the lease to the lane interval
      // (60s). We forcibly release it so a second run can proceed.
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });
      await aws.fetch(
        `${ENDPOINT}/${BUCKET}/${prefix}/_leases/lane-main.json`,
        {
          method: "DELETE",
        },
      );

      // Run 2: id=1 at 2024-06-01 (newer) — same PK, newer cursor.
      // The cursor filter in engine.sync() will only yield rows newer
      // than the high-water mark from run 1, so id=1 with the new
      // updated_at IS newer → it flows through to raw/ as a second row.
      setPluginRows([
        {
          id: 1,
          name: "updated",
          updated_at: "2024-06-01T00:00:00Z",
          org: "x",
        },
      ]);
      await run({ quiet: true });

      // Now raw/ has two files, one with {id:1, name:"original"} and
      // one with {id:1, name:"updated"}. Compact should dedupe to
      // the later one.
      const results = await compact({ quiet: true });
      assert.equal(results[0].status, "ok");
      assert.equal(results[0].rows, 1, "dedupe should leave 1 row");

      const row = await readCuratedById(prefix, "items", 1);
      assert.ok(row);
      assert.equal(row.name, "updated", "newer cursor should win");
      assert.equal(row.updated_at, "2024-06-01T00:00:00Z");
    },
  );

  ift(
    "idempotent: running compact twice produces the same curated state",
    async () => {
      const prefix = freshPrefix("idempotent");
      process.chdir(makeProject(prefix));

      await run({ quiet: true });
      const first = await compact({ quiet: true });
      const second = await compact({ quiet: true });

      assert.equal(first[0].status, "ok");
      // Second compact has no new raw files — correctly skips.
      assert.equal(second[0].status, "skipped");
      assert.equal(second[0].rows, 0);

      // Manifest should still be present and consistent.
      const count = await countCurated(prefix, "items");
      assert.equal(count, first[0].rows);
    },
  );

  ift("lease mutex: second concurrent compactor is skipped", async () => {
    const prefix = freshPrefix("mutex");
    process.chdir(makeProject(prefix));
    await run({ quiet: true });

    // Pre-acquire the compact lease so compact() finds it held.
    const ls = new LeaseStore({
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
    });
    const held = await ls.acquire(compactLeaseName("items"), 30_000);
    assert.ok(held, "test setup: should have acquired the lease");

    try {
      const results = await compact({ quiet: true });
      assert.equal(results.length, 1);
      assert.equal(results[0].status, "skipped");
      assert.match(results[0].reason ?? "", /lease held/);
    } finally {
      await ls.release(held);
    }
  });

  ift("skip-empty: no raw files → skipped, not errored", async () => {
    const prefix = freshPrefix("empty");
    process.chdir(makeProject(prefix));
    // Note: we intentionally DO NOT run() first — raw/ is empty.

    const results = await compact({ quiet: true });
    assert.equal(results[0].status, "skipped");
    assert.match(results[0].reason ?? "", /no raw files/i);
  });

  ift("--table filter picks one table and ignores others", async () => {
    const prefix = freshPrefix("filter");
    process.chdir(makeProject(prefix));
    await run({ quiet: true });

    // Only compactable table is "items"; verify the filter works.
    const results = await compact({ tables: ["items"], quiet: true });
    assert.equal(results.length, 1);
    assert.equal(results[0].table, "items");
  });

  ift("unknown --table target errors clearly", async () => {
    const prefix = freshPrefix("unknown-table");
    process.chdir(makeProject(prefix));

    await assert.rejects(
      () => compact({ tables: ["nope"], quiet: true }),
      /not compactable/,
    );
  });

  // ── Raw cleanup contract ─────────────────────────────────────────

  ift(
    "raw cleanup: successful compact deletes consumed raw files",
    async () => {
      const prefix = freshPrefix("cleanup");
      process.chdir(makeProject(prefix));
      await run({ quiet: true });

      // Sanity: raw/ has at least one file before compact.
      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const beforeRaw = await remote.listObjects("raw/items/");
      assert.ok(beforeRaw.length > 0, "setup: raw/ should have files");

      const results = await compact({ quiet: true });
      assert.equal(results[0].status, "ok");
      assert.equal(
        results[0].rawCleaned,
        beforeRaw.length,
        "compact should report cleaning the exact set of raw files it saw",
      );

      const afterRaw = await remote.listObjects("raw/items/");
      assert.equal(afterRaw.length, 0, "raw/ should be empty after compact");

      // And curated/ is intact.
      const curatedCount = await countCurated(prefix, "items");
      assert.equal(curatedCount, 3);
    },
  );

  ift(
    "raw cleanup: snapshot semantics — only consumed files are deleted",
    async () => {
      const prefix = freshPrefix("snapshot");
      process.chdir(makeProject(prefix));

      // Run once to populate raw/.
      await run({ quiet: true });

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const consumedKeys = await remote.listObjects("raw/items/");
      assert.ok(consumedKeys.length > 0);

      // We can't easily inject a planted file at the exact instant
      // between snapshot and delete from a black-box test, but we CAN
      // verify the contract by another route: call deleteObjects with
      // a SUBSET of the actual raw files and verify only that subset
      // is gone. This proves deleteObjects targets exactly the keys
      // we pass and nothing else — which is the load-bearing claim
      // behind the snapshot-then-delete pattern in compact().
      const subsetToDelete = consumedKeys.slice(0, 1);
      await remote.deleteObjects(subsetToDelete);
      const remaining = await remote.listObjects("raw/items/");
      assert.equal(
        remaining.length,
        consumedKeys.length - subsetToDelete.length,
        "deleteObjects should delete exactly the keys passed, no more",
      );
      for (const key of subsetToDelete) {
        assert.ok(
          !remaining.includes(key),
          `deleted key ${key} should not be in remaining`,
        );
      }
    },
  );

  ift("raw cleanup: idempotent across multiple compact cycles", async () => {
    const prefix = freshPrefix("cleanup-idempotent");
    process.chdir(makeProject(prefix));

    // Three sync+compact cycles, each producing 3 rows that cumulatively
    // dedupe down to 3 in curated (cursor blocks re-syncs).
    const remote = new Remote({
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      secretType: "S3",
    });

    const aws = new AwsClient({
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      service: "s3",
      region: "auto",
    });
    for (let cycle = 0; cycle < 3; cycle++) {
      // Force-release the lane lease so we can re-run.
      await aws.fetch(
        `${ENDPOINT}/${BUCKET}/${prefix}/_leases/lane-main.json`,
        { method: "DELETE" },
      );
      await run({ quiet: true });
      const compactResult = await compact({ quiet: true });
      // First cycle has raw files → "ok". Subsequent cycles: cursor
      // blocks re-syncs, run() produces 0 rows, no raw → "skipped".
      if (cycle === 0) {
        assert.equal(compactResult[0].status, "ok");
      } else {
        assert.equal(compactResult[0].status, "skipped");
      }

      // Raw should be empty after every cycle (either cleaned or never written).
      const rawAfter = await remote.listObjects("raw/items/");
      assert.equal(
        rawAfter.length,
        0,
        `cycle ${cycle}: raw/ should be empty after compact, got ${rawAfter.length}`,
      );
    }

    // Curated row count is stable across cycles.
    assert.equal(await countCurated(prefix, "items"), 3);
  });

  ift(
    "incremental: second compact only rewrites partitions with new raw data",
    async () => {
      const prefix = freshPrefix("incremental");
      process.chdir(makeProject(prefix));

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });

      // Cycle 1: sync 3 rows in org "x", compact.
      setPluginRows([
        { id: 1, name: "a", updated_at: "2024-01-01T00:00:00Z", org: "x" },
        { id: 2, name: "b", updated_at: "2024-02-01T00:00:00Z", org: "x" },
        { id: 3, name: "c", updated_at: "2024-03-01T00:00:00Z", org: "x" },
      ]);
      await run({ quiet: true });
      const first = await compact({ quiet: true });
      assert.equal(first[0].status, "ok");
      assert.equal(first[0].rows, 3);

      // Record the curated file for org "x" — we'll verify it survives.
      const orgXFiles = await remote.listObjects("curated/items/org=x/");
      assert.ok(orgXFiles.length > 0, "org x should have curated files");

      // Get last-modified timestamps for org "x" files to prove they
      // aren't rewritten by the second compact.
      const getLastModified = async (key: string) => {
        const r = await aws.fetch(`${ENDPOINT}/${BUCKET}/${prefix}/${key}`, {
          method: "HEAD",
        });
        return r.headers.get("last-modified");
      };
      const orgXTimestamps = await Promise.all(
        orgXFiles.map(async (k) => ({ key: k, ts: await getLastModified(k) })),
      );

      // Cycle 2: sync 2 NEW rows in org "y" (different partition).
      // Release the lane lease so run() can re-acquire.
      await aws.fetch(
        `${ENDPOINT}/${BUCKET}/${prefix}/_leases/lane-main.json`,
        { method: "DELETE" },
      );
      // Also clear the cursor state so the engine re-syncs.
      const stateKeys = await remote.listObjects("_state/main/");
      if (stateKeys.length > 0) await remote.deleteObjects(stateKeys);

      setPluginRows([
        { id: 10, name: "d", updated_at: "2024-04-01T00:00:00Z", org: "y" },
        { id: 11, name: "e", updated_at: "2024-05-01T00:00:00Z", org: "y" },
      ]);
      await run({ quiet: true });

      // Verify raw/ has files (from org "y" sync).
      const rawBefore = await remote.listObjects("raw/items/");
      assert.ok(rawBefore.length > 0, "raw/ should have org y files");

      // Compact — should only process the org="y" partition.
      const second = await compact({ quiet: true });
      assert.equal(second[0].status, "ok");
      // Total curated rows: 3 (org x) + 2 (org y) = 5.
      assert.equal(second[0].rows, 5);

      // Verify org "x" files were NOT rewritten (same last-modified).
      // This is the core assertion: incremental compact skips
      // partitions that have no new raw data.
      for (const { key, ts } of orgXTimestamps) {
        const current = await getLastModified(key);
        assert.equal(
          current,
          ts,
          `org x file ${key} should not have been rewritten`,
        );
      }

      // Verify org "y" data is correct.
      const orgYRow = await readCuratedById(prefix, "items", 10);
      assert.ok(orgYRow, "org y row should exist in curated");
      assert.equal(orgYRow.name, "d");

      // Verify org "x" data is still correct.
      const orgXRow = await readCuratedById(prefix, "items", 1);
      assert.ok(orgXRow, "org x row should still exist in curated");
      assert.equal(orgXRow.name, "a");
    },
  );

  ift(
    "incremental: raw spanning multiple partitions reads all affected curated",
    async () => {
      const prefix = freshPrefix("incr-multi");
      process.chdir(makeProject(prefix));

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });

      // Cycle 1: seed both orgs.
      setPluginRows([
        { id: 1, name: "a", updated_at: "2024-01-01T00:00:00Z", org: "x" },
        { id: 2, name: "b", updated_at: "2024-01-01T00:00:00Z", org: "y" },
      ]);
      await run({ quiet: true });
      await compact({ quiet: true });

      // Cycle 2: update rows in BOTH orgs (raw spans two partitions).
      await aws.fetch(
        `${ENDPOINT}/${BUCKET}/${prefix}/_leases/lane-main.json`,
        { method: "DELETE" },
      );
      const stateKeys = await remote.listObjects("_state/main/");
      if (stateKeys.length > 0) await remote.deleteObjects(stateKeys);

      setPluginRows([
        {
          id: 1,
          name: "a-updated",
          updated_at: "2024-06-01T00:00:00Z",
          org: "x",
        },
        {
          id: 2,
          name: "b-updated",
          updated_at: "2024-06-01T00:00:00Z",
          org: "y",
        },
      ]);
      await run({ quiet: true });
      const results = await compact({ quiet: true });

      assert.equal(results[0].status, "ok");
      assert.equal(results[0].rows, 2, "should still be 2 rows after dedupe");

      // Both rows should have the updated name.
      const row1 = await readCuratedById(prefix, "items", 1);
      assert.equal(row1?.name, "a-updated");
      const row2 = await readCuratedById(prefix, "items", 2);
      assert.equal(row2?.name, "b-updated");
    },
  );

  ift(
    "incremental: PK migrates to different partition leaves stale row (known limitation)",
    async () => {
      // This documents a known limitation of incremental compaction:
      // if a row's partition column value changes between syncs, the
      // old partition retains the stale row because incremental compact
      // only reads partitions present in the new raw data.
      //
      // For real-world data (org_id, business_date) this is extremely
      // rare — org_id never changes and business_date corrections are
      // uncommon. A periodic full compact (--full flag) would clean it.
      const prefix = freshPrefix("incr-migrate");
      process.chdir(makeProject(prefix));

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });

      // Cycle 1: id=1 in org "x".
      setPluginRows([
        {
          id: 1,
          name: "original",
          updated_at: "2024-01-01T00:00:00Z",
          org: "x",
        },
      ]);
      await run({ quiet: true });
      await compact({ quiet: true });

      // Cycle 2: same PK id=1 now in org "y" (partition migration).
      await aws.fetch(
        `${ENDPOINT}/${BUCKET}/${prefix}/_leases/lane-main.json`,
        { method: "DELETE" },
      );
      const stateKeys = await remote.listObjects("_state/main/");
      if (stateKeys.length > 0) await remote.deleteObjects(stateKeys);

      setPluginRows([
        {
          id: 1,
          name: "migrated",
          updated_at: "2024-06-01T00:00:00Z",
          org: "y",
        },
      ]);
      await run({ quiet: true });
      const results = await compact({ quiet: true });
      assert.equal(results[0].status, "ok");

      // Known limitation: stale row in org "x" survives because
      // incremental compact only read org "y" from curated.
      // Row exists in BOTH partitions — 2 total instead of 1.
      assert.equal(
        results[0].rows,
        2,
        "known limitation: stale row in old partition survives incremental compact",
      );

      // Verify both partitions have the row by counting across all curated.
      const { Database: DB } = await import("../core/db.js");
      const verifyDb = await DB.create(":memory:");
      try {
        await remote.attach(verifyDb);
        const url = `s3://${BUCKET}/${prefix}/curated/items/**/*.parquet`;
        const allRows = await verifyDb.all(
          `SELECT id, name, org FROM read_parquet('${url}', hive_partitioning => true) WHERE id = 1 ORDER BY org`,
        );
        assert.equal(allRows.length, 2, "PK=1 should appear in 2 partitions");
        // org "x" has the stale row, org "y" has the migrated row.
        assert.equal((allRows[0] as any).org, "x");
        assert.equal((allRows[0] as any).name, "original");
        assert.equal((allRows[1] as any).org, "y");
        assert.equal((allRows[1] as any).name, "migrated");
      } finally {
        await verifyDb.close();
      }
    },
  );

  ift(
    "incremental: new partition value not in curated creates cleanly",
    async () => {
      const prefix = freshPrefix("incr-newpart");
      process.chdir(makeProject(prefix));

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });

      // Cycle 1: org "x" only.
      setPluginRows([
        { id: 1, name: "a", updated_at: "2024-01-01T00:00:00Z", org: "x" },
      ]);
      await run({ quiet: true });
      await compact({ quiet: true });

      // Cycle 2: brand new org "z" that has no curated partition yet.
      await aws.fetch(
        `${ENDPOINT}/${BUCKET}/${prefix}/_leases/lane-main.json`,
        { method: "DELETE" },
      );
      const stateKeys = await remote.listObjects("_state/main/");
      if (stateKeys.length > 0) await remote.deleteObjects(stateKeys);

      setPluginRows([
        {
          id: 10,
          name: "z-item",
          updated_at: "2024-06-01T00:00:00Z",
          org: "z",
        },
      ]);
      await run({ quiet: true });
      const results = await compact({ quiet: true });

      assert.equal(results[0].status, "ok");
      assert.equal(results[0].rows, 2, "1 from org x + 1 from org z");

      const orgZRow = await readCuratedById(prefix, "items", 10);
      assert.ok(orgZRow);
      assert.equal(orgZRow.name, "z-item");

      // Org "x" untouched.
      const orgXRow = await readCuratedById(prefix, "items", 1);
      assert.ok(orgXRow);
      assert.equal(orgXRow.name, "a");
    },
  );

  ift(
    "incremental: all raw rows are exact duplicates of curated — row count stable",
    async () => {
      const prefix = freshPrefix("incr-dupes");
      process.chdir(makeProject(prefix));

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });

      const rows = [
        { id: 1, name: "a", updated_at: "2024-01-01T00:00:00Z", org: "x" },
        { id: 2, name: "b", updated_at: "2024-02-01T00:00:00Z", org: "x" },
      ];

      // Cycle 1: initial.
      setPluginRows(rows);
      await run({ quiet: true });
      await compact({ quiet: true });

      // Cycle 2: exact same rows re-synced.
      await aws.fetch(
        `${ENDPOINT}/${BUCKET}/${prefix}/_leases/lane-main.json`,
        { method: "DELETE" },
      );
      const stateKeys = await remote.listObjects("_state/main/");
      if (stateKeys.length > 0) await remote.deleteObjects(stateKeys);

      setPluginRows(rows);
      await run({ quiet: true });
      const results = await compact({ quiet: true });

      assert.equal(results[0].status, "ok");
      assert.equal(results[0].rows, 2, "dedupe should keep exactly 2 rows");
    },
  );

  ift(
    "incremental: dedupe within affected partition picks latest cursor",
    async () => {
      const prefix = freshPrefix("incr-dedupe");
      process.chdir(makeProject(prefix));

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });

      // Cycle 1: original row.
      setPluginRows([
        { id: 1, name: "old", updated_at: "2024-01-01T00:00:00Z", org: "x" },
      ]);
      await run({ quiet: true });
      await compact({ quiet: true });

      // Cycle 2: same PK, same partition, newer cursor — should overwrite.
      await aws.fetch(
        `${ENDPOINT}/${BUCKET}/${prefix}/_leases/lane-main.json`,
        { method: "DELETE" },
      );
      const stateKeys = await remote.listObjects("_state/main/");
      if (stateKeys.length > 0) await remote.deleteObjects(stateKeys);

      setPluginRows([
        { id: 1, name: "new", updated_at: "2024-12-01T00:00:00Z", org: "x" },
      ]);
      await run({ quiet: true });
      const results = await compact({ quiet: true });

      assert.equal(results[0].status, "ok");
      assert.equal(results[0].rows, 1, "dedupe should keep 1 row");

      const row = await readCuratedById(prefix, "items", 1);
      assert.ok(row);
      assert.equal(row.name, "new", "newer cursor should win");
      assert.equal(row.updated_at, "2024-12-01T00:00:00Z");
    },
  );

  // ── Multi-column partition tests ──────────────────────────────────
  // These bypass run() and write raw parquet directly to test compact
  // in isolation with multi-column partitions (org + biz_date).

  /** Write rows as a raw parquet file via DuckDB, simulating a sync run. */
  async function writeRawParquet(
    remote: Remote,
    table: string,
    rows: Record<string, unknown>[],
    runId: string,
  ): Promise<void> {
    const { Database: DB } = await import("../core/db.js");
    const db = await DB.create(":memory:");
    try {
      await remote.attach(db);
      // Create temp table from the rows.
      const cols = Object.keys(rows[0]);
      const colDefs = cols.map((c) => `"${c}" VARCHAR`).join(", ");
      await db.exec(`CREATE TEMP TABLE _upload (${colDefs});`);
      for (const row of rows) {
        const vals = cols.map((c) => {
          const v = row[c];
          return v == null ? "NULL" : `'${String(v).replace(/'/g, "''")}'`;
        });
        await db.exec(`INSERT INTO _upload VALUES (${vals.join(", ")});`);
      }
      const url = remote.s3(`raw/${table}/lane=test/run=${runId}.parquet`);
      await db.exec(`
        COPY (SELECT * FROM _upload) TO '${url}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
      `);
    } finally {
      await db.close();
    }
  }

  ift(
    "multi-partition: incremental compact with org + biz_date partitioning",
    async () => {
      registerMultiPartPlugin();
      const prefix = freshPrefix("multi-part");
      process.chdir(makeMultiProject(prefix));

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });

      // Cycle 1: write raw with two orgs, two dates.
      await writeRawParquet(
        remote,
        "sales",
        [
          {
            id: "1",
            name: "a",
            updated_at: "2024-01-01T00:00:00Z",
            org: "x",
            biz_date: "2024-01-01",
          },
          {
            id: "2",
            name: "b",
            updated_at: "2024-01-01T00:00:00Z",
            org: "x",
            biz_date: "2024-01-02",
          },
          {
            id: "3",
            name: "c",
            updated_at: "2024-01-01T00:00:00Z",
            org: "y",
            biz_date: "2024-01-01",
          },
        ],
        "run-1",
      );
      const r1 = await compact({ tables: ["sales"], quiet: true });
      assert.equal(r1[0].status, "ok");

      const count1 = await countCurated(prefix, "sales");
      assert.equal(count1, 3);

      // Record timestamps for org "y" partitions.
      const getLastModified = async (key: string) => {
        const r = await aws.fetch(`${ENDPOINT}/${BUCKET}/${prefix}/${key}`, {
          method: "HEAD",
        });
        return r.headers.get("last-modified");
      };
      const orgYFiles = await remote.listObjects("curated/sales/org=y/");
      assert.ok(orgYFiles.length > 0);
      const orgYTimestamps = await Promise.all(
        orgYFiles.map(async (k) => ({ key: k, ts: await getLastModified(k) })),
      );

      // Cycle 2: update only org "x", date "2024-01-01".
      await writeRawParquet(
        remote,
        "sales",
        [
          {
            id: "1",
            name: "a-updated",
            updated_at: "2024-06-01T00:00:00Z",
            org: "x",
            biz_date: "2024-01-01",
          },
        ],
        "run-2",
      );
      const results = await compact({ tables: ["sales"], quiet: true });

      assert.equal(results[0].status, "ok");
      assert.equal(results[0].rows, 3, "still 3 rows after dedupe");

      const row1 = await readCuratedById(prefix, "sales", 1);
      assert.ok(row1);
      assert.equal(row1.name, "a-updated");

      // org "y" partition should NOT have been rewritten.
      for (const { key, ts } of orgYTimestamps) {
        const current = await getLastModified(key);
        assert.equal(
          current,
          ts,
          `org y file ${key} should not have been rewritten`,
        );
      }
    },
  );

  ift(
    "multi-partition: new date for existing org creates partition without touching others",
    async () => {
      registerMultiPartPlugin();
      const prefix = freshPrefix("multi-newdate");
      process.chdir(makeMultiProject(prefix));

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });

      // Cycle 1: org "x", one date.
      await writeRawParquet(
        remote,
        "sales",
        [
          {
            id: "1",
            name: "a",
            updated_at: "2024-01-01T00:00:00Z",
            org: "x",
            biz_date: "2024-01-01",
          },
        ],
        "run-1",
      );
      await compact({ tables: ["sales"], quiet: true });

      const getLastModified = async (key: string) => {
        const r = await aws.fetch(`${ENDPOINT}/${BUCKET}/${prefix}/${key}`, {
          method: "HEAD",
        });
        return r.headers.get("last-modified");
      };
      const jan1Files = await remote.listObjects(
        "curated/sales/org=x/biz_date=2024-01-01/",
      );
      const jan1Timestamps = await Promise.all(
        jan1Files.map(async (k) => ({ key: k, ts: await getLastModified(k) })),
      );

      // Cycle 2: same org, new date.
      await writeRawParquet(
        remote,
        "sales",
        [
          {
            id: "2",
            name: "b",
            updated_at: "2024-06-01T00:00:00Z",
            org: "x",
            biz_date: "2024-01-02",
          },
        ],
        "run-2",
      );
      const results = await compact({ tables: ["sales"], quiet: true });

      assert.equal(results[0].status, "ok");
      assert.equal(results[0].rows, 2, "1 old + 1 new");

      // Jan 1 partition untouched.
      for (const { key, ts } of jan1Timestamps) {
        const current = await getLastModified(key);
        assert.equal(
          current,
          ts,
          `jan 1 file ${key} should not have been rewritten`,
        );
      }

      const row2 = await readCuratedById(prefix, "sales", 2);
      assert.ok(row2);
      assert.equal(row2.name, "b");
    },
  );

  ift(
    "multi-partition: literal IN handles single quotes in partition values",
    async () => {
      registerMultiPartPlugin();
      const prefix = freshPrefix("multi-special");
      process.chdir(makeMultiProject(prefix));

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });

      // Cycle 1: org with a single quote.
      await writeRawParquet(
        remote,
        "sales",
        [
          {
            id: "1",
            name: "a",
            updated_at: "2024-01-01T00:00:00Z",
            org: "o'reilly",
            biz_date: "2024-01-01",
          },
        ],
        "run-1",
      );
      await compact({ tables: ["sales"], quiet: true });

      // Cycle 2: same org, updated — verifies no SQL injection.
      await writeRawParquet(
        remote,
        "sales",
        [
          {
            id: "1",
            name: "a-updated",
            updated_at: "2024-06-01T00:00:00Z",
            org: "o'reilly",
            biz_date: "2024-01-01",
          },
        ],
        "run-2",
      );
      const results = await compact({ tables: ["sales"], quiet: true });

      assert.equal(results[0].status, "ok");
      assert.equal(results[0].rows, 1);

      const row = await readCuratedById(prefix, "sales", 1);
      assert.ok(row);
      assert.equal(row.name, "a-updated");
    },
  );

  ift(
    "multi-partition: high cardinality — many partition combos in one raw batch",
    async () => {
      registerMultiPartPlugin();
      const prefix = freshPrefix("multi-highcard");
      process.chdir(makeMultiProject(prefix));

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });

      // 30 distinct partition combos in one raw file.
      const rows: Record<string, unknown>[] = [];
      for (let d = 1; d <= 30; d++) {
        rows.push({
          id: String(d),
          name: `day-${d}`,
          updated_at: "2024-01-01T00:00:00Z",
          org: "x",
          biz_date: `2024-01-${String(d).padStart(2, "0")}`,
        });
      }
      await writeRawParquet(remote, "sales", rows, "run-1");
      const results = await compact({ tables: ["sales"], quiet: true });

      assert.equal(results[0].status, "ok");
      assert.equal(results[0].rows, 30);
      assert.ok(
        results[0].files >= 30,
        "should have at least 30 partition files",
      );
    },
  );

  // ── Regressions from production incidents ────────────────────────
  //
  // Each test below reproduces a specific failure we hit in a live
  // warehouse and then fixed. They're small but load-bearing —
  // without them the next refactor to Remote.compact could silently
  // re-break any of these paths and no existing scenario would
  // notice. Keep them together so the "what went wrong" history is
  // discoverable from the test file alone.

  ift(
    "regression: cursor column is also a PK column — no UNION BY NAME dup",
    async () => {
      // A table keyed on (entity, date) with cursor=date (daily
      // aggregates, z-reports, etc.) used to break the narrow-decision
      // compactor: it built `pk, cursor` for the narrow SELECT, which
      // expanded to `"entity", "date", "date"` — and UNION ALL BY
      // NAME rejects duplicate column names in the SELECT list with
      // 'Binder Error: ... the name "date" occurs multiple times'.
      //
      // We register an ad-hoc plugin for this test because the file's
      // default test plugin has a cursor that's NOT in the PK, and
      // the bug only manifests when cursor ∈ PK.
      const plugin: PluginDef = {
        name: "cursor_in_pk_test",
        version: "1.0.0",
        tables: [
          {
            name: "daily_totals",
            columns: [
              { name: "total", type: "number" },
              { name: "date", type: "string" },
            ],
            keyColumns: [
              { name: "org", required: "required" },
              { name: "date", required: "optional" },
            ],
            partitionBy: ["org"],
            primaryKey: ["org", "date"],
            cursor: "date",
            async *list() {
              /* filled by writeRawParquet directly */
            },
          },
        ],
      };
      registry.register(plugin);

      const prefix = freshPrefix("cursor-in-pk");
      // Config points at the ad-hoc plugin.
      const dir = mkdtempSync(join(tmpdir(), "dripline-compact-cip-"));
      mkdirSync(join(dir, ".dripline"), { recursive: true });
      writeFileSync(
        join(dir, ".dripline", "config.json"),
        JSON.stringify({
          connections: [
            { name: "default", plugin: "cursor_in_pk_test", config: {} },
          ],
          cache: { enabled: true, ttl: 300, maxSize: 1000 },
          rateLimits: {},
          lanes: {},
          remote: {
            endpoint: ENDPOINT,
            bucket: BUCKET,
            prefix,
            accessKeyId: KEY,
            secretAccessKey: SECRET,
            secretType: "S3",
          },
        }),
      );
      process.chdir(dir);

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });

      // Cycle 1: seed two days for two orgs.
      await writeRawParquet(
        remote,
        "daily_totals",
        [
          { total: "100", date: "2024-01-01", org: "x" },
          { total: "200", date: "2024-01-02", org: "x" },
          { total: "150", date: "2024-01-01", org: "y" },
        ],
        "cip-1",
      );
      const r1 = await compact({ tables: ["daily_totals"], quiet: true });
      assert.equal(
        r1[0].status,
        "ok",
        `compact must not error on cursor-in-PK; got ${r1[0].error ?? "?"}`,
      );
      assert.equal(r1[0].rows, 3);

      // Cycle 2: revise one day's total (same PK, same cursor value).
      // The narrow SELECT still must not have duplicate column names.
      await writeRawParquet(
        remote,
        "daily_totals",
        [{ total: "999", date: "2024-01-01", org: "x" }],
        "cip-2",
      );
      const r2 = await compact({ tables: ["daily_totals"], quiet: true });
      assert.equal(r2[0].status, "ok");
      assert.equal(r2[0].rows, 3, "dedupe by PK keeps 3 rows");

      // Row for (x, 2024-01-01) should now reflect the revised total.
      // Use a direct SQL read since there's no `id` column here.
      const { Database: DB } = await import("../core/db.js");
      const db = await DB.create(":memory:");
      try {
        await remote.attach(db);
        const url = `s3://${BUCKET}/${prefix}/curated/daily_totals/**/*.parquet`;
        const rows = await db.all(
          `SELECT total FROM read_parquet('${url}', hive_partitioning => true)
             WHERE org = 'x' AND date = '2024-01-01'`,
        );
        assert.equal(rows.length, 1);
        assert.equal(
          (rows[0] as { total: string | number }).total,
          "999",
          "newer raw should win",
        );
      } finally {
        await db.close();
      }
    },
  );

  ift(
    "regression: cursor column added after curated was first written",
    async () => {
      // Plugin ships v1: columns (id, name, biz_date), cursor=biz_date.
      // After curated is populated, plugin ships v2 that adds a
      // new column ‘last_updated_at’ and switches cursor to it.
      //
      // The existing curated parquets don't have last_updated_at.
      // union_by_name on read_parquet unifies columns that exist in
      // AT LEAST ONE file — if every curated file is pre-v2, the
      // unified schema still lacks last_updated_at and SELECT-ing
      // it errors with 'Referenced column "last_updated_at" not
      // found in FROM clause'.
      //
      // Fix under test: compact() probes curated's columns before
      // building the narrow SELECT; when the cursor is missing from
      // curated it substitutes `NULL AS <cursor>` on the curated
      // side of the UNION. Raw carries the real value; curated
      // contributes NULL and loses every window comparison to raw
      // — exactly the desired semantics.

      const plugin: PluginDef = {
        name: "schema_evo_test",
        version: "1.0.0",
        tables: [
          {
            name: "orders_detail",
            columns: [
              { name: "id", type: "string" },
              { name: "biz_date", type: "string" },
              { name: "last_updated_at", type: "datetime" },
            ],
            keyColumns: [{ name: "org", required: "required" }],
            partitionBy: ["org"],
            primaryKey: ["id", "org"],
            cursor: "last_updated_at",
            async *list() {
              /* raw written directly */
            },
          },
        ],
      };
      registry.register(plugin);

      const prefix = freshPrefix("schema-evo");
      const dir = mkdtempSync(join(tmpdir(), "dripline-compact-evo-"));
      mkdirSync(join(dir, ".dripline"), { recursive: true });
      writeFileSync(
        join(dir, ".dripline", "config.json"),
        JSON.stringify({
          connections: [
            { name: "default", plugin: "schema_evo_test", config: {} },
          ],
          cache: { enabled: true, ttl: 300, maxSize: 1000 },
          rateLimits: {},
          lanes: {},
          remote: {
            endpoint: ENDPOINT,
            bucket: BUCKET,
            prefix,
            accessKeyId: KEY,
            secretAccessKey: SECRET,
            secretType: "S3",
          },
        }),
      );
      process.chdir(dir);

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });

      // Cycle 1: write PRE-migration curated directly. We bypass
      // compact() here because v1 of the plugin didn't emit
      // last_updated_at at all. Simulating an already-populated
      // warehouse from before the schema change.
      const { Database: DB } = await import("../core/db.js");
      const seedDb = await DB.create(":memory:");
      try {
        await remote.attach(seedDb);
        await seedDb.exec(`
          CREATE TEMP TABLE _seed (id VARCHAR, biz_date VARCHAR, org VARCHAR);
          INSERT INTO _seed VALUES
            ('a', '2024-01-01', 'x'),
            ('b', '2024-01-02', 'x'),
            ('c', '2024-01-01', 'y');
        `);
        // Hive-laid curated files — same shape compact() would have
        // written before the schema change.
        await seedDb.exec(`
          COPY (SELECT id, biz_date FROM _seed WHERE org = 'x')
          TO '${remote.s3("curated/orders_detail/org=x/data_0.parquet")}'
          (FORMAT PARQUET);
        `);
        await seedDb.exec(`
          COPY (SELECT id, biz_date FROM _seed WHERE org = 'y')
          TO '${remote.s3("curated/orders_detail/org=y/data_0.parquet")}'
          (FORMAT PARQUET);
        `);
      } finally {
        await seedDb.close();
      }

      // Cycle 2: post-migration raw carries the new cursor column.
      await writeRawParquet(
        remote,
        "orders_detail",
        [
          {
            id: "a",
            biz_date: "2024-01-01",
            last_updated_at: "2024-06-01T00:00:00Z",
            org: "x",
          },
          {
            id: "d",
            biz_date: "2024-06-01",
            last_updated_at: "2024-06-01T00:00:00Z",
            org: "x",
          },
        ],
        "evo-1",
      );

      const results = await compact({
        tables: ["orders_detail"],
        quiet: true,
      });

      // Must not error with 'column not found'.
      assert.equal(
        results[0].status,
        "ok",
        `expected ok, got ${results[0].status}: ${results[0].error ?? ""}`,
      );

      // Row counts: pre-existing b (x), c (y), plus raw overwrote a
      // and added d — so 4 total. Raw always beats NULL cursor on
      // window compare, so the curated version of `a` (pre-migration,
      // NULL last_updated_at) is dropped in favor of raw's v2 row.
      assert.equal(results[0].rows, 4);

      // Verify the raw-side row for `a` replaced the pre-migration
      // one, and last_updated_at now carries the new ISO timestamp.
      const verifyDb = await DB.create(":memory:");
      try {
        await remote.attach(verifyDb);
        const url = `s3://${BUCKET}/${prefix}/curated/orders_detail/**/*.parquet`;
        const a = await verifyDb.all(
          `SELECT id, last_updated_at FROM read_parquet('${url}', union_by_name => true, hive_partitioning => true)
             WHERE id = 'a'`,
        );
        assert.equal(a.length, 1);
        assert.equal(
          (a[0] as { last_updated_at: string | null }).last_updated_at,
          "2024-06-01T00:00:00Z",
          "raw (v2) should win over pre-migration curated",
        );

        // Pre-migration row with no raw counterpart (b, c) survives
        // with NULL last_updated_at — consistent with schema
        // evolution (old rows get a NULL in the new column).
        const b = await verifyDb.all(
          `SELECT last_updated_at FROM read_parquet('${url}', union_by_name => true, hive_partitioning => true)
             WHERE id = 'b'`,
        );
        assert.equal(b.length, 1);
        assert.equal(
          (b[0] as { last_updated_at: string | null }).last_updated_at,
          null,
          "pre-migration untouched row stays at NULL cursor",
        );
      } finally {
        await verifyDb.close();
      }
    },
  );

  ift(
    "regression: cursor-race healing — overlap window re-ingests skipped days",
    async () => {
      // Scenario replayed from a live incident. A table using
      // cursor=business_date was syncing a restaurant that closes
      // orders throughout the day. An early-morning run on day N
      // happened to pull a handful of already-closed orders dated N,
      // which advanced the cursor to N. Subsequent runs queried from
      // N forward, grabbed a few closes that had rolled into day N+1
      // by then, advanced cursor to N+1 — skipping the bulk of day N
      // that only closed later in the evening. Data loss silent.
      //
      // The plugin-level fix is to widen the API window by 7 days on
      // cursor-driven runs so late closes always fall in. The engine
      // cursor filter (v > cursorValue) still dedups row-by-row.
      //
      // This test simulates the fix from compact()'s perspective:
      // a re-ingest that emits rows from earlier dates than the
      // current cursor must dedupe cleanly against existing curated
      // and NOT produce duplicates for already-synced rows.

      const prefix = freshPrefix("cursor-race");
      process.chdir(makeProject(prefix));

      const remote = new Remote({
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      });

      // Day N: a partial pull (early closes) — the bug scenario.
      setPluginRows([
        { id: 1, name: "early-1", updated_at: "2024-01-01T03:00:00Z", org: "x" },
        { id: 2, name: "early-2", updated_at: "2024-01-01T04:00:00Z", org: "x" },
      ]);
      await run({ quiet: true });
      const r1 = await compact({ quiet: true });
      assert.equal(r1[0].status, "ok");
      assert.equal(r1[0].rows, 2);

      // Simulate the widened re-pull: raw now carries BOTH the
      // already-synced rows (unchanged cursor) AND the late closes
      // that were previously stranded.
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });
      await aws.fetch(
        `${ENDPOINT}/${BUCKET}/${prefix}/_leases/lane-main.json`,
        { method: "DELETE" },
      );
      const stateKeys = await remote.listObjects("_state/main/");
      if (stateKeys.length > 0) await remote.deleteObjects(stateKeys);

      // Later pull returns id=1 and id=2 again (unchanged) plus the
      // late closes id=3,4,5 with newer cursor values. The engine's
      // cursor filter will skip id=1,2 because their updated_at is
      // <= cursorValue from cycle 1; only id=3,4,5 reach raw.
      setPluginRows([
        { id: 1, name: "early-1", updated_at: "2024-01-01T03:00:00Z", org: "x" },
        { id: 2, name: "early-2", updated_at: "2024-01-01T04:00:00Z", org: "x" },
        { id: 3, name: "late-1", updated_at: "2024-01-01T21:00:00Z", org: "x" },
        { id: 4, name: "late-2", updated_at: "2024-01-01T22:00:00Z", org: "x" },
        { id: 5, name: "late-3", updated_at: "2024-01-01T23:00:00Z", org: "x" },
      ]);
      await run({ quiet: true });
      const r2 = await compact({ quiet: true });
      assert.equal(r2[0].status, "ok");
      assert.equal(
        r2[0].rows,
        5,
        "all 5 orders should land after re-ingest, no dupes",
      );

      // Sanity: the original early rows were not duplicated.
      const { Database: DB } = await import("../core/db.js");
      const db = await DB.create(":memory:");
      try {
        await remote.attach(db);
        const url = `s3://${BUCKET}/${prefix}/curated/items/**/*.parquet`;
        const dupes = await db.all(
          `SELECT id, COUNT(*) AS n FROM read_parquet('${url}', hive_partitioning => true)
             GROUP BY id HAVING COUNT(*) > 1`,
        );
        assert.equal(
          dupes.length,
          0,
          `no id should appear more than once; found: ${JSON.stringify(dupes)}`,
        );
      } finally {
        await db.close();
      }
    },
  );

  ift("missing remote rejects with a clear error", async () => {
    const dir = mkdtempSync(join(tmpdir(), "dripline-compact-"));
    mkdirSync(join(dir, ".dripline"), { recursive: true });
    writeFileSync(
      join(dir, ".dripline", "config.json"),
      JSON.stringify({
        connections: [],
        cache: { enabled: true, ttl: 300, maxSize: 1000 },
        rateLimits: {},
        lanes: {},
      }),
    );
    process.chdir(dir);

    await assert.rejects(
      () => compact({ quiet: true }),
      /no remote configured/,
    );
  });
});
