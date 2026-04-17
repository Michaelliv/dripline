/**
 * Integration tests for Remote against MinIO.
 *
 * Proves the full publish → hydrate roundtrip with a real DuckDB engine
 * and a real plugin. Auto-skips when the backend is unreachable.
 *
 * Pre-req: same MinIO container as the LeaseStore tests.
 */

import { strict as assert } from "node:assert";
import { after, before, describe, it } from "node:test";
import { AwsClient } from "aws4fetch";
import { Database } from "../core/db.js";
import type { RemoteConfig } from "../config/types.js";
import { Remote } from "../core/remote.js";
import type { PluginDef } from "../plugin/types.js";
import { Dripline } from "../sdk.js";

const ENDPOINT =
  process.env.DRIPLINE_TEST_S3_ENDPOINT ?? "http://localhost:9100";
const BUCKET = process.env.DRIPLINE_TEST_S3_BUCKET ?? "dripline-test";
const KEY = process.env.DRIPLINE_TEST_S3_KEY ?? "testkey";
const SECRET = process.env.DRIPLINE_TEST_S3_SECRET ?? "testsecret123";

const RUN_PREFIX = `remote-tests/${process.pid}-${Date.now()}`;

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

function newRemote(prefix: string): Remote {
  const cfg: RemoteConfig = {
    endpoint: ENDPOINT,
    bucket: BUCKET,
    prefix,
    accessKeyId: KEY,
    secretAccessKey: SECRET,
    secretType: "S3",
  };
  return new Remote(cfg);
}

/**
 * A tiny in-memory plugin that yields three rows on each call. Identical
 * shape to the one in sync.test.ts so we know the engine semantics work
 * the same way through the remote layer.
 */
function makePlugin(): PluginDef {
  return {
    name: "remote_test",
    version: "1.0.0",
    tables: [
      {
        name: "items",
        description: "Test items table",
        columns: [
          { name: "id", type: "number" },
          { name: "name", type: "string" },
          { name: "updated_at", type: "datetime" },
        ],
        keyColumns: [{ name: "org", required: "required" }],
        primaryKey: ["id"],
        cursor: "updated_at",
        async *list() {
          yield {
            id: 1,
            name: "a",
            updated_at: "2024-01-01T00:00:00Z",
            org: "x",
          };
          yield {
            id: 2,
            name: "b",
            updated_at: "2024-02-01T00:00:00Z",
            org: "x",
          };
          yield {
            id: 3,
            name: "c",
            updated_at: "2024-03-01T00:00:00Z",
            org: "x",
          };
        },
      },
    ],
  };
}

let runCounter = 0;
const freshPrefix = (label: string) => `${RUN_PREFIX}/${label}-${++runCounter}`;

describe("Remote (S3-compatible)", { concurrency: false }, () => {
  before(async () => {
    backendUp = await probeBackend();
    if (!backendUp) {
      console.warn(`\n  ⚠ skipping Remote tests: ${ENDPOINT} unreachable.\n`);
    }
  });

  after(async () => {
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

  const ift = (name: string, fn: () => Promise<void>) =>
    it(name, async (t) => {
      if (!backendUp) return t.skip("backend unreachable");
      await fn();
    });

  ift("attach installs httpfs and creates an S3 secret", async () => {
    const remote = newRemote(freshPrefix("attach"));
    const db = await Database.create(":memory:");
    try {
      await remote.attach(db);
      // If attach worked, listing the bucket via DuckDB's httpfs should
      // succeed (even if empty). We use a tiny COPY-then-read roundtrip
      // to prove the secret is wired correctly.
      const url = `s3://${BUCKET}/${freshPrefix("attach-probe")}/probe.csv`;
      await db.exec(
        `COPY (SELECT 1 AS x) TO '${url}' (FORMAT CSV, HEADER true);`,
      );
      const rows = await db.all(`SELECT * FROM read_csv_auto('${url}');`);
      assert.equal(rows.length, 1);
      assert.equal(Number((rows[0] as { x: number }).x), 1);
    } finally {
      await db.close();
    }
  });

  ift("publishRun + hydrateCursors roundtrip across two workers", async () => {
    const prefix = freshPrefix("roundtrip");
    const remote = newRemote(prefix);
    const lane = "main";

    // ── Worker 1: first run, no prior state. Sync 3 rows, publish, push.
    {
      const db = await Database.create(":memory:");
      const dl = await Dripline.create({
        plugins: [makePlugin()],
        connections: [{ name: "default", plugin: "remote_test", config: {} }],
        database: db,
        schema: "lane_main", // matches laneSchema("main")
      });

      await remote.hydrateCursors(db, lane);
      const result = await dl.sync({ items: { org: "x" } });
      assert.equal(result.errors.length, 0);
      assert.equal(result.tables[0].rowsInserted, 3);

      const published = await remote.publishRun(db, lane, ["items"]);
      assert.equal(published.length, 1);
      assert.equal(published[0].rows, 3);

      await remote.pushCursors(db, lane);
      await dl.close();
    }

    // ── Worker 2: fresh process, hydrates state, runs again. Cursor
    //              should advance past worker 1's high-water mark, so
    //              the plugin yields all rows but the cursor filter
    //              skips them — rowsInserted should be 0.
    {
      const db = await Database.create(":memory:");
      const dl = await Dripline.create({
        plugins: [makePlugin()],
        connections: [{ name: "default", plugin: "remote_test", config: {} }],
        database: db,
        schema: "lane_main",
      });

      await remote.hydrateCursors(db, lane);

      // Verify the cursor was hydrated from R2
      const meta = await db.all(`
        SELECT table_name, last_cursor FROM "lane_main"."_dripline_sync"
        WHERE table_name = 'items';
      `);
      assert.equal(meta.length, 1);
      const cursor = JSON.parse(
        (meta[0] as { last_cursor: string }).last_cursor,
      );
      assert.equal(cursor, "2024-03-01T00:00:00Z");

      const result = await dl.sync({ items: { org: "x" } });
      assert.equal(result.errors.length, 0);
      assert.equal(
        result.tables[0].rowsInserted,
        0,
        "second run should insert 0 rows because cursor is at high-water mark",
      );

      await dl.close();
    }
  });

  ift("publishRun skips empty tables", async () => {
    const prefix = freshPrefix("empty");
    const remote = newRemote(prefix);
    const lane = "main";

    const db = await Database.create(":memory:");
    const dl = await Dripline.create({
      plugins: [makePlugin()],
      connections: [{ name: "default", plugin: "remote_test", config: {} }],
      database: db,
      schema: "lane_main",
    });

    // Don't sync — local table is empty.
    await db.exec(`CREATE SCHEMA IF NOT EXISTS "lane_main";`);
    await db.exec(`
      CREATE TABLE IF NOT EXISTS "lane_main"."items" (
        id DOUBLE, name VARCHAR, updated_at VARCHAR, org VARCHAR
      );
    `);

    const published = await remote.publishRun(db, lane, ["items"]);
    assert.equal(published.length, 0, "no files written for empty table");

    await dl.close();
  });

  ift("readManifest returns null when missing", async () => {
    const remote = newRemote(freshPrefix("missing-manifest"));
    const m = await remote.readManifest("nope");
    assert.equal(m, null);
  });

  // Regression — refreshManifest used to fail with
  // "JSON.stringify cannot serialize BigInt" whenever the partition
  // column came back from DuckDB as BIGINT (e.g. integer ID columns
  // landed via the appender). The values are valid JSON numbers; the
  // bug was passing raw bigints through to JSON.stringify.
  ift("refreshManifest serializes BIGINT partition values", async () => {
    const prefix = freshPrefix("bigint-manifest");
    const remote = newRemote(prefix);
    const db = await Database.create(":memory:");
    try {
      await remote.attach(db);
      // Write one curated parquet directly with a BIGINT partition
      // column. We bypass compact() so the test isolates the
      // manifest-write path.
      const url = remote.s3(
        "curated/bg/section_id=42/data.parquet",
      );
      await db.exec(`
        COPY (SELECT 1::INTEGER AS id, 42::BIGINT AS section_id, 'x' AS name)
        TO '${url}' (FORMAT PARQUET);
      `);
      // refreshManifest reads MIN/MAX of the partition column —
      // those come back as BigInt and (used to) blow up JSON.stringify.
      const files = await remote.refreshManifest(db, "bg", ["section_id"]);
      assert.equal(files, 1);
      const manifest = await remote.readManifest("bg");
      assert.ok(manifest, "manifest should be readable after write");
      assert.equal(manifest.files.length, 1);
      // The min/max values must round-trip as JSON-safe numbers, not be
      // dropped or coerced to a string of garbage.
      const f = manifest.files[0] as Record<string, unknown>;
      assert.equal(Number(f.min_section_id), 42);
      assert.equal(Number(f.max_section_id), 42);
    } finally {
      await db.close();
    }
  });
});
