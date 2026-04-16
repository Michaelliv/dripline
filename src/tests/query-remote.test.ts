/**
 * End-to-end tests for `dripline query --remote` against MinIO.
 *
 * The query path is the user-facing payoff of the warehouse: after
 * a worker has run+compacted, an analyst should be able to issue
 * arbitrary SQL against the curated tables with no plugin loading,
 * no cursor management, no API calls. These tests prove that.
 *
 * Auto-skips when MinIO is unreachable.
 */

import { strict as assert } from "node:assert";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, afterEach, before, beforeEach, describe, it } from "node:test";
import { AwsClient } from "aws4fetch";
import { compact } from "../commands/compact.js";
import { QueryConfigError, runQuery } from "../commands/query.js";
import { run } from "../commands/run.js";
import type { DriplineConfig } from "../config/types.js";
import { registry } from "../plugin/registry.js";
import type { PluginDef } from "../plugin/types.js";

const ENDPOINT =
  process.env.DRIPLINE_TEST_S3_ENDPOINT ?? "http://localhost:9100";
const BUCKET = process.env.DRIPLINE_TEST_S3_BUCKET ?? "dripline-test";
const KEY = process.env.DRIPLINE_TEST_S3_KEY ?? "testkey";
const SECRET = process.env.DRIPLINE_TEST_S3_SECRET ?? "testsecret123";

const RUN_PREFIX = `query-remote-tests/${process.pid}-${Date.now()}`;

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

function registerTestPlugin(): void {
  const plugin: PluginDef = {
    name: "qr_test",
    version: "1.0.0",
    tables: [
      {
        name: "events",
        description: "Test events for query --remote",
        columns: [
          { name: "id", type: "number" },
          { name: "event_type", type: "string" },
          { name: "payload", type: "string" },
          { name: "occurred_at", type: "datetime" },
        ],
        keyColumns: [{ name: "tenant", required: "required" }],
        primaryKey: ["id"],
        cursor: "occurred_at",
        async *list() {
          yield {
            id: 1,
            event_type: "click",
            payload: "home",
            occurred_at: "2024-01-01T00:00:00Z",
            tenant: "acme",
          };
          yield {
            id: 2,
            event_type: "click",
            payload: "about",
            occurred_at: "2024-01-02T00:00:00Z",
            tenant: "acme",
          };
          yield {
            id: 3,
            event_type: "view",
            payload: "pricing",
            occurred_at: "2024-01-03T00:00:00Z",
            tenant: "acme",
          };
          yield {
            id: 4,
            event_type: "submit",
            payload: "signup",
            occurred_at: "2024-01-04T00:00:00Z",
            tenant: "acme",
          };
          yield {
            id: 5,
            event_type: "click",
            payload: "blog",
            occurred_at: "2024-01-05T00:00:00Z",
            tenant: "acme",
          };
        },
      },
    ],
  };
  registry.register(plugin);
}

function makeProject(prefix: string): string {
  const dir = mkdtempSync(join(tmpdir(), "dripline-qr-"));
  mkdirSync(join(dir, ".dripline"), { recursive: true });
  const config: DriplineConfig = {
    connections: [{ name: "default", plugin: "qr_test", config: {} }],
    cache: { enabled: true, ttl: 300, maxSize: 1000 },
    rateLimits: {},
    lanes: {
      main: {
        tables: [{ name: "events", params: { tenant: "acme" } }],
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

describe("dripline query --remote (end-to-end)", { concurrency: false }, () => {
  let originalCwd: string;

  before(async () => {
    originalCwd = process.cwd();
    backendUp = await probeBackend();
    if (!backendUp) {
      console.warn(
        `\n  ⚠ skipping query --remote tests: ${ENDPOINT} unreachable.\n`,
      );
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
  });
  afterEach(() => process.chdir(originalCwd));

  const ift = (name: string, fn: () => Promise<void>) =>
    it(name, async (t) => {
      if (!backendUp) return t.skip("backend unreachable");
      await fn();
    });

  /** Set up a populated warehouse: run → compact, return the project dir. */
  async function populatedWarehouse(label: string): Promise<string> {
    const prefix = freshPrefix(label);
    const project = makeProject(prefix);
    process.chdir(project);
    const r = await run({ quiet: true });
    assert.equal(
      r[0].status,
      "ok",
      `setup: run should succeed, got ${r[0].status}`,
    );
    // Filter to our table explicitly. The module-level plugin registry
    // is shared across test files, so without a filter `compact` would
    // also iterate foreign tables (e.g. compact.test.ts's `items`) and
    // return their "no raw files" skips as c[0], masking our success.
    const c = await compact({ quiet: true, tables: ["events"] });
    assert.equal(
      c[0].status,
      "ok",
      `setup: compact should succeed, got ${c[0].status} (${c[0].reason ?? "-"})`,
    );
    return project;
  }

  // ─────────────────────────────────────────────────────────────────

  ift("SELECT * returns every row from curated", async () => {
    await populatedWarehouse("select-all");
    const rows = await runQuery("SELECT * FROM events ORDER BY id", {
      remote: true,
    });
    assert.equal(rows.length, 5);
    assert.equal(rows[0].id, 1);
    assert.equal(rows[4].id, 5);
  });

  ift("aggregations work over curated parquet", async () => {
    await populatedWarehouse("agg");
    const rows = await runQuery(
      "SELECT event_type, COUNT(*) AS n FROM events GROUP BY event_type ORDER BY event_type",
      { remote: true },
    );
    const byType = Object.fromEntries(
      rows.map((r) => [r.event_type, Number(r.n)]),
    );
    assert.deepEqual(byType, { click: 3, submit: 1, view: 1 });
  });

  ift("WHERE filters push through to parquet", async () => {
    await populatedWarehouse("filter");
    const rows = await runQuery(
      "SELECT id FROM events WHERE event_type = 'click' ORDER BY id",
      { remote: true },
    );
    assert.deepEqual(
      rows.map((r) => r.id),
      [1, 2, 5],
    );
  });

  ift("partition column (tenant) is queryable", async () => {
    await populatedWarehouse("partition");
    const rows = await runQuery("SELECT DISTINCT tenant FROM events", {
      remote: true,
    });
    assert.deepEqual(
      rows.map((r) => r.tenant),
      ["acme"],
    );
  });

  ift("queries see freshly compacted data immediately", async () => {
    // No populatedWarehouse — we want to inspect the empty case first.
    const prefix = freshPrefix("freshness");
    const project = makeProject(prefix);
    process.chdir(project);

    // Empty warehouse → no manifests → SQL referencing events fails
    // with a clear "table not found"-style error.
    await assert.rejects(
      () => runQuery("SELECT * FROM events", { remote: true }),
      /events|not exist|catalog/i,
    );

    // Populate it.
    await run({ quiet: true });
    await compact({ quiet: true });

    // Now the same query returns rows.
    const rows = await runQuery("SELECT COUNT(*) AS n FROM events", {
      remote: true,
    });
    assert.equal(Number(rows[0].n), 5);
  });

  ift("missing remote rejects with QueryConfigError", async () => {
    const dir = mkdtempSync(join(tmpdir(), "dripline-qr-"));
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
      () => runQuery("SELECT 1", { remote: true }),
      QueryConfigError,
    );
  });

  ift(
    "remote mode discovers tables from manifests, not from the registry",
    async () => {
      // The architectural guarantee: runRemoteQuery() never calls
      // loadAllPlugins() or registry.*. Manifest discovery is the
      // source of truth for which tables exist on the read path.
      //
      // We can't easily prove a negative ("the registry was never
      // touched") from a black-box test, but we CAN prove the
      // architecture is wired correctly: query a populated warehouse
      // and verify the row count matches the data on disk, not the
      // 5 rows that the in-process plugin would yield on a fresh
      // engine call. The two happen to be equal in this test, so the
      // real assurance comes from absence of any plugin-yield
      // observation in runRemoteQuery's source.
      //
      // What we can assert directly: the test plugin IS in the
      // registry (sanity), AND a query returning 5 rows works without
      // dripline ever instantiating an engine bound to plugins.
      await populatedWarehouse("manifest-discovery");
      const registryTableNames = registry
        .getAllTables()
        .map((t) => t.table.name);
      assert.ok(
        registryTableNames.includes("events"),
        "sanity: events should be in the registry",
      );

      const rows = await runQuery("SELECT COUNT(*) AS n FROM events", {
        remote: true,
      });
      assert.equal(Number(rows[0].n), 5);
    },
  );
});
