import { strict as assert } from "node:assert";
import { afterEach, describe, it } from "node:test";
import { Database } from "../core/db.js";
import type { PluginDef, QueryContext } from "../plugin/types.js";
import { Dripline } from "../sdk.js";

// ── Helpers ──────────────────────────────────────────────────────────

function makePlugin(opts?: {
  cursor?: string;
}): { plugin: PluginDef; calls: () => QueryContext[] } {
  const captured: QueryContext[] = [];
  const plugin: PluginDef = {
    name: "counter",
    version: "1.0.0",
    tables: [
      {
        name: "items",
        columns: [
          { name: "id", type: "number" },
          { name: "name", type: "string" },
          { name: "updated_at", type: "datetime" },
        ],
        keyColumns: [{ name: "org", required: "required" }],
        cursor: opts?.cursor,
        *list(ctx) {
          captured.push(ctx);
          const since = ctx.cursor?.value;
          const all = [
            { id: 1, name: "a", updated_at: "2024-01-01T00:00:00Z", org: "x" },
            { id: 2, name: "b", updated_at: "2024-02-01T00:00:00Z", org: "x" },
            { id: 3, name: "c", updated_at: "2024-03-01T00:00:00Z", org: "x" },
          ];
          for (const row of all) {
            if (since && row.updated_at <= since) continue;
            yield row;
          }
        },
      },
    ],
  };
  return { plugin, calls: () => captured };
}

let dl: Dripline;
let db: Database;

async function setup(plugin: PluginDef, schema = "s") {
  db = await Database.create(":memory:");
  dl = await Dripline.create({ plugins: [plugin], database: db, schema });
}

async function cleanup() {
  if (dl) { try { await dl.close(); } catch {} dl = null as any; }
  if (db) { try { await db.close(); } catch {} db = null as any; }
}

// ── Tests ────────────────────────────────────────────────────────────

describe("sync() — full replace (no cursor)", () => {
  afterEach(cleanup);

  it("inserts all rows", async () => {
    const { plugin } = makePlugin();
    await setup(plugin);

    const result = await dl.sync({ items: { org: "x" } });
    assert.equal(result.tables.length, 1);
    assert.equal(result.tables[0].rowsInserted, 3);
    assert.equal(result.tables[0].rowsTotal, 3);
    assert.equal(result.errors.length, 0);

    const rows = await dl.query('SELECT * FROM "s"."items" ORDER BY id');
    assert.equal(rows.length, 3);
  });

  it("replaces all rows on second sync", async () => {
    const { plugin } = makePlugin();
    await setup(plugin);

    await dl.sync({ items: { org: "x" } });
    await dl.sync({ items: { org: "x" } });

    const rows = await dl.query('SELECT * FROM "s"."items"');
    assert.equal(rows.length, 3); // not 6
  });

  it("preserves sibling partitions when syncing same table with different params", async () => {
    // Regression: a cursor-less table synced across multiple param sets
    // (e.g. section_id=a, then section_id=b) used to wipe all rows before
    // each insert, so only the last param's data survived. The DELETE
    // must be scoped to the current quals.
    const plugin: PluginDef = {
      name: "multi_section",
      version: "1.0.0",
      tables: [
        {
          name: "employees",
          columns: [
            { name: "id", type: "number" },
            { name: "section_id", type: "string" },
            { name: "name", type: "string" },
          ],
          keyColumns: [{ name: "section_id", required: "required" }],
          primaryKey: ["id", "section_id"],
          *list(ctx) {
            const section = ctx.quals.find((q) => q.column === "section_id")
              ?.value as string;
            if (section === "a") {
              yield { id: 1, section_id: "a", name: "Alice" };
              yield { id: 2, section_id: "a", name: "Bob" };
            } else if (section === "b") {
              yield { id: 3, section_id: "b", name: "Charlie" };
            }
          },
        },
      ],
    };

    await setup(plugin);
    await dl.sync({ employees: { section_id: "a" } });
    await dl.sync({ employees: { section_id: "b" } });

    const rows = await dl.query<{ id: number; section_id: string }>(
      'SELECT * FROM "s"."employees" ORDER BY id',
    );
    assert.equal(rows.length, 3);
    assert.equal(rows[0].section_id, "a");
    assert.equal(rows[1].section_id, "a");
    assert.equal(rows[2].section_id, "b");

    // Re-syncing one section replaces only that section's rows
    await dl.sync({ employees: { section_id: "a" } });
    const after = await dl.query<{ section_id: string }>(
      'SELECT section_id FROM "s"."employees" ORDER BY id',
    );
    assert.equal(after.length, 3);
    assert.equal(after[2].section_id, "b");
  });
});

describe("sync() — incremental with cursor", () => {
  afterEach(cleanup);

  it("first sync gets all rows, second gets none (cursor filters)", async () => {
    const { plugin, calls } = makePlugin({ cursor: "updated_at" });
    await setup(plugin);

    const r1 = await dl.sync({ items: { org: "x" } });
    assert.equal(r1.tables[0].rowsTotal, 3);
    assert.equal(calls()[0].cursor, null);

    const r2 = await dl.sync({ items: { org: "x" } });
    assert.ok(calls()[1].cursor);
    assert.equal(calls()[1].cursor!.column, "updated_at");
    assert.equal(calls()[1].cursor!.value, "2024-03-01T00:00:00Z");
    assert.equal(r2.tables[0].rowsInserted, 0);
    assert.equal(r2.tables[0].rowsTotal, 3);
  });

  it("appends new rows on incremental sync", async () => {
    let callCount = 0;
    const plugin: PluginDef = {
      name: "append_test",
      version: "1.0.0",
      tables: [
        {
          name: "events",
          columns: [
            { name: "id", type: "number" },
            { name: "ts", type: "datetime" },
          ],
          cursor: "ts",
          *list() {
            callCount++;
            if (callCount === 1) {
              yield { id: 1, ts: "2024-01-01" };
              yield { id: 2, ts: "2024-02-01" };
            } else {
              yield { id: 3, ts: "2024-03-01" };
            }
          },
        },
      ],
    };

    await setup(plugin);
    await dl.sync();

    const r2 = await dl.sync();
    assert.equal(r2.tables[0].rowsInserted, 1);
    assert.equal(r2.tables[0].rowsTotal, 3);

    const rows = await dl.query<{ id: number }>('SELECT * FROM "s"."events" ORDER BY id');
    assert.equal(rows.length, 3);
    assert.equal(rows[2].id, 3);
  });

  it("engine filters even when plugin yields everything (dumb plugin)", async () => {
    const plugin: PluginDef = {
      name: "dumb",
      version: "1.0.0",
      tables: [
        {
          name: "logs",
          columns: [
            { name: "id", type: "number" },
            { name: "ts", type: "datetime" },
          ],
          cursor: "ts",
          *list() {
            yield { id: 1, ts: "2024-01-01" };
            yield { id: 2, ts: "2024-02-01" };
            yield { id: 3, ts: "2024-03-01" };
          },
        },
      ],
    };

    await setup(plugin);
    await dl.sync();

    const r2 = await dl.sync();
    assert.equal(r2.tables[0].rowsInserted, 0);
    assert.equal(r2.tables[0].rowsTotal, 3);
  });
});

describe("sync() — full replace + dedup (PK, no cursor)", () => {
  afterEach(cleanup);

  it("deduplicates by primary key on full replace", async () => {
    const plugin: PluginDef = {
      name: "dedup_full",
      version: "1.0.0",
      tables: [
        {
          name: "things",
          columns: [
            { name: "id", type: "number" },
            { name: "val", type: "string" },
          ],
          primaryKey: ["id"],
          *list() {
            // Source returns dupes
            yield { id: 1, val: "a" };
            yield { id: 1, val: "b" }; // dupe — should overwrite
            yield { id: 2, val: "c" };
          },
        },
      ],
    };

    await setup(plugin);
    await dl.sync();

    const rows = await dl.query<{ id: number; val: string }>(
      'SELECT * FROM "s"."things" ORDER BY id',
    );
    assert.equal(rows.length, 2); // deduped — no duplicate id=1 rows
  });
});

describe("sync() — incremental append + dedup (cursor + PK)", () => {
  afterEach(cleanup);

  it("upserts updated rows on incremental sync", async () => {
    let callCount = 0;
    const plugin: PluginDef = {
      name: "upsert_test",
      version: "1.0.0",
      tables: [
        {
          name: "things",
          columns: [
            { name: "id", type: "number" },
            { name: "val", type: "string" },
            { name: "updated_at", type: "datetime" },
          ],
          primaryKey: ["id"],
          cursor: "updated_at",
          *list(ctx) {
            callCount++;
            if (callCount === 1) {
              yield { id: 1, val: "original", updated_at: "2024-01-01" };
              yield { id: 2, val: "original", updated_at: "2024-01-01" };
            } else {
              // id=1 updated, id=3 new
              yield { id: 1, val: "updated", updated_at: "2024-02-01" };
              yield { id: 3, val: "new", updated_at: "2024-02-01" };
            }
          },
        },
      ],
    };

    await setup(plugin);
    await dl.sync();
    await dl.sync();

    const rows = await dl.query<{ id: number; val: string }>(
      'SELECT * FROM "s"."things" ORDER BY id',
    );
    assert.equal(rows.length, 3);
    assert.equal(rows[0].val, "updated"); // id=1 upserted
    assert.equal(rows[1].val, "original"); // id=2 unchanged
    assert.equal(rows[2].val, "new"); // id=3 new
  });

  it("deduplicates even when dumb plugin yields everything", async () => {
    const plugin: PluginDef = {
      name: "dumb_dedup",
      version: "1.0.0",
      tables: [
        {
          name: "items",
          columns: [
            { name: "id", type: "number" },
            { name: "val", type: "string" },
            { name: "ts", type: "datetime" },
          ],
          primaryKey: ["id"],
          cursor: "ts",
          *list() {
            // Always yields everything — doesn't use cursor
            yield { id: 1, val: "latest", ts: "2024-01-01" };
            yield { id: 2, val: "latest", ts: "2024-02-01" };
          },
        },
      ],
    };

    await setup(plugin);
    await dl.sync();
    await dl.sync(); // yields same rows again

    const rows = await dl.query('SELECT * FROM "s"."items"');
    assert.equal(rows.length, 2); // no dupes thanks to PK dedup
  });
});

describe("sync() — external database", () => {
  afterEach(cleanup);

  it("creates tables in correct schema", async () => {
    const { plugin } = makePlugin();
    await setup(plugin, "test_schema");

    await dl.sync({ items: { org: "x" } });

    const rows = await db.all('SELECT * FROM "test_schema"."items" ORDER BY id');
    assert.equal(rows.length, 3);
  });

  it("schema isolation — two instances don't interfere", async () => {
    db = await Database.create(":memory:");
    const { plugin: p1 } = makePlugin();
    const { plugin: p2 } = makePlugin();

    const dl1 = await Dripline.create({ plugins: [p1], database: db, schema: "ws_1" });
    const dl2 = await Dripline.create({ plugins: [p2], database: db, schema: "ws_2" });

    await dl1.sync({ items: { org: "x" } });
    await dl2.sync({ items: { org: "x" } });

    const rows1 = await db.all('SELECT COUNT(*) as cnt FROM "ws_1"."items"');
    const rows2 = await db.all('SELECT COUNT(*) as cnt FROM "ws_2"."items"');
    assert.equal(Number(rows1[0].cnt), 3);
    assert.equal(Number(rows2[0].cnt), 3);

    await dl1.close();
    await dl2.close();
    dl = null as any;
  });

  it("close() does NOT close the shared database", async () => {
    db = await Database.create(":memory:");
    const { plugin } = makePlugin();

    dl = await Dripline.create({ plugins: [plugin], database: db, schema: "s1" });
    await dl.sync({ items: { org: "x" } });
    await dl.close();
    dl = null as any;

    const rows = await db.all('SELECT * FROM "s1"."items"');
    assert.equal(rows.length, 3);
  });

  it("requires schema when database is provided", async () => {
    db = await Database.create(":memory:");
    const { plugin } = makePlugin();

    await assert.rejects(
      () => Dripline.create({ plugins: [plugin], database: db }),
      /schema is required/,
    );
  });
});

describe("sync() — error handling", () => {
  afterEach(cleanup);

  it("captures error, other tables still sync", async () => {
    const plugin: PluginDef = {
      name: "mixed",
      version: "1.0.0",
      tables: [
        {
          name: "good_table",
          columns: [{ name: "id", type: "number" }],
          *list() { yield { id: 1 }; },
        },
        {
          name: "bad_table",
          columns: [{ name: "id", type: "number" }],
          *list() { throw new Error("api down"); },
        },
      ],
    };

    await setup(plugin);
    const result = await dl.sync();

    assert.equal(result.tables.length, 1);
    assert.equal(result.tables[0].table, "good_table");
    assert.equal(result.errors.length, 1);
    assert.equal(result.errors[0].table, "bad_table");
    assert.ok(result.errors[0].error.includes("api down"));
  });

  it("captures missing required keyColumn params as error", async () => {
    const { plugin } = makePlugin();
    await setup(plugin);

    const result = await dl.sync({ items: {} });
    assert.equal(result.errors.length, 1);
    assert.ok(result.errors[0].error.includes('requires "org"'));
  });

  it("throws on unknown table name", async () => {
    const { plugin } = makePlugin();
    await setup(plugin);

    await assert.rejects(() => dl.sync({ nope: {} }), /Unknown table "nope"/);
  });

  it("throws when called without external database", async () => {
    const { plugin } = makePlugin();
    dl = await Dripline.create({ plugins: [plugin] });

    await assert.rejects(() => dl.sync(), /requires an external database/);
  });
});

describe("sync() — cursor scoped to params", () => {
  afterEach(cleanup);

  it("different params get independent cursors", async () => {
    let calls: Array<{ org: string; cursor: any }> = [];
    const plugin: PluginDef = {
      name: "scoped",
      version: "1.0.0",
      tables: [
        {
          name: "items",
          columns: [
            { name: "id", type: "number" },
            { name: "ts", type: "datetime" },
          ],
          keyColumns: [{ name: "org", required: "required" }],
          cursor: "ts",
          *list(ctx) {
            const org = ctx.quals.find((q) => q.column === "org")?.value;
            calls.push({ org, cursor: ctx.cursor });
            if (org === "a") {
              yield { id: 1, ts: "2024-01-01", org: "a" };
            } else {
              yield { id: 2, ts: "2024-06-01", org: "b" };
            }
          },
        },
      ],
    };

    await setup(plugin);

    // Sync org=a, then org=b
    await dl.sync({ items: { org: "a" } });
    await dl.sync({ items: { org: "b" } });

    // Sync both again — each should have its own cursor
    calls = [];
    await dl.sync({ items: { org: "a" } });
    await dl.sync({ items: { org: "b" } });

    // org=a cursor should be 2024-01-01, org=b should be 2024-06-01
    assert.equal(calls[0].cursor?.value, "2024-01-01");
    assert.equal(calls[1].cursor?.value, "2024-06-01");
  });
});

describe("sync() — plugin-level syncParams", () => {
  afterEach(cleanup);

  it("uses syncParams as defaults", async () => {
    let receivedQuals: any[] = [];
    const plugin: PluginDef = {
      name: "defaults",
      version: "1.0.0",
      tables: [
        {
          name: "items",
          columns: [
            { name: "id", type: "number" },
            { name: "name", type: "string" },
          ],
          keyColumns: [{ name: "org", required: "required" }],
          syncParams: { org: "default-org" },
          *list(ctx) {
            receivedQuals = ctx.quals;
            yield { id: 1, name: "a", org: "default-org" };
          },
        },
      ],
    };

    await setup(plugin);
    // No params passed — syncParams kicks in
    await dl.sync();

    assert.equal(receivedQuals.find((q) => q.column === "org")?.value, "default-org");
  });

  it("caller params override syncParams", async () => {
    let receivedQuals: any[] = [];
    const plugin: PluginDef = {
      name: "override",
      version: "1.0.0",
      tables: [
        {
          name: "items",
          columns: [
            { name: "id", type: "number" },
            { name: "name", type: "string" },
          ],
          keyColumns: [{ name: "org", required: "required" }],
          syncParams: { org: "default-org" },
          *list(ctx) {
            receivedQuals = ctx.quals;
            yield { id: 1, name: "a", org: "custom-org" };
          },
        },
      ],
    };

    await setup(plugin);
    await dl.sync({ items: { org: "custom-org" } });

    assert.equal(receivedQuals.find((q) => q.column === "org")?.value, "custom-org");
  });

  it("merges syncParams with caller params", async () => {
    let receivedQuals: any[] = [];
    const plugin: PluginDef = {
      name: "merge",
      version: "1.0.0",
      tables: [
        {
          name: "items",
          columns: [
            { name: "id", type: "number" },
            { name: "name", type: "string" },
          ],
          keyColumns: [
            { name: "org", required: "required" },
            { name: "team", required: "optional" },
          ],
          syncParams: { org: "default-org", team: "default-team" },
          *list(ctx) {
            receivedQuals = ctx.quals;
            yield { id: 1, name: "a", org: "default-org", team: "custom-team" };
          },
        },
      ],
    };

    await setup(plugin);
    // Override team, keep org from syncParams
    await dl.sync({ items: { team: "custom-team" } });

    assert.equal(receivedQuals.find((q) => q.column === "org")?.value, "default-org");
    assert.equal(receivedQuals.find((q) => q.column === "team")?.value, "custom-team");
  });
});

describe("sync() — metadata table", () => {
  afterEach(cleanup);

  it("creates and updates _dripline_sync rows", async () => {
    const { plugin } = makePlugin({ cursor: "updated_at" });
    await setup(plugin);

    await dl.sync({ items: { org: "x" } });

    const meta = await db.all('SELECT * FROM "s"."_dripline_sync"');
    assert.equal(meta.length, 1);
    assert.equal(meta[0].table_name, "items");
    assert.equal(meta[0].plugin, "counter");
    assert.equal(meta[0].status, "ok");
    assert.equal(JSON.parse(meta[0].last_cursor), "2024-03-01T00:00:00Z");
  });
});

describe("sync() — batched ingestion", () => {
  afterEach(cleanup);

  it("handles more rows than batch size", async () => {
    const ROW_COUNT = 25_000; // 2.5 batches at 10k batch size
    const plugin: PluginDef = {
      name: "big",
      version: "1.0.0",
      tables: [
        {
          name: "big_table",
          columns: [
            { name: "id", type: "number" },
            { name: "val", type: "string" },
          ],
          *list() {
            for (let i = 0; i < ROW_COUNT; i++) {
              yield { id: i, val: `row-${i}` };
            }
          },
        },
      ],
    };

    await setup(plugin);
    const result = await dl.sync();
    assert.equal(result.tables[0].rowsInserted, ROW_COUNT);
    assert.equal(result.tables[0].rowsTotal, ROW_COUNT);

    const rows = await dl.query<{ cnt: number }>(
      'SELECT COUNT(*) as cnt FROM "s"."big_table"',
    );
    assert.equal(rows[0].cnt, ROW_COUNT);
  });

  it("batched upsert with cursor + primaryKey", async () => {
    let callCount = 0;
    const plugin: PluginDef = {
      name: "batch_upsert",
      version: "1.0.0",
      tables: [
        {
          name: "batch_items",
          columns: [
            { name: "id", type: "number" },
            { name: "val", type: "string" },
            { name: "ts", type: "datetime" },
          ],
          primaryKey: ["id"],
          cursor: "ts",
          *list() {
            callCount++;
            if (callCount === 1) {
              for (let i = 0; i < 15_000; i++) {
                yield { id: i, val: "v1", ts: "2024-01-01" };
              }
            } else {
              // Update first 5k, add 5k new
              for (let i = 0; i < 5_000; i++) {
                yield { id: i, val: "v2", ts: "2024-02-01" };
              }
              for (let i = 15_000; i < 20_000; i++) {
                yield { id: i, val: "v2", ts: "2024-02-01" };
              }
            }
          },
        },
      ],
    };

    await setup(plugin);
    await dl.sync();
    await dl.sync();

    const rows = await dl.query<{ cnt: number }>(
      'SELECT COUNT(*) as cnt FROM "s"."batch_items"',
    );
    assert.equal(rows[0].cnt, 20_000); // 15k original + 5k new

    // Verify upserted rows have new value
    const updated = await dl.query<{ cnt: number }>(
      'SELECT COUNT(*) as cnt FROM "s"."batch_items" WHERE val = \'v2\'',
    );
    assert.equal(updated[0].cnt, 10_000); // 5k updated + 5k new
  });
});

describe("sync() — async generator plugin", () => {
  afterEach(cleanup);

  it("works with async *list()", async () => {
    const plugin: PluginDef = {
      name: "async_plugin",
      version: "1.0.0",
      tables: [
        {
          name: "async_items",
          columns: [
            { name: "id", type: "number" },
            { name: "name", type: "string" },
          ],
          async *list() {
            // Simulate async fetch
            await new Promise((r) => setTimeout(r, 5));
            yield { id: 1, name: "a" };
            await new Promise((r) => setTimeout(r, 5));
            yield { id: 2, name: "b" };
          },
        },
      ],
    };

    await setup(plugin);
    const result = await dl.sync();
    assert.equal(result.tables[0].rowsInserted, 2);

    const rows = await dl.query<{ id: number; name: string }>(
      'SELECT * FROM "s"."async_items" ORDER BY id',
    );
    assert.equal(rows.length, 2);
    assert.equal(rows[1].name, "b");
  });

  it("works with async *list() in ephemeral query mode", async () => {
    const plugin: PluginDef = {
      name: "async_query",
      version: "1.0.0",
      tables: [
        {
          name: "aq_items",
          columns: [
            { name: "id", type: "number" },
            { name: "val", type: "string" },
          ],
          async *list() {
            yield { id: 1, val: "x" };
            yield { id: 2, val: "y" };
          },
        },
      ],
    };

    dl = await Dripline.create({ plugins: [plugin] });
    const rows = await dl.query<{ id: number; val: string }>(
      "SELECT * FROM aq_items ORDER BY id",
    );
    assert.equal(rows.length, 2);
    assert.equal(rows[0].val, "x");
  });
});

describe("sync() — onProgress callback", () => {
  afterEach(cleanup);

  it("fires progress events every batch", async () => {
    const ROW_COUNT = 25_000; // 2.5 batches at 10k
    const plugin: PluginDef = {
      name: "progress_test",
      version: "1.0.0",
      tables: [
        {
          name: "big",
          columns: [
            { name: "id", type: "number" },
            { name: "val", type: "string" },
          ],
          *list() {
            for (let i = 0; i < ROW_COUNT; i++) {
              yield { id: i, val: `row-${i}` };
            }
          },
        },
      ],
    };

    await setup(plugin);
    const events: Array<{ table: string; rowsInserted: number; elapsedMs: number }> = [];
    await dl.sync(undefined, (ev) => events.push(ev));

    // 25k rows at 10k batch = 2 progress events (after batch 1 and batch 2)
    assert.ok(events.length >= 2, `expected >= 2 progress events, got ${events.length}`);
    assert.equal(events[0].table, "big");
    assert.equal(events[0].rowsInserted, 10_000);
    assert.equal(events[1].rowsInserted, 20_000);
    for (const ev of events) {
      assert.ok(ev.elapsedMs > 0);
    }
  });

  it("includes cursor value in progress events", async () => {
    const ROW_COUNT = 15_000;
    const plugin: PluginDef = {
      name: "progress_cursor",
      version: "1.0.0",
      tables: [
        {
          name: "events",
          columns: [
            { name: "id", type: "number" },
            { name: "ts", type: "datetime" },
          ],
          cursor: "ts",
          *list() {
            for (let i = 0; i < ROW_COUNT; i++) {
              yield { id: i, ts: `2024-01-${String(1 + Math.floor(i / 500)).padStart(2, "0")}` };
            }
          },
        },
      ],
    };

    await setup(plugin);
    const events: Array<{ cursor: unknown }> = [];
    await dl.sync(undefined, (ev) => events.push(ev));

    assert.ok(events.length >= 1);
    assert.ok(events[0].cursor != null, "cursor should be non-null");
  });

  it("does not fire for small datasets (< batch size)", async () => {
    const plugin: PluginDef = {
      name: "small",
      version: "1.0.0",
      tables: [
        {
          name: "tiny",
          columns: [{ name: "id", type: "number" }],
          *list() {
            for (let i = 0; i < 100; i++) yield { id: i };
          },
        },
      ],
    };

    await setup(plugin);
    const events: unknown[] = [];
    await dl.sync(undefined, (ev) => events.push(ev));

    assert.equal(events.length, 0, "no progress events for sub-batch datasets");
  });
});

describe("sync() — cursor format mismatch (ISO datetime vs plain date)", () => {
  afterEach(cleanup);

  it("plugin must normalize ISO datetime cursors before appending time suffixes", async () => {
    // Pattern test: initialCursor is "2025-01-01" (plain date), but rows
    // yield business_date as "2025-01-15T00:00:00.000Z" (ISO datetime).
    // On the second sync, ctx.cursor.value is the ISO string. Plugins that
    // naively do `${cursor}T00:00:00.000Z` produce garbage. This test
    // documents the correct pattern: normalize to YYYY-MM-DD first.
    let callCount = 0;
    const capturedCursors: any[] = [];
    const capturedFromValues: string[] = [];

    const plugin: PluginDef = {
      name: "cursor_fmt",
      version: "1.0.0",
      tables: [
        {
          name: "orders",
          columns: [
            { name: "id", type: "number" },
            { name: "business_date", type: "string" },
          ],
          primaryKey: ["id"],
          cursor: "business_date",
          initialCursor: "2025-01-01",
          *list(ctx) {
            callCount++;
            capturedCursors.push(ctx.cursor);

            // Simulate: plugin uses cursor to build a date range.
            // Fix: normalize to YYYY-MM-DD before appending time suffix.
            const raw = ctx.cursor?.value ?? "2025-01-01";
            const from = typeof raw === "string" && raw.includes("T") ? raw.slice(0, 10) : raw;
            const fromParam = `${from}T00:00:00.000Z`;
            capturedFromValues.push(fromParam);

            if (callCount === 1) {
              // First sync: yield rows with ISO datetime business_date
              yield { id: 1, business_date: "2025-01-15T00:00:00.000Z" };
              yield { id: 2, business_date: "2025-02-01T00:00:00.000Z" };
            } else {
              // Second sync: yield newer rows
              yield { id: 3, business_date: "2025-03-01T00:00:00.000Z" };
            }
          },
        },
      ],
    };

    await setup(plugin);

    // First sync — cursor starts as initialCursor "2025-01-01" (plain date)
    await dl.sync();
    assert.equal(capturedCursors[0]?.value, "2025-01-01");
    // Plugin appends T00:00:00.000Z — fine on first call, produces valid ISO
    assert.equal(capturedFromValues[0], "2025-01-01T00:00:00.000Z");

    // Second sync — cursor is now "2025-02-01T00:00:00.000Z" (ISO datetime from rows)
    await dl.sync();
    assert.equal(capturedCursors[1]?.value, "2025-02-01T00:00:00.000Z");

    // After fix: plugin normalizes cursor to YYYY-MM-DD before appending time suffix.
    // Without the fix this would be "2025-02-01T00:00:00.000ZT00:00:00.000Z"
    assert.ok(
      !capturedFromValues[1].includes("T00:00:00.000ZT00:00:00.000Z"),
      `Double-suffix bug! Got: ${capturedFromValues[1]}`,
    );
    assert.equal(capturedFromValues[1], "2025-02-01T00:00:00.000Z");
  });
});

describe("query() with external DB", () => {
  afterEach(cleanup);

  it("reads synced data without re-materializing", async () => {
    const { plugin } = makePlugin();
    await setup(plugin);

    await dl.sync({ items: { org: "x" } });
    const rows = await dl.query('SELECT * FROM "s"."items" ORDER BY id');
    assert.equal(rows.length, 3);
  });

  it("query before sync returns empty (no auto-materialize)", async () => {
    const { plugin } = makePlugin();
    await setup(plugin);

    const rows = await dl.query('SELECT * FROM "s"."items"');
    assert.equal(rows.length, 0);
  });
});
