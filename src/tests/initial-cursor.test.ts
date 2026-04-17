/**
 * Tests for `TableDef.initialCursor` — the plugin-declared backfill
 * starting point.
 *
 * Expected behavior:
 *   1. On the very first sync (no _dripline_sync row for this
 *      table+params), the engine uses `initialCursor` to seed
 *      `ctx.cursor.value`.
 *   2. Subsequent syncs ignore `initialCursor` and use the persisted
 *      high-water mark instead.
 *   3. `initialCursor` can be a static value OR a function of the
 *      sync params, enabling dynamic defaults like "last 30 days".
 *   4. `initialCursor` is a no-op when the table has no `cursor` field
 *      (snapshot-mode tables).
 */

import { strict as assert } from "node:assert";
import { afterEach, describe, it } from "node:test";
import { Database } from "../core/db.js";
import type { PluginDef, QueryContext } from "../plugin/types.js";
import { Dripline } from "../sdk.js";

interface Captured {
  cursor: QueryContext["cursor"];
  params: Record<string, unknown>;
}

function makePlugin(
  initialCursor: unknown | ((p: Record<string, any>) => unknown) | undefined,
  opts?: { cursor?: string },
): { plugin: PluginDef; calls: () => Captured[] } {
  const captured: Captured[] = [];
  const plugin: PluginDef = {
    name: "backfill_test",
    version: "1.0.0",
    tables: [
      {
        name: "items",
        columns: [
          { name: "id", type: "number" },
          { name: "updated_at", type: "datetime" },
        ],
        keyColumns: [{ name: "org", required: "required" }],
        cursor: opts?.cursor === undefined ? "updated_at" : opts.cursor,
        ...(initialCursor !== undefined ? { initialCursor } : {}),
        *list(ctx) {
          captured.push({
            cursor: ctx.cursor,
            params: {
              org: ctx.quals.find((q) => q.column === "org")?.value,
            },
          });
          // Emit rows strictly above the incoming cursor so the engine's
          // post-filter and our plugin agree on "new" rows.
          const since = ctx.cursor?.value as string | undefined;
          const all = [
            { id: 1, updated_at: "2024-01-15T00:00:00Z", org: "x" },
            { id: 2, updated_at: "2024-02-15T00:00:00Z", org: "x" },
            { id: 3, updated_at: "2024-03-15T00:00:00Z", org: "x" },
          ];
          for (const r of all) {
            if (since && r.updated_at <= since) continue;
            yield r;
          }
        },
      },
    ],
  };
  return { plugin, calls: () => captured };
}

let dl: Dripline | null = null;
let db: Database | null = null;

afterEach(async () => {
  if (dl) {
    try {
      await dl.close();
    } catch {}
    dl = null;
  }
  if (db) {
    try {
      await db.close();
    } catch {}
    db = null;
  }
});

async function setup(plugin: PluginDef) {
  db = await Database.create(":memory:");
  dl = await Dripline.create({ plugins: [plugin], database: db, schema: "s" });
}

describe("initialCursor (static value)", () => {
  it("seeds ctx.cursor on the very first sync", async () => {
    const { plugin, calls } = makePlugin("2024-02-01T00:00:00Z");
    await setup(plugin);

    const result = await dl!.sync({ items: { org: "x" } });
    assert.equal(result.errors.length, 0);

    // Plugin saw the seeded cursor
    assert.deepEqual(calls()[0].cursor, {
      column: "updated_at",
      value: "2024-02-01T00:00:00Z",
    });

    // Only rows strictly greater than the seed were inserted
    assert.equal(result.tables[0].rowsInserted, 2);
  });

  it("is ignored on subsequent syncs (real high-water mark wins)", async () => {
    const { plugin, calls } = makePlugin("2020-01-01T00:00:00Z");
    await setup(plugin);

    // First sync: seed fires, all 3 rows returned
    const r1 = await dl!.sync({ items: { org: "x" } });
    assert.equal(r1.tables[0].rowsInserted, 3);

    // Second sync: persisted cursor should be the max from r1
    // ("2024-03-15..."), NOT the seed value.
    const r2 = await dl!.sync({ items: { org: "x" } });
    assert.equal(r2.tables[0].rowsInserted, 0);

    const lastCall = calls().at(-1);
    assert.equal(lastCall?.cursor?.value, "2024-03-15T00:00:00Z");
    assert.notEqual(lastCall?.cursor?.value, "2020-01-01T00:00:00Z");
  });

  it("scopes the seed per params key", async () => {
    // Two orgs should each see the seed on their OWN first sync,
    // independent of the other's high-water mark.
    const { plugin, calls } = makePlugin("2024-02-01T00:00:00Z");
    await setup(plugin);

    await dl!.sync({ items: { org: "x" } });
    // Different params key → seed fires again for org=y
    await dl!.sync({ items: { org: "y" } });

    const orgXFirst = calls()[0];
    const orgYFirst = calls()[1];
    assert.equal(orgXFirst.cursor?.value, "2024-02-01T00:00:00Z");
    assert.equal(orgYFirst.cursor?.value, "2024-02-01T00:00:00Z");
  });
});

describe("initialCursor (function)", () => {
  it("invokes the function with the sync params", async () => {
    let received: Record<string, any> | null = null;
    const { plugin, calls } = makePlugin((params) => {
      received = params;
      return "2024-01-20T00:00:00Z";
    });
    await setup(plugin);

    await dl!.sync({ items: { org: "acme" } });

    assert.deepEqual(received, { org: "acme" });
    assert.equal(calls()[0].cursor?.value, "2024-01-20T00:00:00Z");
  });

  it("computes a fresh value per (table, params) pair", async () => {
    const seeds: string[] = [];
    const { plugin, calls } = makePlugin((params) => {
      const seed = `2024-01-${String(10 + seeds.length).padStart(2, "0")}T00:00:00Z`;
      seeds.push(seed);
      return seed;
    });
    await setup(plugin);

    await dl!.sync({ items: { org: "a" } });
    await dl!.sync({ items: { org: "b" } });

    assert.equal(calls()[0].cursor?.value, "2024-01-10T00:00:00Z");
    assert.equal(calls()[1].cursor?.value, "2024-01-11T00:00:00Z");
  });
});

describe("initialCursor (absent)", () => {
  it("first sync sees null cursor when initialCursor is omitted", async () => {
    const { plugin, calls } = makePlugin(undefined);
    await setup(plugin);

    await dl!.sync({ items: { org: "x" } });
    assert.equal(calls()[0].cursor, null);
  });
});

describe("initialCursor (snapshot-mode table)", () => {
  it("is a no-op when the plugin doesn't declare a cursor field", async () => {
    // cursor: "" disables cursor handling entirely in the engine path
    // guarded by `if (table.cursor)`.
    const { plugin, calls } = makePlugin("2024-02-01T00:00:00Z", {
      cursor: "",
    });
    await setup(plugin);

    await dl!.sync({ items: { org: "x" } });
    // Plugin saw NO cursor at all — initialCursor was ignored.
    assert.equal(calls()[0].cursor, undefined);
  });
});
