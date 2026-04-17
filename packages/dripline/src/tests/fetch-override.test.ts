/**
 * `ConnectionConfig.fetch` override tests.
 *
 * The contract:
 *   - When a connection supplies `fetch`, plugins see it as `ctx.fetch`.
 *   - When it doesn't, `ctx.fetch` equals `globalThis.fetch` — plugins
 *     can always call `ctx.fetch(...)` without a nullish check.
 *   - Applies to both query() and sync() paths.
 *
 * This is the hook dripyard uses to route plugin HTTP through
 * flaregun without dripline knowing anything about proxies.
 */

import { strict as assert } from "node:assert";
import { afterEach, describe, it } from "node:test";
import { Database } from "../core/db.js";
import type { PluginDef, QueryContext } from "../plugin/types.js";
import { Dripline } from "../sdk.js";

let dl: Dripline;
let db: Database;

async function cleanup(): Promise<void> {
  if (dl) {
    try {
      await dl.close();
    } catch {}
    dl = null as any;
  }
  if (db) {
    try {
      await db.close();
    } catch {}
    db = null as any;
  }
}

/** Plugin that captures ctx and yields one row, so we can inspect ctx.fetch. */
function makePlugin(): {
  plugin: PluginDef;
  captured: QueryContext[];
} {
  const captured: QueryContext[] = [];
  const plugin: PluginDef = {
    name: "fetch_test",
    version: "1.0.0",
    tables: [
      {
        name: "items",
        columns: [{ name: "id", type: "number" }],
        primaryKey: ["id"],
        async *list(ctx) {
          captured.push(ctx);
          yield { id: 1 };
        },
      },
    ],
  };
  return { plugin, captured };
}

describe("ConnectionConfig.fetch override", () => {
  afterEach(cleanup);

  it("threads connection.fetch into ctx.fetch during sync()", async () => {
    const { plugin, captured } = makePlugin();

    // A sentinel fetch — not actually called (plugin doesn't do HTTP),
    // just identity-compared. If dripline threads it through, ctx.fetch
    // will be this exact reference.
    const myFetch = (async () =>
      new Response("")) as unknown as typeof globalThis.fetch;

    db = await Database.create(":memory:");
    dl = await Dripline.create({
      plugins: [plugin],
      database: db,
      schema: "s",
      connections: [
        {
          name: "default",
          plugin: "fetch_test",
          config: {},
          fetch: myFetch,
        },
      ],
    });

    await dl.sync({ items: {} });

    assert.equal(captured.length, 1);
    assert.equal(captured[0].fetch, myFetch, "ctx.fetch should be the override");
  });

  it("defaults ctx.fetch to globalThis.fetch when no override", async () => {
    const { plugin, captured } = makePlugin();

    db = await Database.create(":memory:");
    dl = await Dripline.create({
      plugins: [plugin],
      database: db,
      schema: "s",
    });

    await dl.sync({ items: {} });

    assert.equal(captured.length, 1);
    assert.equal(
      captured[0].fetch,
      globalThis.fetch,
      "ctx.fetch should default to globalThis.fetch",
    );
  });

  it("threads connection.fetch into ctx.fetch during query() too", async () => {
    const { plugin, captured } = makePlugin();

    const myFetch = (async () =>
      new Response("")) as unknown as typeof globalThis.fetch;

    // query() path uses engine-owned DB (no `database`/`schema` passed)
    // — populateTable is where ctx is constructed for this code path.
    dl = await Dripline.create({
      plugins: [plugin],
      connections: [
        {
          name: "default",
          plugin: "fetch_test",
          config: {},
          fetch: myFetch,
        },
      ],
    });

    await dl.query("SELECT * FROM items");

    assert.equal(captured.length, 1);
    assert.equal(
      captured[0].fetch,
      myFetch,
      "ctx.fetch should be the override in query() path too",
    );
  });
});
