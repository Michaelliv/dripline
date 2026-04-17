import { strict as assert } from "node:assert";
import { afterEach, describe, it } from "node:test";
import { QueryCache } from "../core/cache.js";
import { QueryEngine } from "../core/engine.js";
import { RateLimiter } from "../core/rate-limiter.js";
import { PluginRegistry } from "../plugin/registry.js";
import type { PluginDef, QueryContext } from "../plugin/types.js";

/**
 * Mirrors engine.test.ts but every plugin uses async *list() generators.
 * Proves the engine handles async plugins identically to sync ones.
 */

let engine: QueryEngine;
let reg: PluginRegistry;
let cache: QueryCache;
let rl: RateLimiter;
let listCalls: number;
let getCalls: number;
let lastCtx: QueryContext | null;

/** Small delay to prove we're actually async. */
const tick = () => new Promise<void>((r) => setTimeout(r, 1));

async function setup(opts?: { cacheEnabled?: boolean; plugins?: PluginDef[] }) {
  reg = new PluginRegistry();
  cache = new QueryCache({ enabled: opts?.cacheEnabled ?? true });
  rl = new RateLimiter();
  listCalls = 0;
  getCalls = 0;
  lastCtx = null;

  const defaultPlugin: PluginDef = {
    name: "mock",
    version: "0.1.0",
    tables: [
      {
        name: "users",
        columns: [
          { name: "id", type: "number" },
          { name: "name", type: "string" },
        ],
        keyColumns: [{ name: "role", required: "optional" }],
        async *list(ctx) {
          listCalls++;
          lastCtx = ctx;
          await tick();
          const role = ctx.quals.find((q) => q.column === "role")?.value;
          const data = [
            { id: 1, name: "Alice", role: "admin" },
            { id: 2, name: "Bob", role: "user" },
            { id: 3, name: "Charlie", role: "user" },
          ];
          for (const d of data) {
            if (role && d.role !== role) continue;
            yield { id: d.id, name: d.name, role: d.role };
          }
        },
      },
      {
        name: "items",
        columns: [
          { name: "id", type: "number" },
          { name: "value", type: "string" },
        ],
        async *list() {
          listCalls++;
          await tick();
          yield { id: 1, value: "a" };
          yield { id: 2, value: "b" };
        },
      },
    ],
  };

  for (const p of opts?.plugins ?? [defaultPlugin]) {
    reg.register(p);
  }

  engine = new QueryEngine(reg, cache, rl);
  await engine.initialize({
    connections: [],
    cache: { enabled: opts?.cacheEnabled ?? true, ttl: 300, maxSize: 100 },
    rateLimits: {},
  });
}

async function teardown() {
  try {
    await engine?.close();
  } catch {}
}

describe("QueryEngine — async generators", () => {
  afterEach(async () => await teardown());

  it("query returns results", async () => {
    await setup();
    const rows = await engine.query("SELECT * FROM users");
    assert.equal(rows.length, 3);
  });

  it("key columns pushed down as parameters", async () => {
    await setup();
    await engine.query("SELECT * FROM users WHERE role = 'admin'");
    assert.ok(lastCtx);
    assert.equal(lastCtx.quals[0].column, "role");
    assert.equal(lastCtx.quals[0].value, "admin");
  });

  it("non-key WHERE filtered by DuckDB", async () => {
    await setup();
    const rows = await engine.query("SELECT * FROM users WHERE name = 'Alice'");
    assert.equal(rows.length, 1);
    assert.equal((rows[0] as any).name, "Alice");
  });

  it("cache prevents second plugin call", async () => {
    await setup();
    await engine.query("SELECT * FROM users");
    assert.equal(listCalls, 1);
    await engine.query("SELECT * FROM users");
    assert.equal(listCalls, 1);
    assert.equal(cache.stats().hits, 1);
  });

  it("cache disabled - list called every time", async () => {
    await setup({ cacheEnabled: false });
    await engine.query("SELECT * FROM users");
    await engine.query("SELECT * FROM users");
    assert.equal(listCalls, 2);
  });

  it("multiple tables from same plugin", async () => {
    await setup();
    const users = await engine.query("SELECT * FROM users");
    const items = await engine.query("SELECT * FROM items");
    assert.equal(users.length, 3);
    assert.equal(items.length, 2);
  });

  it("tables from different plugins", async () => {
    await setup({
      plugins: [
        {
          name: "a",
          version: "0.1.0",
          tables: [
            {
              name: "ta",
              columns: [{ name: "id", type: "number" }],
              async *list() {
                await tick();
                yield { id: 1 };
              },
            },
          ],
        },
        {
          name: "b",
          version: "0.1.0",
          tables: [
            {
              name: "tb",
              columns: [{ name: "id", type: "number" }],
              async *list() {
                await tick();
                yield { id: 2 };
              },
            },
          ],
        },
      ],
    });
    assert.equal(((await engine.query("SELECT * FROM ta")) as any[])[0].id, 1);
    assert.equal(((await engine.query("SELECT * FROM tb")) as any[])[0].id, 2);
  });

  it("empty async list returns no rows", async () => {
    await setup({
      plugins: [
        {
          name: "p",
          version: "0.1.0",
          tables: [
            {
              name: "empty",
              columns: [{ name: "id", type: "number" }],
              async *list() {
                await tick();
                // yields nothing
              },
            },
          ],
        },
      ],
    });
    assert.equal((await engine.query("SELECT * FROM empty")).length, 0);
  });

  it("async plugin error propagates", async () => {
    await setup({
      plugins: [
        {
          name: "p",
          version: "0.1.0",
          tables: [
            {
              name: "broken",
              columns: [{ name: "id", type: "number" }],
              async *list() {
                await tick();
                throw new Error("async boom");
              },
            },
          ],
        },
      ],
    });
    await assert.rejects(
      () => engine.query("SELECT * FROM broken"),
      /async boom/,
    );
  });

  it("hydrate functions enrich async rows", async () => {
    await setup({
      plugins: [
        {
          name: "p",
          version: "0.1.0",
          tables: [
            {
              name: "things",
              columns: [
                { name: "id", type: "number" },
                { name: "extra", type: "string" },
              ],
              async *list() {
                await tick();
                yield { id: 1 };
                yield { id: 2 };
              },
              hydrate: {
                extra: (_ctx, row) => ({ extra: `hydrated-${row.id}` }),
              },
            },
          ],
        },
      ],
    });
    const rows = (await engine.query("SELECT * FROM things ORDER BY id")) as any[];
    assert.equal(rows[0].extra, "hydrated-1");
    assert.equal(rows[1].extra, "hydrated-2");
  });

  it("get path still used (bypasses async list)", async () => {
    await setup({
      plugins: [
        {
          name: "p",
          version: "0.1.0",
          tables: [
            {
              name: "things",
              columns: [
                { name: "id", type: "number" },
                { name: "v", type: "string" },
              ],
              keyColumns: [{ name: "k", required: "required" }],
              async *list() {
                listCalls++;
                await tick();
                yield { id: 1, v: "from-list", k: "x" };
              },
              get(ctx) {
                getCalls++;
                return { id: 99, v: "from-get", k: ctx.quals[0]?.value };
              },
            },
          ],
        },
      ],
    });
    const rows = (await engine.query(
      "SELECT * FROM things WHERE k = 'x'",
    )) as any[];
    assert.equal(getCalls, 1);
    assert.equal(listCalls, 0);
    assert.equal(rows[0].v, "from-get");
  });

  it("query with params", async () => {
    await setup();
    const rows = await engine.query("SELECT * FROM users WHERE name = $1", [
      "Bob",
    ]);
    assert.equal(rows.length, 1);
  });

  // Async subquery tests — prove two-phase materialization works with async plugins

  function makeAsyncShopPlugin(captures: {
    ordersCtx: QueryContext | null;
    itemsCtx: QueryContext | null;
  }): PluginDef {
    return {
      name: "shop",
      version: "0.1.0",
      tables: [
        {
          name: "async_orders",
          columns: [
            { name: "id", type: "number" },
            { name: "status", type: "string" },
            { name: "business_date", type: "string" },
          ],
          keyColumns: [
            { name: "org_id", required: "required" },
            { name: "status", required: "optional" },
            { name: "business_date", required: "optional" },
          ],
          async *list(ctx) {
            captures.ordersCtx = ctx;
            await tick();
            yield { id: 1, org_id: "org1", status: "closed", business_date: "2026-04-03" };
            yield { id: 2, org_id: "org1", status: "open", business_date: "2026-04-03" };
            yield { id: 3, org_id: "org1", status: "closed", business_date: "2026-04-02" };
          },
        },
        {
          name: "async_order_items",
          columns: [
            { name: "order_id", type: "number" },
            { name: "name", type: "string" },
            { name: "quantity", type: "number" },
          ],
          keyColumns: [{ name: "org_id", required: "required" }],
          async *list(ctx) {
            captures.itemsCtx = ctx;
            await tick();
            yield { order_id: 1, org_id: "org1", name: "Pizza", quantity: 2 };
            yield { order_id: 2, org_id: "org1", name: "Salad", quantity: 1 };
          },
        },
      ],
    };
  }

  it("extracts quals from subquery with async plugins", async () => {
    const ctx = {
      ordersCtx: null as QueryContext | null,
      itemsCtx: null as QueryContext | null,
    };
    await setup({ plugins: [makeAsyncShopPlugin(ctx)] });
    await engine.query(`
      SELECT name, SUM(quantity) as qty
      FROM async_order_items
      WHERE org_id = 'org1'
        AND order_id IN (
          SELECT id FROM async_orders
          WHERE org_id = 'org1'
            AND business_date = '2026-04-03'
            AND status = 'closed'
        )
      GROUP BY name
    `);

    assert.ok(ctx.itemsCtx);
    assert.equal(
      ctx.itemsCtx.quals.find((q: any) => q.column === "org_id")?.value,
      "org1",
    );
    assert.ok(ctx.ordersCtx);
    assert.equal(
      ctx.ordersCtx.quals.find((q: any) => q.column === "status")?.value,
      "closed",
    );
  });

  it("two-phase resolves subquery and pushes IDs with async plugins", async () => {
    let itemsQuals: any[] = [];

    const plugin: PluginDef = {
      name: "restaurant",
      version: "0.1.0",
      tables: [
        {
          name: "ar_orders",
          columns: [
            { name: "id", type: "number" },
            { name: "status", type: "string" },
            { name: "business_date", type: "string" },
          ],
          keyColumns: [
            { name: "org_id", required: "required" },
            { name: "status", required: "optional" },
            { name: "business_date", required: "optional" },
          ],
          async *list(ctx) {
            await tick();
            const status = ctx.quals.find((q: any) => q.column === "status")?.value;
            const date = ctx.quals.find((q: any) => q.column === "business_date")?.value;
            const data = [
              { id: 1, org_id: "org1", status: "closed", business_date: "2026-04-03" },
              { id: 2, org_id: "org1", status: "open", business_date: "2026-04-03" },
              { id: 3, org_id: "org1", status: "closed", business_date: "2026-04-02" },
            ];
            for (const d of data) {
              if (status && d.status !== status) continue;
              if (date && d.business_date !== date) continue;
              yield d;
            }
          },
        },
        {
          name: "ar_items",
          columns: [
            { name: "order_id", type: "number" },
            { name: "name", type: "string" },
            { name: "quantity", type: "number" },
          ],
          keyColumns: [{ name: "org_id", required: "required" }],
          async *list(ctx) {
            await tick();
            itemsQuals = ctx.quals;
            const orderIdQual = ctx.quals.find(
              (q: any) => q.column === "order_id",
            );
            const items = [
              { order_id: 1, org_id: "org1", name: "Pizza", quantity: 2 },
              { order_id: 1, org_id: "org1", name: "Salad", quantity: 1 },
              { order_id: 2, org_id: "org1", name: "Burger", quantity: 3 },
              { order_id: 2, org_id: "org1", name: "Fries", quantity: 2 },
              { order_id: 3, org_id: "org1", name: "Soup", quantity: 1 },
            ];
            for (const item of items) {
              if (orderIdQual?.operator === "IN") {
                if (!orderIdQual.value.includes(item.order_id)) continue;
              }
              yield item;
            }
          },
        },
      ],
    };

    await setup({ plugins: [plugin] });
    const rows = await engine.query(`
      SELECT oi.name, oi.quantity
      FROM ar_items oi
      WHERE oi.org_id = 'org1'
        AND oi.order_id IN (
          SELECT id FROM ar_orders
          WHERE org_id = 'org1'
            AND business_date = '2026-04-03'
            AND status = 'closed'
        )
    `);

    const orderIdQual = itemsQuals.find((q: any) => q.column === "order_id");
    assert.ok(orderIdQual, "order_id qual should be pushed after subquery resolution");
    assert.equal(orderIdQual.operator, "IN");
    assert.deepEqual(orderIdQual.value, [1]);

    assert.equal(rows.length, 2);
    assert.ok(rows.find((r: any) => r.name === "Pizza"));
    assert.ok(rows.find((r: any) => r.name === "Salad"));
  });

  it("CTE subquery resolved with async plugins", async () => {
    let itemsQuals: any[] = [];

    const plugin: PluginDef = {
      name: "rest_cte",
      version: "0.1.0",
      tables: [
        {
          name: "ac_orders",
          columns: [
            { name: "id", type: "number" },
            { name: "status", type: "string" },
          ],
          keyColumns: [
            { name: "org_id", required: "required" },
            { name: "status", required: "optional" },
          ],
          async *list(ctx) {
            await tick();
            const status = ctx.quals.find((q: any) => q.column === "status")?.value;
            const data = [
              { id: 1, org_id: "org1", status: "closed" },
              { id: 2, org_id: "org1", status: "open" },
            ];
            for (const d of data) {
              if (status && d.status !== status) continue;
              yield d;
            }
          },
        },
        {
          name: "ac_items",
          columns: [
            { name: "order_id", type: "number" },
            { name: "name", type: "string" },
          ],
          keyColumns: [{ name: "org_id", required: "required" }],
          async *list(ctx) {
            await tick();
            itemsQuals = ctx.quals;
            yield { order_id: 1, org_id: "org1", name: "Pizza" };
            yield { order_id: 2, org_id: "org1", name: "Burger" };
          },
        },
      ],
    };

    await setup({ plugins: [plugin] });
    const rows = await engine.query(`
      WITH closed AS (
        SELECT id FROM ac_orders
        WHERE org_id = 'org1' AND status = 'closed'
      )
      SELECT name FROM ac_items
      WHERE org_id = 'org1'
        AND order_id IN (SELECT id FROM closed)
    `);

    assert.equal(rows.length, 1);
    assert.equal((rows[0] as any).name, "Pizza");
  });

  // Mixed sync + async plugins in the same query
  it("mixed sync and async plugins in same query", async () => {
    await setup({
      plugins: [
        {
          name: "sync_plugin",
          version: "0.1.0",
          tables: [
            {
              name: "sync_table",
              columns: [
                { name: "id", type: "number" },
                { name: "label", type: "string" },
              ],
              *list() {
                yield { id: 1, label: "sync-a" };
                yield { id: 2, label: "sync-b" };
              },
            },
          ],
        },
        {
          name: "async_plugin",
          version: "0.1.0",
          tables: [
            {
              name: "async_table",
              columns: [
                { name: "id", type: "number" },
                { name: "ref_id", type: "number" },
                { name: "value", type: "string" },
              ],
              async *list() {
                await tick();
                yield { id: 10, ref_id: 1, value: "async-x" };
                yield { id: 11, ref_id: 2, value: "async-y" };
              },
            },
          ],
        },
      ],
    });

    const rows = await engine.query(`
      SELECT s.label, a.value
      FROM sync_table s
      JOIN async_table a ON s.id = a.ref_id
      ORDER BY s.id
    `);
    assert.equal(rows.length, 2);
    assert.equal((rows[0] as any).label, "sync-a");
    assert.equal((rows[0] as any).value, "async-x");
    assert.equal((rows[1] as any).label, "sync-b");
    assert.equal((rows[1] as any).value, "async-y");
  });

  it("async plugin with yield between awaits", async () => {
    await setup({
      plugins: [
        {
          name: "p",
          version: "0.1.0",
          tables: [
            {
              name: "streamed",
              columns: [
                { name: "id", type: "number" },
                { name: "batch", type: "number" },
              ],
              async *list() {
                // Simulates paginated API: fetch page, yield rows, fetch next page
                await tick(); // "fetch page 1"
                yield { id: 1, batch: 1 };
                yield { id: 2, batch: 1 };
                await tick(); // "fetch page 2"
                yield { id: 3, batch: 2 };
                yield { id: 4, batch: 2 };
                await tick(); // "fetch page 3"
                yield { id: 5, batch: 3 };
              },
            },
          ],
        },
      ],
    });
    const rows = await engine.query(
      "SELECT * FROM streamed ORDER BY id",
    );
    assert.equal(rows.length, 5);
    assert.equal((rows[4] as any).id, 5);
    assert.equal((rows[4] as any).batch, 3);
  });

  it("connection config passed to async plugin", async () => {
    let receivedConfig: any = null;
    reg = new PluginRegistry();
    cache = new QueryCache();
    rl = new RateLimiter();

    reg.register({
      name: "auth_test",
      version: "1.0.0",
      tables: [
        {
          name: "auth_items",
          columns: [{ name: "id", type: "number" }],
          async *list(ctx) {
            receivedConfig = ctx.connection.config;
            await tick();
            yield { id: 1 };
          },
        },
      ],
    });

    engine = new QueryEngine(reg, cache, rl);
    await engine.initialize({
      connections: [
        { name: "myconn", plugin: "auth_test", config: { token: "secret" } },
      ],
      cache: { enabled: true, ttl: 300, maxSize: 100 },
      rateLimits: {},
    });

    await engine.query("SELECT * FROM auth_items");
    assert.equal(receivedConfig.token, "secret");
  });
});
