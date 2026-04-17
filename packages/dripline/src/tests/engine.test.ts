import { strict as assert } from "node:assert";
import { afterEach, describe, it } from "node:test";
import { QueryCache } from "../core/cache.js";
import { QueryEngine } from "../core/engine.js";
import { RateLimiter } from "../core/rate-limiter.js";
import { PluginRegistry } from "../plugin/registry.js";
import type { PluginDef, QueryContext } from "../plugin/types.js";

let engine: QueryEngine;
let reg: PluginRegistry;
let cache: QueryCache;
let rl: RateLimiter;
let listCalls: number;
let getCalls: number;
let lastCtx: QueryContext | null;

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
        *list(ctx) {
          listCalls++;
          lastCtx = ctx;
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
        *list() {
          listCalls++;
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

describe("QueryEngine", () => {
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

  it("key column qual with escaped single quotes", async () => {
    await setup();
    await engine.query(
      "SELECT * FROM users WHERE role = 'it''s admin'",
    );
    assert.ok(lastCtx);
    assert.equal(lastCtx.quals[0].column, "role");
    assert.equal(lastCtx.quals[0].value, "it's admin");
  });

  it("key column qual with SQL containing quotes", async () => {
    await setup();
    await engine.query(
      "SELECT * FROM users WHERE role = 'SELECT * WHERE x >= ''2026-01-01'''",
    );
    assert.ok(lastCtx);
    assert.equal(
      lastCtx.quals[0].value,
      "SELECT * WHERE x >= '2026-01-01'",
    );
  });

  it("extracts >= operator", async () => {
    await setup();
    await engine.query("SELECT * FROM users WHERE role >= 'admin'");
    assert.ok(lastCtx);
    assert.equal(lastCtx.quals[0].operator, ">=");
    assert.equal(lastCtx.quals[0].value, "admin");
  });

  it("extracts < operator", async () => {
    await setup();
    await engine.query("SELECT * FROM users WHERE role < 'z'");
    assert.ok(lastCtx);
    assert.equal(lastCtx.quals[0].operator, "<");
    assert.equal(lastCtx.quals[0].value, "z");
  });

  it("extracts IN operator", async () => {
    await setup();
    await engine.query("SELECT * FROM users WHERE role IN ('admin', 'user')");
    assert.ok(lastCtx);
    assert.equal(lastCtx.quals[0].operator, "IN");
    assert.deepEqual(lastCtx.quals[0].value, ["admin", "user"]);
  });

  it("extracts NOT IN operator", async () => {
    await setup();
    await engine.query("SELECT * FROM users WHERE role NOT IN ('guest')");
    assert.ok(lastCtx);
    assert.equal(lastCtx.quals[0].operator, "NOT IN");
    assert.deepEqual(lastCtx.quals[0].value, ["guest"]);
  });

  it("extracts BETWEEN operator", async () => {
    await setup();
    await engine.query("SELECT * FROM users WHERE role BETWEEN 'a' AND 'z'");
    assert.ok(lastCtx);
    assert.equal(lastCtx.quals[0].operator, "BETWEEN");
    assert.deepEqual(lastCtx.quals[0].value, ["a", "z"]);
  });

  it("extracts IS NULL operator", async () => {
    await setup();
    await engine.query("SELECT * FROM users WHERE role IS NULL");
    assert.ok(lastCtx);
    assert.equal(lastCtx.quals[0].operator, "IS NULL");
    assert.equal(lastCtx.quals[0].value, null);
  });

  it("extracts IS NOT NULL operator", async () => {
    await setup();
    await engine.query("SELECT * FROM users WHERE role IS NOT NULL");
    assert.ok(lastCtx);
    assert.equal(lastCtx.quals[0].operator, "IS NOT NULL");
    assert.equal(lastCtx.quals[0].value, null);
  });

  it("extracts LIKE operator", async () => {
    await setup();
    await engine.query("SELECT * FROM users WHERE role LIKE '%admin%'");
    assert.ok(lastCtx);
    assert.equal(lastCtx.quals[0].operator, "LIKE");
    assert.equal(lastCtx.quals[0].value, "%admin%");
  });

  it("extracts ILIKE operator", async () => {
    await setup();
    await engine.query("SELECT * FROM users WHERE role ILIKE '%admin%'");
    assert.ok(lastCtx);
    assert.equal(lastCtx.quals[0].operator, "ILIKE");
    assert.equal(lastCtx.quals[0].value, "%admin%");
  });

  it("extracts key column quals alongside non-key WHERE clauses", async () => {
    await setup();
    await engine.query(
      "SELECT * FROM users WHERE role = 'admin' AND name = 'Alice'",
    );
    assert.ok(lastCtx);
    const roleQual = lastCtx.quals.find((q: any) => q.column === "role");
    assert.ok(roleQual);
    assert.equal(roleQual.operator, "=");
    assert.equal(roleQual.value, "admin");
    // name is not a key column, so it shouldn't be in quals
    const nameQual = lastCtx.quals.find((q: any) => q.column === "name");
    assert.equal(nameQual, undefined);
  });

  // Shared plugin for subquery/JOIN/CTE qual extraction tests
  function makeShopPlugin(captures: {
    ordersCtx: QueryContext | null;
    itemsCtx: QueryContext | null;
  }): PluginDef {
    return {
      name: "shop",
      version: "0.1.0",
      tables: [
        {
          name: "shop_orders",
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
          *list(ctx) {
            captures.ordersCtx = ctx;
            yield { id: 1, org_id: "org1", status: "closed", business_date: "2026-04-03" };
            yield { id: 2, org_id: "org1", status: "open", business_date: "2026-04-03" };
            yield { id: 3, org_id: "org1", status: "closed", business_date: "2026-04-02" };
          },
        },
        {
          name: "shop_order_items",
          columns: [
            { name: "order_id", type: "number" },
            { name: "name", type: "string" },
            { name: "quantity", type: "number" },
          ],
          keyColumns: [{ name: "org_id", required: "required" }],
          *list(ctx) {
            captures.itemsCtx = ctx;
            yield { order_id: 1, org_id: "org1", name: "Pizza", quantity: 2 };
            yield { order_id: 2, org_id: "org1", name: "Salad", quantity: 1 };
          },
        },
      ],
    };
  }

  it("extracts quals from subquery in IN clause", async () => {
    const ctx = { ordersCtx: null as QueryContext | null, itemsCtx: null as QueryContext | null };
    await setup({ plugins: [makeShopPlugin(ctx)] });
    await engine.query(`
      SELECT name, SUM(quantity) as qty
      FROM shop_order_items
      WHERE org_id = 'org1'
        AND order_id IN (
          SELECT id FROM shop_orders
          WHERE org_id = 'org1'
            AND business_date = '2026-04-03'
            AND status = 'closed'
        )
      GROUP BY name
    `);

    assert.ok(ctx.itemsCtx);
    assert.equal(ctx.itemsCtx.quals.find((q: any) => q.column === "org_id")?.value, "org1");

    assert.ok(ctx.ordersCtx);
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "org_id")?.value, "org1");
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "business_date")?.value, "2026-04-03");
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "status")?.value, "closed");
  });

  it("extracts quals from aliased JOIN", async () => {
    const ctx = { ordersCtx: null as QueryContext | null, itemsCtx: null as QueryContext | null };
    await setup({ plugins: [makeShopPlugin(ctx)] });
    await engine.query(`
      SELECT oi.name
      FROM shop_order_items oi
      JOIN shop_orders o ON oi.order_id = o.id
      WHERE oi.org_id = 'org1' AND o.status = 'closed'
    `);

    assert.ok(ctx.itemsCtx);
    assert.equal(ctx.itemsCtx.quals.find((q: any) => q.column === "org_id")?.value, "org1");

    assert.ok(ctx.ordersCtx);
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "status")?.value, "closed");
  });

  it("extracts quals from CTE", async () => {
    const ctx = { ordersCtx: null as QueryContext | null, itemsCtx: null as QueryContext | null };
    await setup({ plugins: [makeShopPlugin(ctx)] });
    await engine.query(`
      WITH closed_orders AS (
        SELECT id FROM shop_orders
        WHERE org_id = 'org1' AND status = 'closed' AND business_date = '2026-04-03'
      )
      SELECT name FROM shop_order_items
      WHERE org_id = 'org1' AND order_id IN (SELECT id FROM closed_orders)
    `);

    assert.ok(ctx.ordersCtx);
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "org_id")?.value, "org1");
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "status")?.value, "closed");
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "business_date")?.value, "2026-04-03");
  });

  it("extracts quals from EXISTS subquery", async () => {
    const ctx = { ordersCtx: null as QueryContext | null, itemsCtx: null as QueryContext | null };
    await setup({ plugins: [makeShopPlugin(ctx)] });
    await engine.query(`
      SELECT name FROM shop_order_items oi
      WHERE oi.org_id = 'org1'
        AND EXISTS (
          SELECT 1 FROM shop_orders o
          WHERE o.id = oi.order_id
            AND o.org_id = 'org1'
            AND o.status = 'closed'
        )
    `);

    assert.ok(ctx.ordersCtx);
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "org_id")?.value, "org1");
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "status")?.value, "closed");
  });

  it("extracts quals from derived table (subquery in FROM)", async () => {
    const ctx = { ordersCtx: null as QueryContext | null, itemsCtx: null as QueryContext | null };
    await setup({ plugins: [makeShopPlugin(ctx)] });
    await engine.query(`
      SELECT sub.id FROM (
        SELECT id FROM shop_orders
        WHERE org_id = 'org1' AND status = 'closed'
      ) sub
    `);

    assert.ok(ctx.ordersCtx);
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "org_id")?.value, "org1");
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "status")?.value, "closed");
  });

  it("extracts quals from nested subquery (subquery within subquery)", async () => {
    const ctx = { ordersCtx: null as QueryContext | null, itemsCtx: null as QueryContext | null };
    await setup({ plugins: [makeShopPlugin(ctx)] });
    await engine.query(`
      SELECT * FROM shop_order_items
      WHERE org_id = 'org1'
        AND order_id IN (
          SELECT id FROM shop_orders
          WHERE org_id = 'org1'
            AND status IN (
              SELECT 'closed'
            )
            AND business_date = '2026-04-03'
        )
    `);

    assert.ok(ctx.ordersCtx);
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "org_id")?.value, "org1");
    assert.equal(ctx.ordersCtx.quals.find((q: any) => q.column === "business_date")?.value, "2026-04-03");
  });

  it("two-phase materialization resolves subquery and pushes IDs to outer table", async () => {
    let itemsQuals: any[] = [];

    const plugin: PluginDef = {
      name: "restaurant",
      version: "0.1.0",
      tables: [
        {
          name: "rest_orders",
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
          *list(ctx) {
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
          name: "rest_order_items",
          columns: [
            { name: "order_id", type: "number" },
            { name: "name", type: "string" },
            { name: "quantity", type: "number" },
          ],
          keyColumns: [{ name: "org_id", required: "required" }],
          *list(ctx) {
            itemsQuals = ctx.quals;
            const orderIdQual = ctx.quals.find((q: any) => q.column === "order_id");
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
      FROM rest_order_items oi
      WHERE oi.org_id = 'org1'
        AND oi.order_id IN (
          SELECT id FROM rest_orders
          WHERE org_id = 'org1'
            AND business_date = '2026-04-03'
            AND status = 'closed'
        )
    `);

    // Engine resolved the subquery to IN (1) and pushed it as a qual
    const orderIdQual = itemsQuals.find((q: any) => q.column === "order_id");
    assert.ok(orderIdQual, "order_id qual should be pushed after subquery resolution");
    assert.equal(orderIdQual.operator, "IN");
    assert.deepEqual(orderIdQual.value, [1]);

    // Plugin filtered at source — only items for order 1
    assert.equal(rows.length, 2);
    assert.ok(rows.find((r: any) => r.name === "Pizza"));
    assert.ok(rows.find((r: any) => r.name === "Salad"));
  });

  it("two-phase: NOT IN subquery resolved to literals", async () => {
    let itemsQuals: any[] = [];

    const plugin: PluginDef = {
      name: "rest_notin",
      version: "0.1.0",
      tables: [
        {
          name: "notin_orders",
          columns: [
            { name: "id", type: "number" },
            { name: "status", type: "string" },
          ],
          keyColumns: [
            { name: "org_id", required: "required" },
            { name: "status", required: "optional" },
          ],
          *list(ctx) {
            const status = ctx.quals.find((q: any) => q.column === "status")?.value;
            const data = [
              { id: 1, org_id: "org1", status: "open" },
              { id: 2, org_id: "org1", status: "cancelled" },
            ];
            for (const d of data) {
              if (status && d.status !== status) continue;
              yield d;
            }
          },
        },
        {
          name: "notin_items",
          columns: [
            { name: "order_id", type: "number" },
            { name: "name", type: "string" },
          ],
          keyColumns: [{ name: "org_id", required: "required" }],
          *list(ctx) {
            itemsQuals = ctx.quals;
            yield { order_id: 1, org_id: "org1", name: "A" };
            yield { order_id: 2, org_id: "org1", name: "B" };
            yield { order_id: 3, org_id: "org1", name: "C" };
          },
        },
      ],
    };

    await setup({ plugins: [plugin] });
    const rows = await engine.query(`
      SELECT name FROM notin_items
      WHERE org_id = 'org1'
        AND order_id NOT IN (
          SELECT id FROM notin_orders
          WHERE org_id = 'org1' AND status = 'cancelled'
        )
    `);

    // order_id should NOT include 2 (cancelled)
    assert.equal(rows.length, 2);
    assert.ok(rows.find((r: any) => r.name === "A"));
    assert.ok(rows.find((r: any) => r.name === "C"));
  });

  it("two-phase: EXISTS subquery skipped (correlated, not resolvable)", async () => {
    const ctx = { ordersCtx: null as QueryContext | null, itemsCtx: null as QueryContext | null };
    await setup({ plugins: [makeShopPlugin(ctx)] });

    // EXISTS with correlated reference (o.id = oi.order_id) can't be
    // resolved to literals — engine should skip and still return correct results
    const rows = await engine.query(`
      SELECT name FROM shop_order_items oi
      WHERE oi.org_id = 'org1'
        AND EXISTS (
          SELECT 1 FROM shop_orders o
          WHERE o.id = oi.order_id
            AND o.org_id = 'org1'
            AND o.status = 'closed'
        )
    `);

    // Should still work — DuckDB handles EXISTS after materialization
    assert.ok(rows.length > 0);
  });

  it("two-phase: CTE subquery resolved", async () => {
    let itemsQuals: any[] = [];

    const plugin: PluginDef = {
      name: "rest_cte",
      version: "0.1.0",
      tables: [
        {
          name: "cte_orders",
          columns: [
            { name: "id", type: "number" },
            { name: "status", type: "string" },
          ],
          keyColumns: [
            { name: "org_id", required: "required" },
            { name: "status", required: "optional" },
          ],
          *list(ctx) {
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
          name: "cte_items",
          columns: [
            { name: "order_id", type: "number" },
            { name: "name", type: "string" },
          ],
          keyColumns: [{ name: "org_id", required: "required" }],
          *list(ctx) {
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
        SELECT id FROM cte_orders
        WHERE org_id = 'org1' AND status = 'closed'
      )
      SELECT name FROM cte_items
      WHERE org_id = 'org1'
        AND order_id IN (SELECT id FROM closed)
    `);

    // CTE references a virtual table "closed" which DuckDB resolves
    // after cte_orders is materialized. The inner subquery
    // SELECT id FROM closed should resolve to [1].
    assert.equal(rows.length, 1);
    assert.equal((rows[0] as any).name, "Pizza");
  });

  it("two-phase: multiple subqueries in same query", async () => {
    let itemsQuals: any[] = [];

    const plugin: PluginDef = {
      name: "rest_multi",
      version: "0.1.0",
      tables: [
        {
          name: "multi_orders",
          columns: [
            { name: "id", type: "number" },
            { name: "org_id", type: "string" },
            { name: "status", type: "string" },
          ],
          keyColumns: [{ name: "org_id", required: "required" }],
          *list() {
            yield { id: 1, org_id: "org1", status: "closed" };
            yield { id: 2, org_id: "org1", status: "open" };
            yield { id: 3, org_id: "org2", status: "closed" };
          },
        },
        {
          name: "multi_items",
          columns: [
            { name: "order_id", type: "number" },
            { name: "name", type: "string" },
          ],
          keyColumns: [{ name: "org_id", required: "required" }],
          *list(ctx) {
            itemsQuals = ctx.quals;
            yield { order_id: 1, org_id: "org1", name: "A" };
            yield { order_id: 2, org_id: "org1", name: "B" };
            yield { order_id: 3, org_id: "org2", name: "C" };
          },
        },
      ],
    };

    await setup({ plugins: [plugin] });
    const rows = await engine.query(`
      SELECT name FROM multi_items
      WHERE org_id = 'org1'
        AND order_id IN (SELECT id FROM multi_orders WHERE org_id = 'org1' AND status = 'closed')
    `);

    // Should resolve subquery to IN (1) and push order_id qual
    const orderIdQual = itemsQuals.find((q: any) => q.column === "order_id");
    assert.ok(orderIdQual, "order_id qual should be pushed");
    assert.equal(orderIdQual.operator, "IN");
    assert.deepEqual(orderIdQual.value, [1]);

    assert.equal(rows.length, 1);
    assert.equal((rows[0] as any).name, "A");
  });

  it("two-phase: scalar subquery (= single value) resolved", async () => {
    let itemsQuals: any[] = [];

    const plugin: PluginDef = {
      name: "rest_scalar",
      version: "0.1.0",
      tables: [
        {
          name: "scalar_orders",
          columns: [
            { name: "id", type: "number" },
            { name: "priority", type: "number" },
          ],
          keyColumns: [{ name: "org_id", required: "required" }],
          *list() {
            yield { id: 1, org_id: "org1", priority: 10 };
            yield { id: 2, org_id: "org1", priority: 5 };
          },
        },
        {
          name: "scalar_items",
          columns: [
            { name: "order_id", type: "number" },
            { name: "name", type: "string" },
          ],
          keyColumns: [{ name: "org_id", required: "required" }],
          *list(ctx) {
            itemsQuals = ctx.quals;
            yield { order_id: 1, org_id: "org1", name: "High" };
            yield { order_id: 2, org_id: "org1", name: "Low" };
          },
        },
      ],
    };

    await setup({ plugins: [plugin] });
    const rows = await engine.query(`
      SELECT name FROM scalar_items
      WHERE org_id = 'org1'
        AND order_id = (SELECT id FROM scalar_orders WHERE org_id = 'org1' AND priority = 10)
    `);

    // Scalar subquery resolves to a single value
    assert.equal(rows.length, 1);
    assert.equal((rows[0] as any).name, "High");
  });

  it("two-phase: subquery with string values resolved", async () => {
    let itemsQuals: any[] = [];

    const plugin: PluginDef = {
      name: "rest_str",
      version: "0.1.0",
      tables: [
        {
          name: "str_categories",
          columns: [
            { name: "code", type: "string" },
            { name: "active", type: "boolean" },
          ],
          keyColumns: [],
          *list() {
            yield { code: "pizza", active: true };
            yield { code: "sushi", active: true };
            yield { code: "salad", active: false };
          },
        },
        {
          name: "str_items",
          columns: [
            { name: "category", type: "string" },
            { name: "name", type: "string" },
          ],
          keyColumns: [],
          *list(ctx) {
            itemsQuals = ctx.quals;
            yield { category: "pizza", name: "Margherita" };
            yield { category: "sushi", name: "Salmon Roll" };
            yield { category: "salad", name: "Caesar" };
          },
        },
      ],
    };

    await setup({ plugins: [plugin] });
    const rows = await engine.query(`
      SELECT name FROM str_items
      WHERE category IN (
        SELECT code FROM str_categories WHERE active = true
      )
    `);

    // String subquery values should resolve to IN ('pizza', 'sushi')
    const catQual = itemsQuals.find((q: any) => q.column === "category");
    assert.ok(catQual, "category qual should be pushed after subquery resolution");
    assert.equal(catQual.operator, "IN");
    assert.deepEqual(catQual.value.sort(), ["pizza", "sushi"]);

    assert.equal(rows.length, 2);
  });

  it("two-phase: empty subquery result handled gracefully", async () => {
    const plugin: PluginDef = {
      name: "rest_empty",
      version: "0.1.0",
      tables: [
        {
          name: "empty_orders",
          columns: [{ name: "id", type: "number" }],
          keyColumns: [{ name: "org_id", required: "required" }],
          *list() {
            // yields nothing
          },
        },
        {
          name: "empty_items",
          columns: [
            { name: "order_id", type: "number" },
            { name: "name", type: "string" },
          ],
          keyColumns: [{ name: "org_id", required: "required" }],
          *list() {
            yield { order_id: 1, org_id: "org1", name: "A" };
          },
        },
      ],
    };

    await setup({ plugins: [plugin] });
    const rows = await engine.query(`
      SELECT name FROM empty_items
      WHERE org_id = 'org1'
        AND order_id IN (
          SELECT id FROM empty_orders WHERE org_id = 'org1'
        )
    `);

    // Subquery returns 0 rows — no items should match
    assert.equal(rows.length, 0);
  });

  it("two-phase: query without subqueries unchanged", async () => {
    await setup();
    const rows = await engine.query("SELECT * FROM users WHERE role = 'admin'");
    assert.ok(lastCtx);
    // Only key column quals — no all-column expansion for non-subquery queries
    assert.equal(lastCtx.quals.length, 1);
    assert.equal(lastCtx.quals[0].column, "role");
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

  it("get path used when all key columns have quals and get returns non-null", async () => {
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
              *list() {
                listCalls++;
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

  it("get returns null falls back to list", async () => {
    await setup({
      plugins: [
        {
          name: "p",
          version: "0.1.0",
          tables: [
            {
              name: "things",
              columns: [{ name: "id", type: "number" }],
              keyColumns: [{ name: "k", required: "required" }],
              *list() {
                listCalls++;
                yield { id: 1, k: "x" };
              },
              get() {
                getCalls++;
                return null;
              },
            },
          ],
        },
      ],
    });
    const rows = await engine.query("SELECT * FROM things WHERE k = 'x'");
    assert.equal(getCalls, 1);
    assert.equal(listCalls, 1);
    assert.equal(rows.length, 1);
  });

  it("get not used when not all key columns provided", async () => {
    await setup({
      plugins: [
        {
          name: "p",
          version: "0.1.0",
          tables: [
            {
              name: "things",
              columns: [{ name: "id", type: "number" }],
              keyColumns: [
                { name: "a", required: "required" },
                { name: "b", required: "required" },
              ],
              *list() {
                listCalls++;
                yield { id: 1, a: "x", b: "y" };
              },
              get() {
                getCalls++;
                return { id: 99, a: "x", b: "y" };
              },
            },
          ],
        },
      ],
    });
    await engine.query("SELECT * FROM things WHERE a = 'x'");
    assert.equal(getCalls, 0);
    assert.equal(listCalls, 1);
  });

  it("hydrate functions enrich rows", async () => {
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
              *list() {
                yield { id: 1 };
              },
              hydrate: {
                extra: (_ctx, row) => ({ extra: `hydrated-${row.id}` }),
              },
            },
          ],
        },
      ],
    });
    const rows = (await engine.query("SELECT * FROM things")) as any[];
    assert.equal(rows[0].extra, "hydrated-1");
  });

  it("connection resolved from config when single connection", async () => {
    reg = new PluginRegistry();
    cache = new QueryCache();
    rl = new RateLimiter();
    lastCtx = null;

    reg.register({
      name: "p",
      version: "0.1.0",
      tables: [
        {
          name: "things",
          columns: [{ name: "id", type: "number" }],
          *list(ctx) {
            lastCtx = ctx;
            yield { id: 1 };
          },
        },
      ],
    });

    engine = new QueryEngine(reg, cache, rl);
    await engine.initialize({
      connections: [{ name: "myconn", plugin: "p", config: { key: "val" } }],
      cache: { enabled: true, ttl: 300, maxSize: 100 },
      rateLimits: {},
    });

    await engine.query("SELECT * FROM things");
    assert.ok(lastCtx);
    assert.equal(lastCtx.connection.name, "myconn");
    assert.equal(lastCtx.connection.config.key, "val");
  });

  it("default connection when no config", async () => {
    await setup();
    await engine.query("SELECT * FROM users");
    assert.ok(lastCtx);
    assert.equal(lastCtx.connection.name, "default");
  });

  it("query with params", async () => {
    await setup();
    const rows = await engine.query("SELECT * FROM users WHERE name = $1", [
      "Bob",
    ]);
    assert.equal(rows.length, 1);
  });

  it("close() closes the database", async () => {
    await setup();
    await engine.close();
    await assert.rejects(() => engine.query("SELECT 1"));
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
              *list() {
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
              *list() {
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

  it("empty list returns no rows", async () => {
    await setup({
      plugins: [
        {
          name: "p",
          version: "0.1.0",
          tables: [
            {
              name: "empty",
              columns: [{ name: "id", type: "number" }],
              *list() {},
            },
          ],
        },
      ],
    });
    assert.equal((await engine.query("SELECT * FROM empty")).length, 0);
  });

  it("plugin error propagates", async () => {
    await setup({
      plugins: [
        {
          name: "p",
          version: "0.1.0",
          tables: [
            {
              name: "broken",
              columns: [{ name: "id", type: "number" }],
              *list() {
                throw new Error("boom");
              },
            },
          ],
        },
      ],
    });
    await assert.rejects(() => engine.query("SELECT * FROM broken"), /boom/);
  });

  it("all-null boolean column does not crash Arrow IPC", async () => {
    await setup({
      plugins: [
        {
          name: "bool_null",
          version: "0.1.0",
          tables: [
            {
              name: "bool_all_null",
              columns: [
                { name: "id", type: "number" },
                { name: "name", type: "string" },
                { name: "is_active", type: "boolean" },
              ],
              *list() {
                yield { id: 1, name: "alice", is_active: null };
                yield { id: 2, name: "bob", is_active: null };
                yield { id: 3, name: "charlie", is_active: null };
              },
            },
          ],
        },
      ],
    });
    const rows = await engine.query("SELECT * FROM bool_all_null ORDER BY id");
    assert.equal(rows.length, 3);
    assert.equal(rows[0].name, "alice");
    assert.equal(rows[0].is_active, null);
    assert.equal(rows[1].is_active, null);
    assert.equal(rows[2].is_active, null);
  });
});
