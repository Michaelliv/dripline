import { describe, it, beforeEach, afterEach } from "node:test";
import { strict as assert } from "node:assert";
import { QueryEngine } from "../engine.js";
import { PluginRegistry } from "../plugin/registry.js";
import { QueryCache } from "../cache.js";
import { RateLimiter } from "../rate-limiter.js";
import type { PluginDef, QueryContext } from "../plugin/types.js";

let engine: QueryEngine;
let reg: PluginRegistry;
let cache: QueryCache;
let rl: RateLimiter;
let listCalls: number;
let getCalls: number;
let lastCtx: QueryContext | null;

function setup(opts?: { cacheEnabled?: boolean; plugins?: PluginDef[] }) {
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
            yield { id: d.id, name: d.name };
          }
        },
      },
      {
        name: "items",
        columns: [{ name: "id", type: "number" }, { name: "value", type: "string" }],
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
  engine.initialize({
    connections: [],
    cache: { enabled: opts?.cacheEnabled ?? true, ttl: 300, maxSize: 100 },
    rateLimits: {},
  });
}

function teardown() {
  engine?.close();
}

describe("QueryEngine", () => {
  afterEach(() => teardown());

  it("query returns results", () => {
    setup();
    const rows = engine.query("SELECT * FROM users");
    assert.equal(rows.length, 3);
  });

  it("key columns pushed down as parameters", () => {
    setup();
    engine.query("SELECT * FROM users WHERE role = 'admin'");
    assert.ok(lastCtx);
    assert.equal(lastCtx.quals[0].column, "role");
    assert.equal(lastCtx.quals[0].value, "admin");
  });

  it("non-key WHERE filtered by SQLite", () => {
    setup();
    const rows = engine.query("SELECT * FROM users WHERE name = 'Alice'");
    assert.equal(rows.length, 1);
    assert.equal((rows[0] as any).name, "Alice");
  });

  it("cache prevents second plugin call", () => {
    setup();
    engine.query("SELECT * FROM users");
    assert.equal(listCalls, 1);
    engine.query("SELECT * FROM users");
    assert.equal(listCalls, 1); // still 1 — cache hit
    assert.equal(cache.stats().hits, 1);
  });

  it("cache disabled — list called every time", () => {
    setup({ cacheEnabled: false });
    engine.query("SELECT * FROM users");
    engine.query("SELECT * FROM users");
    assert.equal(listCalls, 2);
  });

  it("get path used when all key columns have quals and get returns non-null", () => {
    setup({
      plugins: [{
        name: "p", version: "0.1.0",
        tables: [{
          name: "things",
          columns: [{ name: "id", type: "number" }, { name: "v", type: "string" }],
          keyColumns: [{ name: "k", required: "required" }],
          *list() { listCalls++; yield { id: 1, v: "from-list" }; },
          get(ctx) { getCalls++; return { id: 99, v: "from-get" }; },
        }],
      }],
    });
    const rows = engine.query("SELECT * FROM things WHERE k = 'x'") as any[];
    assert.equal(getCalls, 1);
    assert.equal(listCalls, 0);
    assert.equal(rows[0].v, "from-get");
  });

  it("get returns null falls back to list", () => {
    setup({
      plugins: [{
        name: "p", version: "0.1.0",
        tables: [{
          name: "things",
          columns: [{ name: "id", type: "number" }],
          keyColumns: [{ name: "k", required: "required" }],
          *list() { listCalls++; yield { id: 1 }; },
          get() { getCalls++; return null; },
        }],
      }],
    });
    const rows = engine.query("SELECT * FROM things WHERE k = 'x'");
    assert.equal(getCalls, 1);
    assert.equal(listCalls, 1);
    assert.equal(rows.length, 1);
  });

  it("get not used when not all key columns provided", () => {
    setup({
      plugins: [{
        name: "p", version: "0.1.0",
        tables: [{
          name: "things",
          columns: [{ name: "id", type: "number" }],
          keyColumns: [
            { name: "a", required: "required" },
            { name: "b", required: "required" },
          ],
          *list() { listCalls++; yield { id: 1 }; },
          get() { getCalls++; return { id: 99 }; },
        }],
      }],
    });
    engine.query("SELECT * FROM things WHERE a = 'x'"); // missing b
    assert.equal(getCalls, 0);
    assert.equal(listCalls, 1);
  });

  it("hydrate functions enrich rows", () => {
    setup({
      plugins: [{
        name: "p", version: "0.1.0",
        tables: [{
          name: "things",
          columns: [
            { name: "id", type: "number" },
            { name: "extra", type: "string" },
          ],
          *list() { yield { id: 1 }; },
          hydrate: {
            extra: (_ctx, row) => ({ extra: `hydrated-${row.id}` }),
          },
        }],
      }],
    });
    const rows = engine.query("SELECT * FROM things") as any[];
    assert.equal(rows[0].extra, "hydrated-1");
  });

  it("connection resolved from config when single connection", () => {
    reg = new PluginRegistry();
    cache = new QueryCache();
    rl = new RateLimiter();
    lastCtx = null;

    reg.register({
      name: "p", version: "0.1.0",
      tables: [{
        name: "things",
        columns: [{ name: "id", type: "number" }],
        *list(ctx) { lastCtx = ctx; yield { id: 1 }; },
      }],
    });

    engine = new QueryEngine(reg, cache, rl);
    engine.initialize({
      connections: [{ name: "myconn", plugin: "p", config: { key: "val" } }],
      cache: { enabled: true, ttl: 300, maxSize: 100 },
      rateLimits: {},
    });

    engine.query("SELECT * FROM things");
    assert.ok(lastCtx);
    assert.equal(lastCtx.connection.name, "myconn");
    assert.equal(lastCtx.connection.config.key, "val");
  });

  it("default connection when no config", () => {
    setup();
    engine.query("SELECT * FROM users");
    assert.ok(lastCtx);
    assert.equal(lastCtx.connection.name, "default");
  });

  it("query with params", () => {
    setup();
    const rows = engine.query("SELECT * FROM users WHERE name = ?", ["Bob"]);
    assert.equal(rows.length, 1);
  });

  it("close() closes the database", () => {
    setup();
    engine.close();
    assert.throws(() => engine.query("SELECT 1"));
  });

  it("multiple tables from same plugin", () => {
    setup();
    const users = engine.query("SELECT * FROM users");
    const items = engine.query("SELECT * FROM items");
    assert.equal(users.length, 3);
    assert.equal(items.length, 2);
  });

  it("tables from different plugins", () => {
    setup({
      plugins: [
        { name: "a", version: "0.1.0", tables: [{ name: "ta", columns: [{ name: "id", type: "number" }], *list() { yield { id: 1 }; } }] },
        { name: "b", version: "0.1.0", tables: [{ name: "tb", columns: [{ name: "id", type: "number" }], *list() { yield { id: 2 }; } }] },
      ],
    });
    assert.equal((engine.query("SELECT * FROM ta") as any[])[0].id, 1);
    assert.equal((engine.query("SELECT * FROM tb") as any[])[0].id, 2);
  });

  it("empty list returns no rows", () => {
    setup({
      plugins: [{
        name: "p", version: "0.1.0",
        tables: [{ name: "empty", columns: [{ name: "id", type: "number" }], *list() {} }],
      }],
    });
    assert.equal(engine.query("SELECT * FROM empty").length, 0);
  });

  it("plugin error propagates", () => {
    setup({
      plugins: [{
        name: "p", version: "0.1.0",
        tables: [{
          name: "broken",
          columns: [{ name: "id", type: "number" }],
          *list() { throw new Error("boom"); },
        }],
      }],
    });
    assert.throws(() => engine.query("SELECT * FROM broken"), /boom/);
  });
});
