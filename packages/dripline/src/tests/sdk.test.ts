import { strict as assert } from "node:assert";
import { afterEach, describe, it } from "node:test";
import type { PluginDef } from "../plugin/types.js";
import { Dripline } from "../sdk.js";

const mockPlugin: PluginDef = {
  name: "mock",
  version: "1.0.0",
  tables: [
    {
      name: "mock_items",
      columns: [
        { name: "id", type: "number" },
        { name: "name", type: "string" },
        { name: "value", type: "number" },
      ],
      keyColumns: [{ name: "category", required: "optional" }],
      *list(ctx) {
        const cat = ctx.quals.find((q) => q.column === "category")?.value;
        const items = [
          { id: 1, name: "a", value: 10, category: "x" },
          { id: 2, name: "b", value: 20, category: "x" },
          { id: 3, name: "c", value: 30, category: "y" },
        ];
        for (const item of items) {
          if (cat && item.category !== cat) continue;
          yield { id: item.id, name: item.name, value: item.value };
        }
      },
    },
  ],
};

let dl: Dripline;

describe("Dripline SDK", () => {
  afterEach(async () => {
    if (dl) {
      try {
        await dl.close();
      } catch {}
      dl = null as any;
    }
  });
  it("construct with plugin and query", async () => {
    dl = await Dripline.create({ plugins: [mockPlugin] });
    const rows = await dl.query("SELECT * FROM mock_items");
    assert.equal(rows.length, 3);
  });

  it("query with WHERE on key column", async () => {
    dl = await Dripline.create({ plugins: [mockPlugin] });
    const rows = await dl.query(
      "SELECT * FROM mock_items WHERE category = 'x'",
    );
    assert.equal(rows.length, 2);
  });

  it("query with aggregation", async () => {
    dl = await Dripline.create({ plugins: [mockPlugin] });
    const rows = await dl.query<{ total: number }>(
      "SELECT SUM(value) as total FROM mock_items",
    );
    assert.equal(rows[0].total, 60);
  });

  it("query with typed result", async () => {
    dl = await Dripline.create({ plugins: [mockPlugin] });
    interface Item {
      id: number;
      name: string;
      value: number;
    }
    const rows = await dl.query<Item>(
      "SELECT * FROM mock_items WHERE category = 'y'",
    );
    assert.equal(rows[0].name, "c");
    assert.equal(rows[0].value, 30);
  });

  it("tables() lists available tables", async () => {
    dl = await Dripline.create({ plugins: [mockPlugin] });
    const tables = dl.tables();
    assert.equal(tables.length, 1);
    assert.equal(tables[0].table, "mock_items");
    assert.equal(tables[0].plugin, "mock");
  });

  it("plugins() lists registered plugins", async () => {
    dl = await Dripline.create({ plugins: [mockPlugin] });
    const plugins = dl.plugins();
    assert.equal(plugins.length, 1);
    assert.equal(plugins[0].name, "mock");
    assert.deepEqual(plugins[0].tables, ["mock_items"]);
  });

  it("cacheStats() returns stats", async () => {
    dl = await Dripline.create({ plugins: [mockPlugin] });
    await dl.query("SELECT * FROM mock_items");
    await dl.query("SELECT * FROM mock_items"); // cache hit
    const stats = dl.cacheStats();
    assert.equal(stats.hits, 1);
  });

  it("clearCache() resets cache", async () => {
    dl = await Dripline.create({ plugins: [mockPlugin] });
    await dl.query("SELECT * FROM mock_items");
    dl.clearCache();
    assert.equal(dl.cacheStats().size, 0);
  });

  it("cache disabled", async () => {
    dl = await Dripline.create({
      plugins: [mockPlugin],
      cache: { enabled: false },
    });
    await dl.query("SELECT * FROM mock_items");
    await dl.query("SELECT * FROM mock_items");
    assert.equal(dl.cacheStats().hits, 0);
  });

  it("connection config passed to plugin", async () => {
    let receivedConfig: any = null;
    const plugin: PluginDef = {
      name: "auth_test",
      version: "1.0.0",
      tables: [
        {
          name: "auth_items",
          columns: [{ name: "id", type: "number" }],
          *list(ctx) {
            receivedConfig = ctx.connection.config;
            yield { id: 1 };
          },
        },
      ],
    };
    dl = await Dripline.create({
      plugins: [plugin],
      connections: [
        { name: "myconn", plugin: "auth_test", config: { token: "secret" } },
      ],
    });
    await dl.query("SELECT * FROM auth_items");
    assert.equal(receivedConfig.token, "secret");
  });

  it("empty constructor works (no plugins)", async () => {
    dl = await Dripline.create();
    const tables = dl.tables();
    assert.equal(tables.length, 0);
  });

  it("query pure SQL without plugins", async () => {
    dl = await Dripline.create();
    const rows = await dl.query<{ v: number }>("SELECT 1 + 1 as v");
    assert.equal(rows[0].v, 2);
  });

  it("close prevents further queries", async () => {
    const localDl = await Dripline.create({ plugins: [mockPlugin] });
    await localDl.close();
    await assert.rejects(() => localDl.query("SELECT 1"));
  });
});

describe("SDK import paths", () => {
  it("index.ts exports Dripline", async () => {
    const mod = await import("../index.js");
    assert.equal(typeof mod.Dripline, "function");
  });

  it("index.ts exports types and utilities", async () => {
    const mod = await import("../index.js");
    assert.equal(typeof mod.QueryEngine, "function");
    assert.equal(typeof mod.PluginRegistry, "function");
    assert.equal(typeof mod.QueryCache, "function");
    assert.equal(typeof mod.RateLimiter, "function");
    assert.equal(typeof mod.formatTable, "function");
    assert.equal(typeof mod.formatJson, "function");
    assert.equal(typeof mod.formatCsv, "function");
    assert.equal(typeof mod.syncGet, "function");
  });

  it("index.ts exports syncExec", async () => {
    const mod = await import("../index.js");
    assert.equal(typeof mod.syncExec, "function");
    assert.equal(typeof mod.commandExists, "function");
  });
});
