import { describe, it } from "node:test";
import { strict as assert } from "node:assert";
import { PluginRegistry } from "../plugin/registry.js";
import type { PluginDef } from "../plugin/types.js";

function makePlugin(name: string, tableNames: string[]): PluginDef {
  return {
    name,
    version: "0.1.0",
    tables: tableNames.map((t) => ({
      name: t,
      columns: [{ name: "id", type: "number" as const }],
      *list() { yield { id: 1 }; },
    })),
  };
}

describe("PluginRegistry", () => {
  it("register adds a plugin", () => {
    const r = new PluginRegistry();
    r.register(makePlugin("a", ["t1"]));
    assert.equal(r.listPlugins().length, 1);
  });

  it("getPlugin returns registered plugin", () => {
    const r = new PluginRegistry();
    r.register(makePlugin("a", ["t1"]));
    assert.equal(r.getPlugin("a")?.name, "a");
  });

  it("getPlugin returns undefined for unknown", () => {
    const r = new PluginRegistry();
    assert.equal(r.getPlugin("nope"), undefined);
  });

  it("register overwrites existing plugin", () => {
    const r = new PluginRegistry();
    r.register(makePlugin("a", ["t1"]));
    r.register(makePlugin("a", ["t1", "t2"]));
    assert.equal(r.getPlugin("a")?.tables.length, 2);
    assert.equal(r.listPlugins().length, 1);
  });

  it("getTable returns correct table", () => {
    const r = new PluginRegistry();
    r.register(makePlugin("a", ["t1", "t2"]));
    assert.equal(r.getTable("a", "t2")?.name, "t2");
  });

  it("getTable returns undefined for unknown plugin", () => {
    const r = new PluginRegistry();
    assert.equal(r.getTable("nope", "t1"), undefined);
  });

  it("getTable returns undefined for unknown table", () => {
    const r = new PluginRegistry();
    r.register(makePlugin("a", ["t1"]));
    assert.equal(r.getTable("a", "nope"), undefined);
  });

  it("getAllTables returns flat list", () => {
    const r = new PluginRegistry();
    r.register(makePlugin("a", ["t1", "t2"]));
    r.register(makePlugin("b", ["t3"]));
    const all = r.getAllTables();
    assert.equal(all.length, 3);
    assert.deepEqual(all.map((x) => x.table.name).sort(), ["t1", "t2", "t3"]);
  });

  it("getAllTables returns empty when no plugins", () => {
    const r = new PluginRegistry();
    assert.deepEqual(r.getAllTables(), []);
  });

  it("listPlugins returns all", () => {
    const r = new PluginRegistry();
    r.register(makePlugin("a", ["t1"]));
    r.register(makePlugin("b", ["t2"]));
    assert.equal(r.listPlugins().length, 2);
  });

  it("listPlugins returns empty when none", () => {
    const r = new PluginRegistry();
    assert.deepEqual(r.listPlugins(), []);
  });

  it("multiple plugins with multiple tables", () => {
    const r = new PluginRegistry();
    r.register(makePlugin("x", ["x1", "x2"]));
    r.register(makePlugin("y", ["y1", "y2", "y3"]));
    assert.equal(r.getAllTables().length, 5);
    assert.equal(r.getTable("x", "x2")?.name, "x2");
    assert.equal(r.getTable("y", "y3")?.name, "y3");
  });
});
