import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import {
  createPluginAPI,
  isPluginFunction,
  resolvePluginExport,
} from "../plugin/api.js";

describe("createPluginAPI", () => {
  it("returns api and resolve", () => {
    const { api, resolve } = createPluginAPI("test");
    assert.ok(api);
    assert.equal(typeof resolve, "function");
  });

  it("setName sets plugin name", () => {
    const { api, resolve } = createPluginAPI("fallback");
    api.setName("custom");
    assert.equal(resolve().name, "custom");
  });

  it("setVersion sets version", () => {
    const { api, resolve } = createPluginAPI("t");
    api.setVersion("2.0.0");
    assert.equal(resolve().version, "2.0.0");
  });

  it("registerTable adds a table", () => {
    const { api, resolve } = createPluginAPI("t");
    api.registerTable("my_table", {
      columns: [{ name: "id", type: "number" }],
      *list() {
        yield { id: 1 };
      },
    });
    const p = resolve();
    assert.equal(p.tables.length, 1);
    assert.equal(p.tables[0].name, "my_table");
  });

  it("setConnectionSchema sets schema", () => {
    const { api, resolve } = createPluginAPI("t");
    api.setConnectionSchema({
      token: { type: "string", required: true, description: "API token" },
    });
    const p = resolve();
    assert.ok(p.connectionConfigSchema?.token);
    assert.equal(p.connectionConfigSchema?.token.type, "string");
  });

  it("resolve returns complete PluginDef", () => {
    const { api, resolve } = createPluginAPI("t");
    api.setName("full");
    api.setVersion("1.0.0");
    api.setConnectionSchema({ key: { type: "string" } });
    api.registerTable("t1", {
      columns: [{ name: "id", type: "number" }],
      *list() {},
    });
    api.registerTable("t2", {
      columns: [{ name: "id", type: "number" }],
      *list() {},
    });
    const p = resolve();
    assert.equal(p.name, "full");
    assert.equal(p.version, "1.0.0");
    assert.equal(p.tables.length, 2);
    assert.ok(p.connectionConfigSchema?.key);
  });

  it("multiple tables", () => {
    const { api, resolve } = createPluginAPI("t");
    for (let i = 0; i < 5; i++) {
      api.registerTable(`t${i}`, {
        columns: [{ name: "id", type: "number" }],
        *list() {},
      });
    }
    assert.equal(resolve().tables.length, 5);
  });

  it("onInit hook is stored", () => {
    const { api, resolve } = createPluginAPI("t");
    let called = false;
    api.onInit(() => {
      called = true;
    });
    const p = resolve() as any;
    assert.ok(p._initHooks);
    assert.equal(p._initHooks.length, 1);
    p._initHooks[0]({});
    assert.ok(called);
  });

  it("default name from pluginId", () => {
    const { resolve } = createPluginAPI("my-plugin");
    assert.equal(resolve().name, "my-plugin");
  });

  it("log methods don't throw", () => {
    const { api } = createPluginAPI("t");
    api.log.info("test");
    api.log.warn("test");
    api.log.error("test");
  });

  it("log.info writes to stderr, never stdout", () => {
    const { api } = createPluginAPI("t");
    const origLog = console.log;
    const origWrite = process.stderr.write;
    const stdoutLines: string[] = [];
    const stderrChunks: string[] = [];
    console.log = (...args: any[]) => stdoutLines.push(args.map(String).join(" "));
    process.stderr.write = ((chunk: any) => {
      stderrChunks.push(String(chunk));
      return true;
    }) as typeof process.stderr.write;
    try {
      api.log.info("hello");
    } finally {
      console.log = origLog;
      process.stderr.write = origWrite;
    }
    assert.equal(stdoutLines.length, 0);
    assert.deepEqual(stderrChunks, ["[t] hello\n"]);
  });
});

describe("resolvePluginExport", () => {
  it("resolves function export", () => {
    const fn = (api: any) => {
      api.setName("fn-plugin");
      api.registerTable("t", {
        columns: [{ name: "id", type: "number" }],
        *list() {},
      });
    };
    const p = resolvePluginExport(fn, "fallback");
    assert.equal(p.name, "fn-plugin");
    assert.equal(p.tables.length, 1);
  });

  it("resolves static object export", () => {
    const obj = { name: "static", version: "1.0.0", tables: [] };
    const p = resolvePluginExport(obj, "fallback");
    assert.equal(p.name, "static");
  });

  it("throws on invalid export", () => {
    assert.throws(
      () => resolvePluginExport("not a plugin" as any, "bad"),
      /Invalid plugin/,
    );
  });
});

describe("isPluginFunction", () => {
  it("true for functions", () => {
    assert.ok(isPluginFunction(() => {}));
  });

  it("false for objects", () => {
    assert.ok(!isPluginFunction({ name: "x", tables: [] }));
  });
});
