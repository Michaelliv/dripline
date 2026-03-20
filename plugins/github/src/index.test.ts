import { describe, it } from "node:test";
import { strict as assert } from "node:assert";
import githubFn from "./index.js";
import { resolvePluginExport } from "dripline";
import type { ColumnType, PluginDef } from "dripline";

const VALID_TYPES: ColumnType[] = ["string", "number", "boolean", "json", "datetime"];

const plugin: PluginDef = resolvePluginExport(githubFn, "github");

function getTable(name: string) {
  return plugin.tables.find((t) => t.name === name)!;
}

describe("GitHub Plugin", () => {
  it("has name github", () => {
    assert.equal(plugin.name, "github");
  });

  it("has version", () => {
    assert.ok(plugin.version);
  });

  it("has 4 tables", () => {
    assert.equal(plugin.tables.length, 4);
  });

  it("has connectionConfigSchema with token", () => {
    assert.ok(plugin.connectionConfigSchema?.token);
    assert.equal(plugin.connectionConfigSchema!.token.type, "string");
  });

  it("github_repos has 13 columns", () => {
    assert.equal(getTable("github_repos").columns.length, 13);
  });

  it("github_repos has owner as required key column", () => {
    const kc = getTable("github_repos").keyColumns!;
    const owner = kc.find((k) => k.name === "owner");
    assert.ok(owner);
    assert.equal(owner!.required, "required");
  });

  it("github_repos has list and get", () => {
    const t = getTable("github_repos");
    assert.equal(typeof t.list, "function");
    assert.equal(typeof t.get, "function");
  });

  it("github_issues has owner and repo as required", () => {
    const kc = getTable("github_issues").keyColumns!;
    assert.ok(kc.find((k) => k.name === "owner" && k.required === "required"));
    assert.ok(kc.find((k) => k.name === "repo" && k.required === "required"));
  });

  it("github_issues has issue_state as optional", () => {
    const kc = getTable("github_issues").keyColumns!;
    assert.ok(kc.find((k) => k.name === "issue_state" && k.required === "optional"));
  });

  it("github_pull_requests structure", () => {
    const t = getTable("github_pull_requests");
    assert.ok(t);
    assert.ok(t.columns.length >= 10);
    const kc = t.keyColumns!;
    assert.ok(kc.find((k) => k.name === "owner" && k.required === "required"));
    assert.ok(kc.find((k) => k.name === "repo" && k.required === "required"));
  });

  it("github_stargazers structure", () => {
    const t = getTable("github_stargazers");
    assert.ok(t);
    assert.equal(t.columns.length, 2);
    const kc = t.keyColumns!;
    assert.ok(kc.find((k) => k.name === "owner"));
    assert.ok(kc.find((k) => k.name === "repo"));
  });

  it("all list functions are generators", () => {
    for (const t of plugin.tables) {
      assert.equal(t.list.constructor.name, "GeneratorFunction");
    }
  });

  it("all tables have description", () => {
    for (const t of plugin.tables) {
      assert.ok(t.description, `${t.name} missing description`);
    }
  });

  it("all column types are valid", () => {
    for (const t of plugin.tables) {
      for (const c of t.columns) {
        assert.ok(VALID_TYPES.includes(c.type), `${t.name}.${c.name} has invalid type: ${c.type}`);
      }
    }
  });

  it("github_repos list with no owner yields nothing", () => {
    const t = getTable("github_repos");
    const ctx = {
      connection: { name: "test", plugin: "github", config: {} },
      quals: [],
      columns: t.columns.map((c) => c.name),
    };
    const rows = [...t.list(ctx)];
    assert.equal(rows.length, 0);
  });
});
