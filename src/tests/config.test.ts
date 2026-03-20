import { strict as assert } from "node:assert";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import {
  addConnection,
  findConfigDir,
  getConnection,
  loadConfig,
  removeConnection,
  saveConfig,
} from "../config/loader.js";
import { DEFAULT_CONFIG } from "../config/types.js";

let origCwd: string;
let tmpDir: string;

function setupTmp() {
  origCwd = process.cwd();
  tmpDir = realpathSync(mkdtempSync(join(tmpdir(), "dripline-test-")));
  process.chdir(tmpDir);
}

function teardownTmp() {
  process.chdir(origCwd);
}

function createDriplineDir() {
  mkdirSync(join(tmpDir, ".dripline"), { recursive: true });
}

function writeConfig(data: any) {
  createDriplineDir();
  writeFileSync(
    join(tmpDir, ".dripline", "config.json"),
    JSON.stringify(data, null, 2),
  );
}

describe("Config", () => {
  beforeEach(() => setupTmp());
  afterEach(() => teardownTmp());

  it("findConfigDir returns null when no .dripline/", () => {
    assert.equal(findConfigDir(), null);
  });

  it("findConfigDir finds .dripline/ in current dir", () => {
    createDriplineDir();
    assert.equal(findConfigDir(), join(tmpDir, ".dripline"));
  });

  it("findConfigDir walks up parent dirs", () => {
    createDriplineDir();
    const sub = join(tmpDir, "a", "b", "c");
    mkdirSync(sub, { recursive: true });
    process.chdir(sub);
    assert.equal(findConfigDir(), join(tmpDir, ".dripline"));
  });

  it("loadConfig returns defaults when no config file", () => {
    const c = loadConfig();
    assert.deepEqual(c.connections, []);
    assert.equal(c.cache.enabled, true);
    assert.equal(c.cache.ttl, 300);
  });

  it("loadConfig reads config.json", () => {
    writeConfig({
      connections: [{ name: "gh", plugin: "github", config: { token: "x" } }],
    });
    const c = loadConfig();
    assert.equal(c.connections.length, 1);
    assert.equal(c.connections[0].name, "gh");
  });

  it("loadConfig merges with defaults", () => {
    writeConfig({ connections: [] }); // no cache field
    const c = loadConfig();
    assert.equal(c.cache.enabled, true);
    assert.equal(c.cache.ttl, 300);
    assert.equal(c.cache.maxSize, 1000);
  });

  it("saveConfig creates config.json", () => {
    createDriplineDir();
    saveConfig({
      ...DEFAULT_CONFIG,
      connections: [{ name: "x", plugin: "y", config: {} }],
    });
    const raw = JSON.parse(
      readFileSync(join(tmpDir, ".dripline", "config.json"), "utf-8"),
    );
    assert.equal(raw.connections[0].name, "x");
  });

  it("saveConfig overwrites existing", () => {
    writeConfig({ connections: [{ name: "old", plugin: "p", config: {} }] });
    saveConfig({
      ...DEFAULT_CONFIG,
      connections: [{ name: "new", plugin: "p", config: {} }],
    });
    const raw = JSON.parse(
      readFileSync(join(tmpDir, ".dripline", "config.json"), "utf-8"),
    );
    assert.equal(raw.connections[0].name, "new");
  });

  it("getConnection returns matching", () => {
    writeConfig({
      connections: [{ name: "gh", plugin: "github", config: { token: "t" } }],
    });
    const c = getConnection("gh");
    assert.equal(c?.plugin, "github");
  });

  it("getConnection returns undefined for unknown", () => {
    writeConfig({ connections: [] });
    assert.equal(getConnection("nope"), undefined);
  });

  it("addConnection adds new", () => {
    writeConfig({ connections: [] });
    addConnection({ name: "new", plugin: "p", config: {} });
    const c = loadConfig();
    assert.equal(c.connections.length, 1);
    assert.equal(c.connections[0].name, "new");
  });

  it("addConnection updates existing by name", () => {
    writeConfig({
      connections: [{ name: "gh", plugin: "github", config: { token: "old" } }],
    });
    addConnection({ name: "gh", plugin: "github", config: { token: "new" } });
    const c = loadConfig();
    assert.equal(c.connections.length, 1);
    assert.equal(c.connections[0].config.token, "new");
  });

  it("removeConnection removes and returns true", () => {
    writeConfig({
      connections: [{ name: "gh", plugin: "github", config: {} }],
    });
    assert.equal(removeConnection("gh"), true);
    assert.equal(loadConfig().connections.length, 0);
  });

  it("removeConnection returns false for unknown", () => {
    writeConfig({ connections: [] });
    assert.equal(removeConnection("nope"), false);
  });

  it("DEFAULT_CONFIG has expected values", () => {
    assert.deepEqual(DEFAULT_CONFIG.connections, []);
    assert.equal(DEFAULT_CONFIG.cache.enabled, true);
    assert.equal(DEFAULT_CONFIG.cache.ttl, 300);
    assert.equal(DEFAULT_CONFIG.cache.maxSize, 1000);
    assert.deepEqual(DEFAULT_CONFIG.rateLimits, {});
  });
});
