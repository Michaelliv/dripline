import { strict as assert } from "node:assert";
import { execSync } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  realpathSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import {
  addConnection,
  applyEnvOverrides,
  getConnection,
  loadConfig,
  removeConnection,
  resolveEnvConnection,
} from "../config/loader.js";

let origCwd: string;
let tmpDir: string;
let origEnv: Record<string, string | undefined>;

function setup() {
  origCwd = process.cwd();
  tmpDir = realpathSync(mkdtempSync(join(tmpdir(), "dripline-conn-test-")));
  process.chdir(tmpDir);
  origEnv = { ...process.env };
}

function teardown() {
  process.chdir(origCwd);
  for (const key of Object.keys(process.env)) {
    if (key.startsWith("DRIPLINE_") && !(key in origEnv)) {
      delete process.env[key];
    }
  }
  for (const [key, val] of Object.entries(origEnv)) {
    if (key.startsWith("DRIPLINE_")) {
      if (val === undefined) delete process.env[key];
      else process.env[key] = val;
    }
  }
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

describe("connection management", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("addConnection creates .dripline/ lazily", () => {
    addConnection({ name: "gh", plugin: "github", config: { token: "abc" } });
    const config = loadConfig();
    assert.equal(config.connections.length, 1);
    assert.equal(config.connections[0].name, "gh");
  });

  it("addConnection updates existing by name", () => {
    writeConfig({
      connections: [{ name: "gh", plugin: "github", config: { token: "old" } }],
    });
    addConnection({ name: "gh", plugin: "github", config: { token: "new" } });
    const config = loadConfig();
    assert.equal(config.connections.length, 1);
    assert.equal(config.connections[0].config.token, "new");
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

  it("getConnection returns matching connection", () => {
    writeConfig({
      connections: [{ name: "gh", plugin: "github", config: { token: "x" } }],
    });
    const c = getConnection("gh");
    assert.equal(c?.config.token, "x");
  });

  it("getConnection returns undefined for unknown", () => {
    writeConfig({ connections: [] });
    assert.equal(getConnection("nope"), undefined);
  });
});

describe("env var resolution", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  const ghSchema = { token: { env: "GITHUB_TOKEN" } };
  const awsSchema = {
    access_key: { env: "AWS_ACCESS_KEY_ID" },
    secret_key: { env: "AWS_SECRET_ACCESS_KEY" },
  };

  it("resolveEnvConnection picks up env var from schema", () => {
    process.env.GITHUB_TOKEN = "env_token";
    const conn = resolveEnvConnection("github", ghSchema);
    assert.ok(conn);
    assert.equal(conn?.config.token, "env_token");
    assert.equal(conn?.plugin, "github");
    delete process.env.GITHUB_TOKEN;
  });

  it("resolveEnvConnection returns null when no schema", () => {
    assert.equal(resolveEnvConnection("github"), null);
  });

  it("resolveEnvConnection returns null when no matching env vars", () => {
    assert.equal(resolveEnvConnection("github", ghSchema), null);
  });

  it("resolveEnvConnection handles multiple keys", () => {
    process.env.AWS_ACCESS_KEY_ID = "ak";
    process.env.AWS_SECRET_ACCESS_KEY = "sk";
    const conn = resolveEnvConnection("aws", awsSchema);
    assert.ok(conn);
    assert.equal(conn?.config.access_key, "ak");
    assert.equal(conn?.config.secret_key, "sk");
    delete process.env.AWS_ACCESS_KEY_ID;
    delete process.env.AWS_SECRET_ACCESS_KEY;
  });

  it("applyEnvOverrides overrides config values", () => {
    process.env.GITHUB_TOKEN = "from_env";
    const conn = applyEnvOverrides(
      { name: "gh", plugin: "github", config: { token: "from_config" } },
      ghSchema,
    );
    assert.equal(conn.config.token, "from_env");
    delete process.env.GITHUB_TOKEN;
  });

  it("applyEnvOverrides preserves config when no env var set", () => {
    const conn = applyEnvOverrides(
      { name: "gh", plugin: "github", config: { token: "original" } },
      ghSchema,
    );
    assert.equal(conn.config.token, "original");
  });

  it("applyEnvOverrides with no schema returns conn unchanged", () => {
    const orig = { name: "gh", plugin: "github", config: { token: "x" } };
    const conn = applyEnvOverrides(orig);
    assert.equal(conn.config.token, "x");
  });
});

const PROJECT_DIR = join(import.meta.dirname, "../..");
const MAIN_TS = join(PROJECT_DIR, "src/main.ts");

describe("connection CLI", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  // node:test's default per-test timeout is 5s, but each `npx tsx`
  // cold-start takes ~2s and these tests shell out 3–4 times. 20s
  // leaves plenty of headroom without masking a genuine hang. Applied
  // per-`it()` because describe-level timeout isn't honored by bun's
  // node:test shim.
  it("connection add + list + remove", { timeout: 20_000 }, () => {
    createDriplineDir();

    execSync(
      `npx tsx ${MAIN_TS} connection add gh --plugin github --set token=test123`,
      {
        cwd: tmpDir,
        encoding: "utf-8",
      },
    );

    const listOut = execSync(`npx tsx ${MAIN_TS} conn list --json`, {
      cwd: tmpDir,
      encoding: "utf-8",
    });
    const connections = JSON.parse(listOut);
    assert.equal(connections.length, 1);
    assert.equal(connections[0].name, "gh");
    assert.equal(connections[0].config.token, "test123");

    execSync(`npx tsx ${MAIN_TS} conn remove gh`, {
      cwd: tmpDir,
      encoding: "utf-8",
    });

    const listOut2 = execSync(`npx tsx ${MAIN_TS} conn list --json`, {
      cwd: tmpDir,
      encoding: "utf-8",
    });
    assert.deepEqual(JSON.parse(listOut2), []);
  });

  it("connection add with multiple --set flags", { timeout: 20_000 }, () => {
    createDriplineDir();

    execSync(
      `npx tsx ${MAIN_TS} connection add myaws --plugin aws --set access_key=ak --set secret_key=sk`,
      {
        cwd: tmpDir,
        encoding: "utf-8",
      },
    );

    const listOut = execSync(`npx tsx ${MAIN_TS} conn list --json`, {
      cwd: tmpDir,
      encoding: "utf-8",
    });
    const connections = JSON.parse(listOut);
    assert.equal(connections[0].config.access_key, "ak");
    assert.equal(connections[0].config.secret_key, "sk");
  });

  it("connection list masks token values", { timeout: 20_000 }, () => {
    createDriplineDir();

    execSync(
      `npx tsx ${MAIN_TS} connection add gh --plugin github --set token=ghp_supersecrettoken`,
      {
        cwd: tmpDir,
        encoding: "utf-8",
      },
    );

    const out = execSync(`npx tsx ${MAIN_TS} conn list`, {
      cwd: tmpDir,
      encoding: "utf-8",
    });
    assert.ok(out.includes("ghp_..."));
    assert.ok(!out.includes("ghp_supersecrettoken"));
  });
});
