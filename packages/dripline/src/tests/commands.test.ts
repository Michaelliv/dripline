import { strict as assert } from "node:assert";
import {
  existsSync,
  mkdtempSync,
  realpathSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import { init } from "../commands/init.js";

let origCwd: string;
let tmpDir: string;
let _captured: string[];

function setup() {
  origCwd = process.cwd();
  tmpDir = realpathSync(mkdtempSync(join(tmpdir(), "dripline-cmd-test-")));
  process.chdir(tmpDir);
  _captured = [];
}

function teardown() {
  process.chdir(origCwd);
}

function captureLog(fn: () => Promise<void>): Promise<string[]> {
  const origLog = console.log;
  const origErr = console.error;
  const lines: string[] = [];
  console.log = (...args: any[]) => lines.push(args.map(String).join(" "));
  console.error = (...args: any[]) => lines.push(args.map(String).join(" "));
  return fn()
    .then(() => {
      console.log = origLog;
      console.error = origErr;
      return lines;
    })
    .catch((e) => {
      console.log = origLog;
      console.error = origErr;
      throw e;
    });
}

describe("init command", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("creates .dripline/ directory", async () => {
    await init([], {});
    assert.ok(existsSync(join(tmpDir, ".dripline")));
  });

  it("creates subdirs", async () => {
    await init([], {});
    assert.ok(existsSync(join(tmpDir, ".dripline", "plugins")));
    assert.ok(existsSync(join(tmpDir, ".dripline", "connections")));
  });

  it("with --json outputs JSON", async () => {
    const lines = await captureLog(() => init([], { json: true }));
    const output = lines.join("\n");
    const parsed = JSON.parse(output);
    assert.equal(parsed.success, true);
  });

  it("is idempotent", async () => {
    await init([], {});
    await init([], {}); // should not throw
    assert.ok(existsSync(join(tmpDir, ".dripline")));
  });
});

const PROJECT_DIR = join(import.meta.dirname, "../..");
const MAIN_TS = join(PROJECT_DIR, "src/main.ts");

describe("query command", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("executes SQL via CLI", async () => {
    const { execSync } = await import("node:child_process");
    const out = execSync(
      `npx tsx ${MAIN_TS} query "SELECT 1 as test" --output json`,
      { cwd: tmpDir, encoding: "utf-8" },
    );
    const parsed = JSON.parse(out);
    assert.equal(parsed[0].test, 1);
  });

  it("query with --output csv", async () => {
    const { execSync } = await import("node:child_process");
    const out = execSync(
      `npx tsx ${MAIN_TS} query "SELECT 1 as a, 2 as b" --output csv`,
      { cwd: tmpDir, encoding: "utf-8" },
    );
    assert.ok(out.includes("a,b"));
    assert.ok(out.includes("1,2"));
  });

  it("query with --output line", async () => {
    const { execSync } = await import("node:child_process");
    const out = execSync(
      `npx tsx ${MAIN_TS} query "SELECT 1 as val" --output line`,
      { cwd: tmpDir, encoding: "utf-8" },
    );
    assert.ok(out.includes("val"));
    assert.ok(out.includes("1"));
  });

  it("query with invalid SQL shows error", async () => {
    const { execSync } = await import("node:child_process");
    try {
      execSync(`npx tsx ${MAIN_TS} query "INVALID SQL GARBAGE"`, {
        cwd: tmpDir,
        encoding: "utf-8",
        stdio: "pipe",
      });
      assert.fail("should have thrown");
    } catch (e: any) {
      assert.ok(e.stderr.includes("✗") || e.status !== 0);
    }
  });
});
