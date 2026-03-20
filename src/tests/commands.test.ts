import { describe, it, beforeEach, afterEach } from "node:test";
import { strict as assert } from "node:assert";
import { mkdtempSync, mkdirSync, existsSync, readFileSync, writeFileSync, realpathSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { init } from "../commands/init.js";
import { onboard } from "../commands/onboard.js";

let origCwd: string;
let tmpDir: string;
let captured: string[];

function setup() {
  origCwd = process.cwd();
  tmpDir = realpathSync(mkdtempSync(join(tmpdir(), "dripline-cmd-test-")));
  process.chdir(tmpDir);
  captured = [];
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
  return fn().then(() => {
    console.log = origLog;
    console.error = origErr;
    return lines;
  }).catch((e) => {
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

describe("onboard command", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("creates CLAUDE.md with instructions", async () => {
    await onboard([], {});
    assert.ok(existsSync(join(tmpDir, "CLAUDE.md")));
    const content = readFileSync(join(tmpDir, "CLAUDE.md"), "utf-8");
    assert.ok(content.includes("<dripline>"));
  });

  it("appends to existing CLAUDE.md", async () => {
    writeFileSync(join(tmpDir, "CLAUDE.md"), "# Existing\n");
    await onboard([], {});
    const content = readFileSync(join(tmpDir, "CLAUDE.md"), "utf-8");
    assert.ok(content.includes("# Existing"));
    assert.ok(content.includes("<dripline>"));
  });

  it("is idempotent", async () => {
    await onboard([], {});
    await onboard([], {});
    const content = readFileSync(join(tmpDir, "CLAUDE.md"), "utf-8");
    const count = content.split("<dripline>").length - 1;
    assert.equal(count, 1);
  });

  it("prefers existing CLAUDE.md over AGENTS.md", async () => {
    writeFileSync(join(tmpDir, "CLAUDE.md"), "# Claude\n");
    writeFileSync(join(tmpDir, "AGENTS.md"), "# Agents\n");
    await onboard([], {});
    const claude = readFileSync(join(tmpDir, "CLAUDE.md"), "utf-8");
    const agents = readFileSync(join(tmpDir, "AGENTS.md"), "utf-8");
    assert.ok(claude.includes("<dripline>"));
    assert.ok(!agents.includes("<dripline>"));
  });

  it("with --json outputs JSON", async () => {
    const lines = await captureLog(() => onboard([], { json: true }));
    const output = lines.join("\n");
    const parsed = JSON.parse(output);
    assert.equal(parsed.success, true);
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
      execSync(
        `npx tsx ${MAIN_TS} query "INVALID SQL GARBAGE"`,
        { cwd: tmpDir, encoding: "utf-8", stdio: "pipe" },
      );
      assert.fail("should have thrown");
    } catch (e: any) {
      assert.ok(e.stderr.includes("✗") || e.status !== 0);
    }
  });
});
