/**
 * Unit tests for `dripline lane add / list / remove`.
 *
 * No backend. Each test gets a fresh temp `.dripline/` directory and
 * runs the command functions directly, asserting on the resulting
 * config file and on thrown LaneConfigError instances.
 */

import { strict as assert } from "node:assert";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import {
  LaneConfigError,
  laneAdd,
  laneList,
  laneRemove,
} from "../commands/lane.js";

let tmpDir: string;
let prevCwd: string;
const logs: string[] = [];
const origLog = console.log;
const origErr = console.error;

function configPath(): string {
  return join(tmpDir, ".dripline", "config.json");
}

function readConfig(): Record<string, any> {
  return JSON.parse(readFileSync(configPath(), "utf-8"));
}

beforeEach(() => {
  tmpDir = mkdtempSync(join(tmpdir(), "dripline-lane-cmd-"));
  // Seed an empty .dripline dir so the config lands there and doesn't
  // walk up to a parent dripline workspace.
  mkdirSync(join(tmpDir, ".dripline"), { recursive: true });
  writeFileSync(
    configPath(),
    JSON.stringify({ connections: [], cache: {}, rateLimits: {}, lanes: {} }),
  );
  prevCwd = process.cwd();
  process.chdir(tmpDir);
  logs.length = 0;
  console.log = (msg?: unknown) => {
    logs.push(String(msg ?? ""));
  };
  console.error = (msg?: unknown) => {
    logs.push(String(msg ?? ""));
  };
});

afterEach(() => {
  console.log = origLog;
  console.error = origErr;
  process.chdir(prevCwd);
  rmSync(tmpDir, { recursive: true, force: true });
});

describe("lane add", () => {
  it("writes a minimal lane to config.json", async () => {
    await laneAdd("fast", {
      table: ["items"],
      interval: "15m",
      json: true,
    });
    const cfg = readConfig();
    assert.deepEqual(cfg.lanes.fast, {
      tables: [{ name: "items" }],
      interval: "15m",
    });
  });

  it("attaches params to the matching table by index", async () => {
    await laneAdd("multi", {
      table: ["a", "b", "c"],
      params: ["k=1", "-", "k=3,x=y"],
      interval: "1h",
      json: true,
    });
    const cfg = readConfig();
    assert.deepEqual(cfg.lanes.multi.tables, [
      { name: "a", params: { k: "1" } },
      { name: "b" },
      { name: "c", params: { k: "3", x: "y" } },
    ]);
  });

  it("honors --max-runtime", async () => {
    await laneAdd("lane", {
      table: ["t"],
      interval: "1h",
      maxRuntime: "10m",
      json: true,
    });
    assert.equal(readConfig().lanes.lane.maxRuntime, "10m");
  });

  it("rejects missing --table", async () => {
    await assert.rejects(
      () => laneAdd("empty", { table: [], interval: "15m" }),
      LaneConfigError,
    );
  });

  it("rejects missing --interval", async () => {
    await assert.rejects(
      () =>
        laneAdd("noint", {
          table: ["t"],
          interval: "" as unknown as string,
        }),
      LaneConfigError,
    );
  });

  it("rejects a malformed --params entry", async () => {
    await assert.rejects(
      () =>
        laneAdd("bad", {
          table: ["t"],
          params: ["not_a_kv_pair"],
          interval: "15m",
        }),
      /invalid --params/,
    );
  });

  it("rejects an invalid interval via validateLane", async () => {
    await assert.rejects(
      () =>
        laneAdd("bad", {
          table: ["t"],
          interval: "not_a_duration",
        }),
      /invalid interval/,
    );
  });

  it("rejects maxRuntime >= interval", async () => {
    await assert.rejects(
      () =>
        laneAdd("bad", {
          table: ["t"],
          interval: "5m",
          maxRuntime: "5m",
        }),
      /maxRuntime/,
    );
  });

  it("rejects lane names with bad characters", async () => {
    await assert.rejects(
      () => laneAdd("bad name!", { table: ["t"], interval: "15m" }),
      /must match/,
    );
  });

  it("refuses to overwrite an existing lane without --force", async () => {
    await laneAdd("dup", { table: ["t"], interval: "15m", json: true });
    await assert.rejects(
      () => laneAdd("dup", { table: ["t2"], interval: "30m" }),
      /already exists/,
    );
  });

  it("overwrites an existing lane when --force is passed", async () => {
    await laneAdd("dup", { table: ["t"], interval: "15m", json: true });
    await laneAdd("dup", {
      table: ["t2"],
      interval: "30m",
      force: true,
      json: true,
    });
    const cfg = readConfig();
    assert.deepEqual(cfg.lanes.dup, {
      tables: [{ name: "t2" }],
      interval: "30m",
    });
  });

  it("emits JSON on --json", async () => {
    await laneAdd("j", { table: ["t"], interval: "15m", json: true });
    const payload = JSON.parse(logs[0]);
    assert.equal(payload.success, true);
    assert.equal(payload.name, "j");
    assert.equal(payload.lane.interval, "15m");
  });
});

describe("lane remove", () => {
  it("removes an existing lane", async () => {
    await laneAdd("goner", { table: ["t"], interval: "15m", json: true });
    await laneRemove("goner", { json: true });
    assert.deepEqual(readConfig().lanes, {});
  });

  it("throws LaneConfigError for an unknown lane in human mode", async () => {
    await assert.rejects(
      () => laneRemove("nope", {}),
      LaneConfigError,
    );
  });

  it("returns success:false JSON for an unknown lane (no throw)", async () => {
    await laneRemove("nope", { json: true });
    const payload = JSON.parse(logs[0]);
    assert.deepEqual(payload, { success: false, name: "nope" });
  });
});

describe("lane list", () => {
  it("prints a friendly empty message when no lanes", async () => {
    await laneList({});
    assert.ok(logs.some((l) => /No lanes configured/.test(l)));
  });

  it("emits empty JSON when no lanes", async () => {
    await laneList({ json: true });
    assert.deepEqual(JSON.parse(logs[0]), { lanes: {} });
  });

  it("prints added lanes in human mode", async () => {
    await laneAdd("alpha", {
      table: ["x", "y"],
      params: ["org=a", "-"],
      interval: "1h",
      json: true,
    });
    logs.length = 0;
    await laneList({});
    const out = logs.join("\n");
    assert.match(out, /alpha/);
    assert.match(out, /2 tables/);
    assert.match(out, /every 1h/);
    assert.match(out, /org=a/);
  });

  it("emits JSON of all lanes", async () => {
    await laneAdd("beta", { table: ["t"], interval: "30m", json: true });
    logs.length = 0;
    await laneList({ json: true });
    const payload = JSON.parse(logs[0]);
    assert.ok(payload.lanes.beta);
    assert.equal(payload.lanes.beta.interval, "30m");
  });
});
