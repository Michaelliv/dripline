import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import {
  DEFAULT_MAX_RUNTIME_MS,
  laneLeaseName,
  laneSchema,
  laneStatePath,
  parseInterval,
  validateLane,
  validateLanes,
} from "../core/lanes.js";

describe("parseInterval", () => {
  it("parses seconds", () => {
    assert.equal(parseInterval("30s"), 30_000);
    assert.equal(parseInterval("1s"), 1_000);
  });
  it("parses minutes", () => {
    assert.equal(parseInterval("15m"), 900_000);
    assert.equal(parseInterval("1m"), 60_000);
  });
  it("parses hours", () => {
    assert.equal(parseInterval("1h"), 3_600_000);
    assert.equal(parseInterval("6h"), 21_600_000);
  });
  it("parses days", () => {
    assert.equal(parseInterval("1d"), 86_400_000);
    assert.equal(parseInterval("7d"), 7 * 86_400_000);
  });
  it("trims whitespace", () => {
    assert.equal(parseInterval("  15m  "), 900_000);
  });
  it("rejects missing unit", () => {
    assert.throws(() => parseInterval("15"), /invalid interval/);
  });
  it("rejects unknown unit", () => {
    assert.throws(() => parseInterval("15w"), /invalid interval/);
  });
  it("rejects empty string", () => {
    assert.throws(() => parseInterval(""), /invalid interval/);
  });
  it("rejects zero", () => {
    assert.throws(() => parseInterval("0m"), /must be positive/);
  });
  it("rejects negative", () => {
    assert.throws(() => parseInterval("-5m"), /invalid interval/);
  });
  it("rejects floats", () => {
    assert.throws(() => parseInterval("1.5h"), /invalid interval/);
  });
});

describe("validateLane", () => {
  const ok = {
    tables: [{ name: "github_issues", params: { owner: "x", repo: "y" } }],
    interval: "15m",
  };

  it("accepts a valid lane", () => {
    const v = validateLane("fast", ok);
    assert.equal(v.name, "fast");
    assert.equal(v.intervalMs, 900_000);
    assert.equal(v.tables.length, 1);
  });

  it("defaults maxRuntime to 10m for long intervals", () => {
    const v = validateLane("slow", { ...ok, interval: "1h" });
    assert.equal(v.maxRuntimeMs, DEFAULT_MAX_RUNTIME_MS);
  });

  it("defaults maxRuntime to interval/2 for short intervals", () => {
    const v = validateLane("tiny", { ...ok, interval: "30s" });
    assert.equal(v.maxRuntimeMs, 15_000);
    assert.ok(v.maxRuntimeMs < v.intervalMs);
  });

  it("default maxRuntime is always strictly less than interval", () => {
    for (const interval of ["1s", "5s", "30s", "1m", "15m", "1h", "6h", "1d"]) {
      const v = validateLane("x", { ...ok, interval });
      assert.ok(
        v.maxRuntimeMs < v.intervalMs,
        `interval=${interval}: maxRuntime ${v.maxRuntimeMs} >= interval ${v.intervalMs}`,
      );
      assert.ok(v.maxRuntimeMs > 0, `interval=${interval}: maxRuntime is 0`);
    }
  });

  it("respects explicit maxRuntime", () => {
    const v = validateLane("custom", {
      ...ok,
      interval: "1h",
      maxRuntime: "5m",
    });
    assert.equal(v.maxRuntimeMs, 300_000);
  });

  it("rejects maxRuntime >= interval", () => {
    assert.throws(
      () => validateLane("bad", { ...ok, interval: "5m", maxRuntime: "5m" }),
      /must be less than interval/,
    );
    assert.throws(
      () => validateLane("bad", { ...ok, interval: "5m", maxRuntime: "10m" }),
      /must be less than interval/,
    );
  });

  it("rejects empty tables", () => {
    assert.throws(
      () => validateLane("empty", { tables: [], interval: "1h" }),
      /non-empty array/,
    );
  });

  it("rejects malformed table entries", () => {
    assert.throws(
      () =>
        validateLane("bad", {
          // biome-ignore lint/suspicious/noExplicitAny: testing runtime guard
          tables: [{ params: {} } as any],
          interval: "1h",
        }),
      /tables\[0\]/,
    );
  });

  it("rejects bad lane names", () => {
    assert.throws(() => validateLane("", ok), /non-empty string/);
    assert.throws(() => validateLane("has space", ok), /must match/);
    assert.throws(() => validateLane("-leading", ok), /must match/);
    assert.throws(() => validateLane("has/slash", ok), /must match/);
  });

  it("accepts conventional lane names", () => {
    for (const n of ["fast", "slow", "lane-1", "lane_1", "Lane1", "a"]) {
      assert.doesNotThrow(() => validateLane(n, ok));
    }
  });

  it("invalid interval surfaces from validateLane", () => {
    assert.throws(
      () => validateLane("x", { ...ok, interval: "nope" }),
      /invalid interval/,
    );
  });
});

describe("validateLanes", () => {
  it("validates all lanes and preserves order", () => {
    const lanes = validateLanes({
      fast: { tables: [{ name: "a" }], interval: "15m" },
      slow: { tables: [{ name: "b" }], interval: "6h" },
    });
    assert.equal(lanes.length, 2);
    assert.deepEqual(
      lanes.map((l) => l.name),
      ["fast", "slow"],
    );
  });

  it("throws on first invalid lane", () => {
    assert.throws(
      () =>
        validateLanes({
          good: { tables: [{ name: "a" }], interval: "15m" },
          bad: { tables: [{ name: "b" }], interval: "nope" },
        }),
      /invalid interval/,
    );
  });

  it("returns empty array for empty input", () => {
    assert.deepEqual(validateLanes({}), []);
  });
});

describe("naming conventions", () => {
  it("laneLeaseName is stable", () => {
    assert.equal(laneLeaseName("fast"), "lane-fast");
    assert.equal(laneLeaseName("lane-1"), "lane-lane-1");
  });

  it("laneStatePath is stable", () => {
    assert.equal(laneStatePath("fast"), "_state/fast/_dripline_sync.parquet");
  });

  it("laneSchema converts dashes to underscores", () => {
    assert.equal(laneSchema("fast"), "lane_fast");
    assert.equal(laneSchema("lane-1"), "lane_lane_1");
    assert.equal(laneSchema("a-b-c"), "lane_a_b_c");
  });
});
