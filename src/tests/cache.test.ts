import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { QueryCache } from "../core/cache.js";

function sleep(ms: number) {
  const end = Date.now() + ms;
  while (Date.now() < end) {}
}

describe("QueryCache", () => {
  it("constructor defaults", () => {
    const c = new QueryCache();
    assert.deepEqual(c.stats(), { size: 0, hits: 0, misses: 0 });
    // enabled by default — set+get should work
    c.set("k", [{ a: 1 }]);
    assert.deepEqual(c.get("k"), [{ a: 1 }]);
  });

  it("constructor with custom opts", () => {
    const c = new QueryCache({ enabled: false, ttl: 10, maxSize: 5 });
    c.set("k", [{ a: 1 }]);
    assert.equal(c.get("k"), null); // disabled
  });

  it("set + get returns data", () => {
    const c = new QueryCache();
    c.set("x", [{ id: 1 }, { id: 2 }]);
    assert.deepEqual(c.get("x"), [{ id: 1 }, { id: 2 }]);
  });

  it("get returns null after TTL expires", () => {
    const c = new QueryCache({ ttl: 0 }); // 0 seconds TTL
    c.set("k", [{ a: 1 }], 0); // 0 TTL
    sleep(5);
    assert.equal(c.get("k"), null);
  });

  it("get returns null when cache disabled", () => {
    const c = new QueryCache({ enabled: false });
    c.set("k", [{ a: 1 }]);
    assert.equal(c.get("k"), null);
  });

  it("set when disabled does nothing", () => {
    const c = new QueryCache({ enabled: false });
    c.set("k", [{ a: 1 }]);
    assert.equal(c.stats().size, 0);
  });

  it("getCacheKey is deterministic", () => {
    const c = new QueryCache();
    const k1 = c.getCacheKey(
      "t",
      [{ column: "a", operator: "=", value: 1 }],
      ["x", "y"],
    );
    const k2 = c.getCacheKey(
      "t",
      [{ column: "a", operator: "=", value: 1 }],
      ["x", "y"],
    );
    assert.equal(k1, k2);
  });

  it("getCacheKey sorts quals and columns", () => {
    const c = new QueryCache();
    const k1 = c.getCacheKey(
      "t",
      [
        { column: "b", operator: "=", value: 2 },
        { column: "a", operator: "=", value: 1 },
      ],
      ["y", "x"],
    );
    const k2 = c.getCacheKey(
      "t",
      [
        { column: "a", operator: "=", value: 1 },
        { column: "b", operator: "=", value: 2 },
      ],
      ["x", "y"],
    );
    assert.equal(k1, k2);
  });

  it("getCacheKey different quals produce different keys", () => {
    const c = new QueryCache();
    const k1 = c.getCacheKey(
      "t",
      [{ column: "a", operator: "=", value: 1 }],
      ["x"],
    );
    const k2 = c.getCacheKey(
      "t",
      [{ column: "a", operator: "=", value: 2 }],
      ["x"],
    );
    assert.notEqual(k1, k2);
  });

  it("maxSize eviction", () => {
    const c = new QueryCache({ maxSize: 3 });
    c.set("a", [1]);
    c.set("b", [2]);
    c.set("c", [3]);
    c.set("d", [4]); // should evict 'a'
    assert.equal(c.get("a"), null);
    assert.deepEqual(c.get("b"), [2]);
    assert.equal(c.stats().size, 3);
  });

  it("invalidate(tableName) clears only matching entries", () => {
    const c = new QueryCache();
    c.set(c.getCacheKey("users", [], ["id"]), [{ id: 1 }]);
    c.set(c.getCacheKey("orders", [], ["id"]), [{ id: 2 }]);
    c.invalidate("users");
    assert.equal(c.stats().size, 1);
  });

  it("invalidate() with no arg clears everything", () => {
    const c = new QueryCache();
    c.set("a", [1]);
    c.set("b", [2]);
    c.invalidate();
    assert.equal(c.stats().size, 0);
  });

  it("clear() resets everything including counters", () => {
    const c = new QueryCache();
    c.set("k", [1]);
    c.get("k"); // hit
    c.get("miss"); // miss
    c.clear();
    assert.deepEqual(c.stats(), { size: 0, hits: 0, misses: 0 });
  });

  it("stats() tracks hits", () => {
    const c = new QueryCache();
    c.set("k", [1]);
    c.get("k");
    c.get("k");
    assert.equal(c.stats().hits, 2);
  });

  it("stats() tracks misses", () => {
    const c = new QueryCache();
    c.get("nope");
    c.get("nada");
    assert.equal(c.stats().misses, 2);
  });

  it("stats() tracks size", () => {
    const c = new QueryCache();
    c.set("a", [1]);
    c.set("b", [2]);
    assert.equal(c.stats().size, 2);
  });

  it("get on expired entry removes it", () => {
    const c = new QueryCache({ ttl: 0 });
    c.set("k", [1], 0);
    assert.equal(c.stats().size, 1);
    sleep(5);
    c.get("k"); // should remove
    assert.equal(c.stats().size, 0);
  });
});
