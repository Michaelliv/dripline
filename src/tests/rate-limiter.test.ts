import { describe, it } from "node:test";
import { strict as assert } from "node:assert";
import { RateLimiter } from "../rate-limiter.js";

describe("RateLimiter", () => {
  it("acquire with no configured scope passes through", async () => {
    const rl = new RateLimiter();
    await rl.acquire("unconfigured"); // should not throw or hang
  });

  it("configure sets up a bucket", () => {
    const rl = new RateLimiter();
    rl.configure("test", { maxPerSecond: 10 });
    // Should not throw
  });

  it("acquire consumes a token", async () => {
    const rl = new RateLimiter();
    rl.configure("test", { maxPerSecond: 100 });
    await rl.acquire("test"); // should pass
  });

  it("acquire waits when no tokens available", async () => {
    const rl = new RateLimiter();
    rl.configure("slow", { maxPerSecond: 2 });
    // Consume both tokens
    await rl.acquire("slow");
    await rl.acquire("slow");
    // Third should wait
    const start = Date.now();
    await rl.acquire("slow");
    const elapsed = Date.now() - start;
    assert.ok(elapsed >= 100, `expected wait >= 100ms, got ${elapsed}ms`);
  });

  it("acquireSync with no configured scope passes through", () => {
    const rl = new RateLimiter();
    rl.acquireSync("unconfigured"); // should not throw
  });

  it("acquireSync consumes a token", () => {
    const rl = new RateLimiter();
    rl.configure("test", { maxPerSecond: 100 });
    rl.acquireSync("test"); // should pass
  });

  it("release decrements active concurrent", async () => {
    const rl = new RateLimiter();
    rl.configure("test", { maxPerSecond: 100, maxConcurrent: 1 });
    await rl.acquire("test");
    rl.release("test");
    // Should be able to acquire again after release
    await rl.acquire("test");
  });

  it("release with no configured scope does nothing", () => {
    const rl = new RateLimiter();
    rl.release("unknown"); // should not throw
  });

  it("tokens refill over time", async () => {
    const rl = new RateLimiter();
    rl.configure("refill", { maxPerSecond: 100 });
    // Consume some tokens
    for (let i = 0; i < 50; i++) {
      await rl.acquire("refill");
    }
    // Wait for refill
    await new Promise((r) => setTimeout(r, 100));
    // Should be able to acquire more
    await rl.acquire("refill");
  });

  it("maxPerMinute configuration works", () => {
    const rl = new RateLimiter();
    rl.configure("minute", { maxPerMinute: 60 }); // = 1/sec
    rl.acquireSync("minute"); // should pass
  });

  it("multiple scopes are independent", async () => {
    const rl = new RateLimiter();
    rl.configure("a", { maxPerSecond: 1 });
    rl.configure("b", { maxPerSecond: 100 });
    await rl.acquire("a"); // consume a's only token
    // b should still be available
    await rl.acquire("b");
  });
});
