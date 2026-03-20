import type { RateLimitConfig } from "../plugin/types.js";

interface TokenBucket {
  tokens: number;
  maxTokens: number;
  refillRate: number;
  lastRefill: number;
  maxConcurrent: number;
  activeConcurrent: number;
  queue: Array<{ resolve: () => void }>;
}

export class RateLimiter {
  private buckets: Map<string, TokenBucket> = new Map();

  configure(scope: string, config: RateLimitConfig): void {
    const rate =
      config.maxPerSecond ??
      (config.maxPerMinute ? config.maxPerMinute / 60 : 0);
    this.buckets.set(scope, {
      tokens: rate || 100,
      maxTokens: rate || 100,
      refillRate: rate || 100,
      lastRefill: Date.now(),
      maxConcurrent: config.maxConcurrent ?? Number.POSITIVE_INFINITY,
      activeConcurrent: 0,
      queue: [],
    });
  }

  async acquire(scope: string): Promise<void> {
    const bucket = this.buckets.get(scope);
    if (!bucket) return;

    this.refill(bucket);

    if (bucket.tokens >= 1 && bucket.activeConcurrent < bucket.maxConcurrent) {
      bucket.tokens -= 1;
      bucket.activeConcurrent++;
      return;
    }

    return new Promise<void>((resolve) => {
      bucket.queue.push({ resolve });
      const interval = setInterval(
        () => {
          this.refill(bucket);
          if (
            bucket.tokens >= 1 &&
            bucket.activeConcurrent < bucket.maxConcurrent
          ) {
            bucket.tokens -= 1;
            bucket.activeConcurrent++;
            clearInterval(interval);
            const idx = bucket.queue.findIndex((q) => q.resolve === resolve);
            if (idx >= 0) bucket.queue.splice(idx, 1);
            resolve();
          }
        },
        Math.max(10, Math.ceil(1000 / bucket.refillRate)),
      );
    });
  }

  acquireSync(scope: string): void {
    const bucket = this.buckets.get(scope);
    if (!bucket) return;

    this.refill(bucket);

    if (bucket.tokens >= 1 && bucket.activeConcurrent < bucket.maxConcurrent) {
      bucket.tokens -= 1;
      bucket.activeConcurrent++;
      return;
    }

    while (true) {
      this.refill(bucket);
      if (
        bucket.tokens >= 1 &&
        bucket.activeConcurrent < bucket.maxConcurrent
      ) {
        bucket.tokens -= 1;
        bucket.activeConcurrent++;
        return;
      }
      const waitUntil =
        Date.now() + Math.max(1, Math.ceil(1000 / bucket.refillRate));
      while (Date.now() < waitUntil) {}
    }
  }

  release(scope: string): void {
    const bucket = this.buckets.get(scope);
    if (!bucket) return;
    bucket.activeConcurrent = Math.max(0, bucket.activeConcurrent - 1);
  }

  private refill(bucket: TokenBucket): void {
    const now = Date.now();
    const elapsed = (now - bucket.lastRefill) / 1000;
    bucket.tokens = Math.min(
      bucket.maxTokens,
      bucket.tokens + elapsed * bucket.refillRate,
    );
    bucket.lastRefill = now;
  }
}

export const rateLimiter = new RateLimiter();
