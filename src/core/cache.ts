import type { CacheEntry, Qual } from "../plugin/types.js";

export class QueryCache {
  private cache: Map<string, CacheEntry<any>> = new Map();
  private maxSize: number;
  private defaultTtl: number;
  private enabled: boolean;
  private hits = 0;
  private misses = 0;

  constructor(
    opts: { enabled?: boolean; ttl?: number; maxSize?: number } = {},
  ) {
    this.enabled = opts.enabled ?? true;
    this.defaultTtl = opts.ttl ?? 300;
    this.maxSize = opts.maxSize ?? 1000;
  }

  getCacheKey(tableName: string, quals: Qual[], columns: string[]): string {
    const sortedQuals = [...quals].sort((a, b) =>
      a.column === b.column
        ? a.operator.localeCompare(b.operator)
        : a.column.localeCompare(b.column),
    );
    const sortedCols = [...columns].sort();
    return JSON.stringify({ t: tableName, q: sortedQuals, c: sortedCols });
  }

  get<T>(key: string): T[] | null {
    if (!this.enabled) {
      this.misses++;
      return null;
    }
    const entry = this.cache.get(key);
    if (!entry) {
      this.misses++;
      return null;
    }
    if (Date.now() - entry.timestamp > entry.ttl * 1000) {
      this.cache.delete(key);
      this.misses++;
      return null;
    }
    this.hits++;
    return entry.data as T[];
  }

  set<T>(key: string, data: T[], ttl?: number): void {
    if (!this.enabled) return;
    if (this.cache.size >= this.maxSize) {
      const oldest = this.cache.keys().next().value;
      if (oldest !== undefined) this.cache.delete(oldest);
    }
    this.cache.set(key, {
      data,
      timestamp: Date.now(),
      ttl: ttl ?? this.defaultTtl,
    });
  }

  invalidate(tableName?: string): void {
    if (!tableName) {
      this.cache.clear();
      return;
    }
    for (const key of this.cache.keys()) {
      if (key.includes(`"t":"${tableName}"`)) {
        this.cache.delete(key);
      }
    }
  }

  clear(): void {
    this.cache.clear();
    this.hits = 0;
    this.misses = 0;
  }

  stats(): { size: number; hits: number; misses: number } {
    return { size: this.cache.size, hits: this.hits, misses: this.misses };
  }
}

export let queryCache = new QueryCache();

export function configureCache(opts: {
  enabled?: boolean;
  ttl?: number;
  maxSize?: number;
}): void {
  queryCache = new QueryCache(opts);
}
