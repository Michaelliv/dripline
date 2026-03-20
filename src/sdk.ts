import { QueryEngine } from "./engine.js";
import { PluginRegistry } from "./plugin/registry.js";
import { QueryCache } from "./cache.js";
import { RateLimiter } from "./rate-limiter.js";
import type { PluginDef, ConnectionConfig, RateLimitConfig } from "./plugin/types.js";
import type { PluginFunction } from "./plugin/api.js";
import { resolvePluginExport } from "./plugin/api.js";
import type { DriplineConfig } from "./config/types.js";
import { DEFAULT_CONFIG } from "./config/types.js";

export interface DriplineOptions {
  /** Plugins to register (function-based or static objects) */
  plugins?: Array<PluginDef | PluginFunction>;
  /** Connection configs for API auth */
  connections?: ConnectionConfig[];
  /** Cache settings */
  cache?: { enabled?: boolean; ttl?: number; maxSize?: number };
  /** Per-plugin rate limits */
  rateLimits?: Record<string, RateLimitConfig>;
}

export class Dripline {
  private engine: QueryEngine;
  private registry: PluginRegistry;
  private cache: QueryCache;
  private rateLimiter: RateLimiter;

  constructor(options: DriplineOptions = {}) {
    this.registry = new PluginRegistry();
    this.cache = new QueryCache({
      enabled: options.cache?.enabled ?? true,
      ttl: options.cache?.ttl ?? 300,
      maxSize: options.cache?.maxSize ?? 1000,
    });
    this.rateLimiter = new RateLimiter();

    for (const pluginOrFn of options.plugins ?? []) {
      const plugin = resolvePluginExport(pluginOrFn, "unknown");
      this.registry.register(plugin);
    }

    const config: DriplineConfig = {
      connections: options.connections ?? [],
      cache: {
        enabled: options.cache?.enabled ?? DEFAULT_CONFIG.cache.enabled,
        ttl: options.cache?.ttl ?? DEFAULT_CONFIG.cache.ttl,
        maxSize: options.cache?.maxSize ?? DEFAULT_CONFIG.cache.maxSize,
      },
      rateLimits: options.rateLimits ?? {},
    };

    this.engine = new QueryEngine(this.registry, this.cache, this.rateLimiter);
    this.engine.initialize(config);
  }

  /** Execute a SQL query and return rows. */
  query<T = Record<string, any>>(sql: string, params?: any[]): T[] {
    return this.engine.query(sql, params) as T[];
  }

  /** Register an additional plugin after construction. Re-initializes the engine. */
  addPlugin(pluginOrFn: PluginDef | PluginFunction, connections?: ConnectionConfig[]): void {
    const plugin = resolvePluginExport(pluginOrFn, "unknown");
    this.registry.register(plugin);
    const config: DriplineConfig = {
      connections: connections ?? [],
      cache: {
        enabled: true,
        ttl: 300,
        maxSize: 1000,
      },
      rateLimits: {},
    };
    this.engine.close();
    this.engine = new QueryEngine(this.registry, this.cache, this.rateLimiter);
    this.engine.initialize(config);
  }

  /** Get cache statistics. */
  cacheStats(): { size: number; hits: number; misses: number } {
    return this.cache.stats();
  }

  /** Clear the query cache. */
  clearCache(): void {
    this.cache.clear();
  }

  /** List all available tables across all plugins. */
  tables(): Array<{ plugin: string; table: string; description?: string }> {
    return this.registry.getAllTables().map(({ plugin, table }) => ({
      plugin,
      table: table.name,
      description: table.description,
    }));
  }

  /** List registered plugins. */
  plugins(): Array<{ name: string; version: string; tables: string[] }> {
    return this.registry.listPlugins().map((p) => ({
      name: p.name,
      version: p.version,
      tables: p.tables.map((t) => t.name),
    }));
  }

  /** Close the database. Call when done. */
  close(): void {
    this.engine.close();
  }
}
