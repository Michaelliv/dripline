import type { DriplineConfig } from "./config/types.js";
import { DEFAULT_CONFIG } from "./config/types.js";
import { QueryCache } from "./core/cache.js";
import { QueryEngine } from "./core/engine.js";
import { RateLimiter } from "./core/rate-limiter.js";
import type { PluginFunction } from "./plugin/api.js";
import { resolvePluginExport } from "./plugin/api.js";
import { PluginRegistry } from "./plugin/registry.js";
import type {
  ConnectionConfig,
  PluginDef,
  RateLimitConfig,
} from "./plugin/types.js";

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
  private engine!: QueryEngine;
  private registry: PluginRegistry;
  private cache: QueryCache;
  private rateLimiter: RateLimiter;
  private options: DriplineOptions;

  private constructor(options: DriplineOptions) {
    this.options = options;
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
  }

  static async create(options: DriplineOptions = {}): Promise<Dripline> {
    const dl = new Dripline(options);
    await dl.init();
    return dl;
  }

  private async init(): Promise<void> {
    const config: DriplineConfig = {
      connections: this.options.connections ?? [],
      cache: {
        enabled: this.options.cache?.enabled ?? DEFAULT_CONFIG.cache.enabled,
        ttl: this.options.cache?.ttl ?? DEFAULT_CONFIG.cache.ttl,
        maxSize: this.options.cache?.maxSize ?? DEFAULT_CONFIG.cache.maxSize,
      },
      rateLimits: this.options.rateLimits ?? {},
    };

    this.engine = new QueryEngine(this.registry, this.cache, this.rateLimiter);
    await this.engine.initialize(config);
  }

  /** Execute a SQL query and return rows. */
  async query<T = Record<string, any>>(
    sql: string,
    params?: any[],
  ): Promise<T[]> {
    return this.engine.query(sql, params) as Promise<T[]>;
  }

  /** Register an additional plugin. Re-initializes the engine. */
  async addPlugin(
    pluginOrFn: PluginDef | PluginFunction,
    connections?: ConnectionConfig[],
  ): Promise<void> {
    const plugin = resolvePluginExport(pluginOrFn, "unknown");
    this.registry.register(plugin);
    if (this.engine) await this.engine.close();
    const config: DriplineConfig = {
      connections: connections ?? this.options.connections ?? [],
      cache: {
        enabled: true,
        ttl: 300,
        maxSize: 1000,
      },
      rateLimits: {},
    };
    this.engine = new QueryEngine(this.registry, this.cache, this.rateLimiter);
    await this.engine.initialize(config);
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

  /** Close the database. */
  async close(): Promise<void> {
    await this.engine.close();
  }
}
