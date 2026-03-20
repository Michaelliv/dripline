import Database from "better-sqlite3";
import type { DriplineConfig } from "./config/types.js";
import { QueryCache } from "./cache.js";
import { RateLimiter } from "./rate-limiter.js";
import { PluginRegistry } from "./plugin/registry.js";
import type {
  TableDef,
  ConnectionConfig,
  QueryContext,
  Qual,
} from "./plugin/types.js";

export class QueryEngine {
  private db: Database.Database;
  private registry: PluginRegistry;
  private cache: QueryCache;
  private rateLimiter: RateLimiter;

  constructor(
    registry: PluginRegistry,
    cache: QueryCache,
    rateLimiter: RateLimiter,
  ) {
    this.db = new Database(":memory:");
    this.registry = registry;
    this.cache = cache;
    this.rateLimiter = rateLimiter;
  }

  initialize(config: DriplineConfig): void {
    // Configure rate limits
    for (const [scope, rl] of Object.entries(config.rateLimits)) {
      this.rateLimiter.configure(scope, rl);
    }

    // Register all tables from all plugins
    for (const { plugin, table } of this.registry.getAllTables()) {
      const connections = config.connections.filter((c) => c.plugin === plugin);
      this.registerTable(plugin, table, connections);
    }
  }

  private registerTable(
    pluginName: string,
    table: TableDef,
    connections: ConnectionConfig[],
  ): void {
    const engine = this;

    // Build column list for better-sqlite3
    const columns = table.columns.map((c) => c.name);

    // Key columns become hidden parameters
    const keyColNames = (table.keyColumns ?? []).map((k) => k.name);
    // Filter out key columns from visible columns (they'll be parameters)
    const visibleColumns = columns.filter((c) => !keyColNames.includes(c));
    // Always add connection as a parameter
    const parameters = [...keyColNames, "_connection"];

    this.db.table(table.name, {
      columns: visibleColumns,
      parameters,
      *rows(...paramValues: any[]) {
        // Map parameter values to key column quals
        const quals: Qual[] = [];
        const paramMap: Record<string, any> = {};

        for (let i = 0; i < keyColNames.length; i++) {
          const val = paramValues[i];
          if (val !== undefined && val !== null) {
            quals.push({ column: keyColNames[i], operator: "=", value: val });
            paramMap[keyColNames[i]] = val;
          }
        }

        // Connection parameter is last
        const connName = paramValues[paramValues.length - 1] as
          | string
          | undefined;

        // Resolve connection
        let connection: ConnectionConfig;
        if (connName) {
          const found = connections.find((c) => c.name === connName);
          connection = found ?? {
            name: connName,
            plugin: pluginName,
            config: {},
          };
        } else if (connections.length === 1) {
          connection = connections[0];
        } else {
          connection = {
            name: "default",
            plugin: pluginName,
            config: {},
          };
        }

        const ctx: QueryContext = {
          connection,
          quals,
          columns: visibleColumns,
        };

        // Check cache
        const cacheKey = engine.cache.getCacheKey(table.name, quals, visibleColumns);
        const cached = engine.cache.get<Record<string, any>>(cacheKey);
        if (cached) {
          for (const row of cached) {
            yield row;
          }
          return;
        }

        // Rate limit
        engine.rateLimiter.acquireSync(pluginName);

        // Decide get vs list
        let rows: Record<string, any>[] = [];

        // Use get only when ALL key columns have quals
        const allKeyColumns = table.keyColumns ?? [];
        const allKeysProvided = allKeyColumns.length > 0 && allKeyColumns.every((k) =>
          quals.some((q) => q.column === k.name && q.operator === "="),
        );

        let usedGet = false;
        if (table.get && allKeysProvided) {
          const result = table.get(ctx);
          if (result) {
            rows = [result];
            usedGet = true;
          }
        }
        if (!usedGet) {
          for (const row of table.list(ctx)) {
            rows.push(row);
          }
        }

        // Hydrate if needed
        if (table.hydrate) {
          for (let i = 0; i < rows.length; i++) {
            for (const [colName, hydrateFn] of Object.entries(table.hydrate)) {
              if (visibleColumns.includes(colName)) {
                const extra = hydrateFn(ctx, rows[i]);
                rows[i] = { ...rows[i], ...extra };
              }
            }
          }
        }

        // Cache results
        engine.cache.set(cacheKey, rows);

        // Release rate limiter
        engine.rateLimiter.release(pluginName);

        // Yield rows, picking only declared columns
        for (const row of rows) {
          const out: Record<string, any> = {};
          for (const col of visibleColumns) {
            out[col] = row[col] ?? null;
          }
          yield out;
        }
      },
    });
  }

  query(sql: string, params?: any[]): any[] {
    const stmt = this.db.prepare(sql);
    if (params && params.length > 0) {
      return stmt.all(...params);
    }
    return stmt.all();
  }

  getDatabase(): Database.Database {
    return this.db;
  }

  close(): void {
    this.db.close();
  }
}

export function createEngine(
  config: DriplineConfig,
  reg: PluginRegistry,
): QueryEngine {
  const cache = new QueryCache({
    enabled: config.cache.enabled,
    ttl: config.cache.ttl,
    maxSize: config.cache.maxSize,
  });
  const rl = new RateLimiter();
  const engine = new QueryEngine(reg, cache, rl);
  engine.initialize(config);
  return engine;
}
