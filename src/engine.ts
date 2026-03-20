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
import { resolveEnvConnection } from "./config/loader.js";

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
    for (const [scope, rl] of Object.entries(config.rateLimits)) {
      this.rateLimiter.configure(scope, rl);
    }

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

    const columns = table.columns.map((c) => c.name);

    const keyColNames = (table.keyColumns ?? []).map((k) => k.name);
    const visibleColumns = columns.filter((c) => !keyColNames.includes(c));
    const parameters = [...keyColNames, "_connection"];

    this.db.table(table.name, {
      columns: visibleColumns,
      parameters,
      *rows(...paramValues: any[]) {
        const quals: Qual[] = [];
        const paramMap: Record<string, any> = {};

        for (let i = 0; i < keyColNames.length; i++) {
          const val = paramValues[i];
          if (val !== undefined && val !== null) {
            quals.push({ column: keyColNames[i], operator: "=", value: val });
            paramMap[keyColNames[i]] = val;
          }
        }

        const connName = paramValues[paramValues.length - 1] as
          | string
          | undefined;

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
          connection = resolveEnvConnection(pluginName) ?? {
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

        const cacheKey = engine.cache.getCacheKey(table.name, quals, visibleColumns);
        const cached = engine.cache.get<Record<string, any>>(cacheKey);
        if (cached) {
          for (const row of cached) {
            yield row;
          }
          return;
        }

        engine.rateLimiter.acquireSync(pluginName);

        let rows: Record<string, any>[] = [];

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

        engine.cache.set(cacheKey, rows);

        engine.rateLimiter.release(pluginName);

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
