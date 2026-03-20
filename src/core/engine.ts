import { Database } from "duckdb-async";
import { applyEnvOverrides, resolveEnvConnection } from "../config/loader.js";
import type { DriplineConfig } from "../config/types.js";
import type { PluginRegistry } from "../plugin/registry.js";
import type {
  ConnectionConfig,
  Qual,
  QueryContext,
  TableDef,
} from "../plugin/types.js";
import { QueryCache } from "./cache.js";
import { RateLimiter } from "./rate-limiter.js";

interface RegisteredTable {
  pluginName: string;
  table: TableDef;
  connections: ConnectionConfig[];
  schema?: Record<string, { env?: string }>;
  keyColNames: string[];
  allColumns: string[];
}

export class QueryEngine {
  private db!: Database;
  private registry: PluginRegistry;
  private cache: QueryCache;
  private rateLimiter: RateLimiter;
  private tables: Map<string, RegisteredTable> = new Map();

  constructor(
    registry: PluginRegistry,
    cache: QueryCache,
    rateLimiter: RateLimiter,
  ) {
    this.registry = registry;
    this.cache = cache;
    this.rateLimiter = rateLimiter;
  }

  async initialize(config: DriplineConfig): Promise<void> {
    this.db = await Database.create(":memory:");

    for (const [scope, rl] of Object.entries(config.rateLimits)) {
      this.rateLimiter.configure(scope, rl);
    }

    for (const { plugin, table } of this.registry.getAllTables()) {
      const connections = config.connections.filter((c) => c.plugin === plugin);
      const pluginDef = this.registry.getPlugin(plugin);
      await this.registerTable(
        plugin,
        table,
        connections,
        pluginDef?.connectionConfigSchema,
      );
    }
  }

  private async registerTable(
    pluginName: string,
    table: TableDef,
    connections: ConnectionConfig[],
    schema?: Record<string, { env?: string }>,
  ): Promise<void> {
    const keyColNames = (table.keyColumns ?? []).map((k) => k.name);
    const allColumns = [...table.columns.map((c) => c.name), ...keyColNames];
    const uniqueColumns = [...new Set(allColumns)];

    const colDefs = uniqueColumns.map((name) => {
      const col = table.columns.find((c) => c.name === name);
      const duckType = col ? toDuckType(col.type) : "VARCHAR";
      return `"${name}" ${duckType}`;
    });

    await this.db.run(`CREATE TABLE "${table.name}" (${colDefs.join(", ")})`);

    this.tables.set(table.name, {
      pluginName,
      table,
      connections,
      schema,
      keyColNames,
      allColumns: uniqueColumns,
    });
  }

  private resolveConnection(
    reg: RegisteredTable,
    connName?: string,
  ): ConnectionConfig {
    const { pluginName, connections, schema } = reg;
    let connection: ConnectionConfig;
    if (connName) {
      const found = connections.find((c) => c.name === connName);
      connection = found ?? { name: connName, plugin: pluginName, config: {} };
    } else if (connections.length === 1) {
      connection = connections[0];
    } else {
      connection = resolveEnvConnection(pluginName, schema) ?? {
        name: "default",
        plugin: pluginName,
        config: {},
      };
    }
    return applyEnvOverrides(connection, schema);
  }

  private extractQuals(sql: string, keyColNames: string[]): Qual[] {
    const quals: Qual[] = [];
    for (const col of keyColNames) {
      const patterns = [
        new RegExp(`${col}\\s*=\\s*'([^']*)'`, "i"),
        new RegExp(`${col}\\s*=\\s*"([^"]*)"`, "i"),
        new RegExp(`${col}\\s*=\\s*(\\d+)`, "i"),
      ];
      for (const p of patterns) {
        const m = sql.match(p);
        if (m) {
          quals.push({ column: col, operator: "=", value: m[1] });
          break;
        }
      }
    }
    return quals;
  }

  private async populateTable(
    reg: RegisteredTable,
    quals: Qual[],
  ): Promise<void> {
    const { table, pluginName } = reg;
    const visibleColumns = table.columns.map((c) => c.name);

    const cacheKey = this.cache.getCacheKey(table.name, quals, visibleColumns);
    const cached = this.cache.get<Record<string, any>>(cacheKey);

    let rows: Record<string, any>[];

    if (cached) {
      rows = cached;
    } else {
      const connection = this.resolveConnection(reg);
      const ctx: QueryContext = { connection, quals, columns: visibleColumns };

      this.rateLimiter.acquireSync(pluginName);

      rows = [];
      const allKeyColumns = table.keyColumns ?? [];
      const allKeysProvided =
        allKeyColumns.length > 0 &&
        allKeyColumns.every((k) =>
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

      this.cache.set(cacheKey, rows);
      this.rateLimiter.release(pluginName);
    }

    await this.db.run(`DELETE FROM "${table.name}"`);

    if (rows.length === 0) return;

    const cols = reg.allColumns;
    const placeholders = cols.map(() => "?").join(", ");
    const insertSql = `INSERT INTO "${table.name}" (${cols.map((c) => `"${c}"`).join(", ")}) VALUES (${placeholders})`;

    const qualMap: Record<string, any> = {};
    for (const q of quals) {
      qualMap[q.column] = q.value;
    }

    for (const row of rows) {
      const values = cols.map((c) => {
        const v = row[c] ?? qualMap[c];
        if (v === undefined || v === null) return null;
        if (typeof v === "object") return JSON.stringify(v);
        return v;
      });
      await this.db.run(insertSql, ...values);
    }
  }

  async query(sql: string, params?: any[]): Promise<any[]> {
    const referencedTables: RegisteredTable[] = [];
    for (const [name, reg] of this.tables) {
      if (sql.includes(name)) {
        referencedTables.push(reg);
      }
    }

    for (const reg of referencedTables) {
      const quals = this.extractQuals(sql, reg.keyColNames);
      await this.populateTable(reg, quals);
    }

    let rows: Record<string, any>[];
    if (params && params.length > 0) {
      rows = await this.db.all(sql, ...params);
    } else {
      rows = await this.db.all(sql);
    }

    return rows.map(normalizeRow);
  }

  getDatabase(): Database {
    return this.db;
  }

  async close(): Promise<void> {
    await this.db.close();
  }
}

function toDuckType(type: string): string {
  switch (type) {
    case "number":
      return "DOUBLE";
    case "boolean":
      return "BOOLEAN";
    case "json":
      return "JSON";
    case "datetime":
      return "VARCHAR";
    default:
      return "VARCHAR";
  }
}

function normalizeRow(row: Record<string, any>): Record<string, any> {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(row)) {
    out[k] = typeof v === "bigint" ? Number(v) : v;
  }
  return out;
}

export async function createEngine(
  config: DriplineConfig,
  reg: PluginRegistry,
): Promise<QueryEngine> {
  const cache = new QueryCache({
    enabled: config.cache.enabled,
    ttl: config.cache.ttl,
    maxSize: config.cache.maxSize,
  });
  const rl = new RateLimiter();
  const engine = new QueryEngine(reg, cache, rl);
  await engine.initialize(config);
  return engine;
}
