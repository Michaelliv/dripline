import { Database } from "duckdb-async";
import * as arrow from "apache-arrow";
import { applyEnvOverrides, resolveEnvConnection } from "../config/loader.js";
import type { DriplineConfig } from "../config/types.js";
import type { PluginRegistry } from "../plugin/registry.js";
import type {
  ColumnType,
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
    await this.db.exec("INSTALL arrow FROM community; LOAD arrow;");

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

  private async parseAst(sql: string): Promise<any> {
    const escaped = sql.replace(/'/g, "''");
    const result = await this.db.all(
      `SELECT json_serialize_sql('${escaped}') as ast`,
    );
    return JSON.parse(result[0].ast);
  }

  private extractQualsForTable(
    ast: any,
    tableName: string,
    keyColNames: string[],
  ): Qual[] {
    const keySet = new Set(keyColNames);
    const quals: Qual[] = [];

    // Find all SELECT nodes that reference the target table and collect their WHERE quals
    const findSelects = (node: any): void => {
      if (!node || typeof node !== "object") return;

      if (node.type === "SELECT_NODE") {
        const referencesTable = this.selectReferencesTable(node, tableName);
        if (referencesTable && node.where_clause) {
          this.walkWhere(node.where_clause, keySet, quals);
        }
      }

      // Recurse into all object/array values to find nested SELECTs (subqueries, CTEs)
      for (const val of Object.values(node)) {
        if (Array.isArray(val)) {
          for (const item of val) findSelects(item);
        } else if (val && typeof val === "object") {
          findSelects(val);
        }
      }
    };

    if (!ast.error && ast.statements?.[0]?.node) {
      findSelects(ast.statements[0].node);
    }
    return quals;
  }

  private selectReferencesTable(selectNode: any, tableName: string): boolean {
    const walk = (from: any): boolean => {
      if (!from) return false;
      if (from.table_name === tableName) return true;
      if (from.type === "JOIN") return walk(from.left) || walk(from.right);
      return false;
    };
    return walk(selectNode.from_table);
  }

  private static readonly COMPARISON_MAP: Record<string, string> = {
    COMPARE_EQUAL: "=",
    COMPARE_NOTEQUAL: "!=",
    COMPARE_GREATERTHAN: ">",
    COMPARE_LESSTHAN: "<",
    COMPARE_GREATERTHANOREQUALTO: ">=",
    COMPARE_LESSTHANOREQUALTO: "<=",
  };

  private static readonly FUNCTION_MAP: Record<string, string> = {
    "~~": "LIKE",
    "!~~": "NOT LIKE",
    "~~*": "ILIKE",
    "!~~*": "NOT ILIKE",
  };

  private walkWhere(node: any, keySet: Set<string>, quals: Qual[]): void {
    if (!node) return;

    // AND/OR — recurse into children
    if (node.class === "CONJUNCTION") {
      for (const child of node.children ?? []) this.walkWhere(child, keySet, quals);
      return;
    }

    // NOT — recurse into child
    if (node.type === "OPERATOR_NOT") {
      for (const child of node.children ?? []) this.walkWhere(child, keySet, quals);
      return;
    }

    // Simple comparisons: =, !=, >, <, >=, <=
    if (node.class === "COMPARISON") {
      const op = QueryEngine.COMPARISON_MAP[node.type];
      const colName = node.left?.column_names?.at(-1);
      if (op && colName && keySet.has(colName)) {
        quals.push({
          column: colName,
          operator: op,
          value: extractAstValue(node.right?.value),
        });
      }
      return;
    }

    // BETWEEN
    if (node.class === "BETWEEN") {
      const colName = node.input?.column_names?.at(-1);
      if (colName && keySet.has(colName)) {
        quals.push({
          column: colName,
          operator: "BETWEEN",
          value: [extractAstValue(node.lower?.value), extractAstValue(node.upper?.value)],
        });
      }
      return;
    }

    // IN / NOT IN
    if (node.type === "COMPARE_IN" || node.type === "COMPARE_NOT_IN") {
      const colName = node.children?.[0]?.column_names?.at(-1);
      if (colName && keySet.has(colName)) {
        const values = node.children
          .slice(1)
          .filter((c: any) => c.class === "CONSTANT")
          .map((c: any) => extractAstValue(c.value));
        if (values.length > 0) {
          quals.push({
            column: colName,
            operator: node.type === "COMPARE_IN" ? "IN" : "NOT IN",
            value: values,
          });
        }
      }
      return;
    }

    // IS NULL / IS NOT NULL
    if (node.type === "OPERATOR_IS_NULL" || node.type === "OPERATOR_IS_NOT_NULL") {
      const colName = node.children?.[0]?.column_names?.at(-1);
      if (colName && keySet.has(colName)) {
        quals.push({
          column: colName,
          operator: node.type === "OPERATOR_IS_NULL" ? "IS NULL" : "IS NOT NULL",
          value: null,
        });
      }
      return;
    }

    // LIKE / ILIKE / NOT LIKE / NOT ILIKE
    if (node.class === "FUNCTION" && node.function_name in QueryEngine.FUNCTION_MAP) {
      const colName = node.children?.[0]?.column_names?.at(-1);
      if (colName && keySet.has(colName)) {
        quals.push({
          column: colName,
          operator: QueryEngine.FUNCTION_MAP[node.function_name],
          value: extractAstValue(node.children?.[1]?.value),
        });
      }
      return;
    }
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

    await this.ingestViaArrow(reg, table, rows, quals);
  }

  private async ingestViaArrow(
    reg: RegisteredTable,
    table: TableDef,
    rows: Record<string, any>[],
    quals: Qual[],
  ): Promise<void> {
    await this.db.run(`DELETE FROM "${table.name}"`);

    if (rows.length === 0) return;

    const colTypes = new Map(table.columns.map((c) => [c.name, c.type]));
    const qualMap = Object.fromEntries(quals.map((q) => [q.column, q.value]));

    const arrowCols: Record<string, arrow.Vector> = {};
    for (const col of reg.allColumns) {
      const values = rows.map((row) => {
        const v = row[col] ?? qualMap[col];
        if (v == null) return null;
        return typeof v === "object" ? JSON.stringify(v) : v;
      });
      const colType = colTypes.get(col) ?? "string";
      // DuckDB's Arrow scanner crashes on all-null Bool buffers (0-byte data buffer).
      // Fall back to Utf8 — DuckDB casts NULL varchar to NULL boolean on INSERT.
      const allNull = colType === "boolean" && values.every((v) => v == null);
      arrowCols[col] = arrow.vectorFromArray(
        values,
        allNull ? new arrow.Utf8() : toArrowType(colType),
      );
    }

    const ipc = arrow.tableToIPC(new arrow.Table(arrowCols), "stream");
    const buf = `_dripline_buf_${table.name}`;
    await this.db.register_buffer(buf, [ipc], true);
    await this.db.run(`INSERT INTO "${table.name}" SELECT * FROM ${buf}`);
    await this.db.unregister_buffer(buf);
  }

  async query(sql: string, params?: any[]): Promise<any[]> {
    const referencedTables: RegisteredTable[] = [];
    for (const [name, reg] of this.tables) {
      if (sql.includes(name)) {
        referencedTables.push(reg);
      }
    }

    const ast = await this.parseAst(sql);

    // Phase 1: Materialize tables that appear inside subqueries first.
    // This lets us resolve subqueries against local DuckDB before
    // materializing the outer tables.
    const subqueryTables = this.findSubqueryTables(ast, referencedTables);
    const outerTables = referencedTables.filter((t) => !subqueryTables.has(t));

    for (const reg of subqueryTables) {
      const quals = this.extractQualsForTable(ast, reg.table.name, reg.keyColNames);
      await this.populateTable(reg, quals);
    }

    // Phase 2: If we materialized subquery tables, resolve subqueries
    // and rewrite the SQL with literal values. This turns
    // `order_id IN (SELECT id FROM orders WHERE ...)` into
    // `order_id IN (1, 2, 3)` — making those predicates available
    // as quals for the outer tables.
    let resolvedSql = sql;
    let resolvedAst = ast;
    if (subqueryTables.size > 0) {
      resolvedSql = await this.resolveSubqueries(sql, ast);
      resolvedAst = await this.parseAst(resolvedSql);
    }

    // Phase 3: Materialize outer tables. When subqueries were resolved,
    // extract quals for all columns (not just key columns) so plugins
    // that build WHERE clauses from quals benefit from the resolved
    // subquery values automatically.
    const subqueriesResolved = resolvedSql !== sql;
    for (const reg of outerTables) {
      const qualColumns = subqueriesResolved ? reg.allColumns : reg.keyColNames;
      const quals = this.extractQualsForTable(
        resolvedAst,
        reg.table.name,
        qualColumns,
      );
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

  /**
   * Find tables that appear inside subqueries (IN, EXISTS, etc.)
   * These should be materialized first so we can resolve the subqueries.
   */
  private findSubqueryTables(
    ast: any,
    referencedTables: RegisteredTable[],
  ): Set<RegisteredTable> {
    const subqueryTableNames = new Set<string>();

    const findSubqueryTableNames = (node: any, depth: number): void => {
      if (!node || typeof node !== "object") return;

      // When we encounter a SELECT_NODE inside a subquery (depth > 0),
      // collect its table references
      if (node.type === "SELECT_NODE" && depth > 0) {
        const walkFrom = (from: any): void => {
          if (!from) return;
          if (from.table_name) subqueryTableNames.add(from.table_name);
          if (from.type === "JOIN") {
            walkFrom(from.left);
            walkFrom(from.right);
          }
        };
        walkFrom(node.from_table);
      }

      // SUBQUERY nodes increase depth
      const nextDepth =
        node.class === "SUBQUERY" || node.subquery ? depth + 1 : depth;

      for (const val of Object.values(node)) {
        if (Array.isArray(val)) {
          for (const item of val) findSubqueryTableNames(item, nextDepth);
        } else if (val && typeof val === "object") {
          findSubqueryTableNames(val, nextDepth);
        }
      }
    };

    if (!ast.error && ast.statements?.[0]?.node) {
      findSubqueryTableNames(ast.statements[0].node, 0);
    }

    // Also check CTE definitions — they're inner tables too
    const cteMap = ast.statements?.[0]?.node?.cte_map?.map ?? [];
    for (const cte of cteMap) {
      const cteNode = cte?.value?.query?.node;
      if (cteNode?.from_table?.table_name) {
        subqueryTableNames.add(cteNode.from_table.table_name);
      }
    }

    const result = new Set<RegisteredTable>();
    for (const reg of referencedTables) {
      if (subqueryTableNames.has(reg.table.name)) {
        result.add(reg);
      }
    }
    return result;
  }

  /**
   * Resolve subqueries by running them against already-materialized
   * local tables. Mutates the AST in place — replacing subquery nodes
   * with literal IN values — then serializes back to SQL via DuckDB.
   */
  private async resolveSubqueries(sql: string, ast: any): Promise<string> {
    const modified = await this.resolveSubqueryNodes(ast.statements[0].node);
    if (!modified) return sql;

    const astStr = JSON.stringify(ast).replace(/'/g, "''");
    const result = await this.db.all(
      `SELECT json_deserialize_sql('${astStr}') as sql`,
    );
    return result[0].sql;
  }

  /**
   * Walk the AST and replace SUBQUERY IN nodes with literal COMPARE_IN
   * nodes using results from local DuckDB. Returns true if any were resolved.
   */
  private async resolveSubqueryNodes(node: any): Promise<boolean> {
    if (!node || typeof node !== "object") return false;

    let modified = false;

    // Look for IN/NOT IN with a subquery child, or SUBQUERY nodes
    if (node.class === "SUBQUERY" && node.subquery?.node) {
      const subSelect = node.subquery.node;
      if (subSelect.type === "SELECT_NODE") {
        try {
          const subSql = await this.subqueryToSql(subSelect);

          // Run the subquery against already-materialized tables
          const rows = await this.db.all(subSql);
          if (rows.length > 0) {
            const cols = Object.keys(rows[0]);
            if (cols.length === 1) {
              const values = rows.map((r) => r[cols[0]]);

              // Build literal constant nodes
              const constants = values.map((v: any) => ({
                class: "CONSTANT",
                type: "VALUE_CONSTANT",
                alias: "",
                query_location: 0,
                value: {
                  type: {
                    id: typeof v === "number" ? "INTEGER" : "VARCHAR",
                    type_info: null,
                  },
                  is_null: false,
                  value: v,
                },
              }));

              if (node.child) {
                // IN/NOT IN subquery — replace with COMPARE_IN
                const colRef = node.child;
                node.class = "OPERATOR";
                node.type = "COMPARE_IN";
                node.children = [colRef, ...constants];
                delete node.subquery;
                delete node.child;
                delete node.subquery_type;
                delete node.comparison_type;
              } else {
                // Scalar subquery (= (SELECT ...)) — replace with constant
                Object.keys(node).forEach((k) => delete node[k]);
                Object.assign(node, constants[0]);
              }
              modified = true;
            }
          }
        } catch {
          // Subquery references tables not yet materialized — skip
        }
      }
    }

    // Recurse into all children
    for (const val of Object.values(node)) {
      if (Array.isArray(val)) {
        for (const item of val) {
          if (await this.resolveSubqueryNodes(item)) modified = true;
        }
      } else if (val && typeof val === "object") {
        if (await this.resolveSubqueryNodes(val)) modified = true;
      }
    }

    return modified;
  }

  /**
   * Convert a subquery SELECT node back to SQL via DuckDB's
   * json_deserialize_sql. Normalizes query_location fields to 0
   * first — they're position metadata that can have types
   * incompatible with deserialization.
   */
  private async subqueryToSql(subSelect: any): Promise<string> {
    const subAst = {
      error: false,
      statements: [{ node: structuredClone(subSelect), named_param_map: [] }],
    };
    normalizeQueryLocations(subAst);
    const subAstStr = JSON.stringify(subAst).replace(/'/g, "''");
    const result = await this.db.all(
      `SELECT json_deserialize_sql('${subAstStr}') as sql`,
    );
    return result[0].sql;
  }

  getDatabase(): Database {
    return this.db;
  }

  async close(): Promise<void> {
    await this.db.close();
  }
}

function normalizeQueryLocations(node: any): void {
  if (!node || typeof node !== "object") return;
  if ("query_location" in node) node.query_location = 0;
  for (const v of Object.values(node)) {
    if (Array.isArray(v)) v.forEach(normalizeQueryLocations);
    else if (v && typeof v === "object") normalizeQueryLocations(v);
  }
}

function extractAstValue(v: any): any {
  if (!v) return null;
  return v.is_null ? null : v.value;
}

function toArrowType(type: ColumnType): arrow.DataType {
  switch (type) {
    case "number":
      return new arrow.Float64();
    case "boolean":
      return new arrow.Bool();
    case "json":
    case "datetime":
    case "string":
    default:
      return new arrow.Utf8();
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
