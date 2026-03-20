export type ColumnType = "string" | "number" | "boolean" | "json" | "datetime";

export interface ColumnDef {
  name: string;
  type: ColumnType;
  description?: string;
}

export interface KeyColumn {
  name: string;
  operators?: string[];
  required: "required" | "optional" | "any_of";
}

export interface Qual {
  column: string;
  operator: string;
  value: any;
}

export interface ConnectionConfig {
  name: string;
  plugin: string;
  config: Record<string, any>;
}

export interface QueryContext {
  connection: ConnectionConfig;
  quals: Qual[];
  columns: string[];
  limit?: number;
}

export type ListFunc = (ctx: QueryContext) => Generator<Record<string, any>>;
export type GetFunc = (ctx: QueryContext) => Record<string, any> | null;
export type HydrateFunc = (
  ctx: QueryContext,
  row: Record<string, any>,
) => Record<string, any>;

export interface TableDef {
  name: string;
  columns: ColumnDef[];
  keyColumns?: KeyColumn[];
  list: ListFunc;
  get?: GetFunc;
  hydrate?: Record<string, HydrateFunc>;
  description?: string;
}

export interface PluginDef {
  name: string;
  version: string;
  tables: TableDef[];
  connectionConfigSchema?: Record<
    string,
    {
      type: string;
      required?: boolean;
      description?: string;
      default?: any;
    }
  >;
}

export interface CacheEntry<T> {
  data: T[];
  timestamp: number;
  ttl: number;
}

export interface RateLimitConfig {
  maxPerSecond?: number;
  maxPerMinute?: number;
  maxConcurrent?: number;
}
