export type ColumnType = "string" | "number" | "boolean" | "json" | "datetime";

export interface ColumnDef {
  name: string;
  type: ColumnType;
  description?: string;
}

export interface KeyColumn {
  name: string;
  /** @deprecated No longer used — all operators are extracted automatically */
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
  /**
   * Optional fetch override. When present, the engine threads this
   * through to `QueryContext.fetch` so plugins can route HTTP through
   * a proxy, a rotating worker network, a mock, etc. without the
   * plugin knowing who's on the other side.
   *
   * Absent (the common case) means plugins see `globalThis.fetch`.
   * Callers (e.g. dripyard wiring in flaregun) set this at runtime
   * — it's never serialized in config.json.
   */
  fetch?: typeof globalThis.fetch;
}

export interface QueryContext {
  connection: ConnectionConfig;
  quals: Qual[];
  columns: string[];
  limit?: number;
  /** High-water mark from previous sync. null on first sync, undefined during query(). */
  cursor?: { column: string; value: any } | null;
  /**
   * Abort signal for the active sync call. Plugins that make HTTP
   * requests should forward this to `fetch({ signal })` so in-flight
   * requests cancel immediately when the caller (e.g. a worker being
   * SIGTERMed) aborts. Absent for query() calls, which are typically
   * short enough that cancellation isn't needed.
   */
  signal?: AbortSignal;
  /**
   * Fetch function plugins should use for all outbound HTTP. Equal to
   * `connection.fetch` when the caller provided one, `globalThis.fetch`
   * otherwise — so plugins can always call `ctx.fetch(...)` without a
   * nullish check. Honor this instead of the global to stay compatible
   * with proxy/rotation/mocking setups at the caller.
   */
  fetch: typeof globalThis.fetch;
}

export type ListFunc = (
  ctx: QueryContext,
) =>
  | Generator<Record<string, any>>
  | AsyncGenerator<Record<string, any>>;
export type GetFunc = (ctx: QueryContext) => Record<string, any> | null;
export type HydrateFunc = (
  ctx: QueryContext,
  row: Record<string, any>,
) => Record<string, any>;

export interface TableDef {
  name: string;
  columns: ColumnDef[];
  keyColumns?: KeyColumn[];
  /** Row identity columns for deduplication during sync. */
  primaryKey?: string[];
  /** Default params for sync() — used when caller doesn't provide params for this table. */
  syncParams?: Record<string, any>;
  /** Columns to partition curated parquet by in the warehouse.
   *  Defaults to keyColumn names when omitted. Set to a subset
   *  (e.g. ["org_id"] instead of ["org_id", "business_date"])
   *  to produce fewer, larger files. */
  partitionBy?: string[];
  /** Column name used as high-water mark for incremental sync. Type inferred from columns[]. */
  cursor?: string;
  /**
   * Default cursor value on the very first sync — i.e. the starting
   * point of a backfill. Only consulted when `cursor` is set AND the
   * engine has no prior metadata row for this (table, params) pair.
   *
   * Accepts a static value (e.g. `"2020-01-01T00:00:00Z"`) or a
   * function of the sync params, which lets plugins express "last 30
   * days" or "since the start of this quarter" without the user
   * having to configure anything.
   *
   * Omit to backfill from whatever the plugin's `list()` picks when
   * `ctx.cursor` is null (typically "all history").
   */
  initialCursor?: unknown | ((params: Record<string, any>) => unknown);
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
      env?: string;
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

export interface SyncProgressEvent {
  table: string;
  rowsInserted: number;
  cursor: unknown;
  elapsedMs: number;
}
