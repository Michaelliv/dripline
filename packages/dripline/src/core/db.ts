/**
 * Thin async wrapper over `@duckdb/node-api` that preserves the small
 * surface dripline used to call on `duckdb-async`. One file, one place
 * to swap bindings if we ever do this again.
 *
 * Why this wrapper exists at all
 *   - `@duckdb/node-api` is async-first and connection-oriented:
 *     `DuckDBInstance.create(path)` → `instance.connect()` → run SQL on
 *     the connection. Our code historically saw a single `Database`
 *     object that did everything; pushing that split into every call
 *     site is noisy. The wrapper hides it.
 *   - Multi-statement strings (`INSTALL httpfs; LOAD httpfs;`) are
 *     common in our DDL but the underlying binding is single-statement.
 *     `exec()` splits and runs them in order.
 *   - Positional `?` parameters were a one-arg ergonomic
 *     (`db.all(sql, ...params)`); the new binding takes an array, so we
 *     normalize between them at this seam.
 *   - The legacy `duckdb-async` segfaulted on Bun atexit. The new
 *     binding does not. Tests, bench, and a real R2 repro confirm.
 */

import {
  type DuckDBAppender as RawAppender,
  type DuckDBConnection,
  DuckDBInstance,
  type DuckDBValue,
} from "@duckdb/node-api";

/**
 * Naive multi-statement splitter for the SQL we actually emit. Splits
 * on `;` boundaries that are NOT inside single quotes. Sufficient for
 * the DDL-shaped strings we pass to `exec()` (e.g.
 * `INSTALL httpfs; LOAD httpfs;`); not a SQL parser.
 */
function splitStatements(sql: string): string[] {
  const out: string[] = [];
  let buf = "";
  let inSingle = false;
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (ch === "'" && sql[i - 1] !== "\\") inSingle = !inSingle;
    if (ch === ";" && !inSingle) {
      const s = buf.trim();
      if (s.length > 0) out.push(s);
      buf = "";
      continue;
    }
    buf += ch;
  }
  const tail = buf.trim();
  if (tail.length > 0) out.push(tail);
  return out;
}

/**
 * Coerce a JS value into something the binding's positional `bind`
 * accepts. The binding is strict about types; the engine historically
 * passed in mixed primitives. We normalize here.
 *   - bigint stays bigint (BIGINT columns)
 *   - number → number (DOUBLE / INTEGER)
 *   - boolean / string / null pass through
 *   - everything else is JSON-stringified (objects land in JSON/VARCHAR)
 */
function toBindValue(v: unknown): DuckDBValue {
  if (v == null) return null as unknown as DuckDBValue;
  if (typeof v === "string" || typeof v === "number" || typeof v === "boolean")
    return v as DuckDBValue;
  if (typeof v === "bigint") return v as DuckDBValue;
  return JSON.stringify(v) as DuckDBValue;
}

/** Result row shape that callers historically expected — plain objects. */
export type Row = Record<string, unknown>;

/**
 * Options for {@link Database.create}. All fields optional — the
 * defaults match DuckDB's own defaults (read/write, all cores).
 *
 * The binding's native config is `Record<string, string>`; this type
 * exposes the handful we actually want to type-check, plus a
 * `duckdbOptions` escape hatch for anything else (see
 * https://duckdb.org/docs/sql/configuration for the full list).
 */
export interface DatabaseOptions {
  /** Shorthand for `accessMode: "read_only"`. */
  readOnly?: boolean;
  /**
   * Explicit DuckDB access mode. Overrides `readOnly` when both are
   * set. `"automatic"` lets DuckDB decide based on file permissions.
   */
  accessMode?: "read_only" | "read_write" | "automatic";
  /** Number of worker threads. Default: one per CPU core. */
  threads?: number;
  /** Memory cap for the instance (e.g. `"1GB"`, `"512MB"`). */
  memoryLimit?: string;
  /**
   * Spill directory for out-of-core hash aggregates, sorts, and
   * joins. When set, DuckDB streams intermediate state to disk
   * instead of OOMing once `memoryLimit` is hit. Critical on
   * memory-constrained containers (Render starter at 512 MB, etc).
   * Without this, a hash-aggregate / sort larger than the memory
   * cap kills the process instead of spilling.
   */
  tempDirectory?: string;
  /**
   * Set `preserve_insertion_order`. Pass `false` to let the planner
   * skip an ordering step on streaming paths — meaningfully cheaper
   * on wide scans and compaction COPYs where insertion order is
   * irrelevant. Default: unset (DuckDB's default, `true`).
   */
  preserveInsertionOrder?: boolean;
  /**
   * Set `enable_object_cache`. Keeps parquet metadata between
   * queries so repeat reads skip the footer download — a big win
   * for the compactor, which reads the same manifest'd curated
   * files on every run.
   */
  objectCache?: boolean;
  /**
   * Extra raw DuckDB config keys. Merged on top of the friendly
   * options above — so setting both `threads: 4` and
   * `duckdbOptions: { threads: "8" }` picks the raw value.
   */
  duckdbOptions?: Record<string, string>;
}

/**
 * Translate {@link DatabaseOptions} into the raw string map the
 * binding expects. Friendly fields lose to `duckdbOptions` when both
 * set the same key — the escape hatch is authoritative.
 */
function buildDuckDBOptions(
  opts: DatabaseOptions,
): Record<string, string> | undefined {
  const out: Record<string, string> = {};
  const mode = opts.accessMode ?? (opts.readOnly ? "read_only" : undefined);
  if (mode) out.access_mode = mode;
  if (opts.threads != null) out.threads = String(opts.threads);
  if (opts.memoryLimit) out.memory_limit = opts.memoryLimit;
  if (opts.tempDirectory) out.temp_directory = opts.tempDirectory;
  if (opts.preserveInsertionOrder != null)
    out.preserve_insertion_order = String(opts.preserveInsertionOrder);
  if (opts.objectCache != null)
    out.enable_object_cache = String(opts.objectCache);
  Object.assign(out, opts.duckdbOptions ?? {});
  return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Defaults applied by {@link Database.createForContainer}. Each can
 * be overridden with the matching env var — operators tuning a box
 * from the outside don't need to patch code.
 *
 *   DRIPLINE_DUCKDB_MEMORY_LIMIT   e.g. "300MB"
 *   DRIPLINE_DUCKDB_THREADS        e.g. "1"
 *   DRIPLINE_DUCKDB_TEMP_DIR       e.g. "/tmp/duckdb-spill"
 *
 * The defaults target a 512 MB container (Render starter). They're
 * conservative on purpose — a 2 GB box just sees more headroom, never
 * less. Callers that explicitly pass any of these fields in the
 * opts argument win over both env vars and defaults.
 */
const CONTAINER_DEFAULTS = {
  memoryLimit: "300MB",
  threads: 1,
  tempDirectory: "/tmp/duckdb-spill",
  preserveInsertionOrder: false,
  objectCache: true,
} as const;

function resolveContainerOptions(
  overrides: DatabaseOptions,
): DatabaseOptions {
  return {
    memoryLimit:
      overrides.memoryLimit ??
      process.env.DRIPLINE_DUCKDB_MEMORY_LIMIT ??
      CONTAINER_DEFAULTS.memoryLimit,
    threads:
      overrides.threads ??
      (process.env.DRIPLINE_DUCKDB_THREADS
        ? Number(process.env.DRIPLINE_DUCKDB_THREADS)
        : CONTAINER_DEFAULTS.threads),
    tempDirectory:
      overrides.tempDirectory ??
      process.env.DRIPLINE_DUCKDB_TEMP_DIR ??
      CONTAINER_DEFAULTS.tempDirectory,
    preserveInsertionOrder:
      overrides.preserveInsertionOrder ??
      CONTAINER_DEFAULTS.preserveInsertionOrder,
    objectCache: overrides.objectCache ?? CONTAINER_DEFAULTS.objectCache,
    // Pass-through fields the caller may have set for other reasons.
    readOnly: overrides.readOnly,
    accessMode: overrides.accessMode,
    duckdbOptions: overrides.duckdbOptions,
  };
}

/**
 * Database — a connection-oriented handle that mimics the historical
 * `duckdb-async` shape (`create`, `exec`, `all`, `run`, `close`).
 *
 * One Database normally owns one DuckDBInstance and one
 * DuckDBConnection. The {@link Database.fromConnection} factory
 * returns a non-owning handle — useful when the caller already has a
 * connection from somewhere else (e.g. an embedding engine) and wants
 * to reuse its buffer pool / extensions instead of opening a second
 * instance on the same file.
 */
export class Database {
  private instance: DuckDBInstance | null;
  private conn: DuckDBConnection;
  private closed = false;
  private owned: boolean;

  private constructor(
    instance: DuckDBInstance | null,
    conn: DuckDBConnection,
    owned: boolean,
  ) {
    this.instance = instance;
    this.conn = conn;
    this.owned = owned;
  }

  /**
   * Open a new in-memory or file-backed database. Pass
   * {@link DatabaseOptions} to configure access mode, threads, or
   * memory limits — the old signature `create(path)` still works,
   * unchanged.
   *
   * @example
   *   const db = await Database.create("./data.duckdb", { readOnly: true });
   *   const mem = await Database.create(":memory:", { threads: 4 });
   */
  static async create(
    path = ":memory:",
    options: DatabaseOptions = {},
  ): Promise<Database> {
    const duckdbOpts = buildDuckDBOptions(options);
    const inst = duckdbOpts
      ? await DuckDBInstance.create(path, duckdbOpts)
      : await DuckDBInstance.create(path);
    const conn = await inst.connect();
    return new Database(inst, conn, true);
  }

  /**
   * Open a DuckDB with container-safe defaults: a hard memory cap,
   * a temp directory so out-of-core operators spill instead of
   * OOMing, a single worker thread (multi-threading is a memory
   * multiplier on tight boxes), `preserve_insertion_order=false`
   * (cheaper streaming plans), and `enable_object_cache=true` (cache
   * parquet footers between queries).
   *
   * This is the right factory for any path that runs inside a
   * fixed-memory container — lane sync, compaction — where the
   * historical failure mode is DuckDB claiming more memory than the
   * cgroup allows and being killed mid-query. Defaults target a
   * 512 MB box; override via env vars or explicit options.
   *
   * Explicit options win over env vars win over defaults.
   */
  static async createForContainer(
    path = ":memory:",
    options: DatabaseOptions = {},
  ): Promise<Database> {
    return Database.create(path, resolveContainerOptions(options));
  }

  /**
   * Wrap an existing {@link DuckDBConnection} without owning it. The
   * resulting Database's `close()` is a no-op for the underlying
   * binding — the caller is responsible for closing the connection
   * (and its instance) through whatever path created them.
   *
   * Use this when you want dripline to share a DuckDB instance with
   * another consumer (e.g. vex's analytical adapter) instead of
   * opening a second instance on the same file.
   */
  static fromConnection(conn: DuckDBConnection): Database {
    return new Database(null, conn, false);
  }

  /**
   * Execute one or more SQL statements with no parameter binding and
   * no row results. Splits on top-level `;` so DDL like
   * `INSTALL httpfs; LOAD httpfs;` runs as two statements.
   */
  async exec(sql: string): Promise<void> {
    for (const stmt of splitStatements(sql)) {
      await this.conn.run(stmt);
    }
  }

  /**
   * Execute a single statement with optional positional parameters.
   * Mirrors the legacy `db.run(sql, ...params)` signature so call
   * sites need no changes. Returns nothing — use `all()` for results.
   */
  async run(sql: string, ...params: unknown[]): Promise<void> {
    if (params.length === 0) {
      await this.conn.run(sql);
      return;
    }
    await this.conn.run(sql, params.map(toBindValue));
  }

  /**
   * Execute a single statement with optional positional parameters and
   * return all rows as plain objects. Same shape as the legacy
   * `db.all(sql, ...params)`. BIGINT values come through as native
   * `bigint`; the engine has a `normalizeRow` step that downgrades
   * them where appropriate, so we don't coerce here.
   */
  async all(sql: string, ...params: unknown[]): Promise<Row[]> {
    const reader =
      params.length === 0
        ? await this.conn.runAndReadAll(sql)
        : await this.conn.runAndReadAll(sql, params.map(toBindValue));
    return reader.getRowObjectsJS() as Row[];
  }

  /**
   * Open an appender on a base table. The appender is the binding's
   * fast bulk-insert path — used by the engine in place of the old
   * Arrow IPC `register_buffer` round-trip. Caller is responsible for
   * `endRow()` / `closeSync()` lifecycle.
   */
  async appender(
    table: string,
    schema: string | null = null,
  ): Promise<RawAppender> {
    return await this.conn.createAppender(table, schema);
  }

  /**
   * The underlying DuckDB connection. Escape hatch for callers that
   * need an API this wrapper doesn't expose. For a non-owning handle
   * (see {@link Database.fromConnection}) this returns the borrowed
   * connection; owning it outside dripline's lifecycle is unsafe.
   */
  getConnection(): DuckDBConnection {
    return this.conn;
  }

  /**
   * Close the connection and the instance. Idempotent. For a handle
   * returned by {@link Database.fromConnection}, close is a no-op —
   * the owner (whoever created the underlying connection) is
   * responsible for teardown.
   *
   * Closing the instance is what actually frees the database; closing
   * only the connection leaves the instance pinned. Both are sync in
   * the underlying binding.
   */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    if (!this.owned) return;
    try {
      this.conn.closeSync();
    } catch {}
    try {
      this.instance?.closeSync();
    } catch {}
  }
}

/** Re-export the appender type so callers can type their helpers. */
export type Appender = RawAppender;
