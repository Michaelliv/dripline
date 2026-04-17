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
  type DuckDBConnection,
  DuckDBInstance,
  type DuckDBValue,
  type DuckDBAppender as RawAppender,
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
 * Database — a connection-oriented handle that mimics the historical
 * `duckdb-async` shape (`create`, `exec`, `all`, `run`, `close`).
 *
 * One Database owns one DuckDBInstance and one DuckDBConnection. We
 * only ever opened one connection per database in the legacy code, so
 * there's no fan-out concurrency to preserve.
 */
export class Database {
  private instance: DuckDBInstance;
  private conn: DuckDBConnection;
  private closed = false;

  private constructor(instance: DuckDBInstance, conn: DuckDBConnection) {
    this.instance = instance;
    this.conn = conn;
  }

  /** Open a new in-memory or file-backed database. */
  static async create(path = ":memory:"): Promise<Database> {
    const inst = await DuckDBInstance.create(path);
    const conn = await inst.connect();
    return new Database(inst, conn);
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
   * Close the connection and the instance. Idempotent. Closing the
   * instance is what actually frees the database; closing only the
   * connection leaves the instance pinned. Both are sync in the
   * underlying binding.
   */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    try {
      this.conn.closeSync();
    } catch {}
    try {
      this.instance.closeSync();
    } catch {}
  }
}

/** Re-export the appender type so callers can type their helpers. */
export type Appender = RawAppender;
