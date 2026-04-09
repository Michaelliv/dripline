import { Database } from "duckdb-async";
import { loadConfig } from "../config/loader.js";
import { Remote } from "../core/remote.js";
import { loadAllPlugins } from "../plugin/loader.js";
import { registry } from "../plugin/registry.js";
import { Dripline } from "../sdk.js";
import { formatCsv, formatJson, formatLine } from "../utils/formatters.js";
import { error } from "../utils/output.js";
import { startSpinner } from "../utils/spinner.js";
import { formatTable } from "../utils/table-formatter.js";

export type OutputFormat = "table" | "json" | "csv" | "line";

export interface QueryOptions {
  output?: OutputFormat;
  json?: boolean;
  quiet?: boolean;
  /** Read from the configured remote warehouse instead of plugins. */
  remote?: boolean;
}

/**
 * Errors thrown by `runQuery()` for misconfiguration. CLI wraps these
 * into `process.exit(1)`; tests catch them as ordinary exceptions.
 */
export class QueryConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "QueryConfigError";
  }
}

/**
 * Pure query function — no printing, no exits. Returns rows.
 *
 * Two modes:
 *   - **Local** (default): loads plugins, materializes tables on demand
 *     via dripline's engine. Identical to the historical `query()`.
 *   - **Remote**: skips plugins entirely, attaches the configured remote,
 *     creates views over every manifested curated table, and runs the
 *     SQL against pure parquet on the bucket. No API calls fire.
 *
 * Both modes share one mental model: SQL in, rows out.
 */
export async function runQuery(
  sql: string,
  options: QueryOptions = {},
): Promise<Record<string, unknown>[]> {
  if (options.remote) {
    return runRemoteQuery(sql);
  }

  await loadAllPlugins();
  const config = loadConfig();
  const dl = await Dripline.create({
    plugins: registry.listPlugins(),
    connections: config.connections,
    cache: config.cache,
    rateLimits: config.rateLimits,
  });
  try {
    return (await dl.query(sql)) as Record<string, unknown>[];
  } finally {
    await dl.close();
  }
}

async function runRemoteQuery(sql: string): Promise<Record<string, unknown>[]> {
  const config = loadConfig();
  if (!config.remote) {
    throw new QueryConfigError(
      "no remote configured. Set `remote` in .dripline/config.json.",
    );
  }
  const remote = new Remote(config.remote);
  const db = await Database.create(":memory:");
  try {
    await remote.attach(db);

    // Discover tables by listing manifests. One LIST request fetches
    // all of `_manifests/`; for each `<table>.json` we create a view.
    // No plugin code, no engine, no cursor state — pure SQL over R2.
    const manifestKeys = await remote.listObjects("_manifests/");
    const tables = manifestKeys
      .map((k) => k.split("/").pop() ?? "")
      .filter((f) => f.endsWith(".json"))
      .map((f) => f.slice(0, -".json".length));

    // attachTable creates a view, not a materialized read — it
    // never touches the data. SELECT against an empty/missing
    // table will surface a clear DuckDB error to the user.
    await Promise.all(tables.map((table) => remote.attachTable(db, table)));

    return (await db.all(sql)) as Record<string, unknown>[];
  } finally {
    await db.close();
  }
}

/**
 * CLI wrapper — formats, prints, and exits on error. Programmatic
 * callers should use `runQuery()` directly.
 */
export async function query(
  sql: string,
  options: QueryOptions = {},
): Promise<void> {
  try {
    const format = options.json ? "json" : (options.output ?? "table");
    const showSpinner = !options.json && !options.quiet && format !== "json";
    const spinner = showSpinner ? startSpinner("Querying...") : null;
    const start = performance.now();
    const rows = await runQuery(sql, options);
    const elapsed = ((performance.now() - start) / 1000).toFixed(3);
    spinner?.stop();

    switch (format) {
      case "json":
        console.log(formatJson(rows));
        break;
      case "csv":
        console.log(formatCsv(rows));
        break;
      case "line":
        console.log(formatLine(rows));
        break;
      default:
        console.log(formatTable(rows));
        if (!options.quiet) {
          console.log(`Time: ${elapsed}s.`);
        }
        break;
    }
  } catch (e) {
    error((e as Error).message);
    process.exit(1);
  }
}
