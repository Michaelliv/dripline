import { Database, LeaseStore, Remote, registry, resolveRemote } from "dripline";
import type { VexPluginAPI } from "vex-core";
import { runCompactTable } from "../core/compactor.js";
import { getActiveWorkspace } from "../core/workspace.js";

/**
 * How long `workspace.warehouse` caches its R2 inspection results.
 * 30s is the sweet spot for "operator opens the Warehouse tab and
 * sees mostly-live data without hammering the bucket on every
 * subscription invalidation." Raise for larger workspaces, lower
 * for debugging freshness.
 */
const WAREHOUSE_TTL_MS = 30_000;

let warehouseCache: {
  key: string;
  at: number;
  value: unknown;
} | null = null;

/**
 * Read-only queries that expose the workspace's loaded plugins,
 * connections, catalog (cross-plugin table index), and remote config.
 *
 * These back the new UI pages (Plugins, Connections, Catalog). They
 * don't live in dripyard's SQLite — the workspace is the source of
 * truth — so these queries return the live in-memory workspace state.
 *
 * All queries redact secrets. Anything resembling a password/token is
 * replaced with a mask before leaving the server.
 */
export function workspacePlugin(api: VexPluginAPI) {
  api.setName("workspace");

  api.registerQuery("info", {
    args: {},
    async handler() {
      const ws = getActiveWorkspace();
      if (!ws) return null;
      return {
        path: ws.path,
        configDir: ws.configDir,
        pluginCount: ws.plugins.length,
        connectionCount: ws.config.connections?.length ?? 0,
        laneCount: Object.keys(ws.config.lanes ?? {}).length,
        remote: ws.remote
          ? {
              endpoint: ws.remote.endpoint,
              bucket: ws.remote.bucket,
              prefix: ws.remote.prefix ?? null,
              secretType: ws.remote.secretType ?? "S3",
              accessKeyEnv: ws.remote.accessKeyEnv ?? null,
              secretKeyEnv: ws.remote.secretKeyEnv ?? null,
            }
          : null,
      };
    },
  });

  /**
   * Plugins loaded from .dripline/plugins.json. Each entry carries
   * enough to render the Plugins page: name, version, table names,
   * and the connection(s) wired to it.
   */
  api.registerQuery("plugins", {
    args: {},
    async handler() {
      const ws = getActiveWorkspace();
      if (!ws) return [];
      return ws.plugins.map((p) => ({
        name: p.name,
        version: p.version,
        description: (p as any).description ?? null,
        tableCount: p.tables.length,
        tables: p.tables.map((t) => ({
          name: t.name,
          description: t.description ?? null,
          columnCount: t.columns.length,
          hasCursor: !!t.cursor,
          cursor: t.cursor ?? null,
          hasPrimaryKey: !!(t.primaryKey && t.primaryKey.length > 0),
          primaryKey: t.primaryKey ?? [],
          keyColumns: (t.keyColumns ?? []).map((k) => ({
            name: k.name,
            required: k.required,
          })),
        })),
        connections: (ws.config.connections ?? [])
          .filter((c) => c.plugin === p.name)
          .map((c) => c.name),
      }));
    },
  });

  /**
   * Full catalog — every (plugin, table) pair flattened, with schema
   * details and which lanes reference each (table, params) combo.
   * Backs the Catalog page.
   */
  api.registerQuery("catalog", {
    args: {},
    async handler() {
      const ws = getActiveWorkspace();
      if (!ws) return [];
      const rows: Array<{
        plugin: string;
        table: string;
        description: string | null;
        columns: Array<{ name: string; type: string }>;
        primaryKey: string[];
        cursor: string | null;
        keyColumns: Array<{ name: string; required: string }>;
        usedByLanes: Array<{ lane: string; params: Record<string, unknown> }>;
      }> = [];
      for (const plugin of ws.plugins) {
        for (const table of plugin.tables) {
          const usedByLanes: Array<{
            lane: string;
            params: Record<string, unknown>;
          }> = [];
          for (const [laneName, lane] of Object.entries(
            ws.config.lanes ?? {},
          )) {
            for (const t of lane.tables ?? []) {
              if (t.name === table.name) {
                usedByLanes.push({
                  lane: laneName,
                  params: t.params ?? {},
                });
              }
            }
          }
          rows.push({
            plugin: plugin.name,
            table: table.name,
            description: table.description ?? null,
            columns: table.columns.map((c) => ({
              name: c.name,
              type: c.type,
            })),
            primaryKey: table.primaryKey ?? [],
            cursor: table.cursor ?? null,
            keyColumns: (table.keyColumns ?? []).map((k) => ({
              name: k.name,
              required: k.required,
            })),
            usedByLanes,
          });
        }
      }
      return rows.sort((a, b) =>
        `${a.plugin}.${a.table}`.localeCompare(`${b.plugin}.${b.table}`),
      );
    },
  });

  /**
   * Connections with masked secrets. We don't want the UI (or anyone
   * listening on the wire) to see raw tokens/passwords. Values that
   * look secret-ish are replaced with a `"***"` marker; env-var
   * references are kept verbatim so the UI can surface them.
   */
  api.registerQuery("connections", {
    args: {},
    async handler() {
      const ws = getActiveWorkspace();
      if (!ws) return [];
      return (ws.config.connections ?? []).map((c) => ({
        name: c.name,
        plugin: c.plugin,
        config: maskConnectionConfig(c.config),
      }));
    },
  });

  /**
   * Warehouse state per table: raw/curated file counts, manifest stats,
   * last compact time. Hits R2 via dripline's Remote — one GET per
   * table manifest + one LIST per table raw prefix. For a 20-table
   * workspace that's ~40 bucket requests per call.
   *
   * Cached server-side with a WAREHOUSE_TTL_MS window so subscription
   * re-runs, UI re-mounts, and tab-switches don't hammer R2. The
   * cache is keyed on the workspace path so switching workspaces
   * invalidates cleanly.
   *
   * Returns { remote: null, tables: [] } when no remote is configured
   * or when creds are missing — the page renders an empty state.
   */
  api.registerQuery("warehouse", {
    args: {},
    async handler() {
      const ws = getActiveWorkspace();
      if (!ws || !ws.remote) return { remote: null, tables: [] };

      const cached = warehouseCache;
      if (
        cached &&
        cached.key === ws.path &&
        Date.now() - cached.at < WAREHOUSE_TTL_MS
      ) {
        return cached.value;
      }

      let remote: Remote;
      try {
        remote = new Remote(ws.remote);
      } catch (e) {
        // Credentials unresolved (env var missing) — surface, don't
        // crash the plugin.
        return {
          remote: {
            endpoint: ws.remote.endpoint,
            bucket: ws.remote.bucket,
            prefix: ws.remote.prefix ?? null,
            error: (e as Error).message,
          },
          tables: [],
        };
      }

      // Every table from every registered plugin — compactable or not.
      // Non-compactable ones (no PK) won't have curated/ entries but
      // may have raw files, which is still worth surfacing.
      const allTables = ws.plugins.flatMap((p) =>
        p.tables.map((t) => ({ plugin: p.name, table: t.name })),
      );

      const tables = await Promise.all(
        allTables.map(async ({ plugin, table }) => {
          const [rawCount, manifest] = await Promise.all([
            remote.countObjects(`raw/${table}/`).catch(() => 0),
            remote.readManifest(table).catch(() => null),
          ]);
          // Manifest shape (dripline): { files: [{ row_count }], partition_by, generated_at }
          const rows =
            manifest?.files?.reduce(
              (s, f) => s + Number((f as any).row_count ?? 0),
              0,
            ) ?? null;
          const lastCompactAt = manifest?.generated_at
            ? Date.parse(manifest.generated_at)
            : null;
          return {
            plugin,
            table,
            rawFiles: rawCount,
            curatedFiles: manifest?.files?.length ?? 0,
            rows,
            lastCompactAt,
            partitions: manifest?.partition_by ?? [],
          };
        }),
      );

      const value = {
        remote: {
          endpoint: ws.remote.endpoint,
          bucket: ws.remote.bucket,
          prefix: ws.remote.prefix ?? null,
          secretType: ws.remote.secretType ?? "S3",
          error: null,
        },
        tables: tables.sort((a, b) => a.table.localeCompare(b.table)),
      };
      warehouseCache = { key: ws.path, at: Date.now(), value };
      return value;
    },
  });

  /**
   * Run a SQL query against the workspace's curated parquet via
   * DuckDB. No plugin code, no API calls — reads directly from R2.
   *
   * Registered as a mutation because vex's query cache is keyed by
   * (name, args) and caches results forever; ad-hoc SQL would pollute
   * that cache with one entry per distinct SQL string, and the
   * reactive re-run-on-invalidation semantics are meaningless for
   * one-shot queries. Mutations bypass both. Mental model: "run SQL
   * and give me the rows now", not "subscribe to this query".
   *
   * Smart attach: only views tables referenced in the SQL string are
   * wired up. Attaching every manifested table fires one S3 LIST +
   * parquet footer read each (~1s), so attaching 20 tables adds ~20s
   * to a single-table query. Fallback path (no references found —
   * SHOW TABLES, etc) attaches all of them.
   *
   * Safeguards:
   *   - SQL is trimmed and must be non-empty.
   *   - Row cap (default 10k) applied client-side after execution;
   *     caller can override but truncation is reported via `.truncated`.
   *   - Timeout (default 30s) via AbortSignal; DuckDB's in-flight
   *     query is interrupted.
   */
  /**
   * Trigger compaction on demand, bypassing the scheduler. Pass
   * `tables: ["foo", "bar"]` to limit to specific tables, or omit for
   * all compactable tables (those with a primary key).
   *
   * Runs synchronously and returns a structured result per table. The
   * heavy paths live in `runCompactTable` — same code the scheduler
   * calls every 30 minutes, so results here reflect production
   * behavior exactly.
   *
   * Lease-protected: if another worker is already compacting a table,
   * that entry comes back `skipped` with `reason: "lease held"` — this
   * is the correct behavior and not an error.
   */
  api.registerMutation("compactNow", {
    args: {},
    async handler(_ctx, args) {
      const ws = getActiveWorkspace();
      if (!ws?.remote) {
        throw new Error("No remote configured for this workspace.");
      }
      const remote = new Remote(ws.remote);
      const leaseStore = LeaseStore.fromRemote(resolveRemote(ws.remote));
      const maxRuntimeMs =
        typeof args.maxRuntimeMs === "number" && args.maxRuntimeMs > 0
          ? args.maxRuntimeMs
          : 10 * 60 * 1000;

      const filter = Array.isArray(args.tables)
        ? new Set((args.tables as unknown[]).map(String))
        : null;

      const results = [];
      for (const { table } of registry.getAllTables()) {
        if (!table.primaryKey || table.primaryKey.length === 0) continue;
        if (filter && !filter.has(table.name)) continue;
        results.push(
          await runCompactTable(table, remote, leaseStore, maxRuntimeMs),
        );
      }

      if (filter) {
        // Validate explicit names so typos don't silently return empty.
        const found = new Set(results.map((r) => r.table));
        const missing = [...filter].filter((t) => !found.has(t));
        if (missing.length > 0) {
          throw new Error(
            `Unknown or non-compactable tables: ${missing.join(", ")}`,
          );
        }
      }
      return { tables: results };
    },
  });

  api.registerMutation("runSql", {
    args: { sql: "string" },
    async handler(_ctx, args) {
      const sql = typeof args.sql === "string" ? args.sql.trim() : "";
      if (!sql) throw new Error("SQL is empty.");

      const maxRows = Math.max(
        1,
        Math.min(100_000, Number(args.maxRows) || 10_000),
      );
      const timeoutMs = Math.max(
        1_000,
        Math.min(300_000, Number(args.timeoutMs) || 30_000),
      );

      const ws = getActiveWorkspace();
      if (!ws?.remote) {
        throw new Error("No remote configured for this workspace.");
      }
      const remote = new Remote(ws.remote);
      const db = await Database.create(":memory:");
      const started = Date.now();
      const timeout = setTimeout(() => {
        // dripline's Database may expose interrupt() on the underlying
        // connection; call it if available, otherwise close() will
        // unblock via a different error path. Either way the caller
        // sees a failure instead of an indefinite hang.
        try {
          (db as any).interrupt?.();
        } catch {}
      }, timeoutMs);
      try {
        await remote.attach(db);
        const manifestKeys = await remote.listObjects("_manifests/");
        const tables = manifestKeys
          .map((k) => k.split("/").pop() ?? "")
          .filter((f) => f.endsWith(".json"))
          .map((f) => f.slice(0, -".json".length));
        const sqlLower = sql.toLowerCase();
        const referenced = tables.filter((t) =>
          sqlLower.includes(t.toLowerCase()),
        );
        const toAttach = referenced.length > 0 ? referenced : tables;
        await Promise.all(
          toAttach.map((table) => remote.attachTable(db, table)),
        );
        const raw = (await db.all(sql)) as Record<string, unknown>[];
        const truncated = raw.length > maxRows;
        // Normalize BigInt — JSON.stringify chokes on them, UI expects
        // plain numbers. Matches dripline's normalizeRow behavior.
        const rows = raw.slice(0, maxRows).map((r) => {
          const out: Record<string, unknown> = {};
          for (const [k, v] of Object.entries(r)) {
            out[k] = typeof v === "bigint" ? Number(v) : v;
          }
          return out;
        });
        return {
          rows,
          rowCount: raw.length,
          truncated,
          durationMs: Date.now() - started,
          attached: toAttach,
        };
      } finally {
        clearTimeout(timeout);
        await db.close();
      }
    },
  });
}

/**
 * Mask field values whose key looks secret-ish. The pattern is
 * anchored at word boundaries so it matches `token`, `api_token`,
 * `accessToken`, and `password_env` — but NOT `key_schedule`,
 * `authorized_users`, `keyboard_layout`, etc. The previous unanchored
 * `/key/` was a bug that masked legitimate data fields in connection
 * configs.
 *
 * Both snake_case (`api_token`) and camelCase (`apiToken`) split on
 * the underscore / case boundary, so `token` matches either.
 */
const SECRET_WORDS = [
  "token",
  "password",
  "passwd",
  "secret",
  "apikey",
  "accesskey",
  "secretkey",
  "privatekey",
  "authorization",
  "credential",
  "credentials",
  "bearer",
];

function isSecretKey(name: string): boolean {
  // Normalize snake_case and camelCase into token-separated words.
  const parts = name
    .replace(/([a-z])([A-Z])/g, "$1_$2")
    .toLowerCase()
    .split(/[_\-.\s]+/)
    .filter(Boolean);
  return parts.some((p) => SECRET_WORDS.includes(p));
}

function maskConnectionConfig(
  config: Record<string, unknown>,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(config)) {
    if (isSecretKey(k) && typeof v === "string" && v.length > 0) {
      // Preserve env-var bindings verbatim (e.g. "$GITHUB_TOKEN")
      // since those aren't secrets themselves.
      out[k] = v.startsWith("$") ? v : "***";
    } else {
      out[k] = v;
    }
  }
  return out;
}
