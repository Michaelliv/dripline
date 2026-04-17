/**
 * Workspace = a dripline project directory containing `.dripline/`.
 *
 * Dripyard adopts dripline's project format as its source of truth:
 *   - `.dripline/plugins.json` — installed plugins (dynamic-imported)
 *   - `.dripline/config.json` — connections, lanes, remote, rate limits
 *
 * On server boot with a workspace path, we:
 *   1. Load plugins via dripline's loader into the shared registry.
 *   2. Hydrate dripyard's `lanes` table from config.json lanes (upsert
 *      by name — the config file wins on restart).
 *   3. Wire a default `resolvePlugin` that reads from the registry.
 *
 * The bucket + dripline config on disk stay the authoritative state.
 * Dripyard's SQLite is a live cache + operational layer (runs, workers,
 * progress) that can be wiped and rebuilt from the workspace any time.
 */

import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import {
  type ConnectionConfig,
  type DriplineConfig,
  type LaneConfig,
  loadPluginsFromConfig,
  type PluginDef,
  type RateLimitConfig,
  registry,
  type RemoteConfig,
} from "dripline";
import type { Vex } from "vex-core";

export interface Workspace {
  /** Absolute path to the workspace directory (contains `.dripline/`). */
  path: string;
  /** Absolute path to `.dripline/`. */
  configDir: string;
  /** Parsed `.dripline/config.json`. */
  config: DriplineConfig;
  /** Plugins registered into dripline's shared registry. */
  plugins: PluginDef[];
  /** Connection configs indexed by plugin name. Used to resolve a
   *  lane's source config when the lane doesn't declare its own. */
  connectionsByPlugin: Map<string, ConnectionConfig>;
  /** Remote config from the workspace, used as the default sink. */
  remote: RemoteConfig | null;
  /** Rate limits from the workspace. */
  rateLimits: Record<string, RateLimitConfig>;
}

/**
 * Module-level handle set by startServer so the UI queries can read
 * workspace-derived data (plugins, connections, catalog) without
 * threading the Workspace through every plugin's ctx.
 */
let activeWorkspace: Workspace | null = null;

export function setActiveWorkspace(ws: Workspace | null): void {
  activeWorkspace = ws;
}

export function getActiveWorkspace(): Workspace | null {
  return activeWorkspace;
}

/**
 * Load a dripline workspace from a directory. Registers its plugins
 * into dripline's module-level registry and returns a Workspace
 * bundle for the server to use.
 *
 * Throws if the directory doesn't contain a `.dripline/config.json`.
 * A dripline project without a config file isn't a workspace we can
 * drive — fail loud at boot rather than silently running empty.
 */
export async function loadWorkspace(path: string): Promise<Workspace> {
  const configDir = join(path, ".dripline");
  const configFile = join(configDir, "config.json");
  if (!existsSync(configFile)) {
    throw new Error(
      `Not a dripline workspace: ${path} (no .dripline/config.json). Run \`dripline init\` first.`,
    );
  }

  const config: DriplineConfig = JSON.parse(readFileSync(configFile, "utf-8"));
  await loadPluginsFromConfig(configDir);
  const plugins = registry.listPlugins();

  const connectionsByPlugin = new Map<string, ConnectionConfig>();
  for (const conn of config.connections ?? []) {
    // First connection per plugin wins — multiple connections for the
    // same plugin are uncommon and the config order is explicit. A
    // future enhancement can let lanes pick a specific connection by
    // name.
    if (!connectionsByPlugin.has(conn.plugin)) {
      connectionsByPlugin.set(conn.plugin, conn);
    }
  }

  return {
    path,
    configDir,
    config,
    plugins,
    connectionsByPlugin,
    remote: config.remote ?? null,
    rateLimits: config.rateLimits ?? {},
  };
}

/**
 * Upsert lanes from `config.json` into dripyard's lanes table. Called
 * at server boot and after any external edit to the config file.
 *
 * Each lane's `sourcePlugin` is inferred from the first table's owning
 * plugin in the registry — dripline lanes don't carry this field
 * explicitly because the engine resolves per-table, but dripyard needs
 * it for UI grouping and the single-plugin Dripline.create() we use
 * in the orchestrator. Japanika's convention (all tables in a lane
 * come from the same plugin) is what this assumes.
 */
export async function hydrateLanes(
  vex: Vex,
  workspace: Workspace,
): Promise<void> {
  const now = Date.now();
  const sinkConfig = remoteToSinkConfig(workspace.remote);
  const sinkType = workspace.remote?.secretType === "R2" ? "r2" : "s3";

  // One list up-front, O(N) on the number of existing lanes. The
  // previous implementation called `lanes.list` inside the loop, which
  // was O(N²) per hydration — invisible at 5 lanes, painful at 50.
  const existing = (await vex.query("lanes.list")) as Array<{
    _id: string;
    name: string;
  }>;
  const byName = new Map(existing.map((l) => [l.name, l._id]));

  for (const [name, lane] of Object.entries(workspace.config.lanes ?? {})) {
    const sourcePlugin = inferSourcePlugin(lane);
    const sourceConfig =
      workspace.connectionsByPlugin.get(sourcePlugin)?.config ?? {};

    const row = {
      name,
      sourcePlugin,
      sourceConfig,
      tables: lane.tables,
      sinkType,
      sinkConfig,
      proxyEnabled: false,
      schedule: lane.interval,
      maxRuntime: lane.maxRuntime ?? null,
      enabled: true,
      updatedAt: now,
    };

    const existingId = byName.get(name);
    if (existingId) {
      // Keep the stable _id across boots so runs/workers keep their
      // laneId references. A raw insert would churn ids every restart.
      await vex.mutate("lanes.update", { id: existingId, ...row });
    } else {
      await vex.mutate("lanes.create", row);
    }
  }
}

/**
 * Flatten a dripline RemoteConfig into the shape our orchestrator
 * passes to `new Remote()`. Env-var bindings are resolved now so the
 * orchestrator doesn't need to re-read the env on every run.
 */
function remoteToSinkConfig(remote: RemoteConfig | null): Record<string, any> {
  if (!remote) return {};
  const accessKeyId =
    remote.accessKeyId ??
    (remote.accessKeyEnv ? process.env[remote.accessKeyEnv] : undefined);
  const secretAccessKey =
    remote.secretAccessKey ??
    (remote.secretKeyEnv ? process.env[remote.secretKeyEnv] : undefined);
  return {
    endpoint: remote.endpoint,
    bucket: remote.bucket,
    prefix: remote.prefix,
    region: remote.region,
    secretType: remote.secretType,
    accessKeyId,
    secretAccessKey,
  };
}

/**
 * Pick the plugin that owns the first table in a lane. Falls back to
 * scanning the registry for a table with that name — the registry is
 * already populated by the time we call this.
 */
function inferSourcePlugin(lane: LaneConfig): string {
  const first = lane.tables[0];
  if (!first) return "unknown";
  for (const plugin of registry.listPlugins()) {
    if (plugin.tables.some((t) => t.name === first.name)) return plugin.name;
  }
  // Fallback: plugins follow the convention of prefixing table names
  // with the plugin name (tabit_orders → tabit). Used if loading was
  // partial and the registry doesn't know about this table yet.
  const underscore = first.name.indexOf("_");
  return underscore > 0 ? first.name.slice(0, underscore) : first.name;
}
