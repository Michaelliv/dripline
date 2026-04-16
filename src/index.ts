export {
  addConnection,
  findConfigDir,
  getConnection,
  loadConfig,
  removeConnection,
  saveConfig,
} from "./config/loader.js";
export type { CacheConfig, DriplineConfig } from "./config/types.js";
export { DEFAULT_CONFIG } from "./config/types.js";
export { configureCache, QueryCache, queryCache } from "./core/cache.js";
export { createEngine, QueryEngine } from "./core/engine.js";
export type {
  SyncOptions,
  SyncProgressCallback,
  SyncResult,
  SyncTableResult,
} from "./core/engine.js";
export { RateLimiter, rateLimiter } from "./core/rate-limiter.js";
export type {
  DriplinePluginAPI,
  PluginFunction,
  SchemaField,
  TableDefinition,
} from "./plugin/api.js";
export {
  createPluginAPI,
  isPluginFunction,
  resolvePluginExport,
} from "./plugin/api.js";
export type { InstalledPlugin, PluginSource } from "./plugin/installer.js";
export {
  installPlugin,
  listInstalled,
  parsePluginSource,
  removePlugin,
} from "./plugin/installer.js";
export {
  loadAllPlugins,
  loadPluginFromPath,
  loadPluginsFromConfig,
} from "./plugin/loader.js";
export { PluginRegistry, registry } from "./plugin/registry.js";
export type {
  CacheEntry,
  ColumnDef,
  ColumnType,
  ConnectionConfig,
  GetFunc,
  HydrateFunc,
  KeyColumn,
  ListFunc,
  PluginDef,
  Qual,
  QueryContext,
  RateLimitConfig,
  TableDef,
} from "./plugin/types.js";
export { Remote, resolveRemote } from "./core/remote.js";
export type { ResolvedRemote } from "./core/remote.js";
export type {
  LaneConfig,
  LaneTable,
  RemoteConfig,
} from "./config/types.js";
export {
  DEFAULT_MAX_RUNTIME_MS,
  laneLeaseName,
  laneSchema,
  laneStatePath,
  parseInterval,
  validateLane,
  validateLanes,
} from "./core/lanes.js";
export type { ValidatedLane } from "./core/lanes.js";
export { LeaseStore } from "./core/lease.js";
export type { Lease, LeaseConfig } from "./core/lease.js";
export { Dripline } from "./sdk.js";
export type { DriplineOptions } from "./sdk.js";
export type { ExecOptions, ExecResult, OutputParser } from "./utils/cli.js";
export { commandExists, syncExec } from "./utils/cli.js";
export { formatCsv, formatJson, formatLine } from "./utils/formatters.js";
export type { HttpResponse } from "./utils/http.js";
export { syncGet, syncGetPaginated } from "./utils/http.js";
export { asyncGet, asyncGetPaginated } from "./utils/async-http.js";
export { formatTable } from "./utils/table-formatter.js";
