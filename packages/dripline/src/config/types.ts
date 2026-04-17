import type { ConnectionConfig, RateLimitConfig } from "../plugin/types.js";

export interface CacheConfig {
  enabled: boolean;
  ttl: number;
  maxSize: number;
}

/**
 * One entry in a lane's table list. The same table can appear multiple
 * times with different `params` to sync the same table for multiple
 * parameter sets (e.g. github_issues for two different orgs).
 */
export interface LaneTable {
  /** Table name as registered by a plugin (e.g. "github_issues"). */
  name: string;
  /** Sync params passed to engine.sync() for this entry. */
  params?: Record<string, unknown>;
}

/**
 * A lane is a named slice of work synced on a schedule by exactly one
 * worker at a time. Workers compete for lanes via R2-native leases
 * (see core/lease.ts). Adding/removing workers is a deployment change;
 * lane ownership is dynamic.
 */
export interface LaneConfig {
  /** Tables to sync in this lane. Order is preserved. */
  tables: LaneTable[];
  /**
   * How often the lane should run. Format: `<n><unit>` where unit is
   * one of `s|m|h|d`. Examples: "30s", "15m", "1h", "6h", "1d".
   */
  interval: string;
  /**
   * Maximum wall-clock time a single run is allowed before its lease
   * is reclaimable by another worker. Must be < interval. Default: 10m.
   */
  maxRuntime?: string;
}

/**
 * Remote warehouse configuration. When set, `dripline run` publishes
 * synced data to this S3-compatible bucket and hydrates cursor state
 * from it. `dripline query --remote` reads from it.
 */
export interface RemoteConfig {
  /** Endpoint URL. R2: "https://<account>.r2.cloudflarestorage.com". */
  endpoint: string;
  bucket: string;
  /** Optional path prefix inside the bucket. */
  prefix?: string;
  /** Region — R2 ignores it but signing requires a value. Default: "auto". */
  region?: string;
  /**
   * S3 secret type DuckDB will use. "R2" for Cloudflare R2,
   * "S3" for MinIO / generic S3-compatible stores. Default: "S3".
   */
  secretType?: "R2" | "S3";
  /** Access key id. Resolved from `accessKeyEnv` if set. */
  accessKeyId?: string;
  /** Secret access key. Resolved from `secretKeyEnv` if set. */
  secretAccessKey?: string;
  /** Env var holding the access key id (preferred over inline). */
  accessKeyEnv?: string;
  /** Env var holding the secret access key (preferred over inline). */
  secretKeyEnv?: string;
}

export interface DriplineConfig {
  connections: ConnectionConfig[];
  defaultConnection?: string;
  cache: CacheConfig;
  rateLimits: Record<string, RateLimitConfig>;
  /** Named lanes for `dripline run`. Empty in local-only mode. */
  lanes: Record<string, LaneConfig>;
  /** Remote warehouse target. Unset in local-only mode. */
  remote?: RemoteConfig;
}

export const DEFAULT_CONFIG: DriplineConfig = {
  connections: [],
  cache: { enabled: true, ttl: 300, maxSize: 1000 },
  rateLimits: {},
  lanes: {},
};
