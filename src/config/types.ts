import type { ConnectionConfig, RateLimitConfig } from "../plugin/types.js";

export interface CacheConfig {
  enabled: boolean;
  ttl: number;
  maxSize: number;
}

export interface DriplineConfig {
  connections: ConnectionConfig[];
  defaultConnection?: string;
  cache: CacheConfig;
  rateLimits: Record<string, RateLimitConfig>;
}

export const DEFAULT_CONFIG: DriplineConfig = {
  connections: [],
  cache: { enabled: true, ttl: 300, maxSize: 1000 },
  rateLimits: {},
};
