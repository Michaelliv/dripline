import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { ConnectionConfig } from "../plugin/types.js";
import { DEFAULT_CONFIG, type DriplineConfig } from "./types.js";

const DATA_DIR = ".dripline";
const CONFIG_FILE = "config.json";

export function findConfigDir(): string | null {
  let dir = process.cwd();
  while (true) {
    if (existsSync(join(dir, DATA_DIR))) {
      return join(dir, DATA_DIR);
    }
    const parent = join(dir, "..");
    if (parent === dir) return null;
    dir = parent;
  }
}

export function loadConfig(): DriplineConfig {
  const configDir = findConfigDir();
  if (!configDir) return { ...DEFAULT_CONFIG };

  const configFile = join(configDir, CONFIG_FILE);
  if (!existsSync(configFile)) return { ...DEFAULT_CONFIG };

  const raw = JSON.parse(readFileSync(configFile, "utf-8"));
  return {
    ...DEFAULT_CONFIG,
    ...raw,
    cache: { ...DEFAULT_CONFIG.cache, ...raw.cache },
    rateLimits: { ...DEFAULT_CONFIG.rateLimits, ...raw.rateLimits },
  };
}

export function saveConfig(config: DriplineConfig): void {
  const configDir = findConfigDir();
  if (!configDir) {
    const newDir = join(process.cwd(), DATA_DIR);
    mkdirSync(newDir, { recursive: true });
    writeFileSync(
      join(newDir, CONFIG_FILE),
      `${JSON.stringify(config, null, 2)}\n`,
    );
    return;
  }
  writeFileSync(
    join(configDir, CONFIG_FILE),
    `${JSON.stringify(config, null, 2)}\n`,
  );
}

export function getConnection(name: string): ConnectionConfig | undefined {
  const config = loadConfig();
  return config.connections.find((c) => c.name === name);
}

export function resolveEnvConnection(
  plugin: string,
  schema?: Record<string, { env?: string }>,
): ConnectionConfig | null {
  if (!schema) return null;
  const envConfig: Record<string, any> = {};

  for (const [field, def] of Object.entries(schema)) {
    if (def.env && process.env[def.env]) {
      envConfig[field] = process.env[def.env];
    }
  }

  if (Object.keys(envConfig).length === 0) return null;
  return { name: `${plugin}_env`, plugin, config: envConfig };
}

export function applyEnvOverrides(
  conn: ConnectionConfig,
  schema?: Record<string, { env?: string }>,
): ConnectionConfig {
  if (!schema) return conn;
  const merged = { ...conn.config };

  for (const [field, def] of Object.entries(schema)) {
    if (def.env && process.env[def.env]) {
      merged[field] = process.env[def.env];
    }
  }

  return { ...conn, config: merged };
}

export function addConnection(conn: ConnectionConfig): void {
  const config = loadConfig();
  const idx = config.connections.findIndex((c) => c.name === conn.name);
  if (idx >= 0) {
    config.connections[idx] = conn;
  } else {
    config.connections.push(conn);
  }
  saveConfig(config);
}

export function removeConnection(name: string): boolean {
  const config = loadConfig();
  const idx = config.connections.findIndex((c) => c.name === name);
  if (idx < 0) return false;
  config.connections.splice(idx, 1);
  saveConfig(config);
  return true;
}
