import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import type { PluginDef } from "./types.js";
import { registry } from "./registry.js";

export async function loadPluginFromPath(path: string): Promise<PluginDef> {
  const absPath = resolve(path);
  const mod = await import(pathToFileURL(absPath).href);
  const plugin: PluginDef = mod.default;
  if (!plugin?.name || !plugin?.tables) {
    throw new Error(`Invalid plugin at ${path}: missing name or tables`);
  }
  return plugin;
}

export async function loadBuiltinPlugins(): Promise<void> {
  const pluginsDir = new URL("../plugins", import.meta.url);
  const dirPath = pluginsDir.pathname;
  if (!existsSync(dirPath)) return;

  const files = readdirSync(dirPath).filter(
    (f) =>
      (f.endsWith(".ts") || f.endsWith(".js")) &&
      !f.endsWith(".test.ts") &&
      !f.startsWith("_"),
  );

  for (const file of files) {
    const mod = await import(pathToFileURL(join(dirPath, file)).href);
    if (mod.default?.name && mod.default?.tables) {
      registry.register(mod.default);
    }
  }
}

export async function loadPluginsFromConfig(
  configDir: string,
): Promise<void> {
  const pluginsFile = join(configDir, "plugins.json");
  if (!existsSync(pluginsFile)) return;

  const paths: string[] = JSON.parse(readFileSync(pluginsFile, "utf-8"));
  for (const p of paths) {
    const plugin = await loadPluginFromPath(p);
    registry.register(plugin);
  }
}
