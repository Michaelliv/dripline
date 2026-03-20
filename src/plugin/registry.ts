import type { PluginDef, TableDef } from "./types.js";

export class PluginRegistry {
  private plugins: Map<string, PluginDef> = new Map();

  register(plugin: PluginDef): void {
    this.plugins.set(plugin.name, plugin);
  }

  getPlugin(name: string): PluginDef | undefined {
    return this.plugins.get(name);
  }

  getTable(pluginName: string, tableName: string): TableDef | undefined {
    const plugin = this.plugins.get(pluginName);
    if (!plugin) return undefined;
    return plugin.tables.find((t) => t.name === tableName);
  }

  getAllTables(): Array<{ plugin: string; table: TableDef }> {
    const result: Array<{ plugin: string; table: TableDef }> = [];
    for (const [name, plugin] of this.plugins) {
      for (const table of plugin.tables) {
        result.push({ plugin: name, table });
      }
    }
    return result;
  }

  listPlugins(): PluginDef[] {
    return Array.from(this.plugins.values());
  }
}

export const registry = new PluginRegistry();
