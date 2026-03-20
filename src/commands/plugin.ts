import chalk from "chalk";
import {
  installPlugin,
  listInstalled,
  removePlugin,
} from "../plugin/installer.js";
import { loadAllPlugins, loadPluginFromPath } from "../plugin/loader.js";
import { registry } from "../plugin/registry.js";
import { bold, dim, error, success } from "../utils/output.js";

export async function pluginInstall(
  source: string,
  options: { global?: boolean; json?: boolean },
): Promise<void> {
  try {
    const result = await installPlugin(source, { global: options.global });
    const plugin = await loadPluginFromPath(result.path);
    registry.register(plugin);

    if (options.json) {
      console.log(
        JSON.stringify({
          success: true,
          name: plugin.name,
          tables: plugin.tables.length,
          path: result.path,
        }),
      );
    } else {
      success(
        `Installed ${bold(plugin.name)} (${plugin.tables.length} table${plugin.tables.length === 1 ? "" : "s"})`,
      );
    }
  } catch (e: any) {
    if (options.json) {
      console.log(JSON.stringify({ success: false, error: e.message }));
    } else {
      error(`Failed to install: ${e.message}`);
    }
    process.exit(1);
  }
}

export async function pluginRemove(
  name: string,
  options: { json?: boolean },
): Promise<void> {
  const removed = removePlugin(name);
  if (options.json) {
    console.log(JSON.stringify({ success: removed, name }));
  } else if (removed) {
    success(`Removed ${bold(name)}`);
  } else {
    error(`Plugin not found: ${name}`);
  }
}

export async function pluginList(options: { json?: boolean }): Promise<void> {
  await loadAllPlugins();

  const builtins = registry.listPlugins();
  const installed = listInstalled();

  if (options.json) {
    console.log(
      JSON.stringify({
        builtin: builtins.map((p) => ({
          name: p.name,
          version: p.version,
          tables: p.tables.map((t) => t.name),
        })),
        installed: installed.map((p) => ({
          name: p.name,
          type: p.type,
          source: p.source,
          path: p.path,
        })),
      }),
    );
    return;
  }

  console.log();
  console.log(bold("Built-in plugins:"));
  for (const p of builtins) {
    const tables = p.tables.map((t) => t.name).join(", ");
    console.log(
      `  ${chalk.cyan(p.name)} ${dim(`v${p.version}`)}  ${dim(tables)}`,
    );
  }

  if (installed.length > 0) {
    console.log();
    console.log(bold("Installed plugins:"));
    for (const p of installed) {
      console.log(
        `  ${chalk.cyan(p.name)} ${dim(`[${p.type}]`)}  ${dim(p.source)}`,
      );
    }
  }

  console.log();
}
