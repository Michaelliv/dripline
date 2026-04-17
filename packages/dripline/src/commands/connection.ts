import chalk from "chalk";
import {
  addConnection,
  loadConfig,
  removeConnection,
} from "../config/loader.js";
import { bold, dim, error, success } from "../utils/output.js";

export async function connectionAdd(
  name: string,
  options: { plugin: string; set?: string[]; json?: boolean },
): Promise<void> {
  const config: Record<string, any> = {};
  for (const kv of options.set ?? []) {
    const eq = kv.indexOf("=");
    if (eq < 0) {
      error(`Invalid --set format: ${kv} (expected key=value)`);
      process.exit(1);
    }
    config[kv.slice(0, eq)] = kv.slice(eq + 1);
  }

  addConnection({ name, plugin: options.plugin, config });

  if (options.json) {
    console.log(
      JSON.stringify({ success: true, name, plugin: options.plugin }),
    );
  } else {
    success(`Added connection ${bold(name)} (${options.plugin})`);
  }
}

export async function connectionRemove(
  name: string,
  options: { json?: boolean },
): Promise<void> {
  const removed = removeConnection(name);
  if (options.json) {
    console.log(JSON.stringify({ success: removed, name }));
  } else if (removed) {
    success(`Removed connection ${bold(name)}`);
  } else {
    error(`Connection not found: ${name}`);
  }
}

export async function connectionList(options: {
  json?: boolean;
}): Promise<void> {
  const config = loadConfig();

  if (options.json) {
    console.log(JSON.stringify(config.connections));
    return;
  }

  if (config.connections.length === 0) {
    console.log("No connections configured.");
    console.log(
      dim(
        `  Add one: dripline connection add <name> --plugin <plugin> --set key=value`,
      ),
    );
    return;
  }

  console.log();
  for (const conn of config.connections) {
    const keys = Object.keys(conn.config);
    const masked = keys.map((k) => {
      const v = String(conn.config[k]);
      return `${k}=${v.length > 8 ? `${v.slice(0, 4)}...` : v}`;
    });
    console.log(
      `  ${chalk.cyan(conn.name)} > ${conn.plugin}  ${dim(masked.join(", "))}`,
    );
  }
  console.log();
}
