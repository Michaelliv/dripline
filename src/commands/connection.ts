import * as readline from "node:readline";
import chalk from "chalk";
import {
  loadConfig,
  addConnection,
  removeConnection,
} from "../config/loader.js";
import { success, error, bold, dim } from "../utils/output.js";

function readStdin(): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = "";
    process.stdin.setEncoding("utf-8");
    process.stdin.on("data", (chunk) => { data += chunk; });
    process.stdin.on("end", () => resolve(data.trim()));
    process.stdin.on("error", reject);
  });
}

function promptSecret(label: string): Promise<string> {
  return new Promise((resolve) => {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stderr,
      terminal: true,
    });
    rl.question(`${label}: `, (answer) => {
      rl.close();
      process.stderr.write("\n");
      resolve(answer.trim());
    });
    // Hide input
    const origWrite = (process.stderr as any)._write;
    (rl as any)._writeToOutput = () => {};
  });
}

export async function connectionAdd(
  name: string,
  options: {
    plugin: string;
    set?: string[];
    prompt?: string;
    stdin?: string;
    json?: boolean;
  },
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

  if (options.stdin) {
    const value = await readStdin();
    if (!value) {
      error("No input received from stdin");
      process.exit(1);
    }
    config[options.stdin] = value;
  }

  if (options.prompt) {
    if (!process.stdin.isTTY) {
      error("--prompt requires an interactive terminal. Use --stdin instead.");
      process.exit(1);
    }
    const value = await promptSecret(options.prompt);
    if (!value) {
      error("No value entered");
      process.exit(1);
    }
    config[options.prompt] = value;
  }

  addConnection({ name, plugin: options.plugin, config });

  if (options.json) {
    console.log(JSON.stringify({ success: true, name, plugin: options.plugin }));
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

export async function connectionList(options: { json?: boolean }): Promise<void> {
  const config = loadConfig();

  if (options.json) {
    console.log(JSON.stringify(config.connections));
    return;
  }

  if (config.connections.length === 0) {
    console.log("No connections configured.");
    console.log(dim(`  Add one: dripline connection add <name> --plugin <plugin> --prompt token`));
    return;
  }

  console.log();
  for (const conn of config.connections) {
    const keys = Object.keys(conn.config);
    const masked = keys.map((k) => {
      const v = String(conn.config[k]);
      return `${k}=${v.length > 8 ? `${v.slice(0, 4)}...` : v}`;
    });
    console.log(`  ${chalk.cyan(conn.name)} → ${conn.plugin}  ${dim(masked.join(", "))}`);
  }
  console.log();
}
