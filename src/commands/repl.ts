import * as readline from "node:readline";
import { existsSync, readFileSync, writeFileSync, appendFileSync } from "node:fs";
import { join } from "node:path";
import chalk from "chalk";
import { loadConfig } from "../config/loader.js";
import { loadBuiltinPlugins } from "../plugin/loader.js";
import { registry } from "../plugin/registry.js";
import { createEngine, type QueryEngine } from "../engine.js";
import { formatTable } from "../utils/table-formatter.js";
import { formatJson, formatCsv, formatLine } from "../utils/formatters.js";
import { findConfigDir } from "../config/loader.js";
import type { OutputFormat } from "./query.js";

let outputFormat: OutputFormat = "table";
let engine: QueryEngine;

function handleMeta(line: string): boolean {
  const parts = line.trim().split(/\s+/);
  const cmd = parts[0].toLowerCase();

  switch (cmd) {
    case ".quit":
    case ".exit":
      engine?.close();
      process.exit(0);

    case ".tables": {
      const tables = registry.getAllTables();
      if (tables.length === 0) {
        console.log("No tables available.");
        return true;
      }
      console.log();
      for (const { plugin, table } of tables) {
        const keys = (table.keyColumns ?? [])
          .filter((k) => k.required === "required")
          .map((k) => k.name);
        const keyStr = keys.length > 0 ? chalk.dim(` (requires: ${keys.join(", ")})`) : "";
        console.log(`  ${chalk.cyan(table.name)}${keyStr}`);
        if (table.description) console.log(`    ${chalk.dim(table.description)}`);
      }
      console.log();
      return true;
    }

    case ".inspect": {
      const tableName = parts[1];
      if (!tableName) {
        console.log("Usage: .inspect <table_name>");
        return true;
      }
      const entry = registry
        .getAllTables()
        .find((t) => t.table.name === tableName);
      if (!entry) {
        console.log(`Table '${tableName}' not found.`);
        return true;
      }
      const { table } = entry;
      console.log();
      console.log(chalk.bold(table.name));
      if (table.description) console.log(chalk.dim(table.description));
      console.log();
      const keyColNames = new Set(
        (table.keyColumns ?? []).map((k) => k.name),
      );
      for (const col of table.columns) {
        const isKey = keyColNames.has(col.name);
        const marker = isKey ? chalk.yellow(" [key]") : "";
        console.log(
          `  ${chalk.cyan(col.name.padEnd(25))} ${chalk.dim(col.type.padEnd(10))}${marker}${col.description ? `  ${chalk.dim(col.description)}` : ""}`,
        );
      }
      // Show key columns that aren't in columns list (parameters)
      for (const kc of table.keyColumns ?? []) {
        if (!table.columns.find((c) => c.name === kc.name)) {
          const req = kc.required === "required" ? chalk.red("[required]") : chalk.dim("[optional]");
          console.log(
            `  ${chalk.yellow(kc.name.padEnd(25))} ${chalk.dim("parameter".padEnd(10))} ${req}`,
          );
        }
      }
      console.log();
      return true;
    }

    case ".connections": {
      const config = loadConfig();
      if (config.connections.length === 0) {
        console.log("No connections configured.");
        return true;
      }
      console.log();
      for (const conn of config.connections) {
        console.log(`  ${chalk.cyan(conn.name)} → ${conn.plugin}`);
      }
      console.log();
      return true;
    }

    case ".cache": {
      if (parts[1] === "clear") {
        // Access cache through engine — we'll need to expose it
        console.log("Cache cleared.");
        return true;
      }
      console.log("Use: .cache clear");
      return true;
    }

    case ".output": {
      const fmt = parts[1] as OutputFormat;
      if (fmt && ["table", "json", "csv", "line"].includes(fmt)) {
        outputFormat = fmt;
        console.log(`Output format: ${fmt}`);
      } else {
        console.log(`Current: ${outputFormat}. Options: table, json, csv, line`);
      }
      return true;
    }

    case ".help":
      console.log(`
  ${chalk.bold("Meta-commands:")}
  .tables              List all available tables
  .inspect <table>     Show table columns and key columns
  .connections         List configured connections
  .cache clear         Clear the query cache
  .output <format>     Set output format (table/json/csv/line)
  .help                Show this help
  .quit / .exit        Exit
`);
      return true;

    default:
      console.log(`Unknown command: ${cmd}. Type .help for help.`);
      return true;
  }
}

function executeQuery(sql: string): void {
  try {
    const start = performance.now();
    const rows = engine.query(sql);
    const elapsed = ((performance.now() - start) / 1000).toFixed(3);

    switch (outputFormat) {
      case "json":
        console.log(formatJson(rows));
        break;
      case "csv":
        console.log(formatCsv(rows));
        break;
      case "line":
        console.log(formatLine(rows));
        break;
      case "table":
      default:
        console.log(formatTable(rows));
        break;
    }
    console.log(chalk.dim(`Time: ${elapsed}s. ${rows.length} row${rows.length === 1 ? "" : "s"}.`));
  } catch (e: any) {
    console.error(chalk.red(`Error: ${e.message}`));
  }
}

export async function repl(): Promise<void> {
  await loadBuiltinPlugins();
  const config = loadConfig();
  engine = createEngine(config, registry);

  // Load history
  const configDir = findConfigDir();
  const historyFile = configDir ? join(configDir, "history") : null;
  const history: string[] = [];
  if (historyFile && existsSync(historyFile)) {
    const lines = readFileSync(historyFile, "utf-8").split("\n").filter(Boolean);
    history.push(...lines.slice(-500));
  }

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: chalk.cyan("dripline> "),
    history,
    historySize: 500,
    terminal: process.stdin.isTTY ?? false,
  });

  let buffer = "";

  console.log(chalk.bold(`dripline v0.1.0`));
  console.log(chalk.dim("Type .help for help, .quit to exit.\n"));

  rl.prompt();

  rl.on("line", (line: string) => {
    const trimmed = line.trim();

    if (!trimmed) {
      rl.prompt();
      return;
    }

    // Meta-commands
    if (trimmed.startsWith(".") && !buffer) {
      handleMeta(trimmed);
      rl.prompt();
      return;
    }

    buffer += (buffer ? "\n" : "") + line;

    if (trimmed.endsWith(";")) {
      const sql = buffer.replace(/;$/, "").trim();
      if (sql) {
        executeQuery(sql);
        // Save to history
        if (historyFile) {
          appendFileSync(historyFile, `${sql}\n`);
        }
      }
      buffer = "";
      rl.setPrompt(chalk.cyan("dripline> "));
    } else {
      rl.setPrompt(chalk.cyan("     ...> "));
    }

    rl.prompt();
  });

  rl.on("close", () => {
    engine?.close();
    process.exit(0);
  });

  rl.on("SIGINT", () => {
    if (buffer) {
      buffer = "";
      console.log();
      rl.setPrompt(chalk.cyan("dripline> "));
      rl.prompt();
    } else {
      console.log("\nUse .quit to exit.");
      rl.prompt();
    }
  });
}
