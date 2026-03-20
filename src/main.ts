#!/usr/bin/env node

import { createRequire } from "node:module";
import { Command } from "commander";
import { init } from "./commands/init.js";
import { onboard } from "./commands/onboard.js";
import { query } from "./commands/query.js";
import { repl } from "./commands/repl.js";
import { pluginInstall, pluginRemove, pluginList } from "./commands/plugin.js";
import { connectionAdd, connectionRemove, connectionList } from "./commands/connection.js";

const require = createRequire(import.meta.url);
const { version } = require("../package.json");

const program = new Command();

program
  .name("dripline")
  .description("Query APIs using SQL")
  .version(`dripline ${version}`, "-v, --version")
  .option("--json", "Output as JSON")
  .option("-q, --quiet", "Suppress output");

program
  .command("init")
  .description("Create .dripline/ in current directory")
  .action(async (_opts, cmd) => {
    const root = cmd.optsWithGlobals();
    await init([], { json: root.json, quiet: root.quiet });
  });

program
  .command("onboard")
  .description("Add dripline instructions to CLAUDE.md or AGENTS.md")
  .action(async (_opts, cmd) => {
    const root = cmd.optsWithGlobals();
    await onboard([], { json: root.json, quiet: root.quiet });
  });

program
  .command("query <sql>")
  .alias("q")
  .description("Execute a SQL query")
  .option("-o, --output <format>", "Output format: table, json, csv, line", "table")
  .action(async (sql, opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await query(sql, {
      output: opts.output,
      json: globals.json,
      quiet: globals.quiet,
    });
  });

program
  .command("repl")
  .description("Start interactive SQL shell")
  .action(async () => {
    await repl();
  });

const pluginCmd = program.command("plugin").description("Manage plugins");

pluginCmd
  .command("install <source>")
  .description("Install a plugin (npm:pkg, git:repo, or local path)")
  .option("-g, --global", "Install globally")
  .action(async (source, opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await pluginInstall(source, { global: opts.global, json: globals.json });
  });

pluginCmd
  .command("remove <name>")
  .description("Remove an installed plugin")
  .action(async (name, _opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await pluginRemove(name, { json: globals.json });
  });

pluginCmd
  .command("list")
  .description("List all plugins")
  .action(async (_opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await pluginList({ json: globals.json });
  });

const connCmd = program.command("connection").alias("conn").description("Manage connections");

connCmd
  .command("add <name>")
  .description("Add a connection")
  .requiredOption("-p, --plugin <plugin>", "Plugin name")
  .option("-s, --set <key=value...>", "Config values", (v: string, prev: string[]) => [...prev, v], [])
  .action(async (name, opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await connectionAdd(name, { plugin: opts.plugin, set: opts.set, json: globals.json });
  });

connCmd
  .command("remove <name>")
  .description("Remove a connection")
  .action(async (name, _opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await connectionRemove(name, { json: globals.json });
  });

connCmd
  .command("list")
  .description("List connections")
  .action(async (_opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await connectionList({ json: globals.json });
  });

if (process.argv.length <= 2) {
  repl();
} else {
  program.parseAsync(process.argv).catch((err) => {
    console.error("Fatal error:", err.message);
    process.exit(1);
  });
}
