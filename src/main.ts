#!/usr/bin/env node

import { createRequire } from "node:module";
import { Command } from "commander";
import { init } from "./commands/init.js";

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
  .option("-q, --quiet", "Suppress output")
  .option("--no-color", "Disable color output")
  .hook("preAction", (thisCommand) => {
    if (thisCommand.opts().color === false) {
      process.env.NO_COLOR = "1";
    }
  })
  .addHelpText("after", `
Examples:
  $ dripline query "SELECT name, stargazers_count FROM github_repos WHERE owner = 'torvalds' LIMIT 5"
  $ dripline connection add gh --plugin github --prompt token
  $ dripline plugin list
  $ dripline                              # start interactive REPL

https://github.com/Michaelliv/dripline`);

const queryCmd = program
  .command("query <sql>")
  .alias("q")
  .description("Execute a SQL query")
  .option("-o, --output <format>", "Output format: table, json, csv, line", "table")
  .addHelpText("after", `
Examples:
  $ dripline query "SELECT * FROM github_repos WHERE owner = 'torvalds'"
  $ dripline q "SELECT name, language FROM github_repos WHERE owner = 'torvalds'" -o json
  $ dripline query "SELECT r.name, COUNT(i.id) as issues FROM github_repos r JOIN github_issues i ON r.name = i.repo WHERE r.owner = 'x' AND i.owner = 'x' GROUP BY r.name"`)
  .action(async (sql, opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await query(sql, {
      output: opts.output,
      json: globals.json,
      quiet: globals.quiet,
    });
  });

const connCmd = program.command("connection").alias("conn").description("Manage connections");

connCmd
  .command("add <name>")
  .description("Add a connection")
  .requiredOption("-p, --plugin <plugin>", "Plugin name")
  .option("-s, --set <key=value...>", "Config values", (v: string, prev: string[]) => [...prev, v], [])
  .option("--prompt <key>", "Prompt for a secret value (hidden input)")
  .option("--stdin <key>", "Read a value from stdin")
  .addHelpText("after", `
Examples:
  $ dripline connection add gh --plugin github --prompt token
  $ echo 'ghp_xxx' | dripline connection add gh --plugin github --stdin token
  $ dripline connection add mydb --plugin postgres --set host=localhost --set port=5432`)
  .action(async (name, opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await connectionAdd(name, { plugin: opts.plugin, set: opts.set, prompt: opts.prompt, stdin: opts.stdin, json: globals.json });
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

const pluginCmd = program.command("plugin").description("Manage plugins");

pluginCmd
  .command("install <source>")
  .description("Install a plugin (npm:pkg, git:repo, or local path)")
  .option("-g, --global", "Install globally")
  .addHelpText("after", `
Examples:
  $ dripline plugin install npm:@dripline/aws
  $ dripline plugin install git:github.com/user/repo
  $ dripline plugin install ./my-plugin.ts`)
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

program
  .command("repl")
  .description("Start interactive SQL shell")
  .action(async () => {
    await repl();
  });

program
  .command("init")
  .description("Create .dripline/ in current directory")
  .action(async (_opts, cmd) => {
    const root = cmd.optsWithGlobals();
    await init([], { json: root.json, quiet: root.quiet });
  });

if (process.argv.length <= 2) {
  repl();
} else {
  program.parseAsync(process.argv).catch((err) => {
    console.error("Fatal error:", err.message);
    process.exit(1);
  });
}
