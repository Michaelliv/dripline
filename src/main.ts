#!/usr/bin/env node

import { createRequire } from "node:module";
import { Command } from "commander";
import { init } from "./commands/init.js";
import { onboard } from "./commands/onboard.js";
import { query } from "./commands/query.js";
import { repl } from "./commands/repl.js";

const require = createRequire(import.meta.url);
const { version } = require("../package.json");

const program = new Command();

program
  .name("dripline")
  .description("Query cloud APIs using SQL")
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

// Default to REPL when no command given
if (process.argv.length <= 2) {
  repl();
} else {
  program.parseAsync(process.argv).catch((err) => {
    console.error("Fatal error:", err.message);
    process.exit(1);
  });
}
