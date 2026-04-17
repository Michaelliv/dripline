#!/usr/bin/env bun

import { createRequire } from "node:module";
import { Command } from "commander";
import { CompactConfigError, compact } from "./commands/compact.js";
import {
  connectionAdd,
  connectionList,
  connectionRemove,
} from "./commands/connection.js";
import { init } from "./commands/init.js";
import {
  LaneConfigError,
  laneAdd,
  laneList,
  laneRemove,
  laneReset,
} from "./commands/lane.js";
import { pluginInstall, pluginList, pluginRemove } from "./commands/plugin.js";
import { query } from "./commands/query.js";
import {
  RemoteConfigError,
  remoteSet,
  remoteShow,
} from "./commands/remote.js";
import { repl } from "./commands/repl.js";
import { RunConfigError, run } from "./commands/run.js";
import { tables } from "./commands/tables.js";

const require = createRequire(import.meta.url);
const { version } = require("../package.json");

const program = new Command();

program
  .name("dripline")
  .description("Query mode for agents")
  .version(`dripline ${version}`, "-v, --version")
  .option("--json", "Output as JSON")
  .option("-q, --quiet", "Suppress output")
  .option("--no-color", "Disable color output")
  .hook("preAction", (thisCommand) => {
    if (thisCommand.opts().color === false) {
      process.env.NO_COLOR = "1";
    }
  })
  .addHelpText(
    "after",
    `
Examples:
  $ dripline query "SELECT name, stargazers_count FROM github_repos WHERE owner = 'torvalds' LIMIT 5"
  $ dripline connection add gh --plugin github --set token=ghp_xxx
  $ dripline plugin list
  $ dripline                              # start interactive REPL

https://github.com/Michaelliv/dripline`,
  );

const _queryCmd = program
  .command("query <sql>")
  .alias("q")
  .description("Execute a SQL query")
  .option(
    "-o, --output <format>",
    "Output format: table, json, csv, line",
    "table",
  )
  .option(
    "--remote",
    "Read from the configured remote warehouse instead of plugins",
  )
  .addHelpText(
    "after",
    `
Examples:
  $ dripline query "SELECT * FROM github_repos WHERE owner = 'torvalds'"
  $ dripline q "SELECT name, language FROM github_repos WHERE owner = 'torvalds'" -o json
  $ dripline query --remote "SELECT COUNT(*) FROM github_issues WHERE state = 'open'"

Local mode (default): plugins materialize tables on demand from live
APIs. Same SQL surface as live dripline.

Remote mode (--remote): plugins are bypassed entirely. Tables are
attached as views over curated parquet files in the configured
remote bucket. No API calls fire. Run \`dripline run\` and \`dripline
compact\` to populate the warehouse.`,
  )
  .action(async (sql, opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await query(sql, {
      output: opts.output,
      remote: opts.remote,
      json: globals.json,
      quiet: globals.quiet,
    });
  });

const connCmd = program
  .command("connection")
  .alias("conn")
  .description("Manage connections");

connCmd
  .command("add <name>")
  .description("Add a connection")
  .requiredOption("-p, --plugin <plugin>", "Plugin name")
  .option(
    "-s, --set <key=value...>",
    "Config values",
    (v: string, prev: string[]) => [...prev, v],
    [],
  )
  .addHelpText(
    "after",
    `
Examples:
  $ dripline connection add gh --plugin github --set token=ghp_xxx
  $ dripline connection add mydb --plugin postgres --set host=localhost --set port=5432`,
  )
  .action(async (name, opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await connectionAdd(name, {
      plugin: opts.plugin,
      set: opts.set,
      json: globals.json,
    });
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
  .addHelpText(
    "after",
    `
Examples:
  $ dripline plugin install npm:@dripline/aws
  $ dripline plugin install git:github.com/user/repo
  $ dripline plugin install ./my-plugin.ts`,
  )
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
  .command("tables")
  .description("List all available tables and their schemas")
  .action(async (_opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await tables({ json: globals.json });
  });

program
  .command("repl")
  .description("Start interactive SQL shell")
  .action(async () => {
    await repl();
  });

program
  .command("run")
  .description("Run due lanes — sync, publish to remote, advance cursors")
  .option("--lane <name>", "Only run the named lane")
  .addHelpText(
    "after",
    `
Examples:
  $ dripline run                       # run every lane that's due
  $ dripline run --lane fast           # run only the "fast" lane
  $ dripline run --json                # machine-readable summary

Lanes are declared in .dripline/config.json under the "lanes" key.
Workers compete for lanes via leases stored in the configured remote;
adding more workers (e.g. more cron entries) is safe and requires no
config changes.`,
  )
  .action(async (opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    try {
      const results = await run({
        lane: opts.lane,
        json: globals.json,
        quiet: globals.quiet,
      });
      if (results.some((r) => r.status === "error")) process.exit(1);
    } catch (e) {
      if (e instanceof RunConfigError) {
        console.error(e.message);
        process.exit(1);
      }
      throw e;
    }
  });

program
  .command("compact")
  .description("Compact raw/ into curated/ — dedupe and refresh manifests")
  .option(
    "-t, --table <name...>",
    "Only compact the named table(s)",
    (v: string, prev: string[]) => [...prev, v],
    [],
  )
  .addHelpText(
    "after",
    `
Examples:
  $ dripline compact                      # compact every compactable table
  $ dripline compact -t github_issues     # compact a single table
  $ dripline compact --json               # machine-readable summary

A table is compactable iff its plugin declares a primaryKey. Compaction
is idempotent and single-writer-per-table via R2-native leases, so
running multiple compactors against the same warehouse is safe — they
will divide the work via lease contention.`,
  )
  .action(async (opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    try {
      const results = await compact({
        tables: opts.table && opts.table.length > 0 ? opts.table : undefined,
        json: globals.json,
        quiet: globals.quiet,
      });
      if (results.some((r) => r.status === "error")) process.exit(1);
    } catch (e) {
      if (e instanceof CompactConfigError) {
        console.error(e.message);
        process.exit(1);
      }
      throw e;
    }
  });

// ── remote ──────────────────────────────────────────────────────────

const remoteCmd = program
  .command("remote")
  .description("Manage the warehouse remote");

remoteCmd
  .command("set <endpoint>")
  .description("Configure the warehouse remote")
  .requiredOption("--bucket <name>", "Bucket name")
  .option("--prefix <path>", "Path prefix inside the bucket")
  .option("--region <name>", "Region (default: auto)")
  .option("--secret-type <type>", "R2 or S3 (default: S3)")
  .option("--access-key <key>", "Access key id (stored inline)")
  .option("--secret-key <key>", "Secret access key (stored inline)")
  .option("--access-key-env <var>", "Env var holding the access key id")
  .option("--secret-key-env <var>", "Env var holding the secret access key")
  .addHelpText(
    "after",
    `
Examples:
  $ dripline remote set https://<account>.r2.cloudflarestorage.com \\
      --bucket warehouse --prefix prod --secret-type R2 \\
      --access-key-env R2_KEY_ID --secret-key-env R2_SECRET

  $ dripline remote set http://localhost:9100 \\
      --bucket dripline-test --access-key testkey --secret-key testsecret123

Prefer --access-key-env / --secret-key-env so credentials stay out
of .dripline/config.json.`,
  )
  .action(async (endpoint, opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    try {
      await remoteSet(endpoint, {
        bucket: opts.bucket,
        prefix: opts.prefix,
        region: opts.region,
        secretType: opts.secretType,
        accessKey: opts.accessKey,
        secretKey: opts.secretKey,
        accessKeyEnv: opts.accessKeyEnv,
        secretKeyEnv: opts.secretKeyEnv,
        json: globals.json,
      });
    } catch (e) {
      if (e instanceof RemoteConfigError) {
        console.error(e.message);
        process.exit(1);
      }
      throw e;
    }
  });

remoteCmd
  .command("show")
  .description("Show the configured warehouse remote (secrets redacted)")
  .action(async (_opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await remoteShow({ json: globals.json });
  });

// ── lane ────────────────────────────────────────────────

const laneCmd = program
  .command("lane")
  .description("Manage warehouse lanes");

laneCmd
  .command("add <name>")
  .description("Add a lane")
  .requiredOption(
    "-t, --table <name...>",
    "Table name (repeatable)",
    (v: string, prev: string[]) => [...prev, v],
    [],
  )
  .option(
    "-p, --params <kv...>",
    'Params for the matching --table, "k=v,k=v" or "-" for none (repeatable)',
    (v: string, prev: string[]) => [...prev, v],
    [],
  )
  .requiredOption("-i, --interval <dur>", "Sync interval (e.g. 15m, 1h, 6h)")
  .option("--max-runtime <dur>", "Max wall-clock per run (default: min(10m, interval/2))")
  .option("--force", "Overwrite an existing lane with the same name")
  .addHelpText(
    "after",
    `
Examples:
  $ dripline lane add fast \\
      --table github_issues --params owner=acme,repo=api \\
      --table github_prs --params owner=acme,repo=api \\
      --interval 15m --max-runtime 5m

  $ dripline lane add slow \\
      --table github_repos --params owner=acme \\
      --interval 6h

Each --params entry corresponds positionally to a --table. Use "-"
for a param-less table that sits between others that have params.`,
  )
  .action(async (name, opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    try {
      await laneAdd(name, {
        table: opts.table,
        params: opts.params,
        interval: opts.interval,
        maxRuntime: opts.maxRuntime,
        force: opts.force,
        json: globals.json,
      });
    } catch (e) {
      if (e instanceof LaneConfigError) {
        console.error(e.message);
        process.exit(1);
      }
      throw e;
    }
  });

laneCmd
  .command("remove <name>")
  .description("Remove a lane")
  .action(async (name, _opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    try {
      await laneRemove(name, { json: globals.json });
    } catch (e) {
      if (e instanceof LaneConfigError) {
        console.error(e.message);
        process.exit(1);
      }
      throw e;
    }
  });

laneCmd
  .command("reset <name>")
  .description("Release a lane's lease (or fully reset with --hard)")
  .option("--yes", "Skip the confirmation prompt")
  .option("--hard", "Also delete cursor state — next run will backfill from scratch")
  .addHelpText(
    "after",
    `
By default, releases the lane's lease so the next run can start
immediately. Cursor state is preserved — sync resumes where it
left off.

With --hard, also deletes _state/<lane>/. The next \`dripline run\`
will see "first sync ever" and backfill according to each plugin's
initialCursor. raw/ and curated/ are never touched.`,
  )
  .action(async (name, opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    try {
      await laneReset(name, { yes: opts.yes, json: globals.json, hard: opts.hard });
    } catch (e) {
      if (e instanceof LaneConfigError) {
        console.error(e.message);
        process.exit(1);
      }
      throw e;
    }
  });

laneCmd
  .command("list")
  .description("List configured lanes")
  .action(async (_opts, cmd) => {
    const globals = cmd.optsWithGlobals();
    await laneList({ json: globals.json });
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
