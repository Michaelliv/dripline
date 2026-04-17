#!/usr/bin/env bun
import { Command } from "commander";
import chalk from "chalk";

const BASE_URL =
  process.env.DRIPYARD_URL ?? "http://localhost:3457";

async function queryServer(name: string, args: Record<string, any> = {}) {
  const res = await fetch(`${BASE_URL}/vex/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, args }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(json.error ?? "Query failed");
  return json.data;
}

async function mutateServer(name: string, args: Record<string, any> = {}) {
  const res = await fetch(`${BASE_URL}/vex/mutate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, args }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(json.error ?? "Mutation failed");
  return json.data;
}

const program = new Command()
  .name("dripyard")
  .description("Dashboard + worker supervisor for dripline projects")
  .version("0.1.0");

// ── serve ──

program
  .command("serve [workspace]")
  .description(
    "Start the dripyard server. Pass a dripline workspace path (directory with .dripline/) to load plugins and lanes. Defaults to cwd.",
  )
  .option("-p, --port <port>", "Port to listen on", "3457")
  .option("--socket <path>", "Unix socket path for local workers")
  .action(async (workspace: string | undefined, opts) => {
    const { startServer } = await import("./server.js");
    await startServer({
      port: Number(opts.port),
      socketPath: opts.socket,
      workspace: workspace ?? process.cwd(),
    });
  });

// ── worker ──

program
  .command("worker [workspace]")
  .description(
    "Start a standalone worker. Pass a dripline workspace path to load plugins; defaults to cwd.",
  )
  .requiredOption(
    "--socket <path>",
    "Unix socket path of the dashboard to connect to",
  )
  .option("--name <name>", "Worker name (defaults to hostname-pid)")
  .action(async (workspace: string | undefined, opts) => {
    const { loadWorkspace } = await import("./core/workspace.js");
    const { registry } = await import("dripline");
    const { startWorker } = await import("./worker.js");

    // Load the workspace so plugins land in dripline's shared registry.
    // Resolver then reads from the registry.
    const ws = await loadWorkspace(workspace ?? process.cwd());
    console.log(
      `[worker] workspace ${ws.path} — ${ws.plugins.length} plugin(s) loaded`,
    );

    await startWorker({
      socketPath: opts.socket,
      name: opts.name,
      resolvePlugin: (name: string) => {
        const plugin = registry.getPlugin(name);
        if (!plugin)
          throw new Error(
            `Plugin not registered: ${name}. Check ${ws.configDir}/plugins.json`,
          );
        return plugin;
      },
    });
    await new Promise(() => {});
  });

// ── lane ──

const lane = program
  .command("lane")
  .description("Manage lanes");

lane
  .command("create")
  .description("Create a new lane")
  .requiredOption("-n, --name <name>", "Lane name")
  .requiredOption("-s, --source <plugin>", "Source plugin name")
  .requiredOption("--schedule <schedule>", "Sync schedule (e.g. 'every 15m')")
  .option("--sink-type <type>", "Sink type (s3 or r2)", "s3")
  .option("--source-config <json>", "Source config JSON", "{}")
  .option("--sink-config <json>", "Sink config JSON", "{}")
  .option("--proxy", "Enable proxy rotation")
  .action(async (opts) => {
    try {
      const id = await mutateServer("lanes.create", {
        name: opts.name,
        sourcePlugin: opts.source,
        sourceConfig: JSON.parse(opts.sourceConfig),
        sinkType: opts.sinkType,
        sinkConfig: JSON.parse(opts.sinkConfig),
        schedule: opts.schedule,
        proxyEnabled: opts.proxy ?? false,
      });
      console.log(chalk.green(`✓ Lane created: ${id}`));
    } catch (e: any) {
      console.error(chalk.red(`✗ ${e.message}`));
      process.exit(1);
    }
  });

lane
  .command("list")
  .description("List all lanes")
  .option("--json", "Output as JSON")
  .action(async (opts) => {
    const lanes = await queryServer("lanes.list");
    if (opts.json) {
      console.log(JSON.stringify(lanes, null, 2));
      return;
    }
    if (lanes.length === 0) {
      console.log("No lanes configured.");
      return;
    }
    for (const p of lanes) {
      const status = p.enabled ? chalk.green("●") : chalk.gray("○");
      console.log(
        `${status} ${chalk.bold(p.name)}  ${chalk.dim(p.sourcePlugin)}  ${chalk.dim(p.schedule)}  ${chalk.dim(p._id)}`,
      );
    }
  });

lane
  .command("run <id>")
  .description("Trigger an immediate lane run")
  .action(async (id) => {
    try {
      const runId = await mutateServer("runs.start", { laneId: id });
      console.log(chalk.green(`✓ Run started: ${runId}`));
    } catch (e: any) {
      console.error(chalk.red(`✗ ${e.message}`));
      process.exit(1);
    }
  });

// ── runs ──

program
  .command("runs [lane-id]")
  .description("List runs for a lane")
  .option("--json", "Output as JSON")
  .option("-l, --limit <n>", "Max results", "20")
  .action(async (laneId, opts) => {
    const args: any = { limit: Number(opts.limit) };
    if (laneId) args.laneId = laneId;
    const runs = await queryServer("runs.list", args);
    if (opts.json) {
      console.log(JSON.stringify(runs, null, 2));
      return;
    }
    if (runs.length === 0) {
      console.log("No runs found.");
      return;
    }
    for (const r of runs) {
      const icon =
        r.status === "ok"
          ? chalk.green("✓")
          : r.status === "error"
            ? chalk.red("✗")
            : chalk.yellow("…");
      const dur = r.durationMs ? `${r.durationMs}ms` : "";
      console.log(
        `${icon} ${r._id}  ${chalk.dim(r.status)}  ${chalk.dim(String(r.rowsSynced))} rows  ${chalk.dim(dur)}`,
      );
    }
  });

// ── status ──

program
  .command("status")
  .description("Show orchestrator status")
  .option("--json", "Output as JSON")
  .action(async (opts) => {
    const lanes = await queryServer("lanes.list");
    const workers = await queryServer("workers.list");
    const runs = await queryServer("runs.list", { limit: 50 });

    const status = {
      lanes: {
        total: lanes.length,
        enabled: lanes.filter((p: any) => p.enabled).length,
      },
      workers: workers.length,
      recentRuns: {
        ok: runs.filter((r: any) => r.status === "ok").length,
        error: runs.filter((r: any) => r.status === "error").length,
        running: runs.filter((r: any) => r.status === "running").length,
      },
    };

    if (opts.json) {
      console.log(JSON.stringify(status, null, 2));
      return;
    }

    console.log(chalk.bold("Dripyard Status"));
    console.log(
      `  Lanes: ${status.lanes.enabled}/${status.lanes.total} enabled`,
    );
    console.log(`  Workers:   ${status.workers}`);
    console.log(
      `  Runs:      ${chalk.green(String(status.recentRuns.ok))} ok, ${chalk.red(String(status.recentRuns.error))} errors, ${chalk.yellow(String(status.recentRuns.running))} running`,
    );
  });

program.parse();
