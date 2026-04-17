import { parseInterval } from "dripline";
import type { VexPluginAPI } from "vex-core";
import type { Orchestrator } from "../core/orchestrator.js";

/**
 * Dashboard wires its Orchestrator in here at startup so the UI's
 * "Run now" button has something to dispatch to. Null in environments
 * where runs are triggered exclusively by the scheduler (no ad-hoc UI).
 */
let activeOrchestrator: Orchestrator | null = null;
let activeWorkerId: string | null = null;

export function setLanesOrchestrator(
  orchestrator: Orchestrator | null,
  workerId: string | null = null,
): void {
  activeOrchestrator = orchestrator;
  activeWorkerId = workerId;
}

export function lanesPlugin(api: VexPluginAPI) {
  api.setName("lanes");

  api.registerTable("lanes", {
    columns: {
      name: { type: "string", index: true },
      description: { type: "string", optional: true },
      sourcePlugin: { type: "string" },
      sourceConfig: { type: "json" },
      /**
       * Tables to sync, mirrors dripline's LaneTable[]:
       *   [{ name: "github_issues", params: { org: "acme" } }, ...]
       * Empty array means "sync all tables the plugin exposes".
       */
      tables: { type: "json" },
      sinkType: { type: "string" }, // "s3" | "r2"
      sinkConfig: { type: "json" },
      proxyEnabled: { type: "boolean" },
      proxyStrategy: { type: "string", optional: true },
      /** Interval between runs, e.g. "15m", "1h", "6h". */
      schedule: { type: "string" },
      /**
       * Wall-clock cap per run. Default: half the interval, capped at 10m.
       * Must be strictly less than schedule — enforced in create/update.
       */
      maxRuntime: { type: "string", optional: true },
      enabled: { type: "boolean" },
      createdAt: { type: "number" },
      updatedAt: { type: "number" },
    },
    unique: [["name"]],
  });

  api.registerQuery("list", {
    args: {},
    async handler(ctx) {
      return ctx.db.table("lanes").order("name", "asc").all();
    },
  });

  api.registerQuery("get", {
    args: { id: "string" },
    async handler(ctx, args) {
      return ctx.db.table("lanes").where("_id", "=", args.id).first();
    },
  });

  api.registerMutation("create", {
    args: {
      name: "string",
      sourcePlugin: "string",
      sourceConfig: "any",
      sinkType: "string",
      sinkConfig: "any",
      schedule: "string",
    },
    async handler(ctx, args) {
      validateScheduleAndRuntime(args.schedule, args.maxRuntime);
      const now = Date.now();
      return ctx.db.table("lanes").insert({
        name: args.name,
        description: args.description ?? null,
        sourcePlugin: args.sourcePlugin,
        sourceConfig: args.sourceConfig,
        tables: Array.isArray(args.tables) ? args.tables : [],
        sinkType: args.sinkType,
        sinkConfig: args.sinkConfig,
        proxyEnabled: args.proxyEnabled ?? false,
        proxyStrategy: args.proxyStrategy ?? null,
        schedule: args.schedule,
        maxRuntime: args.maxRuntime ?? null,
        enabled: true,
        createdAt: now,
        updatedAt: now,
      });
    },
  });

  api.registerMutation("update", {
    args: { id: "string" },
    async handler(ctx, args) {
      const { id, ...data } = args;
      if (data.schedule || data.maxRuntime !== undefined) {
        const existing = await ctx.db
          .table("lanes")
          .where("_id", "=", id)
          .first<{ schedule: string; maxRuntime: string | null }>();
        if (!existing) throw new Error(`Lane not found: ${id}`);
        validateScheduleAndRuntime(
          data.schedule ?? existing.schedule,
          data.maxRuntime !== undefined
            ? data.maxRuntime
            : existing.maxRuntime,
        );
      }
      await ctx.db.table("lanes").update(id, {
        ...data,
        updatedAt: Date.now(),
      });
    },
  });

  api.registerMutation("delete", {
    args: { id: "string" },
    async handler(ctx, args) {
      await ctx.db.table("lanes").delete(args.id);
    },
  });

  api.registerMutation("toggle", {
    args: { id: "string" },
    async handler(ctx, args) {
      const lane = await ctx.db
        .table("lanes")
        .where("_id", "=", args.id)
        .first<{ enabled: boolean }>();
      if (!lane) throw new Error(`Lane not found: ${args.id}`);
      await ctx.db.table("lanes").update(args.id, {
        enabled: !lane.enabled,
        updatedAt: Date.now(),
      });
      return { enabled: !lane.enabled };
    },
  });

  /**
   * Trigger a lane run immediately, bypassing its schedule.
   *
   * Fire-and-forget: we kick off executeLane() on the dashboard's
   * embedded orchestrator and return as soon as the work is scheduled.
   * The UI relies on its live `runs.list` subscription to render the
   * new run row within ~100ms, so there's no benefit to blocking here
   * until the sync finishes.
   *
   * Errors during the run land in the runs table (status="error") and
   * surface in the UI the same way scheduled failures do — this
   * mutation only rejects if the dispatch itself is impossible
   * (no orchestrator wired, lane missing, lane disabled).
   */
  api.registerMutation("runNow", {
    args: { id: "string" },
    async handler(ctx, args) {
      if (!activeOrchestrator) {
        throw new Error(
          "No orchestrator configured. The dashboard must be running in embedded-worker mode for runNow to work.",
        );
      }
      const lane = await ctx.db
        .table("lanes")
        .where("_id", "=", args.id)
        .first<{ enabled: boolean; name: string }>();
      if (!lane) throw new Error(`Lane not found: ${args.id}`);
      if (!lane.enabled) {
        throw new Error(
          `Lane "${lane.name}" is disabled. Enable it before triggering a run.`,
        );
      }
      // Fire and forget — the orchestrator writes runs.start/finish
      // itself, which the UI subscribes to. Swallow the promise so
      // this mutation returns immediately.
      const orchestrator = activeOrchestrator;
      const workerId = activeWorkerId ?? undefined;
      orchestrator.executeLane(args.id, workerId).catch((err) => {
        console.error(
          `[lanes.runNow] ${lane.name} failed:`,
          (err as Error)?.message ?? err,
        );
      });
      return { triggered: true };
    },
  });
}

/**
 * Mirror dripline's lane validation: interval parses, maxRuntime parses,
 * maxRuntime < interval. Fail loudly at create/update so bad config
 * never lands in the DB.
 *
 * We accept both vex-core's "every 15m" form (what the scheduler passes
 * to vex.addJob) and dripline's bare "15m" form — strip the prefix
 * before handing to dripline's parser.
 */
function validateScheduleAndRuntime(
  schedule: string,
  maxRuntime: string | null | undefined,
): void {
  const intervalMs = parseInterval(stripEvery(schedule));
  if (maxRuntime) {
    const maxRuntimeMs = parseInterval(stripEvery(maxRuntime));
    if (maxRuntimeMs >= intervalMs) {
      throw new Error(
        `maxRuntime (${maxRuntime}) must be less than schedule (${schedule})`,
      );
    }
  }
}

function stripEvery(spec: string): string {
  return spec.replace(/^every\s+/, "").trim();
}
