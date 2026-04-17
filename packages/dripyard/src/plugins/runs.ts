import type { VexPluginAPI } from "vex-core";

export function runsPlugin(api: VexPluginAPI) {
  api.setName("runs");

  api.registerTable("runs", {
    columns: {
      laneId: { type: "string", index: true },
      // pending | running | ok | error | skipped
      status: { type: "string", index: true },
      // For skipped runs: why (e.g. "lease held"). For ok/error: null.
      reason: { type: "string", optional: true },
      startedAt: { type: "number", index: true },
      finishedAt: { type: "number", optional: true },
      rowsSynced: { type: "number" },
      filesPublished: { type: "number" },
      error: { type: "string", optional: true },
      workerId: { type: "string", optional: true },
      durationMs: { type: "number", optional: true },
    },
  });

  api.registerTable("run_table_results", {
    columns: {
      runId: { type: "string", index: true },
      tableName: { type: "string" },
      rowsInserted: { type: "number" },
      cursor: { type: "string", optional: true },
      durationMs: { type: "number" },
      error: { type: "string", optional: true },
    },
  });

  /**
   * Live progress ticker — one row per batch emitted by dripline's
   * SyncProgressEvent (fired every 10k rows per table). Enables the UI
   * to show rows-per-sec climbing in real time while a run is active.
   */
  api.registerTable("run_progress", {
    columns: {
      runId: { type: "string", index: true },
      tableName: { type: "string" },
      rowsInserted: { type: "number" }, // cumulative for this table
      cursor: { type: "string", optional: true },
      elapsedMs: { type: "number" },
      rate: { type: "number" }, // rows/sec
      timestamp: { type: "number", index: true },
    },
  });

  api.registerQuery("list", {
    args: {},
    async handler(ctx, args) {
      let q = ctx.db.table("runs").order("startedAt", "desc");
      if (args.laneId) q = q.where("laneId", "=", args.laneId);
      return q.limit(args.limit ?? 50).all();
    },
  });

  api.registerQuery("get", {
    args: { id: "string" },
    async handler(ctx, args) {
      const run = await ctx.db
        .table("runs")
        .where("_id", "=", args.id)
        .first();
      if (!run) return null;
      const tableResults = await ctx.db
        .table("run_table_results")
        .where("runId", "=", args.id)
        .all();
      return { ...run, tableResults };
    },
  });

  api.registerQuery("stats", {
    args: { laneId: "string" },
    async handler(ctx, args) {
      const total = await ctx.db
        .table("runs")
        .where("laneId", "=", args.laneId)
        .count();
      const ok = await ctx.db
        .table("runs")
        .where("laneId", "=", args.laneId)
        .where("status", "=", "ok")
        .count();
      const errors = await ctx.db
        .table("runs")
        .where("laneId", "=", args.laneId)
        .where("status", "=", "error")
        .count();
      const avgDuration = await ctx.db
        .table("runs")
        .where("laneId", "=", args.laneId)
        .where("status", "=", "ok")
        .avg("durationMs");
      const lastRun = await ctx.db
        .table("runs")
        .where("laneId", "=", args.laneId)
        .order("startedAt", "desc")
        .first();
      return { total, ok, errors, avgDurationMs: Math.round(avgDuration), lastRun };
    },
  });

  api.registerMutation("start", {
    args: { laneId: "string" },
    async handler(ctx, args) {
      return ctx.db.table("runs").insert({
        laneId: args.laneId,
        status: "running",
        startedAt: Date.now(),
        rowsSynced: 0,
        filesPublished: 0,
        workerId: args.workerId ?? null,
      });
    },
  });

  api.registerQuery("progress", {
    args: { runId: "string" },
    async handler(ctx, args) {
      return ctx.db
        .table("run_progress")
        .where("runId", "=", args.runId)
        .order("timestamp", "asc")
        .all();
    },
  });

  api.registerMutation("progressAppend", {
    args: {
      runId: "string",
      tableName: "string",
      rowsInserted: "number",
      elapsedMs: "number",
    },
    async handler(ctx, args) {
      const rate =
        args.elapsedMs > 0
          ? Math.round((args.rowsInserted / args.elapsedMs) * 1000)
          : 0;
      await ctx.db.table("run_progress").insert({
        runId: args.runId,
        tableName: args.tableName,
        rowsInserted: args.rowsInserted,
        cursor: args.cursor ?? null,
        elapsedMs: args.elapsedMs,
        rate,
        timestamp: Date.now(),
      });
    },
  });

  api.registerMutation("finish", {
    args: { id: "string", status: "string" },
    async handler(ctx, args) {
      const now = Date.now();
      const run = await ctx.db
        .table("runs")
        .where("_id", "=", args.id)
        .first<{ startedAt: number }>();
      const durationMs = run ? now - run.startedAt : 0;

      await ctx.db.table("runs").update(args.id, {
        status: args.status,
        finishedAt: now,
        durationMs,
        rowsSynced: args.rowsSynced ?? 0,
        filesPublished: args.filesPublished ?? 0,
        error: args.error ?? null,
        reason: args.reason ?? null,
      });

      // Record per-table results if provided
      if (Array.isArray(args.tableResults)) {
        for (const tr of args.tableResults) {
          await ctx.db.table("run_table_results").insert({
            runId: args.id,
            tableName: tr.tableName,
            rowsInserted: tr.rowsInserted ?? 0,
            cursor: tr.cursor ?? null,
            durationMs: tr.durationMs ?? 0,
            error: tr.error ?? null,
          });
        }
      }
    },
  });
}
