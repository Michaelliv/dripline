import type { VexPluginAPI } from "vex-core";
import { getActiveFlareGun } from "../core/flaregun.js";

/**
 * Proxies plugin — surfaces flaregun state and lifecycle controls
 * in the dashboard.
 *
 * Two tables back the UI:
 *
 *   flaregun_workers   Mirror of the live CF worker pool. `proxies.refresh`
 *                      syncs it from `fg.ls()`; scale/down trigger a refresh
 *                      after the mutation returns. UI subscribers re-render
 *                      automatically via vex's table-level invalidation.
 *
 *   flaregun_samples   Rolling stats history for sparklines. Sampled every
 *                      30s by a job registered in server.ts when flaregun
 *                      is available. Stores cumulative totalRequests; the
 *                      UI computes rate between adjacent samples.
 *
 * All mutations refuse to act when flaregun isn't configured — they
 * throw with the same actionable message as the orchestrator so users
 * don't have to guess why the Proxies page is empty.
 */

function requireFlareGun() {
  const fg = getActiveFlareGun();
  if (!fg) {
    throw new Error(
      "Flaregun is not configured. Set CLOUDFLARE_API_TOKEN and " +
        "CLOUDFLARE_ACCOUNT_ID in the server environment to enable proxies.",
    );
  }
  return fg;
}

/**
 * Replace the `flaregun_workers` table contents with the given list.
 * Called after every fg mutation (refresh/scale/down) to keep the
 * vex mirror in sync with the live CF pool. Truncate + re-insert is
 * fine — the table holds dozens of rows at most, and diffing would
 * only save subscription noise the UI handles cleanly either way.
 */
async function writeWorkerList(
  ctx: { db: { table: (name: string) => any } },
  workers: Array<{ name: string; url: string; createdOn?: string | null }>,
): Promise<void> {
  const existing = (await ctx.db
    .table("flaregun_workers")
    .select(["_id"])
    .all()) as Array<{ _id: string }>;
  for (const row of existing) {
    await ctx.db.table("flaregun_workers").delete(row._id);
  }
  for (const w of workers) {
    await ctx.db.table("flaregun_workers").insert({
      name: w.name,
      url: w.url,
      createdOn: w.createdOn ?? null,
    });
  }
}

export function proxiesPlugin(api: VexPluginAPI) {
  api.setName("proxies");

  api.registerTable("flaregun_workers", {
    columns: {
      name: { type: "string" },
      url: { type: "string" },
      createdOn: { type: "string", optional: true },
    },
    unique: [["name"]],
  });

  api.registerTable("flaregun_samples", {
    columns: {
      timestamp: { type: "number", index: true },
      workerCount: { type: "number" },
      totalRequests: { type: "number" },
      /** Per-worker request counts. JSON-encoded Record<string, number>. */
      perWorker: { type: "json" },
    },
  });

  /**
   * Availability + high-level stats. Pure in-memory read — safe to call
   * on every UI render. Returns `available: false` when flaregun isn't
   * configured so the page can render an empty state instead of erroring.
   */
  api.registerQuery("status", {
    args: {},
    async handler() {
      const fg = getActiveFlareGun();
      if (!fg) {
        return {
          available: false,
          workerCount: 0,
          totalRequests: 0,
        } as const;
      }
      const stats = fg.stats();
      return {
        available: true,
        workerCount: stats.workers,
        totalRequests: stats.totalRequests,
      } as const;
    },
  });

  api.registerQuery("list", {
    args: {},
    async handler(ctx) {
      return ctx.db.table("flaregun_workers").order("name", "asc").all();
    },
  });

  api.registerQuery("samplesRecent", {
    args: {},
    async handler(ctx, args) {
      const since = args.since ?? Date.now() - 30 * 60 * 1000;
      return ctx.db
        .table("flaregun_samples")
        .where("timestamp", ">=", since)
        .order("timestamp", "asc")
        .limit(args.limit ?? 300)
        .all();
    },
  });

  /**
   * Resync `flaregun_workers` from `fg.ls()`. Hits the Cloudflare API,
   * so callers should debounce rather than poll. Server calls this
   * once at boot; scale/down call the helper inline after their own
   * fg mutation succeeds.
   */
  api.registerMutation("refresh", {
    args: {},
    async handler(ctx) {
      const fg = requireFlareGun();
      const workers = await fg.ls();
      await writeWorkerList(ctx, workers);
      return { count: workers.length };
    },
  });

  api.registerMutation("scale", {
    args: { count: "number" },
    async handler(ctx, args) {
      const fg = requireFlareGun();
      if (args.count < 0) {
        throw new Error(`scale count must be >= 0, got ${args.count}`);
      }
      await fg.scale(args.count);
      const workers = await fg.ls();
      await writeWorkerList(ctx, workers);
      return { count: workers.length };
    },
  });

  api.registerMutation("down", {
    args: {},
    async handler(ctx) {
      const fg = requireFlareGun();
      const removed = await fg.down();
      await writeWorkerList(ctx, []);
      return { removed };
    },
  });

  /**
   * Write one sample from flaregun's in-memory stats. Called by the
   * server's `proxies.sample` job every 30s when flaregun is active.
   * Accepts pre-computed values rather than reading flaregun itself so
   * the mutation is testable without an active instance.
   */
  api.registerMutation("sampleRecord", {
    args: {
      workerCount: "number",
      totalRequests: "number",
      perWorker: "json",
    },
    async handler(ctx, args) {
      await ctx.db.table("flaregun_samples").insert({
        timestamp: Date.now(),
        workerCount: args.workerCount,
        totalRequests: args.totalRequests,
        perWorker: args.perWorker,
      });
    },
  });
}
