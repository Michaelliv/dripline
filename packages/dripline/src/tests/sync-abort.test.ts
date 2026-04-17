/**
 * sync() AbortSignal cancellation tests.
 *
 * The contract, exercised end-to-end here:
 *   - signal already aborted → sync() throws AbortError immediately,
 *     no plugin code runs, no rows in the table.
 *   - signal aborted mid-stream → throws at the next checkpoint (top
 *     of the row iterator or between batches), leaving already-flushed
 *     rows in place but NOT writing cursor metadata for the aborted
 *     table. Next sync resumes from the prior cursor — no re-backfill.
 *   - signal propagates into ctx.signal so plugins can wire it to
 *     fetch() for instant HTTP cancellation.
 *   - signal fires between tables → subsequent tables don't start.
 *   - back-compat: passing a bare SyncProgressCallback (not an options
 *     object) still works, same as before the opts change.
 *   - rate-limiter permit is released on abort (try/finally), so the
 *     next sync isn't starved by a zombie permit.
 */

import { strict as assert } from "node:assert";
import { afterEach, describe, it } from "node:test";
import { Database } from "../core/db.js";
import type {
  PluginDef,
  QueryContext,
  SyncProgressEvent,
} from "../plugin/types.js";
import { Dripline } from "../sdk.js";

let dl: Dripline;
let db: Database;

async function setup(plugin: PluginDef, schema = "s"): Promise<void> {
  db = await Database.create(":memory:");
  dl = await Dripline.create({ plugins: [plugin], database: db, schema });
}

async function cleanup(): Promise<void> {
  if (dl) {
    try {
      await dl.close();
    } catch {}
    dl = null as any;
  }
  if (db) {
    try {
      await db.close();
    } catch {}
    db = null as any;
  }
}

/**
 * Plugin that yields `rows.length` items, with an optional per-yield
 * hook so tests can abort mid-stream. Captures the ctx so tests can
 * assert that ctx.signal was populated.
 */
function makePlugin(opts: {
  rows: Array<Record<string, any>>;
  afterYield?: (index: number, ctx: QueryContext) => void | Promise<void>;
  cursor?: string;
}): { plugin: PluginDef; capture: () => QueryContext[] } {
  const captured: QueryContext[] = [];
  const plugin: PluginDef = {
    name: "abort_test",
    version: "1.0.0",
    tables: [
      {
        name: "items",
        columns: [
          { name: "id", type: "number" },
          { name: "updated_at", type: "datetime" },
        ],
        primaryKey: ["id"],
        cursor: opts.cursor,
        async *list(ctx) {
          captured.push(ctx);
          for (let i = 0; i < opts.rows.length; i++) {
            yield opts.rows[i];
            if (opts.afterYield) await opts.afterYield(i, ctx);
          }
        },
      },
    ],
  };
  return { plugin, capture: () => captured };
}

describe("sync() — AbortSignal", () => {
  afterEach(cleanup);

  it("throws AbortError when signal is already aborted", async () => {
    const { plugin, capture } = makePlugin({
      rows: [{ id: 1, updated_at: "2024-01-01" }],
    });
    await setup(plugin);

    const controller = new AbortController();
    controller.abort();

    await assert.rejects(
      () => dl.sync({ items: {} }, { signal: controller.signal }),
      (err: Error) => err.name === "AbortError",
    );

    // Plugin's list() should never have been entered — the between-tables
    // checkpoint fires before syncTable is called.
    assert.equal(capture().length, 0);

    // Table exists (CREATE TABLE runs at init) but is empty.
    const rows = await dl.query('SELECT * FROM "s"."items"');
    assert.equal(rows.length, 0);
  });

  it("aborts mid-stream at the row-iterator checkpoint", async () => {
    // 25_000 rows so we blow past the BATCH_SIZE (10_000) checkpoint
    // at least twice. Abort partway in.
    const rows = Array.from({ length: 25_000 }, (_, i) => ({
      id: i + 1,
      updated_at: "2024-01-01",
    }));
    const controller = new AbortController();
    const { plugin } = makePlugin({
      rows,
      afterYield: (index) => {
        // Abort after the 12_000th yield — guaranteed to be past the
        // first batch flush (10_000) but before the second (20_000).
        if (index === 12_000) controller.abort();
      },
    });
    await setup(plugin);

    await assert.rejects(
      () => dl.sync({ items: {} }, { signal: controller.signal }),
      (err: Error) => err.name === "AbortError",
    );

    // The first batch (10_000 rows) was flushed before the abort, so
    // it's visible in the table. The aborted second batch isn't.
    // This documents current behavior: flushed rows are durable,
    // in-flight batches are lost on abort.
    const result = (await dl.query<{ n: number }>(
      'SELECT COUNT(*) AS n FROM "s"."items"',
    )) as { n: number | bigint }[];
    const n = Number(result[0].n);
    assert.ok(
      n >= 10_000 && n < 25_000,
      `expected 10_000 <= n < 25_000 (flushed but not all), got ${n}`,
    );
  });

  it("ctx.signal is populated for plugin HTTP cancellation", async () => {
    const { plugin, capture } = makePlugin({
      rows: [{ id: 1, updated_at: "2024-01-01" }],
    });
    await setup(plugin);

    const controller = new AbortController();
    await dl.sync({ items: {} }, { signal: controller.signal });

    const ctx = capture()[0];
    assert.ok(ctx.signal, "ctx.signal should be defined");
    assert.equal(ctx.signal!.aborted, false);

    // Sanity check: aborting the controller flips ctx.signal.aborted,
    // because ctx.signal is a live reference to the same AbortSignal.
    controller.abort();
    assert.equal(ctx.signal!.aborted, true);
  });

  it("ctx.signal is undefined when no signal is passed", async () => {
    const { plugin, capture } = makePlugin({
      rows: [{ id: 1, updated_at: "2024-01-01" }],
    });
    await setup(plugin);

    await dl.sync({ items: {} });
    assert.equal(capture()[0].signal, undefined);
  });

  it("aborts between tables without touching subsequent ones", async () => {
    // Two-table plugin. Abort fires after the first table finishes.
    // The second table's list() must NOT be called.
    let firstCalls = 0;
    let secondCalls = 0;
    const controller = new AbortController();

    const plugin: PluginDef = {
      name: "two_tables",
      version: "1.0.0",
      tables: [
        {
          name: "t1",
          columns: [{ name: "id", type: "number" }],
          async *list() {
            firstCalls++;
            yield { id: 1 };
            // Trigger the abort AFTER this table finishes. The engine's
            // between-tables checkpoint will catch it before t2.
            controller.abort();
          },
        },
        {
          name: "t2",
          columns: [{ name: "id", type: "number" }],
          async *list() {
            secondCalls++;
            yield { id: 2 };
          },
        },
      ],
    };

    await setup(plugin);

    await assert.rejects(
      () => dl.sync(undefined, { signal: controller.signal }),
      (err: Error) => err.name === "AbortError",
    );

    assert.equal(firstCalls, 1, "t1 should have run");
    assert.equal(secondCalls, 0, "t2 should NOT have run");
  });

  it("preserves prior cursor across an abort (no full re-backfill)", async () => {
    // Arrange: do a successful sync first so _dripline_sync has a
    // cursor row for "items". Then run a sync that aborts mid-stream
    // and verify the cursor row is unchanged.
    const cursorRows = [
      { id: 1, updated_at: "2024-01-01T00:00:00Z" },
      { id: 2, updated_at: "2024-02-01T00:00:00Z" },
      { id: 3, updated_at: "2024-03-01T00:00:00Z" },
    ];
    const { plugin } = makePlugin({ rows: cursorRows, cursor: "updated_at" });
    await setup(plugin);

    const r1 = await dl.sync({ items: {} });
    assert.equal(r1.tables[0].rowsInserted, 3);

    // Read the cursor the successful sync wrote.
    const metaBefore = await dl.query<{ last_cursor: string; status: string }>(
      'SELECT last_cursor, status FROM "s"."_dripline_sync" WHERE table_name = \'items\'',
    );
    assert.equal(metaBefore.length, 1);
    assert.equal(metaBefore[0].status, "ok");
    const cursorBefore = metaBefore[0].last_cursor;
    assert.ok(cursorBefore, "first sync should have recorded a cursor");

    // Now run a sync with a pre-aborted signal. Must NOT overwrite the
    // cursor row — that would force a full re-backfill next time.
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(
      () => dl.sync({ items: {} }, { signal: controller.signal }),
      (err: Error) => err.name === "AbortError",
    );

    const metaAfter = await dl.query<{ last_cursor: string; status: string }>(
      'SELECT last_cursor, status FROM "s"."_dripline_sync" WHERE table_name = \'items\'',
    );
    assert.equal(metaAfter.length, 1);
    assert.equal(
      metaAfter[0].status,
      "ok",
      "aborted sync should not downgrade status to error",
    );
    assert.equal(
      metaAfter[0].last_cursor,
      cursorBefore,
      "aborted sync must not clobber the prior cursor",
    );
  });

  it("back-compat: bare SyncProgressCallback as second arg still works", async () => {
    const rows = Array.from({ length: 12_000 }, (_, i) => ({
      id: i + 1,
      updated_at: "2024-01-01",
    }));
    const { plugin } = makePlugin({ rows });
    await setup(plugin);

    const events: SyncProgressEvent[] = [];
    // Legacy form: pass a function, not { onProgress }.
    await dl.sync({ items: {} }, (ev) => events.push(ev));

    assert.ok(
      events.length > 0,
      "progress callback should still fire when passed as a bare function",
    );
    assert.equal(events[0].table, "items");
    assert.ok(events[0].rowsInserted > 0);
  });

  it("releases rate-limit permit after abort so next sync isn't starved", async () => {
    // Arrange a rate limit of 1 concurrent. If abort leaks the permit,
    // the second sync will hang. We use a short timer to fail fast.
    const rows = Array.from({ length: 15_000 }, (_, i) => ({
      id: i + 1,
      updated_at: "2024-01-01",
    }));
    const controller = new AbortController();
    const { plugin } = makePlugin({
      rows,
      afterYield: (i) => {
        if (i === 11_000) controller.abort();
      },
    });

    db = await Database.create(":memory:");
    dl = await Dripline.create({
      plugins: [plugin],
      database: db,
      schema: "s",
      rateLimits: { abort_test: { maxConcurrent: 1 } },
    });

    await assert.rejects(
      () => dl.sync({ items: {} }, { signal: controller.signal }),
      (err: Error) => err.name === "AbortError",
    );

    // If the permit leaked, this would hang forever. Use a 3s timeout
    // as a safety net — a healthy release is sub-second.
    const second = dl.sync({ items: {} });
    const timeout = new Promise<"timeout">((resolve) =>
      setTimeout(() => resolve("timeout"), 3000),
    );
    const outcome = await Promise.race([second, timeout]);
    assert.notEqual(
      outcome,
      "timeout",
      "second sync hung — rate-limit permit was not released on abort",
    );
  });
});
