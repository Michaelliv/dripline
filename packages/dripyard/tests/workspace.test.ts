/**
 * Tests for the workspace adoption path.
 *
 *   - loadWorkspace reads `.dripline/config.json`, registers plugins
 *     via dripline's shared loader, returns a normalized bundle.
 *   - hydrateLanes upserts by name so _id is stable across restarts,
 *     and doesn't do an O(N²) scan.
 *   - workspace.info / plugins / catalog / connections return the
 *     expected shapes.
 *   - workspace.runSql validates input and respects row/timeout caps.
 *   - Secret masking covers the canonical keys and leaves look-alikes
 *     (key_schedule, authorized_users) untouched.
 *
 * The tests use a throwaway fixture dir with a fake plugin so we don't
 * need npm-published plugins on disk.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import {
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { registry } from "dripline";
import { Vex, sqliteAdapter } from "vex-core";
import {
  getActiveWorkspace,
  hydrateLanes,
  loadWorkspace,
  setActiveWorkspace,
} from "../src/core/workspace.js";
import { lanesPlugin } from "../src/plugins/lanes.js";
import { runsPlugin } from "../src/plugins/runs.js";
import { workersPlugin } from "../src/plugins/workers.js";
import { workspacePlugin } from "../src/plugins/workspace.js";

let workspaceDir: string;
let vex: Vex;

/**
 * Build a minimal dripline workspace on disk. One fake plugin with
 * two tables (one has a PK + cursor, one doesn't), two connections,
 * two lanes, a remote config. Enough to exercise every query.
 */
function createFixture(): string {
  const dir = mkdtempSync(join(tmpdir(), "dripyard-ws-"));
  const configDir = join(dir, ".dripline");
  mkdirSync(configDir, { recursive: true });

  // Fake plugin file — dripline's loader dynamic-imports this.
  const pluginPath = join(dir, "plugin.ts");
  writeFileSync(
    pluginPath,
    `export default {
      name: "fake",
      version: "1.0.0",
      tables: [
        {
          name: "fake_items",
          description: "Items with cursor + PK",
          columns: [
            { name: "id", type: "number" },
            { name: "updated_at", type: "datetime" },
          ],
          primaryKey: ["id"],
          cursor: "updated_at",
          keyColumns: [{ name: "org", required: "required" }],
          async *list() { yield { id: 1, updated_at: "2024-01-01" }; },
        },
        {
          name: "fake_snapshots",
          description: "Full-replace snapshots",
          columns: [{ name: "name", type: "string" }],
          async *list() { yield { name: "hello" }; },
        },
      ],
    };`,
  );

  writeFileSync(
    join(configDir, "plugins.json"),
    JSON.stringify({ plugins: [{ path: pluginPath, name: "fake" }] }),
  );

  writeFileSync(
    join(configDir, "config.json"),
    JSON.stringify({
      connections: [
        { name: "main", plugin: "fake", config: { token: "secret-abc" } },
        { name: "secondary", plugin: "fake", config: { token: "xyz" } },
      ],
      cache: { enabled: true, ttl: 300, maxSize: 1000 },
      rateLimits: { fake: { maxPerSecond: 1 } },
      lanes: {
        items: {
          tables: [{ name: "fake_items", params: { org: "a" } }],
          interval: "15m",
          maxRuntime: "5m",
        },
        snaps: {
          tables: [{ name: "fake_snapshots" }],
          interval: "1h",
        },
      },
      remote: {
        endpoint: "https://example-r2.cloudflarestorage.com",
        bucket: "test-bucket",
        prefix: "test-prefix",
        accessKeyId: "ak",
        secretAccessKey: "sk",
        secretType: "S3",
      },
    }),
  );

  return dir;
}

beforeEach(async () => {
  // Reset dripline's module-level registry so tests don't leak plugins
  // across fixtures. The easiest way is to call register() with a
  // clearing plugin, but there's no clear() — work around by resetting
  // the private plugins map via the public register API. Since each
  // fixture registers its own "fake" plugin, repeat registration is
  // an overwrite and harmless; what we actually need is to not see
  // foreign tables leaking from a prior test.
  for (const p of registry.listPlugins()) {
    // @ts-expect-error — PluginRegistry.plugins is private; clearing
    // it is the cleanest path for test isolation.
    registry.plugins.delete(p.name);
  }

  workspaceDir = createFixture();
  vex = await Vex.create({
    plugins: [lanesPlugin, runsPlugin, workersPlugin, workspacePlugin],
    transactional: sqliteAdapter(":memory:"),
    analytical: sqliteAdapter(":memory:"),
  });
});

afterEach(async () => {
  setActiveWorkspace(null);
  await vex.close();
  try {
    rmSync(workspaceDir, { recursive: true, force: true });
  } catch {}
});

// ── loadWorkspace ────────────────────────────────────────────────────

describe("loadWorkspace", () => {
  test("reads config.json and registers the plugin", async () => {
    const ws = await loadWorkspace(workspaceDir);

    expect(ws.path).toBe(workspaceDir);
    expect(ws.config.lanes?.items?.interval).toBe("15m");
    expect(ws.plugins).toHaveLength(1);
    expect(ws.plugins[0].name).toBe("fake");
    expect(ws.plugins[0].tables).toHaveLength(2);

    // Connection-by-plugin map uses the first connection for a plugin.
    expect(ws.connectionsByPlugin.get("fake")?.name).toBe("main");

    expect(ws.remote?.bucket).toBe("test-bucket");
  });

  test("throws loudly when the directory isn't a dripline workspace", async () => {
    const empty = mkdtempSync(join(tmpdir(), "dripyard-ws-empty-"));
    try {
      await expect(loadWorkspace(empty)).rejects.toThrow(
        /Not a dripline workspace/,
      );
    } finally {
      rmSync(empty, { recursive: true, force: true });
    }
  });

  test("activeWorkspace starts null and toggles via setActiveWorkspace", async () => {
    expect(getActiveWorkspace()).toBeNull();
    const ws = await loadWorkspace(workspaceDir);
    setActiveWorkspace(ws);
    expect(getActiveWorkspace()?.path).toBe(workspaceDir);
    setActiveWorkspace(null);
    expect(getActiveWorkspace()).toBeNull();
  });
});

// ── hydrateLanes ─────────────────────────────────────────────────────

describe("hydrateLanes", () => {
  test("creates lanes from config, upserting by name", async () => {
    const ws = await loadWorkspace(workspaceDir);
    await hydrateLanes(vex, ws);

    const lanes = (await vex.query("lanes.list")) as Array<{
      name: string;
      sourcePlugin: string;
      schedule: string;
      maxRuntime: string | null;
    }>;
    expect(lanes).toHaveLength(2);

    const items = lanes.find((l) => l.name === "items")!;
    expect(items.sourcePlugin).toBe("fake");
    expect(items.schedule).toBe("15m");
    expect(items.maxRuntime).toBe("5m");

    const snaps = lanes.find((l) => l.name === "snaps")!;
    expect(snaps.maxRuntime).toBeNull();
  });

  test("_id is stable across hydrations so runs keep their laneId", async () => {
    const ws = await loadWorkspace(workspaceDir);
    await hydrateLanes(vex, ws);

    const before = (await vex.query("lanes.list")) as Array<{
      _id: string;
      name: string;
    }>;
    const itemsIdBefore = before.find((l) => l.name === "items")!._id;

    // Re-hydrate with the same config — must be idempotent.
    await hydrateLanes(vex, ws);

    const after = (await vex.query("lanes.list")) as Array<{
      _id: string;
      name: string;
    }>;
    expect(after).toHaveLength(2);
    const itemsIdAfter = after.find((l) => l.name === "items")!._id;
    expect(itemsIdAfter).toBe(itemsIdBefore);
  });

  test("new lanes added to config appear on next hydration", async () => {
    const ws = await loadWorkspace(workspaceDir);
    await hydrateLanes(vex, ws);
    expect((await vex.query("lanes.list")).length).toBe(2);

    // Simulate a config edit by mutating the already-loaded workspace.
    ws.config.lanes!.third = {
      tables: [{ name: "fake_items" }],
      interval: "30m",
    };
    await hydrateLanes(vex, ws);

    const lanes = (await vex.query("lanes.list")) as Array<{ name: string }>;
    expect(lanes).toHaveLength(3);
    expect(lanes.map((l) => l.name).sort()).toEqual(
      ["items", "snaps", "third"].sort(),
    );
  });
});

// ── workspace.* queries ──────────────────────────────────────────────

describe("workspace queries", () => {
  test("info returns workspace summary when set, null otherwise", async () => {
    expect(await vex.query("workspace.info")).toBeNull();

    const ws = await loadWorkspace(workspaceDir);
    setActiveWorkspace(ws);
    const info = (await vex.query("workspace.info")) as any;
    expect(info.path).toBe(workspaceDir);
    expect(info.pluginCount).toBe(1);
    expect(info.connectionCount).toBe(2);
    expect(info.laneCount).toBe(2);
    expect(info.remote?.bucket).toBe("test-bucket");
  });

  test("plugins returns tables + connection wiring", async () => {
    const ws = await loadWorkspace(workspaceDir);
    setActiveWorkspace(ws);

    const plugins = (await vex.query("workspace.plugins")) as any[];
    expect(plugins).toHaveLength(1);
    expect(plugins[0].name).toBe("fake");
    expect(plugins[0].tableCount).toBe(2);
    expect(plugins[0].connections).toEqual(["main", "secondary"]);

    const items = plugins[0].tables.find((t: any) => t.name === "fake_items");
    expect(items.hasCursor).toBe(true);
    expect(items.cursor).toBe("updated_at");
    expect(items.hasPrimaryKey).toBe(true);
    expect(items.primaryKey).toEqual(["id"]);
  });

  test("catalog flattens plugins.tables and annotates usedByLanes", async () => {
    const ws = await loadWorkspace(workspaceDir);
    setActiveWorkspace(ws);

    const catalog = (await vex.query("workspace.catalog")) as any[];
    expect(catalog).toHaveLength(2);
    const items = catalog.find((e) => e.table === "fake_items");
    expect(items.plugin).toBe("fake");
    expect(items.usedByLanes).toHaveLength(1);
    expect(items.usedByLanes[0]).toEqual({
      lane: "items",
      params: { org: "a" },
    });

    const snaps = catalog.find((e) => e.table === "fake_snapshots");
    expect(snaps.usedByLanes[0].lane).toBe("snaps");
  });

  test("connections mask secret-shaped fields and leave data fields alone", async () => {
    const ws = await loadWorkspace(workspaceDir);
    // Hand-craft a tricky connection config with both real secrets
    // and look-alikes to prove the regex got tightened correctly.
    ws.config.connections = [
      {
        name: "tricky",
        plugin: "fake",
        config: {
          token: "real-secret",
          api_token: "real-secret-2",
          accessToken: "real-secret-3",
          password: "real-secret-4",
          // Look-alikes that the old /key/ regex would have masked:
          key_schedule: "MON,TUE",
          authorized_users: "alice,bob",
          keyboard_layout: "qwerty",
          // Env-var bindings preserved verbatim:
          secret_key: "$MY_SECRET",
        },
      },
    ];
    setActiveWorkspace(ws);

    const connections = (await vex.query("workspace.connections")) as any[];
    const cfg = connections[0].config;

    // Masked:
    expect(cfg.token).toBe("***");
    expect(cfg.api_token).toBe("***");
    expect(cfg.accessToken).toBe("***");
    expect(cfg.password).toBe("***");
    // Preserved env-var binding:
    expect(cfg.secret_key).toBe("$MY_SECRET");
    // Untouched data fields:
    expect(cfg.key_schedule).toBe("MON,TUE");
    expect(cfg.authorized_users).toBe("alice,bob");
    expect(cfg.keyboard_layout).toBe("qwerty");
  });

  test("warehouse returns { remote:null, tables:[] } with no workspace", async () => {
    const w = (await vex.query("workspace.warehouse")) as any;
    expect(w).toEqual({ remote: null, tables: [] });
  });
});

// ── workspace.runSql input validation ────────────────────────────────

describe("workspace.runSql validation", () => {
  test("rejects empty SQL", async () => {
    const ws = await loadWorkspace(workspaceDir);
    setActiveWorkspace(ws);

    await expect(vex.mutate("workspace.runSql", { sql: "" })).rejects.toThrow(
      /SQL is empty/,
    );
    await expect(
      vex.mutate("workspace.runSql", { sql: "   \n  " }),
    ).rejects.toThrow(/SQL is empty/);
  });

  test("rejects when no remote is configured", async () => {
    const ws = await loadWorkspace(workspaceDir);
    ws.remote = null; // simulate workspace without a remote
    setActiveWorkspace(ws);

    await expect(
      vex.mutate("workspace.runSql", { sql: "SELECT 1" }),
    ).rejects.toThrow(/No remote configured/);
  });
});
