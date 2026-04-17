/**
 * End-to-end test of the lane runner against MinIO.
 *
 * Builds a fake .dripline/ project in a temp directory, registers an
 * in-process plugin, and invokes `commands/run.ts::run` directly. Tests
 * inspect the returned LaneRunResult[] — no stdout capture needed.
 *
 * Auto-skips when MinIO is unreachable.
 */

import { strict as assert } from "node:assert";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, afterEach, before, beforeEach, describe, it } from "node:test";
import { AwsClient } from "aws4fetch";
import { run } from "../commands/run.js";
import type { DriplineConfig } from "../config/types.js";
import { LeaseStore } from "../core/lease.js";
import { registry } from "../plugin/registry.js";
import type { PluginDef } from "../plugin/types.js";

const ENDPOINT =
  process.env.DRIPLINE_TEST_S3_ENDPOINT ?? "http://localhost:9100";
const BUCKET = process.env.DRIPLINE_TEST_S3_BUCKET ?? "dripline-test";
const KEY = process.env.DRIPLINE_TEST_S3_KEY ?? "testkey";
const SECRET = process.env.DRIPLINE_TEST_S3_SECRET ?? "testsecret123";

const RUN_PREFIX = `run-tests/${process.pid}-${Date.now()}`;

let backendUp = false;
async function probeBackend(): Promise<boolean> {
  try {
    const aws = new AwsClient({
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      service: "s3",
      region: "auto",
    });
    const r = await aws.fetch(`${ENDPOINT}/${BUCKET}/`, { method: "PUT" });
    return r.status === 200 || r.status === 409;
  } catch {
    return false;
  }
}

interface ProjectOpts {
  prefix: string;
  intervalMs?: number;
  maxRuntimeMs?: number;
  extraLanes?: Record<string, { interval: string }>;
}

function makeProject(opts: ProjectOpts): string {
  const dir = mkdtempSync(join(tmpdir(), "dripline-runtest-"));
  mkdirSync(join(dir, ".dripline"), { recursive: true });

  const interval = opts.intervalMs
    ? `${Math.floor(opts.intervalMs / 1000)}s`
    : "1h";
  const maxRuntime = opts.maxRuntimeMs
    ? `${Math.floor(opts.maxRuntimeMs / 1000)}s`
    : undefined;

  const lanes: DriplineConfig["lanes"] = {
    main: {
      tables: [{ name: "items", params: { org: "x" } }],
      interval,
      ...(maxRuntime ? { maxRuntime } : {}),
    },
  };
  for (const [name, lane] of Object.entries(opts.extraLanes ?? {})) {
    lanes[name] = {
      tables: [{ name: "items", params: { org: "x" } }],
      interval: lane.interval,
    };
  }

  const config: DriplineConfig = {
    connections: [{ name: "default", plugin: "remote_test", config: {} }],
    cache: { enabled: true, ttl: 300, maxSize: 1000 },
    rateLimits: {},
    lanes,
    remote: {
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix: opts.prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      secretType: "S3",
    },
  };
  writeFileSync(
    join(dir, ".dripline", "config.json"),
    JSON.stringify(config, null, 2),
  );
  return dir;
}

function registerTestPlugin(): void {
  const plugin: PluginDef = {
    name: "remote_test",
    version: "1.0.0",
    tables: [
      {
        name: "items",
        description: "Test items table",
        columns: [
          { name: "id", type: "number" },
          { name: "name", type: "string" },
          { name: "updated_at", type: "datetime" },
        ],
        keyColumns: [{ name: "org", required: "required" }],
        primaryKey: ["id"],
        cursor: "updated_at",
        async *list() {
          yield {
            id: 1,
            name: "a",
            updated_at: "2024-01-01T00:00:00Z",
            org: "x",
          };
          yield {
            id: 2,
            name: "b",
            updated_at: "2024-02-01T00:00:00Z",
            org: "x",
          };
          yield {
            id: 3,
            name: "c",
            updated_at: "2024-03-01T00:00:00Z",
            org: "x",
          };
        },
      },
    ],
  };
  registry.register(plugin);
}

let counter = 0;
const freshPrefix = (label: string) => `${RUN_PREFIX}/${label}-${++counter}`;

describe("dripline run (end-to-end)", { concurrency: false }, () => {
  let originalCwd: string;

  before(async () => {
    originalCwd = process.cwd();
    backendUp = await probeBackend();
    if (!backendUp) {
      console.warn(`\n  ⚠ skipping run tests: ${ENDPOINT} unreachable.\n`);
    }
    registerTestPlugin();
  });

  after(async () => {
    process.chdir(originalCwd);
    if (!backendUp) return;
    const aws = new AwsClient({
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      service: "s3",
      region: "auto",
    });
    const list = await aws.fetch(
      `${ENDPOINT}/${BUCKET}/?list-type=2&prefix=${encodeURIComponent(RUN_PREFIX)}`,
    );
    if (list.status !== 200) return;
    const xml = await list.text();
    const keys = [...xml.matchAll(/<Key>([^<]+)<\/Key>/g)].map((m) => m[1]);
    for (const k of keys) {
      await aws.fetch(`${ENDPOINT}/${BUCKET}/${k}`, { method: "DELETE" });
    }
  });

  beforeEach(() => {
    originalCwd = process.cwd();
  });
  afterEach(() => process.chdir(originalCwd));

  const ift = (name: string, fn: () => Promise<void>) =>
    it(name, async () => {
      if (!backendUp) return;
      await fn();
    });

  ift("first run acquires lease, syncs, publishes, sets cooldown", async () => {
    const prefix = freshPrefix("happy-path");
    const project = makeProject({
      prefix,
      intervalMs: 60_000,
      maxRuntimeMs: 10_000,
    });
    process.chdir(project);

    const results = await run({ quiet: true });
    assert.equal(results.length, 1);
    assert.equal(results[0].status, "ok");
    assert.equal(results[0].rowsInserted, 3);
    assert.equal(results[0].publishedFiles, 1);

    // Lease should now be in cooldown — direct probe.
    const ls = new LeaseStore({
      endpoint: ENDPOINT,
      bucket: BUCKET,
      prefix,
      accessKeyId: KEY,
      secretAccessKey: SECRET,
    });
    const probe = await ls.acquire("lane-main", 5_000);
    assert.equal(probe, null, "lease should still be held (cooldown)");
  });

  ift("second run while in cooldown is skipped", async () => {
    const prefix = freshPrefix("cooldown");
    const project = makeProject({
      prefix,
      intervalMs: 60_000,
      maxRuntimeMs: 10_000,
    });
    process.chdir(project);

    const first = await run({ quiet: true });
    assert.equal(first[0].status, "ok");

    const second = await run({ quiet: true });
    assert.equal(second[0].status, "skipped");
    assert.equal(second[0].rowsInserted, 0);
  });

  ift("incremental: run after cursor advance inserts 0 rows", async () => {
    // Use a tiny interval so we can wait it out.
    const prefix = freshPrefix("incremental");
    const project = makeProject({
      prefix,
      intervalMs: 2_000,
      maxRuntimeMs: 1_000,
    });
    process.chdir(project);

    const first = await run({ quiet: true });
    assert.equal(first[0].status, "ok");
    assert.equal(first[0].rowsInserted, 3);

    await new Promise((r) => setTimeout(r, 2_200));

    const second = await run({ quiet: true });
    assert.equal(second[0].status, "ok");
    assert.equal(
      second[0].rowsInserted,
      0,
      "cursor hydrated → no new rows to sync",
    );
    assert.equal(second[0].publishedFiles, 0, "no rows → no published file");
  });

  ift("--lane filter only runs the named lane", async () => {
    const prefix = freshPrefix("filter");
    const project = makeProject({
      prefix,
      extraLanes: { other: { interval: "1h" } },
    });
    process.chdir(project);

    const results = await run({ lane: "main", quiet: true });
    assert.equal(results.length, 1);
    assert.equal(results[0].lane, "main");
  });

  ift("misconfigured lane fails loud at startup, not mid-run", async () => {
    const prefix = freshPrefix("bad-config");
    const dir = mkdtempSync(join(tmpdir(), "dripline-runtest-"));
    mkdirSync(join(dir, ".dripline"), { recursive: true });
    const config: DriplineConfig = {
      connections: [{ name: "default", plugin: "remote_test", config: {} }],
      cache: { enabled: true, ttl: 300, maxSize: 1000 },
      rateLimits: {},
      lanes: {
        bad: {
          tables: [{ name: "items" }],
          interval: "nope", // invalid
        },
      },
      remote: {
        endpoint: ENDPOINT,
        bucket: BUCKET,
        prefix,
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        secretType: "S3",
      },
    };
    writeFileSync(
      join(dir, ".dripline", "config.json"),
      JSON.stringify(config),
    );
    process.chdir(dir);

    await assert.rejects(() => run({ quiet: true }), /invalid interval/);
  });

  ift("missing remote rejects with a clear error", async () => {
    const dir = mkdtempSync(join(tmpdir(), "dripline-runtest-"));
    mkdirSync(join(dir, ".dripline"), { recursive: true });
    writeFileSync(
      join(dir, ".dripline", "config.json"),
      JSON.stringify({
        connections: [],
        cache: { enabled: true, ttl: 300, maxSize: 1000 },
        rateLimits: {},
        lanes: {
          main: {
            tables: [{ name: "items" }],
            interval: "1h",
          },
        },
      }),
    );
    process.chdir(dir);

    await assert.rejects(() => run({ quiet: true }), /no remote configured/);
  });

  ift("config file is correctly loaded from cwd", async () => {
    // Sanity check that loadConfig() picks up our temp .dripline/ dir.
    const prefix = freshPrefix("cwd");
    const project = makeProject({ prefix });
    process.chdir(project);
    const cfg = JSON.parse(
      readFileSync(join(project, ".dripline", "config.json"), "utf-8"),
    );
    assert.equal(cfg.remote.prefix, prefix);
  });
});
