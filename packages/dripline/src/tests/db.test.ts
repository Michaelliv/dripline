import { strict as assert } from "node:assert";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import { DuckDBInstance } from "@duckdb/node-api";
import { Database } from "../core/db.js";

describe("Database.create options", () => {
  let dir: string;
  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "dripline-db-"));
  });

  it("opens in :memory: with no options (back-compat)", async () => {
    const db = await Database.create();
    try {
      const rows = await db.all("SELECT 1 AS x");
      assert.equal(rows[0].x, 1);
    } finally {
      await db.close();
    }
  });

  it("honors readOnly on a pre-seeded file", async () => {
    const path = join(dir, "ro.duckdb");
    // Seed a table in read/write mode, then reopen read-only.
    const seed = await Database.create(path);
    await seed.run(`CREATE TABLE t (id INT)`);
    await seed.run(`INSERT INTO t VALUES (?)`, 1);
    await seed.close();

    const ro = await Database.create(path, { readOnly: true });
    try {
      const rows = await ro.all("SELECT id FROM t");
      assert.equal(rows[0].id, 1);
      await assert.rejects(() => ro.run(`INSERT INTO t VALUES (?)`, 2), /.+/);
    } finally {
      await ro.close();
    }
  });

  it("accessMode takes precedence over readOnly", async () => {
    const path = join(dir, "explicit.duckdb");
    const seed = await Database.create(path);
    await seed.run(`CREATE TABLE t (id INT)`);
    await seed.close();

    // readOnly: true + accessMode: "read_write" → read_write wins
    const rw = await Database.create(path, {
      readOnly: true,
      accessMode: "read_write",
    });
    try {
      await rw.run(`INSERT INTO t VALUES (?)`, 42);
      const rows = await rw.all("SELECT id FROM t");
      assert.equal(rows[0].id, 42);
    } finally {
      await rw.close();
    }
  });

  it("duckdbOptions escape hatch wins over friendly keys", async () => {
    // threads: 1 loses to the raw override threads: "2".
    const db = await Database.create(":memory:", {
      threads: 1,
      duckdbOptions: { threads: "2" },
    });
    try {
      const rows = await db.all("SELECT current_setting('threads') AS t");
      assert.equal(rows[0].t, 2n);
    } finally {
      await db.close();
    }
  });

  it("honors tempDirectory / preserveInsertionOrder / objectCache", async () => {
    const tmp = mkdtempSync(join(tmpdir(), "dripline-db-spill-"));
    const db = await Database.create(":memory:", {
      tempDirectory: tmp,
      preserveInsertionOrder: false,
      objectCache: true,
    });
    try {
      const rows = await db.all(`
        SELECT
          current_setting('temp_directory') AS temp_dir,
          current_setting('preserve_insertion_order') AS pio,
          current_setting('enable_object_cache') AS obj_cache
      `);
      assert.equal(rows[0].temp_dir, tmp);
      assert.equal(rows[0].pio, false);
      assert.equal(rows[0].obj_cache, true);
    } finally {
      await db.close();
    }
  });
});

describe("Database.createForContainer", () => {
  let savedEnv: Record<string, string | undefined>;

  beforeEach(() => {
    // Snapshot any env vars this path inspects so each test starts
    // from a known state, regardless of the shell we're run from.
    savedEnv = {
      mem: process.env.DRIPLINE_DUCKDB_MEMORY_LIMIT,
      threads: process.env.DRIPLINE_DUCKDB_THREADS,
      tmp: process.env.DRIPLINE_DUCKDB_TEMP_DIR,
    };
    delete process.env.DRIPLINE_DUCKDB_MEMORY_LIMIT;
    delete process.env.DRIPLINE_DUCKDB_THREADS;
    delete process.env.DRIPLINE_DUCKDB_TEMP_DIR;
  });

  afterEach(() => {
    for (const [key, val] of Object.entries({
      DRIPLINE_DUCKDB_MEMORY_LIMIT: savedEnv.mem,
      DRIPLINE_DUCKDB_THREADS: savedEnv.threads,
      DRIPLINE_DUCKDB_TEMP_DIR: savedEnv.tmp,
    })) {
      if (val === undefined) delete process.env[key];
      else process.env[key] = val;
    }
  });

  it("applies container defaults when no options / env set", async () => {
    const db = await Database.createForContainer();
    try {
      const rows = await db.all(`
        SELECT
          current_setting('memory_limit') AS mem,
          current_setting('threads') AS t,
          current_setting('temp_directory') AS tmp,
          current_setting('preserve_insertion_order') AS pio,
          current_setting('enable_object_cache') AS obj
      `);
      // DuckDB normalizes "300MB" to its canonical form. We just
      // assert the settings are non-empty / match the shape we set.
      assert.match(String(rows[0].mem), /MiB|MB|300/);
      assert.equal(rows[0].t, 1n);
      assert.equal(rows[0].tmp, "/tmp/duckdb-spill");
      assert.equal(rows[0].pio, false);
      assert.equal(rows[0].obj, true);
    } finally {
      await db.close();
    }
  });

  it("env vars override defaults", async () => {
    const spill = mkdtempSync(join(tmpdir(), "dripline-db-env-"));
    process.env.DRIPLINE_DUCKDB_MEMORY_LIMIT = "256MB";
    process.env.DRIPLINE_DUCKDB_THREADS = "2";
    process.env.DRIPLINE_DUCKDB_TEMP_DIR = spill;

    const db = await Database.createForContainer();
    try {
      const rows = await db.all(`
        SELECT
          current_setting('threads') AS t,
          current_setting('temp_directory') AS tmp
      `);
      assert.equal(rows[0].t, 2n);
      assert.equal(rows[0].tmp, spill);
    } finally {
      await db.close();
    }
  });

  it("explicit options win over env vars", async () => {
    process.env.DRIPLINE_DUCKDB_THREADS = "4";
    const db = await Database.createForContainer(":memory:", { threads: 1 });
    try {
      const rows = await db.all(
        "SELECT current_setting('threads') AS t",
      );
      assert.equal(rows[0].t, 1n);
    } finally {
      await db.close();
    }
  });
});

describe("Database.fromConnection", () => {
  it("borrows a connection without owning it", async () => {
    const inst = await DuckDBInstance.create(":memory:");
    const conn = await inst.connect();
    try {
      const db = Database.fromConnection(conn);
      await db.run(`CREATE TABLE t (id INT)`);
      await db.run(`INSERT INTO t VALUES (?)`, 7);
      const rows = await db.all("SELECT id FROM t");
      assert.equal(rows[0].id, 7);

      // close() on a borrowed handle is a no-op — the underlying
      // connection must still work afterwards.
      await db.close();
      const again = Database.fromConnection(conn);
      const rows2 = await again.all("SELECT id FROM t");
      assert.equal(rows2[0].id, 7);
    } finally {
      conn.closeSync();
      inst.closeSync();
    }
  });

  it("shares state with the original connection", async () => {
    const inst = await DuckDBInstance.create(":memory:");
    const conn = await inst.connect();
    try {
      const db = Database.fromConnection(conn);
      await db.run(`CREATE TABLE shared (n INT)`);
      await db.run(`INSERT INTO shared VALUES (?)`, 99);
      // Read through the raw connection, confirm the write landed.
      const reader = await conn.runAndReadAll("SELECT n FROM shared");
      const rows = reader.getRowObjectsJS();
      assert.equal(rows[0].n, 99);
    } finally {
      conn.closeSync();
      inst.closeSync();
    }
  });

  it("getConnection returns the underlying handle", async () => {
    const inst = await DuckDBInstance.create(":memory:");
    const conn = await inst.connect();
    try {
      const db = Database.fromConnection(conn);
      assert.equal(db.getConnection(), conn);
    } finally {
      conn.closeSync();
      inst.closeSync();
    }
  });
});
