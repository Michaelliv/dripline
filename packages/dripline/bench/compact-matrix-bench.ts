#!/usr/bin/env bun
/**
 * Compaction memory + correctness matrix.
 *
 * Goal: find a strategy that
 *   1. produces identical output to the current production query on
 *      every dripline-supported scenario (correctness),
 *   2. fits in a ~150 MB DuckDB budget on a realistically-sized
 *      warehouse (memory),
 *   3. stays general — no shortcut that only works for one schema.
 *
 * Scenarios cover the dimensions the plugin contract allows:
 *   - single-column PK vs multi-column PK
 *   - cursor present vs cursor absent
 *   - no partition vs 1-col partition vs 2-col partition
 *   - empty curated (first compact) vs pre-existing baseline
 *   - raw overlap = all-new vs all-updates vs mixed
 *   - many distinct partition values (high cardinality)
 *   - narrow schema vs wide schema (50 cols, JSON blobs)
 *
 * For every scenario, every strategy's output is read back and
 * compared against the baseline strategy's output row-by-row. A
 * strategy that's cheaper on memory but subtly wrong is worse than
 * useless.
 *
 * Run:
 *   BENCH_MEMORY_LIMIT=150MB bun run bench/compact-matrix-bench.ts
 *
 * Requires MinIO at localhost:9100 (testkey/testsecret123).
 */

import { AwsClient } from "aws4fetch";
import { Database } from "../src/core/db.js";
import { Remote } from "../src/core/remote.js";

const ENDPOINT = "http://localhost:9100";
const BUCKET = "dripline-test";
const KEY = "testkey";
const SECRET = "testsecret123";

const MEMORY_LIMIT = process.env.BENCH_MEMORY_LIMIT ?? "200MB";
const TEMP_DIR = "/tmp/dripline-bench-spill";
const RUN_ID = `matrix/${process.pid}-${Date.now()}`;

const aws = new AwsClient({
  accessKeyId: KEY,
  secretAccessKey: SECRET,
  service: "s3",
  region: "auto",
});

function log(msg: string): void {
  const ts = new Date().toISOString().slice(11, 23);
  console.log(`[${ts}] ${msg}`);
}

async function ensureBucket(): Promise<void> {
  const r = await aws.fetch(`${ENDPOINT}/${BUCKET}/`, { method: "PUT" });
  if (r.status !== 200 && r.status !== 409) {
    throw new Error(`bucket PUT failed: ${r.status}`);
  }
}

async function cleanupPrefix(prefix: string): Promise<void> {
  const list = await aws.fetch(
    `${ENDPOINT}/${BUCKET}/?list-type=2&prefix=${encodeURIComponent(prefix)}`,
  );
  if (list.status !== 200) return;
  const xml = await list.text();
  const keys = [...xml.matchAll(/<Key>([^<]+)<\/Key>/g)].map((m) => m[1]);
  for (const k of keys) {
    await aws.fetch(`${ENDPOINT}/${BUCKET}/${k}`, { method: "DELETE" });
  }
}

// ─── Scenario schemas ───────────────────────────────────────────────

/**
 * Abstract description of what a scenario's data looks like. The
 * bench generates raw + curated parquet to match, then compares
 * strategies' compact outputs to baseline's.
 */
interface Scenario {
  name: string;
  description: string;
  /** Column names that together form the primary key. */
  primaryKey: string[];
  /** Column name that acts as the cursor. null = no cursor. */
  cursor: string | null;
  /** Columns to partition writes by. Empty array = single file. */
  partitionBy: string[];
  /** Build the DuckDB SELECT expression that produces seed rows. */
  buildSelect(opts: {
    rangeStart: number;
    rangeEnd: number;
    cursorValue: string | null;
    partitionValues: Record<string, string>;
  }): string;
  /** Columns the output parquet must have, in order. For equality checks. */
  allColumns: string[];
  /** How many curated rows to seed per partition combo. */
  curatedRowsPerPartition: number;
  /** How many raw rows to seed per partition combo. */
  rawRowsPerPartition: number;
  /** Partition combos to seed. */
  partitionCombos: Array<Record<string, string>>;
  /** Fraction of raw rows that are updates to existing curated PKs (0..1). */
  updateFraction: number;
  /** Whether to skip the curated baseline (simulates first compact). */
  skipCurated?: boolean;
}

/**
 * Wide-schema (50 cols, 3 JSON blobs) — the shape that blew the budget
 * in production. PK=id, cursor=updated_at, partition=org_id.
 */
function scenarioWide(
  overrides: Partial<Scenario> & { name: string } = { name: "wide" },
): Scenario {
  const NUM_COLS = 50;
  const JSON_IDXS = [5, 15, 30];
  const allColumns: string[] = [];
  for (let i = 0; i < NUM_COLS; i++) {
    if (i === 0) allColumns.push("id");
    else if (i === 1) allColumns.push("updated_at");
    else if (i === 2) allColumns.push("org_id");
    else if (JSON_IDXS.includes(i)) allColumns.push(`json_${i}`);
    else allColumns.push(`col_${i}`);
  }
  return {
    name: "wide",
    description: "50 cols, 3 JSON blobs, org_id partition (production shape)",
    primaryKey: ["id"],
    cursor: "updated_at",
    partitionBy: ["org_id"],
    allColumns,
    curatedRowsPerPartition: 40_000,
    rawRowsPerPartition: 4_000,
    partitionCombos: [
      { org_id: "org-a" },
      { org_id: "org-b" },
      { org_id: "org-c" },
    ],
    updateFraction: 0.5,
    buildSelect({ rangeStart, rangeEnd, cursorValue, partitionValues }) {
      const org = partitionValues.org_id;
      const cols: string[] = [];
      for (let i = 0; i < NUM_COLS; i++) {
        if (i === 0) cols.push(`(${rangeStart} + i)::BIGINT AS "id"`);
        else if (i === 1)
          cols.push(`'${cursorValue}' AS "updated_at"`);
        else if (i === 2) cols.push(`'${org}' AS "org_id"`);
        else if (JSON_IDXS.includes(i))
          cols.push(
            `'{"id":' || (${rangeStart} + i)::VARCHAR || ',"items":[{"sku":"sku-' || (${rangeStart} + i)::VARCHAR || '-0","qty":1,"price":12.5,"modifier":"mod-' || ((${rangeStart} + i) % 17)::VARCHAR || '"},{"sku":"sku-' || (${rangeStart} + i)::VARCHAR || '-1","qty":2,"price":25,"modifier":"mod-' || ((${rangeStart} + i) % 17)::VARCHAR || '"},{"sku":"sku-' || (${rangeStart} + i)::VARCHAR || '-2","qty":3,"price":37.5,"modifier":"mod-' || ((${rangeStart} + i) % 17)::VARCHAR || '"}],"notes":"row ' || (${rangeStart} + i)::VARCHAR || ' in ${org}"}' AS "json_${i}"`,
          );
        else
          cols.push(
            `'col${i}-' || (${rangeStart} + i)::VARCHAR || '-${org}' AS "col_${i}"`,
          );
      }
      return `SELECT ${cols.join(", ")} FROM range(${rangeEnd - rangeStart}) t(i)`;
    },
    ...overrides,
  };
}

/** Narrow schema — 10 cols, no JSON. Tests that strategies don't over-fit to wide. */
function scenarioNarrow(): Scenario {
  return {
    name: "narrow",
    description: "10 cols, no JSON, org_id partition",
    primaryKey: ["id"],
    cursor: "updated_at",
    partitionBy: ["org_id"],
    allColumns: [
      "id",
      "updated_at",
      "org_id",
      "col_3",
      "col_4",
      "col_5",
      "col_6",
      "col_7",
      "col_8",
      "col_9",
    ],
    curatedRowsPerPartition: 200_000,
    rawRowsPerPartition: 20_000,
    partitionCombos: [
      { org_id: "org-a" },
      { org_id: "org-b" },
      { org_id: "org-c" },
    ],
    updateFraction: 0.5,
    buildSelect({ rangeStart, rangeEnd, cursorValue, partitionValues }) {
      const org = partitionValues.org_id;
      const cols = [
        `(${rangeStart} + i)::BIGINT AS "id"`,
        `'${cursorValue}' AS "updated_at"`,
        `'${org}' AS "org_id"`,
        `'col3-' || (${rangeStart} + i)::VARCHAR AS "col_3"`,
        `'col4-' || (${rangeStart} + i)::VARCHAR AS "col_4"`,
        `'col5-' || (${rangeStart} + i)::VARCHAR AS "col_5"`,
        `'col6-' || (${rangeStart} + i)::VARCHAR AS "col_6"`,
        `'col7-' || (${rangeStart} + i)::VARCHAR AS "col_7"`,
        `'col8-' || (${rangeStart} + i)::VARCHAR AS "col_8"`,
        `'col9-' || (${rangeStart} + i)::VARCHAR AS "col_9"`,
      ];
      return `SELECT ${cols.join(", ")} FROM range(${rangeEnd - rangeStart}) t(i)`;
    },
  };
}

/** Wide + no cursor (falls back to PK DESC tiebreaker). */
function scenarioNoCursor(): Scenario {
  return { ...scenarioWide(), name: "no-cursor", cursor: null };
}

/** Wide + two-column PK (id, variant). */
function scenarioMultiPk(): Scenario {
  const base = scenarioWide();
  return {
    ...base,
    name: "multi-pk",
    primaryKey: ["id", "variant"],
    allColumns: ["id", "variant", ...base.allColumns.filter((c) => c !== "id")],
    buildSelect(opts) {
      // Cheat: extend the wide select with a "variant" column. We
      // inject "A" for even i, "B" for odd so every (id, variant) is
      // unique but both variants share an id.
      const base = scenarioWide().buildSelect(opts);
      return base.replace(
        /^SELECT /,
        `SELECT CASE WHEN (${opts.rangeStart} + i) % 2 = 0 THEN 'A' ELSE 'B' END AS "variant", `,
      );
    },
  };
}

/** Wide + no partition (single file output). */
function scenarioNoPartition(): Scenario {
  return {
    ...scenarioWide(),
    name: "no-part",
    partitionBy: [],
    partitionCombos: [{}],
    curatedRowsPerPartition: 150_000,
    rawRowsPerPartition: 15_000,
  };
}

/** Wide + many partitions (30 orgs) to stress the partitioned writer. */
function scenarioManyPartitions(): Scenario {
  const base = scenarioWide();
  const combos: Array<Record<string, string>> = [];
  for (let i = 0; i < 30; i++) combos.push({ org_id: `org-${i}` });
  return {
    ...base,
    name: "many-partitions",
    curatedRowsPerPartition: 10_000,
    rawRowsPerPartition: 1_000,
    partitionCombos: combos,
  };
}

/** Wide + first compact (no curated baseline). */
function scenarioFirstCompact(): Scenario {
  return { ...scenarioWide(), name: "first-compact", skipCurated: true };
}

/** All-new raw (no PK overlap with curated). */
function scenarioAllNew(): Scenario {
  return { ...scenarioWide(), name: "all-new", updateFraction: 0 };
}

/** All-updates raw (every raw PK exists in curated with older cursor). */
function scenarioAllUpdates(): Scenario {
  return { ...scenarioWide(), name: "all-updates", updateFraction: 1 };
}

const SCENARIOS: Scenario[] = [
  scenarioWide(),
  scenarioNarrow(),
  scenarioNoCursor(),
  scenarioMultiPk(),
  scenarioNoPartition(),
  scenarioManyPartitions(),
  scenarioFirstCompact(),
  scenarioAllNew(),
  scenarioAllUpdates(),
];

// ─── Seed ───────────────────────────────────────────────────────────

/**
 * Seed both curated (baseline) and raw (incremental) for a scenario.
 * The id ranges are offset per partition so primary keys stay unique
 * across partitions. Raw's update half shares id ranges with curated;
 * raw's new half extends beyond. Deterministic across runs.
 */
async function seedScenario(
  remote: Remote,
  scenario: Scenario,
  prefix: string,
): Promise<void> {
  const OFFSET_PER_PART = 10_000_000;

  // Curated baseline
  if (!scenario.skipCurated) {
    for (let p = 0; p < scenario.partitionCombos.length; p++) {
      const part = scenario.partitionCombos[p];
      const base = p * OFFSET_PER_PART;
      const partSeg =
        scenario.partitionBy.length > 0
          ? scenario.partitionBy
              .map((c) => `${c}=${part[c]}`)
              .join("/") + "/"
          : "_/";
      const url = `s3://${BUCKET}/${prefix}/curated/${scenario.name}/${partSeg}data_0.parquet`;
      const sql = scenario.buildSelect({
        rangeStart: base,
        rangeEnd: base + scenario.curatedRowsPerPartition,
        cursorValue: "2026-01-01T00:00:00Z",
        partitionValues: part,
      });
      const db = await Database.create(":memory:");
      try {
        await remote.attach(db);
        await db.exec(
          `COPY (${sql}) TO '${url}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);`,
        );
      } finally {
        await db.close();
      }
    }
  }

  // Raw incremental — half updates (reuse curated id range), half new.
  for (let p = 0; p < scenario.partitionCombos.length; p++) {
    const part = scenario.partitionCombos[p];
    const base = p * OFFSET_PER_PART;
    const updateCount = Math.floor(
      scenario.rawRowsPerPartition * scenario.updateFraction,
    );
    const newCount = scenario.rawRowsPerPartition - updateCount;
    const selects: string[] = [];
    if (updateCount > 0) {
      selects.push(
        scenario.buildSelect({
          rangeStart: base,
          rangeEnd: base + updateCount,
          cursorValue: "2026-04-19T00:00:00Z",
          partitionValues: part,
        }),
      );
    }
    if (newCount > 0) {
      selects.push(
        scenario.buildSelect({
          rangeStart: base + scenario.curatedRowsPerPartition,
          rangeEnd: base + scenario.curatedRowsPerPartition + newCount,
          cursorValue: "2026-04-19T00:00:00Z",
          partitionValues: part,
        }),
      );
    }
    if (selects.length === 0) continue;
    const combined = selects.join(" UNION ALL BY NAME ");
    // Encode the partition combo into the filename so reads can still
    // glob; keep raw "flat" under lane=bench since real dripline writes
    // per-run parquets there.
    const partSuffix = Object.values(part).join("_") || "all";
    const url = `s3://${BUCKET}/${prefix}/raw/${scenario.name}/lane=bench/run=2026-04-19_${partSuffix}.parquet`;
    const db = await Database.create(":memory:");
    try {
      await remote.attach(db);
      await db.exec(
        `COPY (${combined}) TO '${url}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);`,
      );
    } finally {
      await db.close();
    }
  }
}

// ─── Strategies ─────────────────────────────────────────────────────

interface Strategy {
  name: string;
  description: string;
  run(ctx: StrategyCtx): Promise<void>;
}

interface StrategyCtx {
  db: Database;
  scenario: Scenario;
  prefix: string;
  /** DuckDB read expression for raw. */
  rawRead: string;
  /** DuckDB read expression for curated (may be empty view when first-compact). */
  curatedRead: string | null;
  /** Output prefix in s3://... (no trailing slash). */
  outDir: string;
  /** Distinct partition values observed in raw. */
  partitionsInRaw: Array<Record<string, string>>;
}

/** Build the window ORDER BY clause based on whether a cursor exists. */
function orderByClause(scenario: Scenario): string {
  if (scenario.cursor) return `"${scenario.cursor}" DESC NULLS LAST`;
  // No cursor — use PK DESC as stable tiebreaker (matches production code).
  return scenario.primaryKey.map((p) => `"${p}" DESC`).join(", ");
}

function pkClause(scenario: Scenario): string {
  return scenario.primaryKey.map((p) => `"${p}"`).join(", ");
}

/**
 * Produce the COPY TO target clause for a given write destination and
 * partition set. Unpartitioned writes go to a single file (_/data_0.parquet).
 */
function writeTarget(
  outDir: string,
  partitionBy: string[],
  rowGroupSize: number,
): string {
  if (partitionBy.length === 0) {
    return `TO '${outDir}/_/data_0.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE ${rowGroupSize})`;
  }
  const parts = partitionBy.map((c) => `"${c}"`).join(", ");
  return `TO '${outDir}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE ${rowGroupSize}, PARTITION_BY (${parts}), OVERWRITE_OR_IGNORE)`;
}

/** One output file per explicit partition combo — bypasses PARTITION_BY writer. */
function writeTargetForPartition(
  outDir: string,
  partition: Record<string, string>,
  partitionBy: string[],
  rowGroupSize: number,
): string {
  const segs = partitionBy.map((c) => `${c}=${partition[c]}`);
  const path =
    segs.length > 0
      ? `${outDir}/${segs.join("/")}/data_0.parquet`
      : `${outDir}/_/data_0.parquet`;
  return `TO '${path}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE ${rowGroupSize})`;
}

const baselineStrategy: Strategy = {
  name: "baseline",
  description: "current prod: ROW_NUMBER OVER UNION ALL, PARTITION_BY writer",
  async run(ctx) {
    const pk = pkClause(ctx.scenario);
    const orderBy = orderByClause(ctx.scenario);
    const unionBody = ctx.curatedRead
      ? `SELECT * FROM ${ctx.rawRead}
         UNION ALL BY NAME
         SELECT * FROM ${ctx.curatedRead}`
      : `SELECT * FROM ${ctx.rawRead}`;
    const target = writeTarget(
      ctx.outDir,
      ctx.scenario.partitionBy,
      100_000,
    );
    await ctx.db.exec(`
      COPY (
        SELECT * EXCLUDE (_rn) FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY ${pk} ORDER BY ${orderBy}) AS _rn
          FROM (${unionBody})
        ) WHERE _rn = 1
      ) ${target};
    `);
  },
};

const antijoinPerPartition: Strategy = {
  name: "antijoin-per-part",
  description:
    "per-partition: materialize raw winners tiny, antijoin curated, UNION ALL",
  async run(ctx) {
    const pk = pkClause(ctx.scenario);
    const orderBy = orderByClause(ctx.scenario);
    // If no curated OR no partitions: one big pass without the loop.
    const partitions =
      ctx.scenario.partitionBy.length > 0 && ctx.curatedRead
        ? ctx.partitionsInRaw
        : [Object.create(null)];

    for (const part of partitions) {
      // Build partition filter SQL (e.g. org_id = 'org-a'). Empty when
      // scenario has no partition columns.
      const partFilter =
        ctx.scenario.partitionBy.length > 0 && Object.keys(part).length > 0
          ? ` WHERE ${ctx.scenario.partitionBy
              .map((c) => `"${c}" = '${String(part[c]).replace(/'/g, "''")}'`)
              .join(" AND ")}`
          : "";

      // Narrow cursor comparison for the antijoin. Without cursor we
      // just say "raw always wins" (matches baseline's PK DESC tiebreak
      // since raw's PK DESC ordering would rank higher than curated's
      // only when raw has that PK — which is exactly the case we want
      // to exclude from curated).
      const cursorAntijoinCond = ctx.scenario.cursor
        ? `AND c."${ctx.scenario.cursor}" <= w."${ctx.scenario.cursor}"`
        : ""; // no cursor → any raw row wins over curated for the same PK

      const pkJoin = ctx.scenario.primaryKey
        .map((p) => `c."${p}" = w."${p}"`)
        .join(" AND ");

      // Phase 1: dedup raw by PK (keep max cursor per PK). Small — one
      // sync's worth of rows. Lives in a temp table so the second
      // query can reuse it without re-reading S3.
      await ctx.db.exec(`DROP TABLE IF EXISTS _raw_winners;`);
      await ctx.db.exec(`
        CREATE TEMP TABLE _raw_winners AS
        SELECT * EXCLUDE (_rn) FROM (
          SELECT *, ROW_NUMBER() OVER (PARTITION BY ${pk} ORDER BY ${orderBy}) AS _rn
          FROM (SELECT * FROM ${ctx.rawRead}${partFilter})
        ) WHERE _rn = 1;
      `);

      // Phase 2: anti-join curated against raw winners. Curated rows
      // survive iff there's no raw winner with equal-or-newer cursor.
      const curatedSurvivors = ctx.curatedRead
        ? `
          SELECT c.* FROM (SELECT * FROM ${ctx.curatedRead}${partFilter}) c
          ANTI JOIN _raw_winners w
            ON (${pkJoin}) ${cursorAntijoinCond}
        `
        : ``;

      const unionAll = curatedSurvivors
        ? `
          SELECT * FROM _raw_winners
          UNION ALL BY NAME
          ${curatedSurvivors}
        `
        : `SELECT * FROM _raw_winners`;

      const target =
        ctx.scenario.partitionBy.length > 0 && Object.keys(part).length > 0
          ? writeTargetForPartition(
              ctx.outDir,
              part,
              ctx.scenario.partitionBy,
              10_000,
            )
          : writeTarget(ctx.outDir, ctx.scenario.partitionBy, 10_000);

      await ctx.db.exec(`COPY (${unionAll}) ${target};`);
    }
    await ctx.db.exec(`DROP TABLE IF EXISTS _raw_winners;`);
  },
};

/**
 * Variant: same antijoin approach but keep the PARTITION_BY writer to
 * see whether the writer or the anti-join is the memory bottleneck.
 */
const antijoinSinglePass: Strategy = {
  name: "antijoin-single",
  description:
    "antijoin without per-partition loop; PARTITION_BY writer handles split",
  async run(ctx) {
    const pk = pkClause(ctx.scenario);
    const orderBy = orderByClause(ctx.scenario);
    const pkJoin = ctx.scenario.primaryKey
      .map((p) => `c."${p}" = w."${p}"`)
      .join(" AND ");
    const cursorAntijoinCond = ctx.scenario.cursor
      ? `AND c."${ctx.scenario.cursor}" <= w."${ctx.scenario.cursor}"`
      : "";

    await ctx.db.exec(`DROP TABLE IF EXISTS _raw_winners;`);
    await ctx.db.exec(`
      CREATE TEMP TABLE _raw_winners AS
      SELECT * EXCLUDE (_rn) FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ${pk} ORDER BY ${orderBy}) AS _rn
        FROM ${ctx.rawRead}
      ) WHERE _rn = 1;
    `);

    const union = ctx.curatedRead
      ? `
        SELECT * FROM _raw_winners
        UNION ALL BY NAME
        SELECT c.* FROM ${ctx.curatedRead} c
        ANTI JOIN _raw_winners w ON (${pkJoin}) ${cursorAntijoinCond}
      `
      : `SELECT * FROM _raw_winners`;

    const target = writeTarget(ctx.outDir, ctx.scenario.partitionBy, 10_000);
    await ctx.db.exec(`COPY (${union}) ${target};`);
    await ctx.db.exec(`DROP TABLE IF EXISTS _raw_winners;`);
  },
};

/**
 * The core insight this strategy exploits: the anti-join key is
 * tiny (pk + cursor). Don't materialise the full wide row into a
 * temp table; express raw winners as an inline CTE that DuckDB can
 * stream. The payload columns only arrive right before the COPY.
 *
 * Per-partition loop kills the PARTITION_BY writer's per-partition
 * floor. For scenarios without curated (first compact) we skip the
 * antijoin entirely — raw wins by default.
 */
const cteAntijoinPerPartition: Strategy = {
  name: "cte-anti-per-part",
  description:
    "CTE raw-winners (no materialisation) + anti-join curated per partition",
  async run(ctx) {
    const pk = pkClause(ctx.scenario);
    const orderBy = orderByClause(ctx.scenario);
    const pkJoin = ctx.scenario.primaryKey
      .map((p) => `c."${p}" = w."${p}"`)
      .join(" AND ");
    const cursorAntijoin = ctx.scenario.cursor
      ? `AND c."${ctx.scenario.cursor}" <= w."${ctx.scenario.cursor}"`
      : "";

    const partitions =
      ctx.scenario.partitionBy.length > 0
        ? ctx.partitionsInRaw
        : [Object.create(null)];

    for (const part of partitions) {
      const partFilter =
        ctx.scenario.partitionBy.length > 0 && Object.keys(part).length > 0
          ? ` WHERE ${ctx.scenario.partitionBy
              .map(
                (c) =>
                  `"${c}" = '${String(part[c]).replace(/'/g, "''")}'`,
              )
              .join(" AND ")}`
          : "";

      // First-compact short-circuit: no curated, so raw-winners IS
      // the output. Don't build an antijoin against a nonexistent
      // table — DuckDB will still try to resolve the relation and
      // fail if we let it.
      const body = ctx.curatedRead
        ? `
          WITH raw_winners AS (
            SELECT * EXCLUDE (_rn) FROM (
              SELECT *, ROW_NUMBER() OVER (
                PARTITION BY ${pk} ORDER BY ${orderBy}
              ) AS _rn
              FROM (SELECT * FROM ${ctx.rawRead}${partFilter})
            ) WHERE _rn = 1
          ),
          curated_survivors AS (
            SELECT c.* FROM (SELECT * FROM ${ctx.curatedRead}${partFilter}) c
            ANTI JOIN raw_winners w
              ON (${pkJoin}) ${cursorAntijoin}
          )
          SELECT * FROM raw_winners
          UNION ALL BY NAME
          SELECT * FROM curated_survivors
        `
        : `
          SELECT * EXCLUDE (_rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
              PARTITION BY ${pk} ORDER BY ${orderBy}
            ) AS _rn
            FROM (SELECT * FROM ${ctx.rawRead}${partFilter})
          ) WHERE _rn = 1
        `;

      const target =
        ctx.scenario.partitionBy.length > 0 &&
        Object.keys(part).length > 0
          ? writeTargetForPartition(
              ctx.outDir,
              part,
              ctx.scenario.partitionBy,
              10_000,
            )
          : writeTarget(ctx.outDir, ctx.scenario.partitionBy, 10_000);

      await ctx.db.exec(`COPY (${body}) ${target};`);
    }
  },
};

/**
 * As above, but the raw-winner dedup uses GROUP BY pk + JOIN on
 * max(cursor) instead of a window. Hash aggregate is streamable and
 * should have a smaller pinned footprint than ROW_NUMBER when raw
 * has many duplicates per pk.
 */
const groupbyAntijoinPerPartition: Strategy = {
  name: "groupby-anti-per-part",
  description:
    "GROUP BY pk + JOIN max(cursor) for raw winners, anti-join curated, per partition",
  async run(ctx) {
    if (!ctx.scenario.cursor) {
      // GROUP BY+max only makes sense with a real cursor. Fall back
      // to the CTE/window version for no-cursor scenarios so we still
      // emit a row for the correctness report.
      return cteAntijoinPerPartition.run(ctx);
    }
    const pk = pkClause(ctx.scenario);
    const pkJoin = ctx.scenario.primaryKey
      .map((p) => `c."${p}" = w."${p}"`)
      .join(" AND ");
    const pkSelfJoin = ctx.scenario.primaryKey
      .map((p) => `r."${p}" = k."${p}"`)
      .join(" AND ");
    const cursor = ctx.scenario.cursor;
    const cursorAntijoin = `AND c."${cursor}" <= w."${cursor}"`;

    const partitions =
      ctx.scenario.partitionBy.length > 0
        ? ctx.partitionsInRaw
        : [Object.create(null)];

    for (const part of partitions) {
      const partFilter =
        ctx.scenario.partitionBy.length > 0 && Object.keys(part).length > 0
          ? ` WHERE ${ctx.scenario.partitionBy
              .map(
                (c) =>
                  `"${c}" = '${String(part[c]).replace(/'/g, "''")}'`,
              )
              .join(" AND ")}`
          : "";

      const body = ctx.curatedRead
        ? `
          WITH raw_src AS (
            SELECT * FROM ${ctx.rawRead}${partFilter}
          ),
          raw_keys AS (
            SELECT ${pk}, MAX("${cursor}") AS max_c FROM raw_src GROUP BY ${pk}
          ),
          raw_winners AS (
            SELECT r.* FROM raw_src r
            JOIN raw_keys k ON (${pkSelfJoin}) AND r."${cursor}" = k.max_c
          ),
          curated_survivors AS (
            SELECT c.* FROM (SELECT * FROM ${ctx.curatedRead}${partFilter}) c
            ANTI JOIN raw_winners w
              ON (${pkJoin}) ${cursorAntijoin}
          )
          SELECT * FROM raw_winners
          UNION ALL BY NAME
          SELECT * FROM curated_survivors
        `
        : `
          WITH raw_src AS (
            SELECT * FROM ${ctx.rawRead}${partFilter}
          ),
          raw_keys AS (
            SELECT ${pk}, MAX("${cursor}") AS max_c FROM raw_src GROUP BY ${pk}
          )
          SELECT r.* FROM raw_src r
          JOIN raw_keys k ON (${pkSelfJoin}) AND r."${cursor}" = k.max_c
        `;

      const target =
        ctx.scenario.partitionBy.length > 0 &&
        Object.keys(part).length > 0
          ? writeTargetForPartition(
              ctx.outDir,
              part,
              ctx.scenario.partitionBy,
              10_000,
            )
          : writeTarget(ctx.outDir, ctx.scenario.partitionBy, 10_000);

      await ctx.db.exec(`COPY (${body}) ${target};`);
    }
  },
};

/**
 * The move the web suggested: decide which rows win using ONLY the
 * narrow key columns, then re-join the full wide payload only at
 * the final COPY. The window never sees the JSON blobs.
 *
 * Shape:
 *   WITH src AS (select all rows in this partition from raw + curated),
 *        winners AS (  -- narrow (pk, cursor) only, spillable
 *          SELECT pk, cursor FROM src
 *          QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY cursor DESC) = 1
 *        )
 *   COPY (SELECT src.* FROM src SEMI JOIN winners USING (pk, cursor))
 *
 * The `SEMI JOIN ... USING (pk, cursor)` keeps src rows that appear
 * in winners — with (pk, cursor) as the join key, every winner
 * matches exactly one src row. Duplicate (pk, cursor) pairs across
 * raw + curated would double-emit; dripline's semantics require pk
 * uniqueness per cursor value so this is safe in practice.
 *
 * Important: we DON'T materialise `src` as a temp table. It's a CTE
 * DuckDB can plan as two independent reads of the same parquet files
 * (DuckDB caches parquet metadata via object_cache so the second
 * read is cheap), keeping pinned memory small.
 */
const narrowDecisionPerPartition: Strategy = {
  name: "narrow-decision",
  description:
    "decide winners on (pk, cursor) only; re-join wide payload at COPY time; per-partition",
  async run(ctx) {
    const pk = pkClause(ctx.scenario);
    const orderBy = orderByClause(ctx.scenario);
    const pkUsing = ctx.scenario.primaryKey.map((p) => `"${p}"`).join(", ");
    const cursorCol = ctx.scenario.cursor ? `"${ctx.scenario.cursor}"` : null;
    const joinUsing = cursorCol ? `${pkUsing}, ${cursorCol}` : pkUsing;

    // Column list that the narrow winners projection yields. Without
    // a cursor we still need PK so the semi-join works.
    const narrowCols = cursorCol ? `${pk}, ${cursorCol}` : pk;

    const partitions =
      ctx.scenario.partitionBy.length > 0
        ? ctx.partitionsInRaw
        : [Object.create(null)];

    for (const part of partitions) {
      const partFilter =
        ctx.scenario.partitionBy.length > 0 && Object.keys(part).length > 0
          ? ` WHERE ${ctx.scenario.partitionBy
              .map(
                (c) =>
                  `"${c}" = '${String(part[c]).replace(/'/g, "''")}'`,
              )
              .join(" AND ")}`
          : "";

      // src is the logical union of raw + curated for this partition.
      // Expressed twice in the SQL (once for narrow, once for wide
      // SEMI JOIN) so DuckDB can plan each read as a separate scan
      // with only the needed columns projected out of parquet.
      const srcSql = ctx.curatedRead
        ? `SELECT * FROM ${ctx.rawRead}${partFilter}
           UNION ALL BY NAME
           SELECT * FROM ${ctx.curatedRead}${partFilter}`
        : `SELECT * FROM ${ctx.rawRead}${partFilter}`;

      const narrowSrcSql = srcSql;

      // Without a cursor, the SEMI JOIN on PK alone would match BOTH
      // the raw row and the curated row for the same PK — giving us
      // duplicates. The window's ROW_NUMBER tiebreaker picks one, but
      // the semi-join doesn't know about that pick. Solution: tag each
      // winner with a synthetic rank and semi-join on that too. When
      // there IS a cursor, (pk, cursor) already disambiguates (raw's
      // newer cursor wins).
      //
      // The narrow-projection trick still works here — we add a row
      // identity column BEFORE the wide payload fans out.
      const body = cursorCol
        ? `
          WITH winners AS (
            SELECT ${narrowCols} FROM (
              SELECT ${narrowCols},
                     ROW_NUMBER() OVER (
                       PARTITION BY ${pk} ORDER BY ${orderBy}
                     ) AS _rn
              FROM (${narrowSrcSql})
            ) WHERE _rn = 1
          )
          SELECT src.* FROM (${srcSql}) src
          SEMI JOIN winners USING (${joinUsing})
        `
        : `
          -- No cursor: tag every source row with a stable rank, pick
          -- rank=1 per pk. Raw rows come before curated rows in the
          -- UNION so 'src_order ASC' prefers raw (same as baseline's
          -- PK DESC tiebreak for raw rows).
          WITH src_tagged AS (
            SELECT *, 0::INTEGER AS _src_order FROM ${ctx.rawRead}${partFilter}
            ${
              ctx.curatedRead
                ? `UNION ALL BY NAME
                   SELECT *, 1::INTEGER AS _src_order FROM ${ctx.curatedRead}${partFilter}`
                : ""
            }
          ),
          winners AS (
            SELECT ${pk}, _src_order FROM (
              SELECT ${pk}, _src_order,
                     ROW_NUMBER() OVER (
                       PARTITION BY ${pk}
                       ORDER BY _src_order ASC, ${orderBy}
                     ) AS _rn
              FROM src_tagged
            ) WHERE _rn = 1
          )
          SELECT src.* EXCLUDE (_src_order)
          FROM src_tagged src
          SEMI JOIN winners USING (${pkUsing}, _src_order)
        `;

      const target =
        ctx.scenario.partitionBy.length > 0 &&
        Object.keys(part).length > 0
          ? writeTargetForPartition(
              ctx.outDir,
              part,
              ctx.scenario.partitionBy,
              10_000,
            )
          : writeTarget(ctx.outDir, ctx.scenario.partitionBy, 10_000);

      await ctx.db.exec(`COPY (${body}) ${target};`);
    }
  },
};

const STRATEGIES: Strategy[] = [
  baselineStrategy,
  antijoinPerPartition,
  antijoinSinglePass,
  cteAntijoinPerPartition,
  groupbyAntijoinPerPartition,
  narrowDecisionPerPartition,
];

// ─── Measurement + correctness ──────────────────────────────────────

interface RunResult {
  scenario: string;
  strategy: string;
  status: "ok" | "error";
  error?: string;
  durationMs: number;
  peakRssMB: number;
  outputRows: number | null;
  /** Hash of sorted (pk, cursor) pairs for correctness comparison. */
  outputDigest: string | null;
}

class RssProbe {
  private peak = 0;
  private timer: ReturnType<typeof setInterval> | null = null;
  start(intervalMs = 25): void {
    this.timer = setInterval(() => {
      const mb = process.memoryUsage().rss / 1024 / 1024;
      if (mb > this.peak) this.peak = mb;
    }, intervalMs);
  }
  stop(): number {
    if (this.timer) clearInterval(this.timer);
    return this.peak;
  }
}

/**
 * Compute a digest of a compact's output so two strategies can be
 * compared for row-equality. We hash the sorted concatenation of
 * every PK + cursor value; wide payloads aren't hashed (saves memory)
 * under the assumption that the seed data is deterministic per PK —
 * i.e. if baseline and candidate both emit the same (pk, cursor)
 * set, they emit the same full rows.
 */
async function outputDigest(
  db: Database,
  outDir: string,
  scenario: Scenario,
): Promise<{ rows: number; digest: string }> {
  const pkCols = scenario.primaryKey.map((p) => `"${p}"::VARCHAR`).join(" || '|' || ");
  const cursorCol = scenario.cursor ? `"${scenario.cursor}"::VARCHAR` : "''";
  const readExpr = `read_parquet('${outDir}/**/*.parquet', hive_partitioning => ${scenario.partitionBy.length > 0})`;
  const result = await db.all(`
    SELECT
      COUNT(*) AS n,
      md5(string_agg(key, '\n' ORDER BY key)) AS digest
    FROM (
      SELECT ${pkCols} || '|' || ${cursorCol} AS key FROM ${readExpr}
    );
  `);
  const row = result[0] as { n: bigint | number; digest: string | null };
  return {
    rows: Number(row.n),
    digest: row.digest ?? "empty",
  };
}

async function runStrategyForScenario(
  scenario: Scenario,
  strategy: Strategy,
  prefix: string,
): Promise<RunResult> {
  const remote = new Remote({
    endpoint: ENDPOINT,
    bucket: BUCKET,
    prefix,
    accessKeyId: KEY,
    secretAccessKey: SECRET,
    secretType: "S3",
  });

  const db = await Database.create(":memory:", {
    memoryLimit: MEMORY_LIMIT,
    threads: 1,
    tempDirectory: TEMP_DIR,
    preserveInsertionOrder: false,
    objectCache: true,
  });

  const outDir = `s3://${BUCKET}/${prefix}/out/${scenario.name}/${strategy.name}`;
  await cleanupPrefix(`${prefix}/out/${scenario.name}/${strategy.name}/`);

  const rawRead = `read_parquet('s3://${BUCKET}/${prefix}/raw/${scenario.name}/**/*.parquet', union_by_name => true, hive_partitioning => false)`;
  const curatedRead = scenario.skipCurated
    ? null
    : `read_parquet('s3://${BUCKET}/${prefix}/curated/${scenario.name}/**/*.parquet', hive_partitioning => ${scenario.partitionBy.length > 0})`;

  // Discover partition combos actually present in raw — real compact
  // does this too (to build the curated IN filter).
  let partitionsInRaw: Array<Record<string, string>> = [{}];
  if (scenario.partitionBy.length > 0) {
    await remote.attach(db);
    const partCols = scenario.partitionBy.map((c) => `"${c}"`).join(", ");
    const rows = (await db.all(
      `SELECT DISTINCT ${partCols} FROM ${rawRead}`,
    )) as Array<Record<string, string>>;
    partitionsInRaw = rows.map((r) => {
      const o: Record<string, string> = {};
      for (const c of scenario.partitionBy) o[c] = String(r[c]);
      return o;
    });
  }

  await remote.attach(db);

  const ctx: StrategyCtx = {
    db,
    scenario,
    prefix,
    rawRead,
    curatedRead,
    outDir,
    partitionsInRaw,
  };

  const probe = new RssProbe();
  probe.start();
  const start = Date.now();
  const result: RunResult = {
    scenario: scenario.name,
    strategy: strategy.name,
    status: "ok",
    durationMs: 0,
    peakRssMB: 0,
    outputRows: null,
    outputDigest: null,
  };

  try {
    await strategy.run(ctx);
    result.durationMs = Date.now() - start;
    result.peakRssMB = probe.stop();
    const { rows, digest } = await outputDigest(db, outDir, scenario);
    result.outputRows = rows;
    result.outputDigest = digest;
  } catch (err) {
    result.durationMs = Date.now() - start;
    result.peakRssMB = probe.stop();
    result.status = "error";
    result.error = (err as Error).message;
  } finally {
    await db.close();
  }
  return result;
}

// ─── Main ───────────────────────────────────────────────────────────

async function main(): Promise<void> {
  log(`bench: memory=${MEMORY_LIMIT}  spill=${TEMP_DIR}`);
  log(`prefix: ${RUN_ID}`);

  await ensureBucket();

  const remote = new Remote({
    endpoint: ENDPOINT,
    bucket: BUCKET,
    prefix: RUN_ID,
    accessKeyId: KEY,
    secretAccessKey: SECRET,
    secretType: "S3",
  });

  log("─── seeding scenarios ───");
  for (const s of SCENARIOS) {
    const t0 = Date.now();
    await seedScenario(remote, s, RUN_ID);
    log(`  ${s.name.padEnd(16)} seeded in ${Date.now() - t0}ms`);
  }

  // Run baseline first per scenario — sets the correctness truth.
  const allResults: RunResult[] = [];
  const baselineDigests: Record<string, string | null> = {};
  const baselineRows: Record<string, number | null> = {};

  log("─── strategies × scenarios ───");
  for (const scenario of SCENARIOS) {
    for (const strategy of STRATEGIES) {
      const r = await runStrategyForScenario(scenario, strategy, RUN_ID);
      allResults.push(r);
      const tag = `${scenario.name}/${strategy.name}`;
      if (r.status === "ok") {
        log(
          `  ${tag.padEnd(40)} OK   dur=${r.durationMs}ms  peakRss=${r.peakRssMB.toFixed(0)}MB  rows=${r.outputRows}`,
        );
      } else {
        log(
          `  ${tag.padEnd(40)} FAIL peakRss=${r.peakRssMB.toFixed(0)}MB  err=${r.error?.slice(0, 100)}`,
        );
      }
      if (strategy.name === baselineStrategy.name) {
        baselineDigests[scenario.name] = r.outputDigest;
        baselineRows[scenario.name] = r.outputRows;
      }
    }
  }

  // ── Summary + correctness check ──
  log("─── summary ───");
  console.log();
  console.log(
    "scenario             strategy               status  dur     peakRss  rows       correctness",
  );
  console.log(
    "───────────────────  ─────────────────────  ──────  ──────  ───────  ─────────  ───────────",
  );
  // Build cross-strategy consensus when baseline crashes: if multiple
  // passing strategies agree on (rows, digest), treat that as ground
  // truth. Baseline's verdict still wins when it finishes.
  const consensusByScenario: Record<string, { rows: number; digest: string } | null> = {};
  for (const scenario of SCENARIOS) {
    const passing = allResults.filter(
      (r) =>
        r.scenario === scenario.name &&
        r.status === "ok" &&
        r.outputDigest != null,
    );
    if (passing.length === 0) {
      consensusByScenario[scenario.name] = null;
      continue;
    }
    const baselineR = passing.find((r) => r.strategy === baselineStrategy.name);
    if (baselineR) {
      consensusByScenario[scenario.name] = {
        rows: baselineR.outputRows!,
        digest: baselineR.outputDigest!,
      };
    } else {
      // Vote: most common (rows, digest) pair among passing strategies.
      const counts: Record<string, { count: number; rows: number; digest: string }> = {};
      for (const r of passing) {
        const key = `${r.outputRows}|${r.outputDigest}`;
        counts[key] = counts[key]
          ? { ...counts[key], count: counts[key].count + 1 }
          : { count: 1, rows: r.outputRows!, digest: r.outputDigest! };
      }
      const winner = Object.values(counts).sort((a, b) => b.count - a.count)[0];
      consensusByScenario[scenario.name] = { rows: winner.rows, digest: winner.digest };
    }
  }

  for (const r of allResults) {
    const consensus = consensusByScenario[r.scenario];
    const correctness =
      r.strategy === baselineStrategy.name && r.status === "ok"
        ? "TRUTH"
        : r.status === "ok" && consensus
          ? r.outputDigest === consensus.digest && r.outputRows === consensus.rows
            ? "MATCH"
            : `MISMATCH (rows ${r.outputRows} vs ${consensus.rows})`
          : r.status === "ok"
            ? "no-consensus"
            : "n/a";
    console.log(
      `${r.scenario.padEnd(19)}  ${r.strategy.padEnd(21)}  ${r.status.padEnd(6)}  ${
        `${r.durationMs}ms`.padEnd(6)
      }  ${`${r.peakRssMB.toFixed(0)}MB`.padEnd(7)}  ${
        `${r.outputRows ?? "-"}`.padEnd(9)
      }  ${correctness}`,
    );
  }
  console.log();

  log("cleanup: deleting bench prefix");
  await cleanupPrefix(`${RUN_ID}/`);
  log("done");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
