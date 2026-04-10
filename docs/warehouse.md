# Warehouse Mode

Warehouse mode turns dripline into a lightweight data warehouse on top of any S3-compatible bucket. The same plugins and SQL surface from local mode — but data accumulates over time and queries read from parquet instead of hitting live APIs.

## How It Works

```
Plugin APIs ──→ dripline run ──→ raw/ (append-only parquet)
                                   │
                            dripline compact
                                   │
                                   ▼
                            curated/ (deduped, partitioned parquet)
                                   │
                         dripline query --remote
                                   │
                                   ▼
                              DuckDB SQL
```

Three commands, one bucket:

- **`dripline run`** syncs configured lanes. Each lane is a group of tables on a schedule. The engine fetches from plugin APIs, writes append-only parquet files to `raw/`, and advances cursors. Workers coordinate via leases stored in the bucket — no external scheduler needed.

- **`dripline compact`** merges `raw/` into `curated/`. Deduplicates by primary key (latest cursor wins), partitions by key columns (e.g. `org_id/business_date`), and cleans up consumed raw files. Compaction is incremental — it only reads the curated partitions that overlap with new raw data.

- **`dripline query --remote`** creates DuckDB views over `curated/` parquet and runs your SQL. DuckDB's hive partition pruning and parquet statistics handle predicate pushdown — a query filtering on `org_id = 'x' AND business_date = '2024-01-15'` reads one file, not thousands.

## Setup

```bash
# Point at a bucket
export R2_KEY_ID=...
export R2_SECRET=...
dripline remote set https://<account>.r2.cloudflarestorage.com \
  --bucket my-warehouse --prefix prod --secret-type S3 \
  --access-key-env R2_KEY_ID --secret-key-env R2_SECRET

# Define lanes
dripline lane add orders \
  --table orders --params org_id=abc123 \
  --interval 15m

dripline lane add dimensions \
  --table products --table categories \
  --interval 1h
```

## Lanes

A lane is a named group of tables synced together on a schedule. Each table in a lane can have its own params (key column values passed to the plugin).

```json
{
  "lanes": {
    "orders": {
      "tables": [
        { "name": "orders", "params": { "org_id": "abc123" } },
        { "name": "orders", "params": { "org_id": "def456" } }
      ],
      "interval": "15m"
    },
    "detail": {
      "tables": [
        { "name": "order_detail", "params": { "org_id": "abc123" } }
      ],
      "interval": "6h",
      "maxRuntime": "30m"
    }
  }
}
```

- **`interval`** — minimum time between runs. After a successful run, the lane's lease holds for this duration.
- **`maxRuntime`** — optional hard cap. Useful for long-running backfills that should yield to other lanes.

The same table can appear in multiple lanes with different params (e.g. different orgs). Tables within a lane sync sequentially; lanes themselves are independent.

## Leases

Workers coordinate via JSON files in `_leases/`. When `dripline run` starts a lane, it writes a lease file with its holder ID and expiry. Other workers see the lease and skip that lane. After the run completes, the lease holds for the lane's interval (cooldown), preventing unnecessary re-runs.

This means you can run `dripline run` from multiple machines on a cron — they divide lanes via contention. No leader election, no rebalancing.

### Releasing stale leases

If a run is killed mid-sync, the lease persists until it expires. To release immediately:

```bash
dripline lane reset <name> --yes          # release lease, keep cursor
dripline lane reset <name> --hard --yes   # release lease AND delete cursor
```

The default (`--yes` without `--hard`) is safe — it releases the lease so the next run can start, but cursor state is preserved. Sync resumes where it left off. Use `--hard` only when you want a full rebackfill from `initialCursor`.

## Compaction

### How it works

Compact reads raw parquet files (new data from recent runs) and merges them with the affected curated partitions. The result is deduplicated by primary key — when the same PK appears in both raw and curated, the row with the latest cursor value wins. The merged data is written back as partitioned parquet files.

### Incremental compaction

Compaction is incremental by default. When raw data only touches 2 out of 8,000 partitions, compact reads those 2 curated partitions, not all 8,000.

The algorithm:

1. List raw files for the table. If none exist, skip (no work to do).
2. Read the raw parquet and extract distinct partition column values as literals (e.g. `('x', '2024-01-15'), ('x', '2024-01-16')`).
3. Read only the curated partitions matching those literals. DuckDB's hive partition pruning skips non-matching files at the directory level.
4. Union raw + affected curated, deduplicate by primary key (latest cursor wins via `ROW_NUMBER`).
5. Write back to `curated/` with `PARTITION_BY`. Only the affected partition files are overwritten.
6. Delete the consumed raw files.

This turns a 66-minute full recompaction into a 5-second incremental compact for a daily sync.

### Known limitation: partition migration

If a row's partition column value changes between syncs (e.g. `business_date` corrected from `2024-01-15` to `2024-01-16`), the old partition retains the stale copy. Incremental compact only reads the new partition from curated and doesn't see the old row.

In practice this is rare — partition columns like `org_id` and `business_date` are effectively immutable for most data sources. If it matters for your use case, run a periodic full compact with `--hard` to reset.

## Sync Progress

During long-running syncs, the engine fires progress events every 10,000 rows:

```
ℹ [detail] github_issues: 50,000 rows (6.8/s cursor=2025-08-28)
```

This is available via the `onProgress` callback in the SDK:

```typescript
await dl.sync(params, (event) => {
  console.log(`${event.table}: ${event.rowsInserted} rows, cursor=${event.cursor}`);
});
```

## Bucket Layout

```
<prefix>/
├── _leases/
│   └── lane-<name>.json              lease + cooldown timer
├── _state/
│   └── <lane>/_dripline_sync.parquet  cursor metadata
├── _manifests/
│   └── <table>.json                   file index + partition stats
├── raw/
│   └── <table>/lane=<lane>/run=<id>.parquet   append-only
└── curated/
    └── <table>/<partition_col>=<val>/data_0.parquet   deduplicated
```

- **raw/** is append-only. Each `dripline run` adds one file per synced table. Never modified in place.
- **curated/** is rewritten by `compact`. Partitioned by the table's `keyColumns`. DuckDB reads these via hive partitioning.
- **_state/** holds cursor metadata per lane as parquet. One row per table+params combo with `last_cursor`, `last_sync_at`, `rows_synced`.
- **_leases/** holds JSON lease files. One per lane, one per compact table.
- **_manifests/** holds per-table JSON with file list, row counts, and partition min/max.

## Production Checklist

```bash
# One cron line is all you need:
*/15 * * * * cd /path/to/project && source .env && dripline run && dripline compact
```

- **Interval** — set cron to your shortest lane interval (e.g. 15m for orders). Longer lanes (1h, 6h) skip automatically when not due.
- **Multiple workers** — add more cron entries on different machines. Lease contention divides work.
- **Credentials** — use env vars (`--access-key-env`, `--secret-key-env`) to keep secrets out of config files.
- **Monitoring** — `dripline run` exits 0 on success, 1 on failure. Logs include row counts, durations, and cursor positions.
- **Backfill** — set `initialCursor` in the plugin table definition (e.g. `"2025-01-01"`). First run backfills from there.
- **Cost** — R2 has no egress fees. Storage is ~$0.015/GB/month. PUT operations are $4.50/million. A typical warehouse costs pennies.
