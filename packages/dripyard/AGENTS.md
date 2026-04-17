# Dripyard

Dashboard + worker supervisor for dripline projects. Reactive UI over `.dripline/` workspaces, built on vex-core + dripline + flaregun.

Lives at `packages/dripyard/` in the [dripline monorepo](../../CLAUDE.md). Depends on `packages/dripline` via `workspace:*`.

## Commands

```bash
bun test                              # 50 tests across 6 files
bun run dev -- serve [workspace]      # start server → http://localhost:3457
bun run dev -- lane list              # list lanes
bun run dev -- status                 # orchestrator snapshot
bun run typecheck                     # tsc --noEmit
bun run format                        # biome format
bun run lint                          # biome lint
```

## Project Structure

```
src/
  core/
    orchestrator.ts   # Lane execution: pull config → dripline sync → publish → record
    scheduler.ts      # Vex job registration per lane schedule
    workspace.ts      # loadWorkspace / hydrateLanes — adopts a dripline project
    client.ts         # VexClient interface: LocalVexClient + SocketVexClient
    spawner.ts        # Spawner interface + ForkSpawner for the +1 worker flow
  plugins/
    lanes.ts          # lanes table + CRUD/toggle/runNow mutations
    runs.ts           # Run history, per-table results, live progress stream
    workers.ts        # Worker registration, heartbeat, spawn/kill, draining
    workspace.ts      # Read-only queries over the loaded workspace + warehouse + runSql
  app/ui/             # React dashboard (Vite + Tailwind + shadcn)
  server.ts           # Bun.serve + vex handler + static UI, TCP + unix socket bound together
  worker.ts           # Standalone worker binary, graceful drain via AbortController
  main.ts             # CLI: serve, worker, lane, runs, status
  index.ts            # Library exports
tests/
  plugins.test.ts     # Lane/run/worker CRUD + runNow + setDraining
  orchestrator.test.ts # Execution, error handling, worker coordination
  scheduler.test.ts   # Job reconciliation
  server.test.ts      # HTTP smoke tests
  worker.test.ts      # Unix socket transport + graceful drain
  workspace.test.ts   # loadWorkspace, hydrateLanes, workspace queries, runSql
```

## Architecture

Three layers:

**Workspace** — a `.dripline/` directory on disk is the source of truth. `loadWorkspace()` reads `plugins.json` and dynamic-imports plugins into dripline's shared registry; `hydrateLanes()` upserts the workspace's lanes into the local SQLite by name, so the config file wins on every boot.

**Plugins** store operational state in vex-core (SQLite). `lanes` mirrors the workspace lanes. `runs` tracks every execution with per-table results. `workers` tracks live processes + telemetry. `workspace` exposes read-only queries for the UI (plugins, catalog, connections, warehouse) plus the ad-hoc `runSql` mutation for querying curated parquet.

**Orchestrator** is the brain. `executeLane()` reads a lane from vex, creates a Dripline instance with the right plugin + connection + sink, runs sync, publishes parquet via dripline's Remote, records per-table results back to vex. When `proxyEnabled`, HTTP routes through flaregun.

**Scheduler** maps lane schedules to vex-core jobs. `start()` reads enabled lanes and registers a job each. `refresh()` reconciles. `stop()` cleans up.

## Tables

| Table | Key Columns |
|-------|-------------|
| lanes | name, sourcePlugin, sourceConfig, sinkType, sinkConfig, schedule, maxRuntime, enabled, proxyEnabled |
| runs | laneId, status, startedAt, finishedAt, rowsSynced, filesPublished, error, workerId, durationMs |
| run_table_results | runId, tableName, rowsInserted, cursor, durationMs, error |
| run_progress | runId, tableName, rowsInserted, cursor, elapsedMs, rate, timestamp |
| workers | name, status, lastHeartbeat, currentLaneId, currentRunId, host, startedAt, spawnHandle |
| worker_samples | workerId, timestamp, heapUsedMb, rssMb, externalMb, loadAvg1m, loadAvg5m, uptimeS |

## Key Patterns

- **Workspace as source of truth**: `.dripline/` on disk owns plugins, connections, lanes, remote. SQLite is a live cache. Wipe + restart produces identical state.
- **VexClient abstraction**: `LocalVexClient` in-process, `SocketVexClient` over unix socket. Orchestrator is transport-agnostic — same code runs in dashboard or standalone worker.
- **Unix socket + TCP duality**: server binds the same handler on `port` (UI/CLI) and on `<tmpdir>/dripyard-<port>.sock` (local workers). Identical surface area.
- **Embedded static UI**: the built React bundle at `dist/app/ui/` is served from the same `Bun.serve` handler as the engine endpoints (`/query`, `/mutate`, `/subscribe`, `/webhook/*`). No separate Vite process in production; `/assets/*` gets `immutable` cache, everything else revalidates, and client-side routes (no extension, no engine-endpoint match) fall through to `index.html`.
- **Graceful drain**: SIGTERM on a worker → setDraining → wait for in-flight runs with grace window → AbortController fires if timeout → dripline throws AbortError at next checkpoint → orchestrator releases R2 lease → deregister → exit.
- **Pluggable spawner**: `workers.spawn` mutation calls configured `Spawner`. `ForkSpawner` handles local child processes; cloud spawners (fly, k8s) implement the same interface.

## Dependencies

- **vex-core** — reactive backend engine (SQLite, subscriptions, jobs, HTTP handler)
- **dripline** — data sync engine (plugins, DuckDB, S3/R2 warehouse, AbortSignal)
- **@miclivs/flaregun** — rotating proxy network (Cloudflare Workers)

## Testing

50 tests across 6 files — `bun test`:
- `plugins.test.ts` — lane CRUD, runNow dispatch, worker setDraining, run lifecycle
- `orchestrator.test.ts` — lane execution wiring, error recording, worker status
- `scheduler.test.ts` — job registration/removal, schedule normalization
- `server.test.ts` — HTTP smoke tests (health, create, list, worker auto-registration)
- `worker.test.ts` — SocketVexClient round trip, startWorker graceful drain via AbortController
- `workspace.test.ts` — loadWorkspace, hydrateLanes idempotency, workspace queries, runSql validation, secret masking
