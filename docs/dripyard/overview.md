# Dripyard overview

Dripyard is a dashboard and worker supervisor that sits on top of a dripline workspace. Where dripline is "SQL over APIs," dripyard is "run that SQL on a schedule, persist it, show it in a UI, let workers react to it."

## What you get

- **A reactive UI** — React app, served by the same Bun process, live-updates as data syncs.
- **Lanes** — named pipelines that run a dripline sync and trigger downstream work. Think "poll GitHub issues every 5 min, push a summary into a dashboard card."
- **Workers** — long-running processes that consume synced data. Supervised by the server, restarted on failure, log-aggregated.
- **Artifacts** — files produced by workers, stored on disk, surfaced in the UI.

All state lives in `.dripyard/` alongside `.dripline/`. DuckDB for queryable data, plain files for artifacts. No external database.

## Relationship to dripline

Dripyard is a *consumer* of dripline — it imports the SDK, loads your workspace's plugins, and calls `sync()` and `query()` on your behalf. You can run dripline standalone without dripyard. You cannot run dripyard without dripline.

A dripyard workspace *is* a dripline workspace. Run `dripyard serve` in the same directory where you ran `dripline init`.

## Starting it

```bash
dripyard serve         # defaults to cwd, port 3457
dripyard serve ./some/workspace --port 8080
```

The CLI talks to a running server over HTTP (the `dripyard` binary is itself a thin client for most commands). `serve` is the one that actually starts the server.

## Running a worker

```bash
dripyard worker --lane my-lane
```

Workers are usually declared in the workspace config and supervised automatically by the server, but you can also run them standalone (useful for local debugging).

## Next

- [Deploying dripyard](./deploy.md) — single-container recipe.
