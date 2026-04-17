# Dripyard

Dashboard + worker supervisor for [dripline](../dripline/) projects. Point it at a workspace (a directory with `.dripline/`) and get a reactive UI over its lanes, runs, workers, catalog, warehouse, and SQL query surface — plus a live control plane for scheduling and scaling workers.

Built on [vex-core](https://github.com/Michaelliv/vex-core) (reactive backend) + [dripline](../dripline/) (data sync engine) + optional [flaregun](https://github.com/Michaelliv/flaregun) (rotating proxy).

> Lives at `packages/dripyard/` in the [dripline monorepo](../../). Depends on `packages/dripline` via `workspace:*`, so changes there are picked up without a publish.

## Architecture

```
┌───────────────────────────────────────────────────────────┐
│                     Dripyard Server                       │
│                                                           │
│  ┌──────────┐  ┌──────────────┐  ┌───────────────────┐    │
│  │Scheduler │→ │ Orchestrator │→ │ Dripline (sync)   │    │
│  │(vex jobs)│  │              │  │  ├─ Plugin (API)  │    │
│  └──────────┘  │              │  │  ├─ DuckDB        │    │
│                │              │→ │  └─ Remote (S3/R2)│    │
│  ┌──────────┐  │              │  └───────────────────┘    │
│  │ Vex Core │  │              │                           │
│  │ (SQLite) │  │              │  ┌───────────────────┐    │
│  │  lanes   │  │              │→ │ FlareGun (proxy)  │    │
│  │  runs    │  │              │  │  └─ CF Workers    │    │
│  │ workers  │  └──────────────┘  └───────────────────┘    │
│  └──────────┘                                             │
└───────────────────────────────────────────────────────────┘
```

**Vex Core** — reactive backend engine. Stores lane definitions, run history, and worker state in SQLite. Provides query/mutate/subscribe API over HTTP with SSE for real-time updates.

**Dripline** — does the actual data sync. Plugins turn APIs into SQL-queryable tables, the sync engine streams data into DuckDB, and the Remote layer publishes parquet files to S3/R2. Dripyard adopts dripline's project format (`.dripline/plugins.json`, `.dripline/config.json`) as its source of truth.

**FlareGun** — optional rotating proxy network on Cloudflare Workers. When a lane has `proxyEnabled: true`, HTTP calls route through rotating IPs.

## Quickstart

```bash
npm install -g dripyard
# or
bun add -g dripyard

# Point it at an existing dripline workspace
dripyard serve /path/to/my-dripline-project

# Or default to cwd
cd /path/to/my-dripline-project
dripyard serve
```

The server loads the workspace's plugins, hydrates lanes from `config.json`, starts an embedded worker, and **serves the React dashboard from the same process** — no separate Vite server. UI at `http://localhost:3457`.

### Development (from this monorepo)

```bash
bun install                                         # workspace linking
bun --filter dripyard build:ui                      # build the React bundle
bun --filter dripyard dev -- serve <workspace-dir>  # http://localhost:3457

# If you want HMR while hacking on the UI:
bun --filter dripyard dev:ui                        # Vite on :5173, proxies /vex
```

### Standalone worker

```bash
bun --filter dripyard dev -- worker /path/to/my-dripline-project \
  --socket /tmp/dripyard-3457.sock
```

Workers register over the dashboard's unix socket and auto-claim lanes via R2 leases. The UI's **+1 worker** button does this for you.

### CLI

```bash
dripyard lane list                # list lanes
dripyard lane run <id>            # trigger a run now
dripyard runs <lane-id>           # show recent runs
dripyard status                   # orchestrator snapshot
```

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `DRIPYARD_PORT` | `3457` | Server port (UI + `/vex` + `/health`) |
| `DRIPYARD_DB` | `:memory:` | SQLite path for operational state (runs, workers, progress) |
| `DRIPYARD_WORKER` | `worker-<hostname>` | Embedded worker name |
| `DRIPYARD_URL` | `http://localhost:3457` | Server URL for CLI commands |
| `DRIPYARD_SOCKET` | `<tmpdir>/dripyard-<port>.sock` | Unix socket for local workers |
| `DRIPYARD_TOKEN` | *(unset)* | Shared bearer token. When set, all HTTP requests require it. |

## Auth

By default, dripyard runs **open** — correct for localhost and for dev where you're the only caller. For anything on a public URL, set `DRIPYARD_TOKEN`:

```bash
export DRIPYARD_TOKEN="$(openssl rand -base64 32)"
dripyard serve
```

With a token set:

- Browsers get redirected to `/login`, a minimal form that POSTs the token and sets an HTTP-only cookie. `/logout` clears it.
- CLIs and curl send `Authorization: Bearer <token>` on every request.
- `/health` bypasses auth so platform probes work without leaking the token.
- Failed logins are rate-limited per IP (5 / minute by default).

`ServerOptions` also accepts `token`, `cors`, and `log` directly if you're embedding `startServer` programmatically. Auth is one token, one gate — for multi-user or SSO, put Cloudflare Access / Tailscale / an oauth-proxy in front.

## Workspace format

A dripyard workspace is literally a dripline workspace. It needs:

- `.dripline/config.json` — connections, lanes, remote, rate limits
- `.dripline/plugins.json` — installed dripline plugins

Both are standard dripline files; nothing dripyard-specific lives on disk. Dripyard's SQLite is a live cache + operational layer (runs, workers, live progress) that can be wiped and rebuilt from the workspace any time.

## License

MIT
