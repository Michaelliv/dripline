# Quickstart

Empty directory → querying APIs → dripyard deployed. Ten minutes if the wind is right.

## 1. Prerequisites

You need [Bun](https://bun.sh) 1.3+ on PATH. The CLIs run on Bun (shebang: `#!/usr/bin/env bun`), and dripyard's HTTP router uses the `URLPattern` global that Bun ships in 1.3+. You can install via `npm` or `bun`, but Bun must be available at runtime.

```bash
bun --version   # 1.3.0 or later
```

## 2. Install the CLIs

```bash
npm install -g dripline dripyard
```

Both ship from the same monorepo and publish lockstep on every `v*` tag, so their versions always match.

## 3. Initialize a project

```bash
mkdir my-workspace && cd my-workspace
dripline init
```

You now have a `.dripline/` directory with a config file. Env vars always override config — tokens go in `.env` or your shell, not in git.

## 4. Get some plugins

Plugins are **not** published to npm. They live in the dripline repo and get installed via git:

```bash
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/github
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/stripe
```

See [Installing plugins](./plugins/install.md) for the full catalogue and pinning to tags.

If none of the bundled plugins fit your data source, skip to [Writing your own plugin](./plugins/write-your-own.md) — it's a single TypeScript file.

## 5. Provide credentials

Each plugin declares a connection schema with env var names. For GitHub:

```bash
export GITHUB_TOKEN=ghp_xxx
```

Or in `.dripline/config.json`:

```json
{
  "connections": [
    { "name": "gh", "plugin": "github", "config": { "token": "ghp_xxx" } }
  ]
}
```

Run `dripline tables` to see what's available and which WHERE clauses are required.

## 6. Query

```bash
dripline query "SELECT name, stargazers_count FROM github_repos WHERE owner = 'Michaelliv' ORDER BY stargazers_count DESC LIMIT 10"
```

For interactive exploration:

```bash
dripline repl
```

## 7. (Optional) Persist data with sync

Ephemeral queries re-fetch every time. For dashboards you want persistence:

```bash
dripline sync github_issues --param repo=Michaelliv/dripline --database ./analytics.duckdb --schema workspace_1
```

After that, plain DuckDB reads hit the synced data, and incremental cursors keep it fresh.

## 8. Fire up dripyard

Dripyard is the dashboard + worker layer on top of your dripline workspace.

On localhost, an open server is fine:

```bash
dripyard serve
```

For anything reachable from outside your machine, set `DRIPYARD_TOKEN` first — it's a shared bearer token that gates every request:

```bash
export DRIPYARD_TOKEN="$(openssl rand -base64 32)"
dripyard serve
```

Browsers hitting the URL get redirected to `/login`; paste the token once and it lands in an HTTP-only cookie. API clients (CLI, curl, anything scripted) send `Authorization: Bearer <token>`. `/health` stays open so platform probes work.

The UI is served directly from the Bun process — no separate dev server.

## 9. Deploy

Single Bun process. Mount `.dripyard/` as a persistent volume and set `DRIPYARD_DB=/app/.dripyard/dripyard.db` so lane runs, worker telemetry, and job state survive container restarts — without that env var dripyard keeps state in memory and every redeploy wipes the dashboard back to zero.

See [Deploying dripyard](./dripyard/deploy.md) for a Dockerfile + generic deployment recipe.

## 10. Release a new version (maintainers)

```bash
# bump BOTH packages/dripline/package.json and packages/dripyard/package.json to the same version
git commit -am "chore: release vX.Y.Z"
git tag vX.Y.Z
git push origin main --tags
```

CI (`.github/workflows/release.yml`) tests both packages, then publishes `dripline` and `dripyard` to npm using the `NPM_TOKEN` GitHub secret. The token needs to be a **classic automation token** (granular tokens can't create the first version on a new package name). Tags containing `rc`, `alpha`, or `beta` publish under `--tag next` instead of `latest`.

Plugins aren't published — consumers always pin to a git ref.

Dripline and dripyard ship **lockstep**: identical version numbers on every `v*` tag. No compatibility matrix, just match the two.
