# Quickstart

Empty directory → querying APIs → dripyard deployed. Ten minutes if the wind is right.

## 1. Prerequisites

You need [Bun](https://bun.sh) 1.1+ on PATH. Node works for running the CLIs, but the build tooling is Bun-native.

```bash
bun --version   # should print a version
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

Dripyard is the dashboard + worker layer on top of your dripline workspace:

```bash
dripyard serve
```

Open the URL it prints. The UI is served directly from the Bun process — no separate dev server.

## 9. Deploy

Single Bun process, no database required beyond DuckDB files in `.dripyard/`. See [Deploying dripyard](./dripyard/deploy.md) for a Dockerfile + generic deployment recipe.

## 10. Release a new version (maintainers)

```bash
git tag v0.8.0
git push --tags
```

CI tests, publishes `dripline` + `dripyard` to npm, cuts a GitHub release. Plugins aren't published — consumers always pin to a git ref.
