# dripline

Query anything, one drip at a time.

## Commands

```bash
npm run dev -- query "SELECT 1"
npm run dev -- repl
npm run dev -- --help
npm test
npm run format
npm run lint
npm run build
```

## Architecture

```
SQL query > CLI/SDK > QueryEngine > DuckDB > Plugin (sync generator) > API/CLI
```

### Layers

| Layer | File | Purpose |
|-------|------|---------|
| SDK | `src/sdk.ts`, `src/index.ts` | `Dripline` class, library entrypoint |
| CLI | `src/main.ts` | Commander setup, routes to commands |
| Commands | `src/commands/` | query, repl, tables, init, connection, plugin |
| Engine | `src/core/engine.ts` | DuckDB, table materialization, query execution |
| Cache | `src/core/cache.ts` | In-memory query result cache with TTL |
| Rate Limiter | `src/core/rate-limiter.ts` | Token bucket per-scope |
| Store | `src/core/store.ts` | File-based record storage |
| Plugin API | `src/plugin/api.ts` | `DriplinePluginAPI` interface, `createPluginAPI()` |
| Plugin Registry | `src/plugin/registry.ts` | Plugin/table storage and lookup |
| Plugin Loader | `src/plugin/loader.ts` | Discovery, loading from paths/dirs |
| Plugin Installer | `src/plugin/installer.ts` | npm/git/local install, `plugins.json` |
| Config | `src/config/` | `.dripline/config.json`, env var resolution |
| HTTP | `src/utils/http.ts` | `syncGet`, `syncGetPaginated` (curl-based) |
| CLI exec | `src/utils/cli.ts` | `syncExec`, `commandExists` (for wrapping local CLIs) |
| Formatters | `src/utils/` | Table, JSON, CSV, line output, spinner |

### Plugins (separate packages)

| Package | Dir | Tables |
|---------|-----|--------|
| `dripline-plugin-github` | `plugins/github/` | github_repos, github_issues, github_pull_requests, github_stargazers |
| `dripline-plugin-docker` | `plugins/docker/` | docker_containers, docker_images, docker_volumes, docker_networks |
| `dripline-plugin-brew` | `plugins/brew/` | brew_formulae, brew_casks, brew_outdated, brew_services |
| `dripline-plugin-ps` | `plugins/ps/` | ps_processes, ps_ports |
| `dripline-plugin-git` | `plugins/git/` | git_commits, git_branches, git_tags, git_remotes, git_status |
| `dripline-plugin-system-profiler` | `plugins/system-profiler/` | sys_software, sys_hardware, sys_network_interfaces, sys_storage, sys_displays |
| `dripline-plugin-pi` | `plugins/pi/` | pi_sessions, pi_messages, pi_tool_calls, pi_costs, pi_prompt, pi_generate |
| `dripline-plugin-kubectl` | `plugins/kubectl/` | k8s_pods, k8s_services, k8s_deployments, k8s_nodes, k8s_namespaces, k8s_configmaps, k8s_secrets, k8s_ingresses |
| `dripline-plugin-npm` | `plugins/npm/` | npm_packages, npm_outdated, npm_global, npm_scripts |
| `dripline-plugin-spotlight` | `plugins/spotlight/` | spotlight_search, spotlight_apps, spotlight_recent |
| `dripline-plugin-skills-sh` | `plugins/skills-sh/` | skills_search |
| `dripline-plugin-cloudflare` | `plugins/cloudflare/` | cf_workers, cf_zones, cf_dns_records, cf_pages_projects, cf_pages_deployments, cf_d1_databases, cf_kv_namespaces, cf_r2_buckets, cf_queues, cf_dns_lookup, cf_domain_check |
| `dripline-plugin-vercel` | `plugins/vercel/` | vercel_projects, vercel_deployments, vercel_domains, vercel_env_vars |

Plugins live in `plugins/` as npm workspaces. Core ships with zero plugins.

## Writing a Plugin

Two patterns: **API plugins** use `syncGet`/`syncGetPaginated`, **CLI plugins** use `syncExec`.

```typescript
// API plugin
import type { DriplinePluginAPI } from "dripline";
import { syncGetPaginated } from "dripline";

export default function(dl: DriplinePluginAPI) {
  dl.setName("myplugin");
  dl.setVersion("1.0.0");
  dl.setConnectionSchema({
    api_key: { type: "string", required: true, env: "MY_API_KEY" },
  });

  dl.registerTable("my_table", {
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
    ],
    keyColumns: [
      { name: "org", required: "required" },
    ],
    *list(ctx) {
      const org = ctx.quals.find(q => q.column === "org")?.value;
      if (!org) return;
      const headers = { Authorization: `Bearer ${ctx.connection.config.api_key}` };
      const data = syncGetPaginated(`https://api.example.com/orgs/${org}/items`, headers);
      for (const item of data) {
        yield { id: item.id, name: item.name };
      }
    },
  });
}
```

```typescript
// CLI plugin
import type { DriplinePluginAPI } from "dripline";
import { syncExec } from "dripline";

export default function(dl: DriplinePluginAPI) {
  dl.setName("mycli");
  dl.setVersion("1.0.0");

  dl.registerTable("my_things", {
    columns: [
      { name: "name", type: "string" },
      { name: "status", type: "string" },
    ],
    *list() {
      const { rows } = syncExec("mytool", ["list", "--json"], { parser: "json" });
      for (const r of rows) yield { name: r.name, status: r.status };
    },
  });
}
```

`syncExec` parsers: `json`, `jsonlines`, `csv`, `tsv`, `lines`, `kv`, `raw`.

Plugins are sync generators. Data is materialized into DuckDB temp tables before query execution. Key column values are extracted from WHERE clauses and passed to plugins as quals. Non-key WHERE clauses are filtered by DuckDB after materialization.

## Config

`.dripline/config.json`:
```json
{
  "connections": [
    { "name": "gh", "plugin": "github", "config": { "token": "ghp_xxx" } }
  ],
  "cache": { "enabled": true, "ttl": 300, "maxSize": 1000 },
  "rateLimits": { "github": { "maxPerSecond": 5 } }
}
```

Env vars override config. Plugins declare env var names in `connectionConfigSchema` via the `env` field (e.g. `GITHUB_TOKEN`).

## Adding a New Plugin

1. Create `plugins/<name>/` with `package.json`, `tsconfig.json`, `src/index.ts`
2. Set `"name": "dripline-plugin-<name>"` in package.json with `dripline` as peer dep
3. Export a default function that receives `DriplinePluginAPI`
4. Call `dl.setName()`, `dl.registerTable()`, etc.
5. Install locally: `dripline plugin install ./plugins/<name>/src/index.ts`

## pi Extension

A pi coding agent extension lives at `.pi/extensions/pi-dripline-context/`. On session start, it runs `dripline tables --json`, formats a compact summary of all available tables, and injects it into the agent's context. Shows `💧 N tables` in the TUI status bar.

## Adding a New Command

1. Create `src/commands/<name>.ts`
2. Export an async function
3. Import and register in `src/main.ts`

## Exit Codes

- `0`. success
- `1`. error
- `2`. user error
- `3`. not found
