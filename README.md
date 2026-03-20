# dripline 💧

Query anything, one drip at a time.

## Install

```bash
npm install -g dripline
```

## Quick Start

```bash
dripline init

dripline query "SELECT name, stargazers_count, language
  FROM github_repos
  WHERE owner = 'torvalds'
  ORDER BY stargazers_count DESC
  LIMIT 10"
```

Authenticate:

```bash
dripline connection add gh --plugin github --set token=ghp_xxx

# or via environment variable
export GITHUB_TOKEN=ghp_xxx
```

Start the interactive shell:

```bash
dripline
```

## How It Works

Plugins define tables backed by API calls. dripline materializes API data into DuckDB and runs your SQL against it. Key columns (like `owner`, `repo`) are pushed down to the API as parameters.

```
SQL query > DuckDB > plugin (sync generator) > API call > yield rows > materialize > query
```

## Commands

```bash
dripline                              # Interactive REPL
dripline query "<sql>"                # Execute a query (alias: dripline q)
dripline tables                       # List all tables and schemas (--json for full schema)
dripline init                         # Create .dripline/ directory
dripline connection add <name>        # Add a connection (--plugin, --set key=val)
dripline connection list              # List connections
dripline connection remove <name>     # Remove a connection
dripline plugin list                  # List all plugins
dripline plugin install <source>      # Install from npm/git/local
dripline plugin remove <name>         # Uninstall a plugin
```

### Query options

| Flag | Description |
|------|-------------|
| `-o, --output <format>` | `table` (default), `json`, `csv`, `line` |
| `--json` | Same as `-o json` |
| `-q, --quiet` | Suppress timing output |

### REPL commands

| Command | Description |
|---------|-------------|
| `.tables` | List all available tables |
| `.inspect <table>` | Show columns and key columns |
| `.connections` | List configured connections |
| `.output <format>` | Change output format |
| `.help` | Show help |
| `.quit` | Exit |

## Plugins

dripline ships with no built-in plugins. Install what you need:

```bash
dripline plugin install git:github.com/Michaelliv/dripline#plugins/github
dripline plugin install git:github.com/Michaelliv/dripline#plugins/docker
```

### Official Plugins

All plugins install via `dripline plugin install git:github.com/Michaelliv/dripline#plugins/<name>`.

| Plugin | Tables | Source |
|--------|--------|--------|
| **github** | `github_repos`, `github_issues`, `github_pull_requests`, `github_stargazers` | GitHub API |
| **docker** | `docker_containers`, `docker_images`, `docker_volumes`, `docker_networks` | Docker CLI |
| **brew** | `brew_formulae`, `brew_casks`, `brew_outdated`, `brew_services` | Homebrew |
| **ps** | `ps_processes`, `ps_ports` | ps, lsof |
| **git** | `git_commits`, `git_branches`, `git_tags`, `git_remotes`, `git_status` | Git CLI |
| **system-profiler** | `sys_software`, `sys_hardware`, `sys_network_interfaces`, `sys_storage`, `sys_displays` | macOS system_profiler |
| **pi** | `pi_sessions`, `pi_messages`, `pi_tool_calls`, `pi_costs`, `pi_prompt`, `pi_generate` | pi session files + pi CLI |
| **kubectl** | `k8s_pods`, `k8s_services`, `k8s_deployments`, `k8s_nodes`, `k8s_namespaces`, `k8s_configmaps`, `k8s_secrets`, `k8s_ingresses` | kubectl |
| **npm** | `npm_packages`, `npm_outdated`, `npm_global`, `npm_scripts` | npm CLI |
| **spotlight** | `spotlight_search`, `spotlight_apps`, `spotlight_recent` | macOS Spotlight |
| **skills-sh** | `skills_search` | skills.sh API |
| **cloudflare** | `cf_workers`, `cf_zones`, `cf_dns_records`, `cf_pages_projects`, `cf_pages_deployments`, `cf_d1_databases`, `cf_kv_namespaces`, `cf_r2_buckets`, `cf_queues`, `cf_dns_lookup`, `cf_domain_check` | Cloudflare API |
| **vercel** | `vercel_projects`, `vercel_deployments`, `vercel_domains`, `vercel_env_vars` | Vercel API |

```sql
-- GitHub: top repos by stars
SELECT name, stargazers_count, language
FROM github_repos
WHERE owner = 'torvalds'
ORDER BY stargazers_count DESC LIMIT 5;

-- Docker: running containers
SELECT name, image, state FROM docker_containers;

-- Join across plugins
SELECT r.name, COUNT(i.id) as issues
FROM github_repos r
JOIN github_issues i ON r.name = i.repo
WHERE r.owner = 'Michaelliv' AND i.owner = 'Michaelliv'
GROUP BY r.name;
```

### Mix APIs with anything

Query APIs, local files, remote files, and databases in the same SQL:

```sql
-- Join API data with a local CSV
SELECT r.name, r.stargazers_count, s.revenue
FROM github_repos r
JOIN read_csv_auto('./revenue.csv') s ON r.name = s.repo
WHERE r.owner = 'torvalds'
ORDER BY s.revenue DESC;

-- Enrich a Parquet file on S3 with live API data
SELECT p.user_id, p.event, g.login, g.starred_at
FROM read_parquet('s3://bucket/events.parquet') p
JOIN github_stargazers g ON p.github_user = g.login
WHERE g.owner = 'facebook' AND g.repo = 'react';

-- Query a JSON API directly (no plugin needed)
SELECT login, type
FROM read_json_auto('https://api.github.com/orgs/vercel/members');

-- Window functions on API data
SELECT name, stargazers_count,
  RANK() OVER (ORDER BY stargazers_count DESC) as rank,
  ROUND(stargazers_count * 100.0 / SUM(stargazers_count) OVER (), 1) as pct
FROM github_repos
WHERE owner = 'torvalds' AND stargazers_count > 0;
```

### Installing plugins

```bash
dripline plugin install git:github.com/Michaelliv/dripline#plugins/brew
dripline plugin install git:github.com/user/their-plugin
dripline plugin install ./my-plugin.ts
```

Plugins auto-discover from `.dripline/plugins/` (project) and `~/.dripline/plugins/` (global).

### Writing a plugin

Plugins can wrap **APIs** (using `syncGet`) or **local CLIs** (using `syncExec`):

```typescript
// API plugin
import type { DriplinePluginAPI } from "dripline";
import { syncGetPaginated } from "dripline";

export default function(dl: DriplinePluginAPI) {
  dl.setName("my-api");
  dl.setVersion("1.0.0");
  dl.setConnectionSchema({
    token: { type: "string", required: true, description: "API token", env: "MY_API_TOKEN" },
  });

  dl.registerTable("my_items", {
    columns: [
      { name: "id", type: "number" },
      { name: "title", type: "string" },
    ],
    keyColumns: [
      { name: "project", required: "required" },
    ],
    *list(ctx) {
      const project = ctx.quals.find(q => q.column === "project")?.value;
      if (!project) return;
      const headers = { Authorization: `Bearer ${ctx.connection.config.token}` };
      const data = syncGetPaginated(`https://api.example.com/${project}/items`, headers);
      for (const item of data) {
        yield { id: item.id, title: item.title };
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
  dl.setName("my-cli");
  dl.setVersion("1.0.0");

  dl.registerTable("my_processes", {
    columns: [
      { name: "name", type: "string" },
      { name: "cpu", type: "number" },
    ],
    *list() {
      const { rows } = syncExec("my-tool", ["list", "--json"], { parser: "json" });
      for (const r of rows) {
        yield { name: r.name, cpu: r.cpu };
      }
    },
  });
}
```

Plugins are sync generators. `list` yields rows. Key columns are extracted from WHERE clauses and passed to the plugin. DuckDB handles the rest (joins, window functions, aggregation).

#### `syncExec` parsers

| Parser | Description |
|--------|-------------|
| `json` | Parse stdout as JSON (array or object) |
| `jsonlines` | One JSON object per line |
| `csv` | Comma-separated with headers |
| `tsv` | Tab-separated with headers |
| `lines` | Each line as `{ line_number, line }` |
| `kv` | Key-value pairs (`key=value` per line) |
| `raw` | Raw string as `{ output }` |

## SDK

Use dripline as a library:

```typescript
import { Dripline } from "dripline";
import githubPlugin from "dripline-plugin-github";

const dl = await Dripline.create({
  plugins: [githubPlugin],
  connections: [{ name: "gh", plugin: "github", config: { token: "ghp_xxx" } }],
});

const repos = await dl.query<{ name: string; stars: number }>(
  "SELECT name, stargazers_count as stars FROM github_repos WHERE owner = 'torvalds' ORDER BY stars DESC LIMIT 5"
);

await dl.close();
```

## Configuration

Connections are stored in `.dripline/config.json`. Manage them with the CLI:

```bash
dripline connection add gh --plugin github --set token=ghp_xxx
dripline connection list
dripline connection remove gh
```

Env vars override config. Each plugin declares its own env var names (e.g. `GITHUB_TOKEN`).

Full config format:

```json
{
  "connections": [
    { "name": "gh", "plugin": "github", "config": { "token": "ghp_xxx" } }
  ],
  "cache": { "enabled": true, "ttl": 300, "maxSize": 1000 },
  "rateLimits": { "github": { "maxPerSecond": 5 } }
}
```

## For Agents

Every command supports `--json`. Use `dripline tables --json` to get full table schemas.

A [pi](https://github.com/badlogic/pi-mono) extension is included at `.pi/extensions/pi-dripline-context/` that automatically injects available tables into the agent's context on session start.

## Development

```bash
npm install
npm run dev -- query "SELECT 1"
npm test
npm run check
```

## License

MIT
