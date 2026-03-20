# dripline

Query APIs using SQL. One drip at a time.

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

Add auth for higher rate limits:

```bash
dripline connection add gh --plugin github --prompt token
```

Or use environment variables (no config needed):

```bash
export GITHUB_TOKEN=ghp_xxx
```

Start the interactive shell:

```bash
dripline
```

## How It Works

Plugins define tables backed by API calls. dripline registers them as SQLite virtual tables via `better-sqlite3`. You write SQL — SQLite handles filtering, joins, aggregation. Key columns (like `owner`, `repo`) are pushed down to the API as parameters.

```
SQL query → SQLite virtual table → sync generator → API call → yield rows
```

## Commands

```bash
dripline                              # Interactive REPL
dripline query "<sql>"                # Execute a query (alias: dripline q)
dripline init                         # Create .dripline/ directory
dripline onboard                      # Add instructions to CLAUDE.md
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

### GitHub (built-in)

| Table | Required WHERE | Description |
|-------|---------------|-------------|
| `github_repos` | `owner` | Repositories |
| `github_issues` | `owner`, `repo` | Issues |
| `github_pull_requests` | `owner`, `repo` | Pull requests |
| `github_stargazers` | `owner`, `repo` | Stargazers |

```sql
SELECT r.name, COUNT(i.id) as issues
FROM github_repos r
JOIN github_issues i ON r.name = i.repo
WHERE r.owner = 'Michaelliv' AND i.owner = 'Michaelliv'
GROUP BY r.name;
```

### Installing plugins

```bash
dripline plugin install npm:@dripline/aws
dripline plugin install git:github.com/user/repo
dripline plugin install ./my-plugin.ts
```

Plugins auto-discover from `.dripline/plugins/` (project) and `~/.dripline/plugins/` (global).

### Writing a plugin

```typescript
import type { DriplinePluginAPI } from "dripline";

export default function(dl: DriplinePluginAPI) {
  dl.setName("my-api");
  dl.setVersion("1.0.0");
  dl.setConnectionSchema({
    token: { type: "string", required: true, description: "API token" },
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

Plugins are sync generators — `list` yields rows, HTTP calls use `execFileSync("curl", ...)`. Key columns become hidden WHERE parameters pushed down to the API.

## SDK

Use dripline as a library:

```typescript
import { Dripline, githubPlugin } from "dripline";

const dl = new Dripline({
  plugins: [githubPlugin],
  connections: [{ name: "gh", plugin: "github", config: { token: "ghp_xxx" } }],
});

const repos = dl.query<{ name: string; stars: number }>(
  "SELECT name, stargazers_count as stars FROM github_repos WHERE owner = 'torvalds' ORDER BY stars DESC LIMIT 5"
);

dl.close();
```

## Configuration

Connections are stored in `.dripline/config.json`. Manage them with the CLI:

```bash
dripline connection add gh --plugin github --prompt token
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

Every command supports `--json`. Run `dripline onboard` to add usage instructions to CLAUDE.md.

## Development

```bash
npm install
npm run dev -- query "SELECT 1"
npm test
npm run check
```

## License

MIT
