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
SQL query > CLI/SDK > QueryEngine > DuckDB > Plugin (sync generator) > API
```

### Layers

| Layer | File | Purpose |
|-------|------|---------|
| SDK | `src/sdk.ts`, `src/index.ts` | `Dripline` class, library entrypoint |
| CLI | `src/main.ts` | Commander setup, routes to commands |
| Commands | `src/commands/` | query, repl, init, connection, plugin |
| Engine | `src/engine.ts` | DuckDB, table materialization, query execution |
| Plugin API | `src/plugin/api.ts` | `DriplinePluginAPI` interface, `createPluginAPI()` |
| Plugin Registry | `src/plugin/registry.ts` | Plugin/table storage and lookup |
| Plugin Loader | `src/plugin/loader.ts` | Auto-discovery, loading from paths/dirs |
| Plugin Installer | `src/plugin/installer.ts` | npm/git/local install, `plugins.json` |
| Plugins | `src/plugins/` | GitHub plugin |
| Config | `src/config/` | `.dripline/config.json`, env var resolution |
| Cache | `src/cache.ts` | In-memory query result cache with TTL |
| Rate Limiter | `src/rate-limiter.ts` | Token bucket per-scope |
| HTTP | `src/plugins/utils/http.ts` | `syncGet`, `syncGetPaginated` (curl-based) |
| Formatters | `src/utils/` | Table, JSON, CSV, line output, spinner |

## Writing a Plugin

```typescript
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

Plugins are sync generators. Data is materialized into DuckDB temp tables before query execution. HTTP calls use `execFileSync("curl", ...)` via `syncGet`/`syncGetPaginated`.

Key column values are extracted from WHERE clauses and passed to plugins as quals. Non-key WHERE clauses are filtered by DuckDB after materialization.

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

1. Create `src/plugins/<name>.ts`
2. Export a default function that receives `DriplinePluginAPI`
3. Call `dl.setName()`, `dl.registerTable()`, etc.
4. Auto-loads via `loadBuiltinPlugins()`

## Adding a New Command

1. Create `src/commands/<name>.ts`
2. Export an async function
3. Import and register in `src/main.ts`

## Exit Codes

- `0`. success
- `1`. error
- `2`. user error
- `3`. not found
