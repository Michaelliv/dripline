# dripline

Query cloud APIs using SQL. One drip at a time.

## Commands

```bash
npm run dev -- query "SELECT 1"   # Run a query in dev mode
npm run dev -- repl               # Start interactive REPL
npm run dev -- --help             # Show help
npm test                          # Run tests
npm run format                    # Format with Biome
npm run lint                      # Lint with Biome
npm run build                     # Compile TypeScript (for npm)
```

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  User: dripline query "SELECT * FROM github_repos   │
│         WHERE owner = 'torvalds'"                   │
└──────────────┬──────────────────────────────────────┘
               │
       ┌───────▼────────┐
       │  CLI (Commander)│  src/main.ts
       └───────┬─────────┘
               │
       ┌───────▼──────────────┐
       │  Query Engine         │  src/engine.ts
       │  (better-sqlite3)     │
       │  ┌─────────────────┐  │
       │  │ Virtual Tables  │  │  db.table() registers plugins
       │  └────────┬────────┘  │
       │  ┌────────▼────────┐  │
       │  │ Cache + Rate    │  │  src/cache.ts, src/rate-limiter.ts
       │  │ Limiter         │  │
       │  └────────┬────────┘  │
       └───────────┼───────────┘
               │
       ┌───────▼──────────────┐
       │  Plugin (sync gen)    │  src/plugins/github.ts
       │  list(ctx) → yield {} │
       └───────┬──────────────┘
               │ execSync curl
       ┌───────▼──────────────┐
       │  Cloud API            │
       └──────────────────────┘
```

### Key Design Decision: Sync Generators

better-sqlite3's virtual table API requires **sync** generators. Plugins use `execFileSync("curl", ...)` for HTTP calls. This is intentional — SQLite drives the execution synchronously.

### Layers

| Layer | File | Purpose |
|-------|------|---------|
| CLI | `src/main.ts` | Commander setup, routes to commands |
| Commands | `src/commands/` | query, repl, init, onboard |
| Engine | `src/engine.ts` | Creates SQLite DB, registers virtual tables, runs queries |
| Plugin SDK | `src/plugin/` | Types, registry, loader |
| Plugins | `src/plugins/` | GitHub (and future plugins) |
| Config | `src/config/` | Connection config, .dripline/config.json |
| Cache | `src/cache.ts` | In-memory query result cache |
| Rate Limiter | `src/rate-limiter.ts` | Token bucket per-scope |
| Formatters | `src/utils/` | Table, JSON, CSV, line output |

## Plugin SDK

### Creating a Plugin

```typescript
import type { PluginDef } from "../plugin/types.js";
import { syncGetPaginated } from "./utils/http.js";

const myPlugin: PluginDef = {
  name: "myplugin",
  version: "0.1.0",
  connectionConfigSchema: {
    api_key: { type: "string", required: true, description: "API key" },
  },
  tables: [{
    name: "my_table",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
    ],
    keyColumns: [
      { name: "org", required: "required", operators: ["="] },
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
    get(ctx) {
      // Optional: single-item lookup when all key cols provided
      return null;
    },
  }],
};

export default myPlugin;
```

### Key Types

- `ColumnDef` — `{ name, type: 'string'|'number'|'boolean'|'json'|'datetime' }`
- `KeyColumn` — `{ name, required: 'required'|'optional', operators: ['='] }` — becomes a hidden WHERE parameter
- `ListFunc` — sync generator: `(ctx: QueryContext) => Generator<Record<string, any>>`
- `GetFunc` — sync: `(ctx: QueryContext) => Record<string, any> | null`
- `QueryContext` — `{ connection, quals, columns, limit? }`

### How Virtual Tables Work

1. Engine registers each plugin table via `db.table(name, { columns, parameters, *rows(...) })`
2. Key columns become `parameters` — values are pushed down from WHERE clauses
3. Non-key WHERE clauses are filtered by SQLite after data is fetched
4. The `*rows()` generator: checks cache → rate limit → get/list → hydrate → cache → yield

## Config

`.dripline/config.json`:
```json
{
  "connections": [
    { "name": "my_github", "plugin": "github", "config": { "token": "ghp_xxx" } }
  ],
  "cache": { "enabled": true, "ttl": 300, "maxSize": 1000 },
  "rateLimits": { "github": { "maxPerSecond": 5 } }
}
```

## Adding a New Plugin

1. Create `src/plugins/<name>.ts`
2. Define tables with columns, key columns, list/get generators
3. Default export the `PluginDef`
4. It auto-loads via `loadBuiltinPlugins()`

## Adding a New Command

1. Create `src/commands/<name>.ts`
2. Export an async function
3. Import and register in `src/main.ts` with Commander

## Exit Codes

- `0` — success
- `1` — error
- `2` — user error (bad input)
- `3` — not found (.dripline/ missing)
