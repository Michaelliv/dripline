# Writing your own plugin

A dripline plugin is a single TypeScript file that exports a function. That function receives a `DriplinePluginAPI` and registers tables. No base class, no decorators, no build manifest.

## Minimum viable plugin

```typescript
// my-plugin.ts
import type { DriplinePluginAPI } from "dripline";

export default function (dl: DriplinePluginAPI) {
  dl.setName("hello");
  dl.setVersion("0.1.0");

  dl.registerTable("hello_world", {
    columns: [
      { name: "greeting", type: "string" },
      { name: "n", type: "number" },
    ],
    *list() {
      yield { greeting: "hi", n: 1 };
      yield { greeting: "hello", n: 2 };
    },
  });
}
```

Install and query it:

```bash
dripline plugin install ./my-plugin.ts
dripline query "SELECT * FROM hello_world"
```

That's a working plugin.

## The three shapes of `list()`

A table's `list` is a generator (sync or async) that yields one row per `yield`. Dripline materializes the yielded rows into a DuckDB temp table, then runs the SQL query against it.

**Sync generator** — for blocking HTTP (curl-based) or CLI wrapping:

```typescript
*list(ctx) {
  const rows = syncGetPaginated("https://api.example.com/items", headers);
  for (const r of rows) yield r;
}
```

**Async generator** — for `fetch`-based HTTP, non-blocking:

```typescript
async *list(ctx) {
  const rows = await asyncGetPaginated("https://api.example.com/items", headers);
  for (const r of rows) yield r;
}
```

**CLI wrapping** — for local tools that emit JSON/CSV/lines:

```typescript
*list() {
  const { rows } = syncExec("mytool", ["list", "--json"], { parser: "json" });
  for (const r of rows) yield r;
}
```

Dripline handles both sync and async generators transparently — pick based on your data source.

## Connection config

If your plugin talks to an authenticated API, declare a connection schema:

```typescript
dl.setConnectionSchema({
  api_key: {
    type: "string",
    required: true,
    description: "My Service API key",
    env: "MYSERVICE_API_KEY",
  },
});
```

The user either exports `MYSERVICE_API_KEY` or adds a `connections[]` entry in `.dripline/config.json`. The value arrives at query time as `ctx.connection.config.api_key`.

## Key columns — required WHERE clauses

Many APIs require a scope (an org, a repo, a channel id) before they'll return anything. Declare key columns so the SQL engine enforces them:

```typescript
dl.registerTable("my_items", {
  columns: [
    { name: "id", type: "number" },
    { name: "name", type: "string" },
  ],
  keyColumns: [
    { name: "org", required: "required" },
    { name: "status", required: "optional" },
  ],
  *list(ctx) {
    const org = ctx.quals.find(q => q.column === "org")?.value;
    if (!org) return;
    // ... fetch /orgs/${org}/items ...
  },
});
```

A user who writes `SELECT * FROM my_items` without `WHERE org = '...'` gets a clear error. Key columns with `required: "optional"` are passed through when present, ignored when absent. Non-key WHERE clauses are filtered by DuckDB after materialization — you don't need to implement server-side filtering unless you want to.

## Incremental sync

For tables that grow over time, add a cursor:

```typescript
dl.registerTable("events", {
  columns: [
    { name: "id", type: "number" },
    { name: "data", type: "json" },
    { name: "updated_at", type: "datetime" },
  ],
  primaryKey: ["id"],           // dedup key
  cursor: "updated_at",         // high-water mark
  async *list(ctx) {
    const since = ctx.cursor?.value ?? "1970-01-01T00:00:00Z";
    const rows = await asyncGetPaginated(
      `https://api.example.com/events?since=${since}`,
      headers,
    );
    for (const e of rows) yield e;
  },
});
```

`dripline sync events` will now persist to an external DuckDB and only fetch rows newer than the last run. Even if your plugin yields stale rows, the engine filters them out. Cursors are scoped per params, so `org=a` and `org=b` keep independent high-water marks.

## Utilities dripline exports

```typescript
import {
  syncGet, syncGetPaginated,       // blocking HTTP via curl
  asyncGet, asyncGetPaginated,     // non-blocking HTTP via fetch
  syncExec, commandExists,         // shelling out to CLIs
} from "dripline";
```

`syncExec` parsers: `json`, `jsonlines`, `csv`, `tsv`, `lines`, `kv`, `raw`.

## Full example: SaaS API with pagination, auth, key columns

```typescript
import type { DriplinePluginAPI, QueryContext } from "dripline";
import { asyncGetPaginated } from "dripline";

export default function (dl: DriplinePluginAPI) {
  dl.setName("acme");
  dl.setVersion("0.1.0");

  dl.setConnectionSchema({
    api_key: {
      type: "string",
      required: true,
      description: "Acme API key",
      env: "ACME_API_KEY",
    },
  });

  dl.registerTable("acme_projects", {
    description: "Projects in an Acme org",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [{ name: "org", required: "required" }],
    async *list(ctx: QueryContext) {
      const org = ctx.quals.find((q) => q.column === "org")?.value;
      if (!org) return;
      const headers = {
        Authorization: `Bearer ${ctx.connection.config.api_key}`,
      };
      const data = await asyncGetPaginated(
        `https://api.acme.com/orgs/${org}/projects`,
        headers,
      );
      for (const p of data) {
        yield { id: p.id, name: p.name, created_at: p.created_at };
      }
    },
  });
}
```

## Packaging and distribution

Dev: point `dripline plugin install` at the source file. Edits hot-reload on next run.

For others to install: commit to a git repo and tell them to run

```bash
dripline plugin install git:github.com/you/your-plugin
```

That's it. No npm publish needed. If the repo contains multiple plugins, use `#subpath` to pick one (see [Installing plugins](./install.md)).

## Inspiration

Read the plugins in [`packages/plugins/`](../../packages/plugins/) — 58 of them covering most patterns you're likely to hit. `github` is a good model for paginated REST APIs, `docker` for CLI wrapping, `pi` for reading from a local DuckDB/SQLite.
