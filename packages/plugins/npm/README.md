# dripline-plugin-npm

npm plugin for [dripline](https://github.com/Michaelliv/dripline) — query installed packages, outdated deps, global packages, and scripts with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/npm
```

Requires `npm` on PATH.

## Tables

| Table | Description |
|-------|-------------|
| `npm_packages` | Installed packages in current project |
| `npm_outdated` | Outdated packages |
| `npm_global` | Globally installed packages |
| `npm_scripts` | Scripts from package.json |

Use `WHERE dir = '/path/to/project'` to query a specific project.

## Examples

### Installed packages

```sql
SELECT name, version, type FROM npm_packages LIMIT 5;
```

```
┌───────────┬────────┬───────────────┐
│ name      │ version│ type          │
├───────────┼────────┼───────────────┤
│ chalk     │ 5.6.2  │ dependency    │
│ commander │ 14.0.3 │ dependency    │
│ typescript│ 5.8.0  │ devDependency │
│ tsx       │ 4.21.0 │ devDependency │
│ duckdb    │ 1.4.4  │ dependency    │
└───────────┴────────┴───────────────┘
```

### Outdated packages

```sql
SELECT name, current, wanted, latest FROM npm_outdated;
```

### npm scripts

```sql
SELECT name, command FROM npm_scripts;
```

```
┌───────┬──────────────────────────────────────────┐
│ name  │ command                                  │
├───────┼──────────────────────────────────────────┤
│ build │ tsc && npm run build --workspaces        │
│ test  │ npx tsx --test src/tests/**/*.test.ts    │
│ dev   │ npx tsx src/main.ts                      │
│ lint  │ npx @biomejs/biome lint src/             │
└───────┴──────────────────────────────────────────┘
```

### Global packages

```sql
SELECT name, version FROM npm_global ORDER BY name;
```

### Count deps vs devDeps

```sql
SELECT type, COUNT(*) as count FROM npm_packages GROUP BY type;
```
