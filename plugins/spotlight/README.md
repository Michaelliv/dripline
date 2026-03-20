# dripline-plugin-spotlight

macOS Spotlight plugin for [dripline](https://github.com/Michaelliv/dripline) — search files, apps, and recent documents with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#plugins/spotlight
```

macOS only. Uses `mdfind` (Spotlight).

## Tables

| Table | Description |
|-------|-------------|
| `spotlight_search` | Search by text query or filename glob |
| `spotlight_apps` | All installed applications |
| `spotlight_recent` | Recently modified files |

## Examples

### Search by filename

```sql
SELECT name, path FROM spotlight_search
WHERE filename = '*.ts' AND dir = '/Users/me/Projects/my-app/src'
LIMIT 10;
```

```
┌──────────────┬──────────────────────────────────────────────┐
│ name         │ path                                         │
├──────────────┼──────────────────────────────────────────────┤
│ index.ts     │ /Users/me/Projects/my-app/src/index.ts       │
│ engine.ts    │ /Users/me/Projects/my-app/src/engine.ts      │
│ cache.ts     │ /Users/me/Projects/my-app/src/cache.ts       │
│ sdk.ts       │ /Users/me/Projects/my-app/src/sdk.ts         │
└──────────────┴──────────────────────────────────────────────┘
```

### Text search

```sql
SELECT name, path FROM spotlight_search WHERE query = 'TODO refactor' LIMIT 5;
```

### Installed apps

```sql
SELECT name, path FROM spotlight_apps WHERE path LIKE '/Applications/%' ORDER BY name LIMIT 10;
```

### Recently modified files

```sql
SELECT name, path, modified_at FROM spotlight_recent
WHERE dir = '/Users/me/Projects' AND days = '3'
ORDER BY modified_at DESC
LIMIT 10;
```

### Recent files by kind

```sql
SELECT name, path FROM spotlight_recent WHERE kind = 'markdown' AND days = '7' LIMIT 10;
```

Supported kinds: `pdf`, `image`, `text`, `markdown`, `typescript`, `javascript`, `json`, `python` (or any UTI like `com.adobe.pdf`).
