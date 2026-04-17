# dripline-plugin-brew

Homebrew plugin for [dripline](https://github.com/Michaelliv/dripline) — query installed formulae, casks, outdated packages, and services with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/brew
```

Requires `brew` on PATH.

## Tables

| Table | Description |
|-------|-------------|
| `brew_formulae` | Installed formulae |
| `brew_casks` | Installed casks |
| `brew_outdated` | Outdated formulae and casks |
| `brew_services` | Homebrew services |

## Examples

### Installed formulae

```sql
SELECT name, version, description FROM brew_formulae ORDER BY name LIMIT 5;
```

```
┌──────────┬─────────┬──────────────────────────────────────────────┐
│ name     │ version │ description                                  │
├──────────┼─────────┼──────────────────────────────────────────────┤
│ curl     │ 8.7.1   │ Get a file from an HTTP, HTTPS or FTP server │
│ git      │ 2.44.0  │ Distributed revision control system          │
│ jq       │ 1.7.1   │ Lightweight command-line JSON processor      │
│ node     │ 22.3.0  │ Platform built on V8 to build network apps   │
│ ripgrep  │ 14.1.0  │ Search tool like grep and The Silver Searcher │
└──────────┴─────────┴──────────────────────────────────────────────┘
```

### Outdated packages

```sql
SELECT name, installed_version, current_version FROM brew_outdated LIMIT 5;
```

```
┌─────────┬───────────────────┬─────────────────┐
│ name    │ installed_version │ current_version │
├─────────┼───────────────────┼─────────────────┤
│ curl    │ 8.7.1             │ 8.8.0           │
│ ffmpeg  │ 7.0               │ 7.0.1           │
│ node    │ 22.3.0            │ 22.4.0          │
│ python  │ 3.12.3            │ 3.12.4          │
│ ripgrep │ 14.1.0            │ 14.1.1          │
└─────────┴───────────────────┴─────────────────┘
```

### Installed casks

```sql
SELECT name, version, installed_version FROM brew_casks;
```

```
┌─────────────────┬─────────┬───────────────────┐
│ name            │ version │ installed_version │
├─────────────────┼─────────┼───────────────────┤
│ docker          │ 4.30.0  │ 4.29.0            │
│ firefox         │ 126.0   │ 126.0             │
│ visual-studio-… │ 1.90.0  │ 1.89.1            │
│ warp            │ 2024.06 │ 2024.05           │
└─────────────────┴─────────┴───────────────────┘
```

### Count by tap

```sql
SELECT tap, COUNT(*) as count
FROM brew_formulae
GROUP BY tap
ORDER BY count DESC;
```
