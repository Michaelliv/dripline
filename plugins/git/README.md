# dripline-plugin-git

Git plugin for [dripline](https://github.com/Michaelliv/dripline) — query commits, branches, tags, remotes, and working tree status with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#plugins/git
```

Requires `git` on PATH. Queries the repo in the current directory by default, or pass `repo = '/path/to/repo'` in WHERE.

## Tables

| Table | Description |
|-------|-------------|
| `git_commits` | Commit log (default: last 500) |
| `git_branches` | Local and remote branches |
| `git_tags` | Tags sorted by date |
| `git_remotes` | Configured remotes |
| `git_status` | Working tree status (modified/staged/untracked) |

## Examples

### Recent commits

```sql
SELECT short_hash, author, date, subject FROM git_commits LIMIT 5;
```

```
┌────────────┬──────────────┬───────────────────────────┬──────────────────────────────────────┐
│ short_hash │ author       │ date                      │ subject                              │
├────────────┼──────────────┼───────────────────────────┼──────────────────────────────────────┤
│ a1b2c3d    │ Jane Smith   │ 2026-03-20T15:02:25+00:00 │ feat: add user authentication       │
│ e4f5a6b    │ John Doe     │ 2026-03-20T14:30:00+00:00 │ fix: resolve race condition in cache │
│ c7d8e9f    │ Jane Smith   │ 2026-03-19T18:45:12+00:00 │ refactor: extract query parser       │
│ 1a2b3c4    │ Alex Chen    │ 2026-03-19T16:20:33+00:00 │ docs: update API reference           │
│ 5d6e7f8    │ John Doe     │ 2026-03-19T12:10:05+00:00 │ chore: bump dependencies             │
└────────────┴──────────────┴───────────────────────────┴──────────────────────────────────────┘
```

### Local branches

```sql
SELECT name, is_current, upstream FROM git_branches WHERE is_remote = false;
```

```
┌─────────────────┬────────────┬──────────────────────────┐
│ name            │ is_current │ upstream                 │
├─────────────────┼────────────┼──────────────────────────┤
│ main            │ true       │ origin/main              │
│ feat/auth       │ false      │ origin/feat/auth         │
│ fix/cache-race  │ false      │                          │
└─────────────────┴────────────┴──────────────────────────┘
```

### Uncommitted changes

```sql
SELECT path, status, staged, unstaged FROM git_status;
```

### Commits per author

```sql
SELECT author, COUNT(*) as commits
FROM git_commits
GROUP BY author
ORDER BY commits DESC;
```

### Query a different repo

```sql
SELECT short_hash, subject FROM git_commits WHERE repo = '/path/to/other/repo' LIMIT 5;
```

### Commits by day of week

```sql
SELECT DAYNAME(CAST(date AS TIMESTAMP)) as day, COUNT(*) as commits
FROM git_commits
GROUP BY day
ORDER BY commits DESC;
```
