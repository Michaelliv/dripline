# dripline-plugin-github

GitHub plugin for [dripline](https://github.com/Michaelliv/dripline) — query repos, issues, PRs, and stargazers with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/github
```

## Setup

```bash
dripline connection add gh --plugin github --set token=ghp_xxx
# or
export GITHUB_TOKEN=ghp_xxx
```

## Tables

| Table | Required WHERE | Description |
|-------|---------------|-------------|
| `github_repos` | `owner` | Repositories |
| `github_issues` | `owner`, `repo` | Issues |
| `github_pull_requests` | `owner`, `repo` | Pull requests |
| `github_stargazers` | `owner`, `repo` | Stargazers |

## Examples

### List repos by stars

```sql
SELECT name, stargazers_count, language
FROM github_repos
WHERE owner = 'torvalds'
ORDER BY stargazers_count DESC
LIMIT 5;
```

### Open issues for a repo

```sql
SELECT number, title, state, user_login
FROM github_issues
WHERE owner = 'torvalds' AND repo = 'linux'
LIMIT 10;
```

### Recent pull requests

```sql
SELECT number, title, user_login, head_ref, base_ref
FROM github_pull_requests
WHERE owner = 'torvalds' AND repo = 'linux'
LIMIT 5;
```

### Count issues per repo

```sql
SELECT r.name, COUNT(i.id) as open_issues
FROM github_repos r
JOIN github_issues i ON r.name = i.repo
WHERE r.owner = 'facebook' AND i.owner = 'facebook'
GROUP BY r.name
ORDER BY open_issues DESC;
```
