# dripline-plugin-vercel

Vercel plugin for [dripline](https://github.com/Michaelliv/dripline) — query projects, deployments, domains, and environment variables with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#plugins/vercel
```

Auth is auto-detected from `vercel login` or `VERCEL_TOKEN` env var.

## Tables

| Table | Description |
|-------|-------------|
| `vercel_projects` | All projects with framework and node version |
| `vercel_deployments` | Deployments with git info (optionally filter by project_name) |
| `vercel_domains` | Custom domains |
| `vercel_env_vars` | Environment variables for a project (WHERE project_name = '...') |

## Examples

### Projects

```sql
SELECT name, framework, node_version, created_at FROM vercel_projects;
```

```
┌──────────┬───────────┬──────────────┬──────────────────────────┐
│ name     │ framework │ node_version │ created_at               │
├──────────┼───────────┼──────────────┼──────────────────────────┤
│ my-blog  │ astro     │ 22.x         │ 2025-11-27T22:07:03.051Z │
│ my-app   │ nextjs    │ 22.x         │ 2026-02-23T17:58:11.163Z │
└──────────┴───────────┴──────────────┴──────────────────────────┘
```

### Recent deployments

```sql
SELECT name, state, target, git_branch, git_commit_message
FROM vercel_deployments
WHERE project_name = 'my-blog'
LIMIT 5;
```

```
┌─────────┬───────┬────────────┬────────────┬──────────────────────────────┐
│ name    │ state │ target     │ git_branch │ git_commit_message           │
├─────────┼───────┼────────────┼────────────┼──────────────────────────────┤
│ my-blog │ READY │ production │ main       │ feat: add dark mode support  │
│ my-blog │ READY │ production │ main       │ fix: mobile nav overflow     │
│ my-blog │ READY │ preview    │ feat/auth  │ wip: auth flow               │
└─────────┴───────┴────────────┴────────────┴──────────────────────────────┘
```

### Environment variables

```sql
SELECT key, target, type FROM vercel_env_vars WHERE project_name = 'my-app';
```

### Domains

```sql
SELECT name, verified, expires_at FROM vercel_domains;
```

### Deployment frequency

```sql
SELECT name, COUNT(*) as deploys, MIN(created_at) as first, MAX(created_at) as last
FROM vercel_deployments
GROUP BY name;
```
