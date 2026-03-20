# dripline-plugin-docker

Docker plugin for [dripline](https://github.com/Michaelliv/dripline) — query containers, images, volumes, and networks with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#plugins/docker
```

Requires `docker` on PATH.

## Tables

| Table | Description |
|-------|-------------|
| `docker_containers` | Running containers (use `show_all = 'true'` for stopped too) |
| `docker_images` | Local images |
| `docker_volumes` | Volumes |
| `docker_networks` | Networks |

## Examples

### Running containers

```sql
SELECT name, image, state, status FROM docker_containers;
```

```
┌───────────────┬──────────────────┬─────────┬───────────┐
│ name          │ image            │ state   │ status    │
├───────────────┼──────────────────┼─────────┼───────────┤
│ my-app        │ node:20-alpine   │ running │ Up 3 days │
│ my-postgres   │ postgres:16      │ running │ Up 3 days │
│ redis-cache   │ redis:7          │ running │ Up 1 hour │
└───────────────┴──────────────────┴─────────┴───────────┘
```

### Images by size

```sql
SELECT repository, tag, size FROM docker_images WHERE repository != '<none>';
```

```
┌──────────────┬────────────┬───────┐
│ repository   │ tag        │ size  │
├──────────────┼────────────┼───────┤
│ node         │ 20-alpine  │ 180MB │
│ postgres     │ 16         │ 430MB │
│ redis        │ 7          │ 140MB │
│ nginx        │ latest     │ 190MB │
└──────────────┴────────────┴───────┘
```

### Networks

```sql
SELECT name, driver, scope FROM docker_networks;
```

```
┌────────┬────────┬───────┐
│ name   │ driver │ scope │
├────────┼────────┼───────┤
│ bridge │ bridge │ local │
│ host   │ host   │ local │
│ none   │ null   │ local │
└────────┴────────┴───────┘
```

### All containers including stopped

```sql
SELECT name, state, status FROM docker_containers WHERE show_all = 'true';
```
