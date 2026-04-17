# dripline-plugin-ps

Process plugin for [dripline](https://github.com/Michaelliv/dripline) — query running processes and listening ports with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/ps
```

Uses `ps` and `lsof` (built into macOS/Linux).

## Tables

| Table | Description |
|-------|-------------|
| `ps_processes` | Running processes with CPU, memory, and command info |
| `ps_ports` | Processes listening on TCP ports |

## Examples

### Top processes by CPU

```sql
SELECT pid, user, ROUND(cpu, 1) as cpu, ROUND(mem, 1) as mem,
       SUBSTR(command, 1, 50) as command
FROM ps_processes
WHERE cpu > 5
ORDER BY cpu DESC
LIMIT 5;
```

```
┌───────┬───────┬──────┬─────┬────────────────────────────────────────────────────┐
│ pid   │ user  │ cpu  │ mem │ command                                            │
├───────┼───────┼──────┼─────┼────────────────────────────────────────────────────┤
│ 12345 │ alice │ 45.2 │ 3.1 │ /Applications/Cursor.app/Contents/MacOS/Cursor     │
│ 67890 │ alice │ 32.8 │ 6.9 │ /Applications/Docker.app/Contents/MacOS/com.docker │
│ 11223 │ root  │ 12.5 │ 0.6 │ /System/Library/PrivateFrameworks/SkyLight.framewo │
│ 44556 │ alice │ 8.3  │ 1.2 │ /Applications/Safari.app/Contents/MacOS/Safari     │
│ 77889 │ root  │ 5.1  │ 0.1 │ /usr/libexec/syspolicyd                            │
└───────┴───────┴──────┴─────┴────────────────────────────────────────────────────┘
```

### Listening ports

```sql
SELECT command, pid, local_address, local_port
FROM ps_ports
LIMIT 5;
```

```
┌───────────────┬───────┬───────────────┬────────────┐
│ command       │ pid   │ local_address │ local_port │
├───────────────┼───────┼───────────────┼────────────┤
│ node          │ 19894 │ 127.0.0.1     │ 3000       │
│ postgres      │ 21001 │ 127.0.0.1     │ 5432       │
│ ControlCenter │ 21406 │ *             │ 7000       │
│ nginx         │ 22100 │ *             │ 8080       │
└───────────────┴───────┴───────────────┴────────────┘
```

### Memory hogs

```sql
SELECT pid, user, ROUND(mem, 1) as mem_pct, rss_kb / 1024 as rss_mb,
       SUBSTR(command, 1, 40) as command
FROM ps_processes
WHERE mem > 1
ORDER BY mem DESC
LIMIT 10;
```

### Processes by user

```sql
SELECT user, COUNT(*) as count, ROUND(SUM(cpu), 1) as total_cpu
FROM ps_processes
GROUP BY user
ORDER BY total_cpu DESC;
```
