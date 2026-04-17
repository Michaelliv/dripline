# dripline-plugin-cloudflare

Cloudflare plugin for [dripline](https://github.com/Michaelliv/dripline) — query Workers, zones, DNS, Pages, D1, KV, R2, and Queues with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/cloudflare
```

Auth is auto-detected from wrangler OAuth (`wrangler login`) or `CLOUDFLARE_API_TOKEN` env var.

## Tables

### Requires auth (wrangler login or CLOUDFLARE_API_TOKEN)

| Table | Description |
|-------|-------------|
| `cf_workers` | Deployed Workers scripts |
| `cf_zones` | DNS zones (domains) |
| `cf_dns_records` | DNS records (WHERE zone_name = '...') |
| `cf_pages_projects` | Pages projects |
| `cf_pages_deployments` | Pages deployments (WHERE project_name = '...') |
| `cf_d1_databases` | D1 SQL databases |
| `cf_kv_namespaces` | Workers KV namespaces |
| `cf_r2_buckets` | R2 storage buckets |
| `cf_queues` | Queues |

### Public (no auth needed)

| Table | Description |
|-------|-------------|
| `cf_dns_lookup` | DNS lookup via 1.1.1.1 (WHERE domain = '...') |
| `cf_domain_check` | Domain availability check (WHERE name_prefix = '...' AND tlds = 'com,dev,sh') |

## Examples

### Workers

```sql
SELECT id, created_on, modified_on FROM cf_workers;
```

```
┌────────────────────┬────────────────────────────┬────────────────────────────┐
│ id                 │ created_on                 │ modified_on                │
├────────────────────┼────────────────────────────┼────────────────────────────┤
│ my-api-worker      │ 2025-11-24T21:27:28.890Z   │ 2025-12-30T23:12:55.215Z   │
│ my-proxy           │ 2026-02-18T00:27:50.226Z   │ 2026-02-18T00:27:50.226Z   │
└────────────────────┴────────────────────────────┴────────────────────────────┘
```

### Zones (domains)

```sql
SELECT name, status, plan FROM cf_zones;
```

```
┌────────────────┬────────┬──────────────┐
│ name           │ status │ plan         │
├────────────────┼────────┼──────────────┤
│ example.com    │ active │ Free Website │
│ myapp.dev      │ active │ Free Website │
└────────────────┴────────┴──────────────┘
```

### DNS records for a domain

```sql
SELECT name, type, content, proxied FROM cf_dns_records WHERE zone_name = 'example.com';
```

### Pages deployments

```sql
SELECT id, environment, url, created_on
FROM cf_pages_deployments
WHERE project_name = 'my-site'
LIMIT 5;
```

### Check domain availability

```sql
SELECT domain, available, has_dns
FROM cf_domain_check
WHERE name_prefix = 'myproject' AND tlds = 'com,dev,sh,io,app,ai';
```

```
┌────────────────┬───────────┬─────────┐
│ domain         │ available │ has_dns │
├────────────────┼───────────┼─────────┤
│ myproject.com  │ false     │ true    │
│ myproject.dev  │ true      │ false   │
│ myproject.sh   │ true      │ false   │
│ myproject.io   │ false     │ true    │
│ myproject.app  │ false     │ true    │
│ myproject.ai   │ false     │ true    │
└────────────────┴───────────┴─────────┘
```

### DNS lookup

```sql
SELECT domain, record_type, value, ttl FROM cf_dns_lookup WHERE domain = 'example.com';
```

```
┌─────────────┬─────────────┬──────────────────┬───────┐
│ domain      │ record_type │ value            │ ttl   │
├─────────────┼─────────────┼──────────────────┼───────┤
│ example.com │ A           │ 93.184.216.34    │ 3600  │
│ example.com │ AAAA        │ 2606:2800:220::1 │ 3600  │
│ example.com │ NS          │ a.iana-servers.… │ 86400 │
│ example.com │ MX          │ 0 .              │ 3600  │
└─────────────┴─────────────┴──────────────────┴───────┘
```

### Full infrastructure overview

```sql
SELECT 'workers' as resource, COUNT(*) as count FROM cf_workers
UNION ALL SELECT 'zones', COUNT(*) FROM cf_zones
UNION ALL SELECT 'pages', COUNT(*) FROM cf_pages_projects
UNION ALL SELECT 'd1', COUNT(*) FROM cf_d1_databases
UNION ALL SELECT 'kv', COUNT(*) FROM cf_kv_namespaces
UNION ALL SELECT 'r2', COUNT(*) FROM cf_r2_buckets
UNION ALL SELECT 'queues', COUNT(*) FROM cf_queues;
```
