# dripline-plugin-kubectl

Kubernetes plugin for [dripline](https://github.com/Michaelliv/dripline) — query pods, services, deployments, nodes, namespaces, configmaps, secrets, and ingresses with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/kubectl
```

Requires `kubectl` on PATH with a configured cluster context.

## Tables

| Table | Description |
|-------|-------------|
| `k8s_pods` | Pods across all namespaces |
| `k8s_services` | Services |
| `k8s_deployments` | Deployments |
| `k8s_nodes` | Cluster nodes |
| `k8s_namespaces` | Namespaces |
| `k8s_configmaps` | ConfigMaps (keys only) |
| `k8s_secrets` | Secrets (keys only, no values) |
| `k8s_ingresses` | Ingress resources |

All pod/service/deployment/configmap/secret/ingress tables support `WHERE namespace = 'my-ns'` to filter by namespace.

## Examples

### All pods

```sql
SELECT name, namespace, status, ready, restarts FROM k8s_pods;
```

```
┌───────────────────────────┬────────────┬─────────┬───────┬──────────┐
│ name                      │ namespace  │ status  │ ready │ restarts │
├───────────────────────────┼────────────┼─────────┼───────┼──────────┤
│ nginx-7c5b8d6c88-x4k2n   │ default    │ Running │ 1/1   │ 0        │
│ redis-master-0            │ default    │ Running │ 1/1   │ 0        │
│ coredns-7d764666f9-d8t6n  │ kube-system│ Running │ 1/1   │ 2        │
│ etcd-control-plane        │ kube-system│ Running │ 1/1   │ 1        │
└───────────────────────────┴────────────┴─────────┴───────┴──────────┘
```

### Pods with restarts

```sql
SELECT name, namespace, restarts FROM k8s_pods WHERE restarts > 0 ORDER BY restarts DESC;
```

### Services by type

```sql
SELECT name, namespace, type, cluster_ip FROM k8s_services ORDER BY type;
```

### Deployment readiness

```sql
SELECT name, namespace, replicas, ready_replicas, available_replicas
FROM k8s_deployments
WHERE ready_replicas < replicas;
```

### Node info

```sql
SELECT name, status, roles, version, cpu, memory FROM k8s_nodes;
```

### Cross-resource queries

```sql
SELECT p.name as pod, p.status, s.name as service, s.type
FROM k8s_pods p
JOIN k8s_services s ON p.namespace = s.namespace
WHERE p.namespace = 'default';
```
