import type { DriplinePluginAPI } from "dripline";
import { syncExec, commandExists } from "dripline";

export default function kubectl(dl: DriplinePluginAPI) {
  dl.setName("kubectl");
  dl.setVersion("0.1.0");

  dl.onInit(() => {
    if (!commandExists("kubectl")) {
      dl.log.warn("kubectl not found on PATH");
    }
  });

  function getQual(ctx: { quals: { column: string; value: any }[] }, name: string): string | undefined {
    return ctx.quals.find((q) => q.column === name)?.value;
  }

  function kubectlGet(resource: string, namespace?: string): any[] {
    const args = ["get", resource, "-o", "json"];
    if (namespace && namespace !== "all") {
      args.push("-n", namespace);
    } else {
      args.push("-A");
    }
    const { rows: [data] } = syncExec("kubectl", args, { parser: "json" });
    return data?.items ?? [];
  }

  dl.registerTable("k8s_pods", {
    description: "Kubernetes pods across all namespaces",
    columns: [
      { name: "name", type: "string" },
      { name: "namespace", type: "string" },
      { name: "status", type: "string" },
      { name: "ready", type: "string" },
      { name: "restarts", type: "number" },
      { name: "node", type: "string" },
      { name: "ip", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "labels", type: "json" },
      { name: "containers", type: "json" },
    ],
    keyColumns: [
      { name: "namespace", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const ns = getQual(ctx, "namespace");
      const items = kubectlGet("pods", ns);

      for (const p of items) {
        const containerStatuses = p.status?.containerStatuses ?? [];
        const readyCount = containerStatuses.filter((c: any) => c.ready).length;
        const totalCount = containerStatuses.length || p.spec?.containers?.length || 0;
        const restarts = containerStatuses.reduce((sum: number, c: any) => sum + (c.restartCount ?? 0), 0);

        yield {
          name: p.metadata?.name ?? "",
          namespace: p.metadata?.namespace ?? "",
          status: p.status?.phase ?? "",
          ready: `${readyCount}/${totalCount}`,
          restarts,
          node: p.spec?.nodeName ?? "",
          ip: p.status?.podIP ?? "",
          created_at: p.metadata?.creationTimestamp ?? "",
          labels: JSON.stringify(p.metadata?.labels ?? {}),
          containers: JSON.stringify(
            (p.spec?.containers ?? []).map((c: any) => ({
              name: c.name,
              image: c.image,
            })),
          ),
        };
      }
    },
  });

  dl.registerTable("k8s_services", {
    description: "Kubernetes services",
    columns: [
      { name: "name", type: "string" },
      { name: "namespace", type: "string" },
      { name: "type", type: "string" },
      { name: "cluster_ip", type: "string" },
      { name: "external_ip", type: "string" },
      { name: "ports", type: "json" },
      { name: "selector", type: "json" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "namespace", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const ns = getQual(ctx, "namespace");
      const items = kubectlGet("services", ns);

      for (const s of items) {
        const ports = (s.spec?.ports ?? []).map((p: any) => ({
          port: p.port,
          targetPort: p.targetPort,
          protocol: p.protocol,
          nodePort: p.nodePort,
        }));

        yield {
          name: s.metadata?.name ?? "",
          namespace: s.metadata?.namespace ?? "",
          type: s.spec?.type ?? "",
          cluster_ip: s.spec?.clusterIP ?? "",
          external_ip: (s.status?.loadBalancer?.ingress ?? []).map((i: any) => i.ip || i.hostname).join(",") || "",
          ports: JSON.stringify(ports),
          selector: JSON.stringify(s.spec?.selector ?? {}),
          created_at: s.metadata?.creationTimestamp ?? "",
        };
      }
    },
  });

  dl.registerTable("k8s_deployments", {
    description: "Kubernetes deployments",
    columns: [
      { name: "name", type: "string" },
      { name: "namespace", type: "string" },
      { name: "replicas", type: "number" },
      { name: "ready_replicas", type: "number" },
      { name: "available_replicas", type: "number" },
      { name: "strategy", type: "string" },
      { name: "containers", type: "json" },
      { name: "labels", type: "json" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "namespace", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const ns = getQual(ctx, "namespace");
      const items = kubectlGet("deployments", ns);

      for (const d of items) {
        yield {
          name: d.metadata?.name ?? "",
          namespace: d.metadata?.namespace ?? "",
          replicas: d.spec?.replicas ?? 0,
          ready_replicas: d.status?.readyReplicas ?? 0,
          available_replicas: d.status?.availableReplicas ?? 0,
          strategy: d.spec?.strategy?.type ?? "",
          containers: JSON.stringify(
            (d.spec?.template?.spec?.containers ?? []).map((c: any) => ({
              name: c.name,
              image: c.image,
            })),
          ),
          labels: JSON.stringify(d.metadata?.labels ?? {}),
          created_at: d.metadata?.creationTimestamp ?? "",
        };
      }
    },
  });

  dl.registerTable("k8s_nodes", {
    description: "Kubernetes cluster nodes",
    columns: [
      { name: "name", type: "string" },
      { name: "status", type: "string" },
      { name: "roles", type: "string" },
      { name: "version", type: "string" },
      { name: "os", type: "string" },
      { name: "arch", type: "string" },
      { name: "container_runtime", type: "string" },
      { name: "cpu", type: "string" },
      { name: "memory", type: "string" },
      { name: "internal_ip", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    *list() {
      const { rows: [data] } = syncExec("kubectl", ["get", "nodes", "-o", "json"], { parser: "json" });
      const items = data?.items ?? [];

      for (const n of items) {
        const conditions = n.status?.conditions ?? [];
        const ready = conditions.find((c: any) => c.type === "Ready");
        const labels = n.metadata?.labels ?? {};
        const roles = Object.keys(labels)
          .filter((k: string) => k.startsWith("node-role.kubernetes.io/"))
          .map((k: string) => k.replace("node-role.kubernetes.io/", ""))
          .join(",") || "worker";
        const addresses = n.status?.addresses ?? [];
        const internalIP = addresses.find((a: any) => a.type === "InternalIP")?.address ?? "";

        yield {
          name: n.metadata?.name ?? "",
          status: ready?.status === "True" ? "Ready" : "NotReady",
          roles,
          version: n.status?.nodeInfo?.kubeletVersion ?? "",
          os: n.status?.nodeInfo?.osImage ?? "",
          arch: n.status?.nodeInfo?.architecture ?? "",
          container_runtime: n.status?.nodeInfo?.containerRuntimeVersion ?? "",
          cpu: n.status?.capacity?.cpu ?? "",
          memory: n.status?.capacity?.memory ?? "",
          internal_ip: internalIP,
          created_at: n.metadata?.creationTimestamp ?? "",
        };
      }
    },
  });

  dl.registerTable("k8s_namespaces", {
    description: "Kubernetes namespaces",
    columns: [
      { name: "name", type: "string" },
      { name: "status", type: "string" },
      { name: "labels", type: "json" },
      { name: "created_at", type: "datetime" },
    ],
    *list() {
      const { rows: [data] } = syncExec("kubectl", ["get", "namespaces", "-o", "json"], { parser: "json" });

      for (const ns of data?.items ?? []) {
        yield {
          name: ns.metadata?.name ?? "",
          status: ns.status?.phase ?? "",
          labels: JSON.stringify(ns.metadata?.labels ?? {}),
          created_at: ns.metadata?.creationTimestamp ?? "",
        };
      }
    },
  });

  dl.registerTable("k8s_configmaps", {
    description: "Kubernetes ConfigMaps",
    columns: [
      { name: "name", type: "string" },
      { name: "namespace", type: "string" },
      { name: "data_keys", type: "json" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "namespace", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const ns = getQual(ctx, "namespace");
      const items = kubectlGet("configmaps", ns);

      for (const cm of items) {
        yield {
          name: cm.metadata?.name ?? "",
          namespace: cm.metadata?.namespace ?? "",
          data_keys: JSON.stringify(Object.keys(cm.data ?? {})),
          created_at: cm.metadata?.creationTimestamp ?? "",
        };
      }
    },
  });

  dl.registerTable("k8s_secrets", {
    description: "Kubernetes secrets (keys only, no values)",
    columns: [
      { name: "name", type: "string" },
      { name: "namespace", type: "string" },
      { name: "type", type: "string" },
      { name: "data_keys", type: "json" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "namespace", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const ns = getQual(ctx, "namespace");
      const items = kubectlGet("secrets", ns);

      for (const s of items) {
        yield {
          name: s.metadata?.name ?? "",
          namespace: s.metadata?.namespace ?? "",
          type: s.type ?? "",
          data_keys: JSON.stringify(Object.keys(s.data ?? {})),
          created_at: s.metadata?.creationTimestamp ?? "",
        };
      }
    },
  });

  dl.registerTable("k8s_ingresses", {
    description: "Kubernetes ingress resources",
    columns: [
      { name: "name", type: "string" },
      { name: "namespace", type: "string" },
      { name: "class", type: "string" },
      { name: "hosts", type: "json" },
      { name: "rules", type: "json" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "namespace", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const ns = getQual(ctx, "namespace");
      const items = kubectlGet("ingresses", ns);

      for (const i of items) {
        const hosts = (i.spec?.rules ?? []).map((r: any) => r.host).filter(Boolean);

        yield {
          name: i.metadata?.name ?? "",
          namespace: i.metadata?.namespace ?? "",
          class: i.spec?.ingressClassName ?? "",
          hosts: JSON.stringify(hosts),
          rules: JSON.stringify(i.spec?.rules ?? []),
          created_at: i.metadata?.creationTimestamp ?? "",
        };
      }
    },
  });
}
