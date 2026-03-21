import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const API = "https://api.digitalocean.com/v2";

function doGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const token = ctx.connection.config.token || "";
  const qs = new URLSearchParams(params).toString();
  const url = `${API}${path}${qs ? `?${qs}` : ""}`;
  const resp = syncGet(url, { Authorization: `Bearer ${token}` });
  return resp.status === 200 ? resp.body : null;
}

function* doPaginate(ctx: QueryContext, path: string, key: string) {
  let page = 1;
  while (true) {
    const body = doGet(ctx, path, { page: String(page), per_page: "200" });
    if (!body?.[key]?.length) return;
    yield* body[key];
    // Check if last page: links.pages.last absent or current page == total
    const meta = body.meta?.total;
    if (meta !== undefined) {
      const total = body[key].length + (page - 1) * 200;
      if (total >= meta) return;
    }
    if (!body.links?.pages?.next) return;
    page++;
  }
}

export default function digitalocean(dl: DriplinePluginAPI) {
  dl.setName("digitalocean");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    token: {
      type: "string",
      required: true,
      description: "DigitalOcean API token",
      env: "DIGITALOCEAN_TOKEN",
    },
  });

  // GET /v2/droplets
  dl.registerTable("do_droplets", {
    description: "DigitalOcean Droplets (virtual machines)",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "status", type: "string" },
      { name: "memory", type: "number" },
      { name: "vcpus", type: "number" },
      { name: "disk", type: "number" },
      { name: "region_slug", type: "string" },
      { name: "size_slug", type: "string" },
      { name: "image_name", type: "string" },
      { name: "vpc_uuid", type: "string" },
      { name: "tags", type: "json" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const d of doPaginate(ctx, "/droplets", "droplets")) {
        yield {
          id: d.id,
          name: d.name || "",
          status: d.status || "",
          memory: d.memory || 0,
          vcpus: d.vcpus || 0,
          disk: d.disk || 0,
          region_slug: d.region?.slug || "",
          size_slug: d.size_slug || d.size?.slug || "",
          image_name: d.image?.name || "",
          vpc_uuid: d.vpc_uuid || "",
          tags: JSON.stringify(d.tags || []),
          created_at: d.created_at || "",
        };
      }
    },
  });

  // GET /v2/kubernetes/clusters
  dl.registerTable("do_kubernetes_clusters", {
    description: "DigitalOcean Kubernetes clusters",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "region_slug", type: "string" },
      { name: "version_slug", type: "string" },
      { name: "status", type: "string" },
      { name: "node_count", type: "number" },
      { name: "endpoint", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "tags", type: "json" },
    ],
    *list(ctx) {
      for (const k of doPaginate(
        ctx,
        "/kubernetes/clusters",
        "kubernetes_clusters",
      )) {
        const nodeCount = (k.node_pools || []).reduce(
          (sum: number, p: any) => sum + (p.count || 0),
          0,
        );
        yield {
          id: k.id,
          name: k.name || "",
          region_slug: k.region || "",
          version_slug: k.version || "",
          status: k.status?.state || "",
          node_count: nodeCount,
          endpoint: k.endpoint || "",
          created_at: k.created_at || "",
          tags: JSON.stringify(k.tags || []),
        };
      }
    },
  });

  // GET /v2/databases
  dl.registerTable("do_databases", {
    description: "DigitalOcean managed databases",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "engine", type: "string" },
      { name: "version", type: "string" },
      { name: "status", type: "string" },
      { name: "size_slug", type: "string" },
      { name: "region_slug", type: "string" },
      { name: "num_nodes", type: "number" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      // databases endpoint doesn't paginate the same way
      const body = doGet(ctx, "/databases");
      if (!body?.databases) return;
      for (const d of body.databases) {
        yield {
          id: d.id,
          name: d.name || "",
          engine: d.engine || "",
          version: d.version || "",
          status: d.status || "",
          size_slug: d.size || "",
          region_slug: d.region || "",
          num_nodes: d.num_nodes || 0,
          created_at: d.created_at || "",
        };
      }
    },
  });

  // GET /v2/domains
  dl.registerTable("do_domains", {
    description: "DigitalOcean domains",
    columns: [
      { name: "name", type: "string" },
      { name: "ttl", type: "number" },
      { name: "zone_file", type: "string" },
    ],
    *list(ctx) {
      for (const d of doPaginate(ctx, "/domains", "domains")) {
        yield {
          name: d.name || "",
          ttl: d.ttl || 0,
          zone_file: d.zone_file || "",
        };
      }
    },
  });

  // GET /v2/firewalls
  dl.registerTable("do_firewalls", {
    description: "DigitalOcean firewalls",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "status", type: "string" },
      { name: "droplet_ids", type: "json" },
      { name: "inbound_rules", type: "json" },
      { name: "outbound_rules", type: "json" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const f of doPaginate(ctx, "/firewalls", "firewalls")) {
        yield {
          id: f.id,
          name: f.name || "",
          status: f.status || "",
          droplet_ids: JSON.stringify(f.droplet_ids || []),
          inbound_rules: JSON.stringify(f.inbound_rules || []),
          outbound_rules: JSON.stringify(f.outbound_rules || []),
          created_at: f.created_at || "",
        };
      }
    },
  });

  // GET /v2/volumes
  dl.registerTable("do_volumes", {
    description: "DigitalOcean block storage volumes",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "size_gigabytes", type: "number" },
      { name: "region_slug", type: "string" },
      { name: "droplet_ids", type: "json" },
      { name: "filesystem_type", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const v of doPaginate(ctx, "/volumes", "volumes")) {
        yield {
          id: v.id,
          name: v.name || "",
          size_gigabytes: v.size_gigabytes || 0,
          region_slug: v.region?.slug || "",
          droplet_ids: JSON.stringify(v.droplet_ids || []),
          filesystem_type: v.filesystem_type || "",
          created_at: v.created_at || "",
        };
      }
    },
  });

  // GET /v2/load_balancers
  dl.registerTable("do_load_balancers", {
    description: "DigitalOcean load balancers",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "ip", type: "string" },
      { name: "status", type: "string" },
      { name: "region_slug", type: "string" },
      { name: "droplet_ids", type: "json" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const lb of doPaginate(ctx, "/load_balancers", "load_balancers")) {
        yield {
          id: lb.id,
          name: lb.name || "",
          ip: lb.ip || "",
          status: lb.status || "",
          region_slug: lb.region?.slug || "",
          droplet_ids: JSON.stringify(lb.droplet_ids || []),
          created_at: lb.created_at || "",
        };
      }
    },
  });

  // GET /v2/vpcs
  dl.registerTable("do_vpcs", {
    description: "DigitalOcean VPCs",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "region", type: "string" },
      { name: "ip_range", type: "string" },
      { name: "description", type: "string" },
      { name: "default", type: "boolean" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const v of doPaginate(ctx, "/vpcs", "vpcs")) {
        yield {
          id: v.id,
          name: v.name || "",
          region: v.region || "",
          ip_range: v.ip_range || "",
          description: v.description || "",
          default: v.default ? 1 : 0,
          created_at: v.created_at || "",
        };
      }
    },
  });

  // GET /v2/account/keys
  dl.registerTable("do_ssh_keys", {
    description: "DigitalOcean SSH keys",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "fingerprint", type: "string" },
      { name: "public_key", type: "string" },
    ],
    *list(ctx) {
      for (const k of doPaginate(ctx, "/account/keys", "ssh_keys")) {
        yield {
          id: k.id,
          name: k.name || "",
          fingerprint: k.fingerprint || "",
          public_key: k.public_key || "",
        };
      }
    },
  });

  // GET /v2/projects
  dl.registerTable("do_projects", {
    description: "DigitalOcean projects",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "purpose", type: "string" },
      { name: "environment", type: "string" },
      { name: "is_default", type: "boolean" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const p of doPaginate(ctx, "/projects", "projects")) {
        yield {
          id: p.id,
          name: p.name || "",
          description: p.description || "",
          purpose: p.purpose || "",
          environment: p.environment || "",
          is_default: p.is_default ? 1 : 0,
          created_at: p.created_at || "",
        };
      }
    },
  });
}
