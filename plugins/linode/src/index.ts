import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// Linode REST API v4: api.linode.com/v4/
// Auth: Bearer token
// Pagination: page + page_size, response has { data: [], page, pages, results }
// Verified from linodego SDK: getPaginatedResults calls

const API = "https://api.linode.com/v4";

function lnGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const token = ctx.connection.config.token || "";
  const qs = new URLSearchParams(params).toString();
  const url = `${API}/${path}${qs ? `?${qs}` : ""}`;
  const resp = syncGet(url, { Authorization: `Bearer ${token}` });
  return resp.status === 200 ? resp.body : null;
}

function* lnPaginate(ctx: QueryContext, path: string) {
  let page = 1;
  while (true) {
    const body = lnGet(ctx, path, { page: String(page), page_size: "500" });
    if (!body?.data?.length) return;
    yield* body.data;
    if (page >= (body.pages || 1)) return;
    page++;
  }
}

export default function linode(dl: DriplinePluginAPI) {
  dl.setName("linode");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    token: {
      type: "string",
      required: true,
      description: "Linode API token",
      env: "LINODE_TOKEN",
    },
  });

  // linode/instances (linodego: ListInstances → "linode/instances")
  dl.registerTable("linode_instances", {
    description: "Linode instances (Linodes)",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
      { name: "status", type: "string" },
      { name: "type", type: "string" },
      { name: "region", type: "string" },
      { name: "image", type: "string" },
      { name: "ipv4", type: "json" },
      { name: "ipv6", type: "string" },
      { name: "vcpus", type: "number" },
      { name: "memory", type: "number" },
      { name: "disk", type: "number" },
      { name: "tags", type: "json" },
      { name: "created", type: "datetime" },
      { name: "updated", type: "datetime" },
    ],
    *list(ctx) {
      for (const i of lnPaginate(ctx, "linode/instances")) {
        yield {
          id: i.id,
          label: i.label || "",
          status: i.status || "",
          type: i.type || "",
          region: i.region || "",
          image: i.image || "",
          ipv4: JSON.stringify(i.ipv4 || []),
          ipv6: i.ipv6 || "",
          vcpus: i.specs?.vcpus || 0,
          memory: i.specs?.memory || 0,
          disk: i.specs?.disk || 0,
          tags: JSON.stringify(i.tags || []),
          created: i.created || "",
          updated: i.updated || "",
        };
      }
    },
  });

  // volumes (linodego: ListVolumes → "volumes")
  dl.registerTable("linode_volumes", {
    description: "Linode block storage volumes",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
      { name: "status", type: "string" },
      { name: "size", type: "number" },
      { name: "region", type: "string" },
      { name: "linode_id", type: "number" },
      { name: "filesystem_path", type: "string" },
      { name: "created", type: "datetime" },
      { name: "updated", type: "datetime" },
    ],
    *list(ctx) {
      for (const v of lnPaginate(ctx, "volumes")) {
        yield {
          id: v.id,
          label: v.label || "",
          status: v.status || "",
          size: v.size || 0,
          region: v.region || "",
          linode_id: v.linode_id || 0,
          filesystem_path: v.filesystem_path || "",
          created: v.created || "",
          updated: v.updated || "",
        };
      }
    },
  });

  // domains (linodego: ListDomains → "domains")
  dl.registerTable("linode_domains", {
    description: "Linode DNS domains",
    columns: [
      { name: "id", type: "number" },
      { name: "domain", type: "string" },
      { name: "type", type: "string" },
      { name: "status", type: "string" },
      { name: "soa_email", type: "string" },
      { name: "ttl_sec", type: "number" },
      { name: "tags", type: "json" },
    ],
    *list(ctx) {
      for (const d of lnPaginate(ctx, "domains")) {
        yield {
          id: d.id,
          domain: d.domain || "",
          type: d.type || "",
          status: d.status || "",
          soa_email: d.soa_email || "",
          ttl_sec: d.ttl_sec || 0,
          tags: JSON.stringify(d.tags || []),
        };
      }
    },
  });

  // networking/firewalls (linodego: ListFirewalls → "networking/firewalls")
  dl.registerTable("linode_firewalls", {
    description: "Linode Cloud Firewalls",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
      { name: "status", type: "string" },
      { name: "tags", type: "json" },
      { name: "rules", type: "json" },
      { name: "created", type: "datetime" },
      { name: "updated", type: "datetime" },
    ],
    *list(ctx) {
      for (const f of lnPaginate(ctx, "networking/firewalls")) {
        yield {
          id: f.id,
          label: f.label || "",
          status: f.status || "",
          tags: JSON.stringify(f.tags || []),
          rules: JSON.stringify(f.rules || {}),
          created: f.created || "",
          updated: f.updated || "",
        };
      }
    },
  });

  // lke/clusters (linodego: ListLKEClusters → "lke/clusters")
  dl.registerTable("linode_lke_clusters", {
    description: "Linode Kubernetes Engine clusters",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
      { name: "region", type: "string" },
      { name: "k8s_version", type: "string" },
      { name: "status", type: "string" },
      { name: "tags", type: "json" },
      { name: "created", type: "datetime" },
      { name: "updated", type: "datetime" },
    ],
    *list(ctx) {
      for (const c of lnPaginate(ctx, "lke/clusters")) {
        yield {
          id: c.id,
          label: c.label || "",
          region: c.region || "",
          k8s_version: c.k8s_version || "",
          status: c.status?.replace("lke_status_", "") || "",
          tags: JSON.stringify(c.tags || []),
          created: c.created || "",
          updated: c.updated || "",
        };
      }
    },
  });

  // images (linodego: ListImages → "images")
  dl.registerTable("linode_images", {
    description: "Linode images",
    columns: [
      { name: "id", type: "string" },
      { name: "label", type: "string" },
      { name: "description", type: "string" },
      { name: "type", type: "string" },
      { name: "vendor", type: "string" },
      { name: "size", type: "number" },
      { name: "is_public", type: "boolean" },
      { name: "status", type: "string" },
      { name: "created", type: "datetime" },
    ],
    *list(ctx) {
      for (const i of lnPaginate(ctx, "images")) {
        yield {
          id: i.id || "",
          label: i.label || "",
          description: i.description || "",
          type: i.type || "",
          vendor: i.vendor || "",
          size: i.size || 0,
          is_public: i.is_public ? 1 : 0,
          status: i.status || "",
          created: i.created || "",
        };
      }
    },
  });
}
