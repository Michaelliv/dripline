import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// MongoDB Atlas API: cloud.mongodb.com/api/atlas/v1.0/
// Auth: Digest auth (public_key:private_key) — we use Basic which Atlas also accepts
// Verified from steampipe source: MONGODB_ATLAS_PUBLIC_API_KEY, MONGODB_ATLAS_PRIVATE_API_KEY

const API = "https://cloud.mongodb.com/api/atlas/v1.0";

function atlasGet(ctx: QueryContext, path: string): any {
  const pubKey = ctx.connection.config.public_key || "";
  const privKey = ctx.connection.config.private_key || "";
  // Atlas accepts both Digest and Basic auth
  const auth = Buffer.from(`${pubKey}:${privKey}`).toString("base64");
  const resp = syncGet(`${API}${path}`, {
    Authorization: `Basic ${auth}`,
    Accept: "application/json",
  });
  // Atlas might require Digest - fall back to curl
  if (resp.status === 401) {
    const { execSync } = require("node:child_process");
    try {
      const result = execSync(
        `curl -s --digest -u "${pubKey}:${privKey}" "${API}${path}"`,
        { encoding: "utf-8", timeout: 15000 },
      );
      return JSON.parse(result);
    } catch {
      return null;
    }
  }
  return resp.status === 200 ? resp.body : null;
}

export default function mongodbatlas(dl: DriplinePluginAPI) {
  dl.setName("mongodbatlas");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    public_key: {
      type: "string",
      required: true,
      description: "Atlas public API key",
      env: "MONGODB_ATLAS_PUBLIC_API_KEY",
    },
    private_key: {
      type: "string",
      required: true,
      description: "Atlas private API key",
      env: "MONGODB_ATLAS_PRIVATE_API_KEY",
    },
  });

  dl.registerTable("atlas_projects", {
    description: "MongoDB Atlas projects",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "org_id", type: "string" },
      { name: "cluster_count", type: "number" },
      { name: "created", type: "datetime" },
    ],
    *list(ctx) {
      const body = atlasGet(ctx, "/groups");
      if (!body?.results) return;
      for (const p of body.results) {
        yield {
          id: p.id,
          name: p.name || "",
          org_id: p.orgId || "",
          cluster_count: p.clusterCount || 0,
          created: p.created || "",
        };
      }
    },
  });

  dl.registerTable("atlas_clusters", {
    description: "MongoDB Atlas clusters",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "project_id", type: "string" },
      { name: "cluster_type", type: "string" },
      { name: "state_name", type: "string" },
      { name: "mongo_db_version", type: "string" },
      { name: "connection_strings_standard", type: "string" },
      { name: "provider_name", type: "string" },
      { name: "region_name", type: "string" },
    ],
    keyColumns: [
      { name: "project_id", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const pid = ctx.quals.find((q) => q.column === "project_id")?.value;
      if (!pid) return;
      const body = atlasGet(ctx, `/groups/${pid}/clusters`);
      if (!body?.results) return;
      for (const c of body.results) {
        yield {
          id: c.id,
          name: c.name || "",
          project_id: pid,
          cluster_type: c.clusterType || "",
          state_name: c.stateName || "",
          mongo_db_version: c.mongoDBVersion || "",
          connection_strings_standard: c.connectionStrings?.standard || "",
          provider_name: c.providerSettings?.providerName || "",
          region_name: c.providerSettings?.regionName || "",
        };
      }
    },
  });

  dl.registerTable("atlas_database_users", {
    description: "MongoDB Atlas database users",
    columns: [
      { name: "username", type: "string" },
      { name: "project_id", type: "string" },
      { name: "database_name", type: "string" },
      { name: "roles", type: "json" },
    ],
    keyColumns: [
      { name: "project_id", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const pid = ctx.quals.find((q) => q.column === "project_id")?.value;
      if (!pid) return;
      const body = atlasGet(ctx, `/groups/${pid}/databaseUsers`);
      if (!body?.results) return;
      for (const u of body.results) {
        yield {
          username: u.username || "",
          project_id: pid,
          database_name: u.databaseName || "",
          roles: JSON.stringify(u.roles || []),
        };
      }
    },
  });
}
