import type { DriplinePluginAPI, QueryContext } from "dripline";

// Fly.io uses GraphQL - we need POST requests via curl
function flyGql(
  ctx: QueryContext,
  query: string,
  variables: Record<string, any> = {},
): any {
  const token = ctx.connection.config.api_token || "";
  const { execSync } = require("node:child_process");
  const body = JSON.stringify({ query, variables });
  try {
    const result = execSync(
      `curl -s -X POST "https://api.fly.io/graphql" -H "Authorization: Bearer ${token}" -H "Content-Type: application/json" -d '${body.replace(/'/g, "'\\''")}'`,
      { encoding: "utf-8", timeout: 30000 },
    );
    return JSON.parse(result);
  } catch {
    return null;
  }
}

function* flyPaginate(
  ctx: QueryContext,
  query: string,
  dataPath: string[],
  variables: Record<string, any> = {},
) {
  let endCursor = "";
  while (true) {
    const vars: any = { first: 100, ...variables };
    if (endCursor) vars.after = endCursor;
    const result = flyGql(ctx, query, vars);
    if (!result?.data) return;
    let node = result.data;
    for (const key of dataPath) node = node?.[key];
    if (!node?.nodes?.length) return;
    yield* node.nodes;
    if (!node.pageInfo?.hasNextPage) return;
    endCursor = node.pageInfo.endCursor;
  }
}

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

export default function fly(dl: DriplinePluginAPI) {
  dl.setName("fly");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_token: {
      type: "string",
      required: true,
      description: "Fly.io API token",
      env: "FLY_API_TOKEN",
    },
  });

  // organizations(first, after) -> nodes { name, id, slug, type, viewerRole, billingStatus }
  dl.registerTable("fly_organizations", {
    description: "Fly.io organizations",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "slug", type: "string" },
      { name: "type", type: "string" },
      { name: "viewer_role", type: "string" },
      { name: "billing_status", type: "string" },
    ],
    *list(ctx) {
      const query = `query($first: Int, $after: String) {
        organizations(first: $first, after: $after) {
          pageInfo { hasNextPage endCursor }
          nodes { name id slug type viewerRole billingStatus }
        }
      }`;
      for (const o of flyPaginate(ctx, query, ["organizations"])) {
        yield {
          id: o.id || "",
          name: o.name || "",
          slug: o.slug || "",
          type: o.type || "",
          viewer_role: o.viewerRole || "",
          billing_status: o.billingStatus || "",
        };
      }
    },
  });

  // apps(first, after) -> nodes { name, id, status, hostname, appUrl, deployed, organization { slug } }
  dl.registerTable("fly_apps", {
    description: "Fly.io applications",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "status", type: "string" },
      { name: "hostname", type: "string" },
      { name: "app_url", type: "string" },
      { name: "deployed", type: "boolean" },
      { name: "organization_slug", type: "string" },
      { name: "network", type: "string" },
    ],
    *list(ctx) {
      const query = `query($first: Int, $after: String) {
        apps(first: $first, after: $after) {
          pageInfo { hasNextPage endCursor }
          nodes { name id status hostname appUrl deployed network
            organization { slug }
          }
        }
      }`;
      for (const a of flyPaginate(ctx, query, ["apps"])) {
        yield {
          id: a.id || "",
          name: a.name || "",
          status: a.status || "",
          hostname: a.hostname || "",
          app_url: a.appUrl || "",
          deployed: a.deployed ? 1 : 0,
          organization_slug: a.organization?.slug || "",
          network: a.network || "",
        };
      }
    },
  });

  // machines(first, after, appId) -> nodes { name, id, state, region, createdAt, updatedAt, instanceId, config }
  dl.registerTable("fly_machines", {
    description: "Fly.io machines for an app",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "state", type: "string" },
      { name: "region", type: "string" },
      { name: "instance_id", type: "string" },
      { name: "image", type: "string" },
      { name: "size", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
    ],
    keyColumns: [{ name: "app_id", required: "optional", operators: ["="] }],
    *list(ctx) {
      const appId = getQual(ctx, "app_id");
      const vars: Record<string, any> = {};
      if (appId) vars.appId = appId;
      const query = `query($first: Int, $after: String, $appId: String) {
        machines(first: $first, after: $after, appId: $appId) {
          pageInfo { hasNextPage endCursor }
          nodes { name id state region instanceId createdAt updatedAt config }
        }
      }`;
      for (const m of flyPaginate(ctx, query, ["machines"], vars)) {
        const config =
          typeof m.config === "string" ? JSON.parse(m.config) : m.config || {};
        yield {
          id: m.id || "",
          name: m.name || "",
          state: m.state || "",
          region: m.region || "",
          instance_id: m.instanceId || "",
          image: config.image || "",
          size: config.size || "",
          created_at: m.createdAt || "",
          updated_at: m.updatedAt || "",
        };
      }
    },
  });

  // app(name).volumes(first, after) -> nodes { name, id, state, region, sizeGb, encrypted, createdAt }
  dl.registerTable("fly_volumes", {
    description: "Fly.io volumes for an app",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "state", type: "string" },
      { name: "region", type: "string" },
      { name: "size_gb", type: "number" },
      { name: "encrypted", type: "boolean" },
      { name: "status", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [{ name: "app_name", required: "required", operators: ["="] }],
    *list(ctx) {
      const appName = getQual(ctx, "app_name");
      if (!appName) return;
      const query = `query($appId: String, $first: Int, $after: String) {
        app(name: $appId) {
          volumes(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            nodes { name id state region sizeGb encrypted status createdAt }
          }
        }
      }`;
      for (const v of flyPaginate(ctx, query, ["app", "volumes"], {
        appId: appName,
      })) {
        yield {
          id: v.id || "",
          name: v.name || "",
          state: v.state || "",
          region: v.region || "",
          size_gb: v.sizeGb || 0,
          encrypted: v.encrypted ? 1 : 0,
          status: v.status || "",
          created_at: v.createdAt || "",
        };
      }
    },
  });

  // app(name).ipAddresses -> nodes { id, address, type, region, createdAt }
  dl.registerTable("fly_ip_addresses", {
    description: "Fly.io IP addresses for an app",
    columns: [
      { name: "id", type: "string" },
      { name: "address", type: "string" },
      { name: "ip_type", type: "string" },
      { name: "region", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [{ name: "app_name", required: "required", operators: ["="] }],
    *list(ctx) {
      const appName = getQual(ctx, "app_name");
      if (!appName) return;
      const query = `query($appId: String, $first: Int, $after: String) {
        app(name: $appId) {
          ipAddresses(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            nodes { id address type region createdAt }
          }
        }
      }`;
      for (const ip of flyPaginate(ctx, query, ["app", "ipAddresses"], {
        appId: appName,
      })) {
        yield {
          id: ip.id || "",
          address: ip.address || "",
          ip_type: ip.type || "",
          region: ip.region || "",
          created_at: ip.createdAt || "",
        };
      }
    },
  });

  // app(name).certificates -> nodes { hostname, id, createdAt, source, clientStatus, isApex, dnsValidationHostname, dnsValidationTarget }
  dl.registerTable("fly_certificates", {
    description: "Fly.io TLS certificates for an app",
    columns: [
      { name: "id", type: "string" },
      { name: "hostname", type: "string" },
      { name: "source", type: "string" },
      { name: "client_status", type: "string" },
      { name: "is_apex", type: "boolean" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [{ name: "app_name", required: "required", operators: ["="] }],
    *list(ctx) {
      const appName = getQual(ctx, "app_name");
      if (!appName) return;
      const query = `query($appId: String, $first: Int, $after: String) {
        app(name: $appId) {
          certificates(first: $first, after: $after) {
            pageInfo { hasNextPage endCursor }
            nodes { id hostname source clientStatus isApex createdAt }
          }
        }
      }`;
      for (const c of flyPaginate(ctx, query, ["app", "certificates"], {
        appId: appName,
      })) {
        yield {
          id: c.id || "",
          hostname: c.hostname || "",
          source: c.source || "",
          client_status: c.clientStatus || "",
          is_apex: c.isApex ? 1 : 0,
          created_at: c.createdAt || "",
        };
      }
    },
  });
}
