import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// Heroku REST API: api.heroku.com
// Auth: Bearer token (verified from heroku-go SDK: DefaultURL = "https://api.heroku.com")
// Env: HEROKU_API_KEY
// Pagination: Range header with id-based pagination

const API = "https://api.heroku.com";

function hkGet(ctx: QueryContext, path: string): any {
  const key = ctx.connection.config.api_key || "";
  const resp = syncGet(`${API}${path}`, {
    Authorization: `Bearer ${key}`,
    Accept: "application/vnd.heroku+json; version=3",
  });
  return resp.status === 200 ? resp.body : null;
}

export default function heroku(dl: DriplinePluginAPI) {
  dl.setName("heroku");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_key: {
      type: "string",
      required: true,
      description: "Heroku API key",
      env: "HEROKU_API_KEY",
    },
  });

  dl.registerTable("heroku_apps", {
    description: "Heroku applications",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "web_url", type: "string" },
      { name: "region", type: "string" },
      { name: "stack", type: "string" },
      { name: "buildpack_provided_description", type: "string" },
      { name: "git_url", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
    ],
    *list(ctx) {
      const body = hkGet(ctx, "/apps");
      if (!body || !Array.isArray(body)) return;
      for (const a of body) {
        yield {
          id: a.id,
          name: a.name || "",
          web_url: a.web_url || "",
          region: a.region?.name || "",
          stack: a.stack?.name || a.build_stack?.name || "",
          buildpack_provided_description:
            a.buildpack_provided_description || "",
          git_url: a.git_url || "",
          created_at: a.created_at || "",
          updated_at: a.updated_at || "",
        };
      }
    },
  });

  dl.registerTable("heroku_addons", {
    description: "Heroku add-ons",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "addon_service_name", type: "string" },
      { name: "plan_name", type: "string" },
      { name: "state", type: "string" },
      { name: "app_name", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      const body = hkGet(ctx, "/addons");
      if (!body || !Array.isArray(body)) return;
      for (const a of body) {
        yield {
          id: a.id,
          name: a.name || "",
          addon_service_name: a.addon_service?.name || "",
          plan_name: a.plan?.name || "",
          state: a.state || "",
          app_name: a.app?.name || "",
          created_at: a.created_at || "",
        };
      }
    },
  });

  dl.registerTable("heroku_dynos", {
    description: "Heroku dynos for an app",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "type", type: "string" },
      { name: "state", type: "string" },
      { name: "size", type: "string" },
      { name: "app_name", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [{ name: "app_name", required: "required", operators: ["="] }],
    *list(ctx) {
      const app = ctx.quals.find((q) => q.column === "app_name")?.value;
      if (!app) return;
      const body = hkGet(ctx, `/apps/${app}/dynos`);
      if (!body || !Array.isArray(body)) return;
      for (const d of body) {
        yield {
          id: d.id,
          name: d.name || "",
          type: d.type || "",
          state: d.state || "",
          size: d.size || "",
          app_name: app,
          created_at: d.created_at || "",
        };
      }
    },
  });

  dl.registerTable("heroku_domains", {
    description: "Heroku custom domains for an app",
    columns: [
      { name: "id", type: "string" },
      { name: "hostname", type: "string" },
      { name: "kind", type: "string" },
      { name: "cname", type: "string" },
      { name: "status", type: "string" },
      { name: "app_name", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [{ name: "app_name", required: "required", operators: ["="] }],
    *list(ctx) {
      const app = ctx.quals.find((q) => q.column === "app_name")?.value;
      if (!app) return;
      const body = hkGet(ctx, `/apps/${app}/domains`);
      if (!body || !Array.isArray(body)) return;
      for (const d of body) {
        yield {
          id: d.id,
          hostname: d.hostname || "",
          kind: d.kind || "",
          cname: d.cname || "",
          status: d.status || "",
          app_name: app,
          created_at: d.created_at || "",
        };
      }
    },
  });
}
