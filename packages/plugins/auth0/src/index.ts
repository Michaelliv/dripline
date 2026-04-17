import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

function a0Get(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const domain = ctx.connection.config.domain || "";
  const token = ctx.connection.config.api_token || "";
  const qs = new URLSearchParams(params).toString();
  const url = `https://${domain}/api/v2${path}${qs ? `?${qs}` : ""}`;
  const resp = syncGet(url, { Authorization: `Bearer ${token}` });
  return resp.status === 200 ? resp.body : null;
}

function* a0Paginate(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
) {
  let page = 0;
  while (true) {
    const body = a0Get(ctx, path, {
      ...params,
      page: String(page),
      per_page: "100",
      include_totals: "true",
    });
    if (!body) return;
    // Auth0 wraps paginated results in various keys or returns array with totals
    const items = Array.isArray(body)
      ? body
      : body.users ||
        body.clients ||
        body.connections ||
        body.roles ||
        body.organizations ||
        body.logs ||
        body.actions ||
        [];
    if (items.length === 0) return;
    yield* items;
    const total = body.total || body.length || 0;
    if ((page + 1) * 100 >= total) return;
    page++;
  }
}

export default function auth0(dl: DriplinePluginAPI) {
  dl.setName("auth0");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    domain: {
      type: "string",
      required: true,
      description: "Auth0 domain (e.g. myapp.auth0.com)",
      env: "AUTH0_DOMAIN",
    },
    api_token: {
      type: "string",
      required: true,
      description: "Auth0 Management API token",
      env: "AUTH0_API_TOKEN",
    },
  });

  // GET /api/v2/users
  dl.registerTable("auth0_users", {
    description: "Auth0 users",
    columns: [
      { name: "user_id", type: "string" },
      { name: "email", type: "string" },
      { name: "name", type: "string" },
      { name: "nickname", type: "string" },
      { name: "picture", type: "string" },
      { name: "email_verified", type: "boolean" },
      { name: "blocked", type: "boolean" },
      { name: "logins_count", type: "number" },
      { name: "last_login", type: "datetime" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const u of a0Paginate(ctx, "/users")) {
        yield {
          user_id: u.user_id || "",
          email: u.email || "",
          name: u.name || "",
          nickname: u.nickname || "",
          picture: u.picture || "",
          email_verified: u.email_verified ? 1 : 0,
          blocked: u.blocked ? 1 : 0,
          logins_count: u.logins_count || 0,
          last_login: u.last_login || "",
          created_at: u.created_at || "",
          updated_at: u.updated_at || "",
        };
      }
    },
  });

  // GET /api/v2/clients
  dl.registerTable("auth0_clients", {
    description: "Auth0 applications (clients)",
    columns: [
      { name: "client_id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "app_type", type: "string" },
      { name: "is_first_party", type: "boolean" },
      { name: "callbacks", type: "json" },
      { name: "allowed_origins", type: "json" },
    ],
    *list(ctx) {
      for (const c of a0Paginate(ctx, "/clients")) {
        yield {
          client_id: c.client_id || "",
          name: c.name || "",
          description: c.description || "",
          app_type: c.app_type || "",
          is_first_party: c.is_first_party ? 1 : 0,
          callbacks: JSON.stringify(c.callbacks || []),
          allowed_origins: JSON.stringify(c.allowed_origins || []),
        };
      }
    },
  });

  // GET /api/v2/connections
  dl.registerTable("auth0_connections", {
    description: "Auth0 connections (identity providers)",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "strategy", type: "string" },
      { name: "enabled_clients", type: "json" },
    ],
    *list(ctx) {
      for (const c of a0Paginate(ctx, "/connections")) {
        yield {
          id: c.id || "",
          name: c.name || "",
          strategy: c.strategy || "",
          enabled_clients: JSON.stringify(c.enabled_clients || []),
        };
      }
    },
  });

  // GET /api/v2/roles
  dl.registerTable("auth0_roles", {
    description: "Auth0 roles",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
    ],
    *list(ctx) {
      for (const r of a0Paginate(ctx, "/roles")) {
        yield {
          id: r.id || "",
          name: r.name || "",
          description: r.description || "",
        };
      }
    },
  });

  // GET /api/v2/organizations
  dl.registerTable("auth0_organizations", {
    description: "Auth0 organizations",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "display_name", type: "string" },
    ],
    *list(ctx) {
      for (const o of a0Paginate(ctx, "/organizations")) {
        yield {
          id: o.id || "",
          name: o.name || "",
          display_name: o.display_name || "",
        };
      }
    },
  });

  // GET /api/v2/logs
  dl.registerTable("auth0_logs", {
    description: "Auth0 log events",
    columns: [
      { name: "log_id", type: "string" },
      { name: "type", type: "string" },
      { name: "date", type: "datetime" },
      { name: "description", type: "string" },
      { name: "client_name", type: "string" },
      { name: "user_name", type: "string" },
      { name: "ip", type: "string" },
    ],
    *list(ctx) {
      for (const l of a0Paginate(ctx, "/logs")) {
        yield {
          log_id: l.log_id || l._id || "",
          type: l.type || "",
          date: l.date || "",
          description: l.description || "",
          client_name: l.client_name || "",
          user_name: l.user_name || "",
          ip: l.ip || "",
        };
      }
    },
  });
}
