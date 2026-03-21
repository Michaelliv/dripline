import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// Okta REST API v1: {domain}/api/v1/
// Auth: SSWS {token} header (Okta API tokens)
// Pagination: Link header with rel="next", or ?after= cursor
// Verified from: okta-sdk-golang client.User.ListUsers, client.Group.ListGroups, client.Application.ListApplications

function oktaGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const domain = (ctx.connection.config.domain || "").replace(/\/$/, "");
  const token = ctx.connection.config.token || "";
  const qs = new URLSearchParams(params).toString();
  const url = `${domain}/api/v1${path}${qs ? `?${qs}` : ""}`;
  const resp = syncGet(url, {
    Authorization: `SSWS ${token}`,
    Accept: "application/json",
  });
  return resp.status === 200 ? resp.body : null;
}

function* oktaPaginate(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
) {
  // Okta uses cursor-based pagination with ?after= param
  let after = "";
  while (true) {
    const p = { ...params, limit: "200" };
    if (after) p.after = after;
    const body = oktaGet(ctx, path, p);
    if (!body || !Array.isArray(body) || body.length === 0) return;
    yield* body;
    if (body.length < 200) return;
    // Use last item's id as cursor
    after = body[body.length - 1]?.id || "";
    if (!after) return;
  }
}

export default function okta(dl: DriplinePluginAPI) {
  dl.setName("okta");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    domain: {
      type: "string",
      required: true,
      description: "Okta org URL (e.g. https://myorg.okta.com)",
      env: "OKTA_CLIENT_ORGURL",
    },
    token: {
      type: "string",
      required: true,
      description: "Okta API token",
      env: "OKTA_CLIENT_TOKEN",
    },
  });

  // GET /api/v1/users (client.User.ListUsers)
  dl.registerTable("okta_users", {
    description: "Okta users",
    columns: [
      { name: "id", type: "string" },
      { name: "login", type: "string" },
      { name: "email", type: "string" },
      { name: "first_name", type: "string" },
      { name: "last_name", type: "string" },
      { name: "status", type: "string" },
      { name: "created", type: "datetime" },
      { name: "last_login", type: "datetime" },
      { name: "last_updated", type: "datetime" },
    ],
    *list(ctx) {
      for (const u of oktaPaginate(ctx, "/users")) {
        yield {
          id: u.id || "",
          login: u.profile?.login || "",
          email: u.profile?.email || "",
          first_name: u.profile?.firstName || "",
          last_name: u.profile?.lastName || "",
          status: u.status || "",
          created: u.created || "",
          last_login: u.lastLogin || "",
          last_updated: u.lastUpdated || "",
        };
      }
    },
  });

  // GET /api/v1/groups (client.Group.ListGroups)
  dl.registerTable("okta_groups", {
    description: "Okta groups",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "type", type: "string" },
      { name: "created", type: "datetime" },
      { name: "last_updated", type: "datetime" },
      { name: "last_membership_updated", type: "datetime" },
    ],
    *list(ctx) {
      for (const g of oktaPaginate(ctx, "/groups")) {
        yield {
          id: g.id || "",
          name: g.profile?.name || "",
          description: g.profile?.description || "",
          type: g.type || "",
          created: g.created || "",
          last_updated: g.lastUpdated || "",
          last_membership_updated: g.lastMembershipUpdated || "",
        };
      }
    },
  });

  // GET /api/v1/apps (client.Application.ListApplications)
  dl.registerTable("okta_applications", {
    description: "Okta applications",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "label", type: "string" },
      { name: "status", type: "string" },
      { name: "sign_on_mode", type: "string" },
      { name: "created", type: "datetime" },
      { name: "last_updated", type: "datetime" },
    ],
    *list(ctx) {
      for (const a of oktaPaginate(ctx, "/apps")) {
        yield {
          id: a.id || "",
          name: a.name || "",
          label: a.label || "",
          status: a.status || "",
          sign_on_mode: a.signOnMode || "",
          created: a.created || "",
          last_updated: a.lastUpdated || "",
        };
      }
    },
  });

  // GET /api/v1/authorizationServers (client.AuthorizationServer.ListAuthorizationServers)
  dl.registerTable("okta_auth_servers", {
    description: "Okta authorization servers",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "issuer", type: "string" },
      { name: "status", type: "string" },
      { name: "audiences", type: "json" },
      { name: "created", type: "datetime" },
    ],
    *list(ctx) {
      for (const s of oktaPaginate(ctx, "/authorizationServers")) {
        yield {
          id: s.id || "",
          name: s.name || "",
          description: s.description || "",
          issuer: s.issuer || "",
          status: s.status || "",
          audiences: JSON.stringify(s.audiences || []),
          created: s.created || "",
        };
      }
    },
  });

  // GET /api/v1/trustedOrigins
  dl.registerTable("okta_trusted_origins", {
    description: "Okta trusted origins",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "origin", type: "string" },
      { name: "status", type: "string" },
      { name: "scopes", type: "json" },
      { name: "created", type: "datetime" },
    ],
    *list(ctx) {
      for (const t of oktaPaginate(ctx, "/trustedOrigins")) {
        yield {
          id: t.id || "",
          name: t.name || "",
          origin: t.origin || "",
          status: t.status || "",
          scopes: JSON.stringify((t.scopes || []).map((s: any) => s.type)),
          created: t.created || "",
        };
      }
    },
  });
}
