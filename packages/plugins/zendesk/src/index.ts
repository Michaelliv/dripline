import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

function zdGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const subdomain = ctx.connection.config.subdomain || "";
  const email = ctx.connection.config.email || "";
  const token = ctx.connection.config.token || "";
  const qs = new URLSearchParams(params).toString();
  const url = `https://${subdomain}.zendesk.com/api/v2${path}${qs ? `?${qs}` : ""}`;
  // Zendesk API token auth: {email}/token:{token}
  const auth = Buffer.from(`${email}/token:${token}`).toString("base64");
  const resp = syncGet(url, { Authorization: `Basic ${auth}` });
  return resp.status === 200 ? resp.body : null;
}

function* zdPaginate(ctx: QueryContext, path: string, key: string) {
  let url: string | null = path;
  while (url) {
    const body = zdGet(ctx, url);
    if (!body?.[key]?.length) return;
    yield* body[key];
    // Zendesk cursor pagination: next_page URL
    const nextPage = body.next_page;
    if (!nextPage) return;
    // next_page is full URL, extract path
    try {
      const parsed = new URL(nextPage);
      url = parsed.pathname.replace("/api/v2", "") + parsed.search;
    } catch {
      return;
    }
  }
}

export default function zendesk(dl: DriplinePluginAPI) {
  dl.setName("zendesk");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    subdomain: {
      type: "string",
      required: true,
      description: "Zendesk subdomain",
      env: "ZENDESK_SUBDOMAIN",
    },
    email: {
      type: "string",
      required: true,
      description: "Zendesk user email",
      env: "ZENDESK_USER",
    },
    token: {
      type: "string",
      required: true,
      description: "Zendesk API token",
      env: "ZENDESK_TOKEN",
    },
  });

  // GET /api/v2/tickets -> tickets
  dl.registerTable("zendesk_tickets", {
    description: "Zendesk support tickets",
    columns: [
      { name: "id", type: "number" },
      { name: "subject", type: "string" },
      { name: "status", type: "string" },
      { name: "priority", type: "string" },
      { name: "type", type: "string" },
      { name: "requester_id", type: "number" },
      { name: "assignee_id", type: "number" },
      { name: "organization_id", type: "number" },
      { name: "tags", type: "json" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const t of zdPaginate(ctx, "/tickets", "tickets")) {
        yield {
          id: t.id,
          subject: t.subject || "",
          status: t.status || "",
          priority: t.priority || "",
          type: t.type || "",
          requester_id: t.requester_id || 0,
          assignee_id: t.assignee_id || 0,
          organization_id: t.organization_id || 0,
          tags: JSON.stringify(t.tags || []),
          created_at: t.created_at || "",
          updated_at: t.updated_at || "",
        };
      }
    },
  });

  // GET /api/v2/users -> users
  dl.registerTable("zendesk_users", {
    description: "Zendesk users",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "email", type: "string" },
      { name: "role", type: "string" },
      { name: "active", type: "boolean" },
      { name: "verified", type: "boolean" },
      { name: "suspended", type: "boolean" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const u of zdPaginate(ctx, "/users", "users")) {
        yield {
          id: u.id,
          name: u.name || "",
          email: u.email || "",
          role: u.role || "",
          active: u.active ? 1 : 0,
          verified: u.verified ? 1 : 0,
          suspended: u.suspended ? 1 : 0,
          created_at: u.created_at || "",
        };
      }
    },
  });

  // GET /api/v2/groups -> groups
  dl.registerTable("zendesk_groups", {
    description: "Zendesk groups",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "default", type: "boolean" },
      { name: "deleted", type: "boolean" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const g of zdPaginate(ctx, "/groups", "groups")) {
        yield {
          id: g.id,
          name: g.name || "",
          description: g.description || "",
          default: g.default ? 1 : 0,
          deleted: g.deleted ? 1 : 0,
          created_at: g.created_at || "",
        };
      }
    },
  });

  // GET /api/v2/organizations -> organizations
  dl.registerTable("zendesk_organizations", {
    description: "Zendesk organizations",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "domain_names", type: "json" },
      { name: "tags", type: "json" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const o of zdPaginate(ctx, "/organizations", "organizations")) {
        yield {
          id: o.id,
          name: o.name || "",
          domain_names: JSON.stringify(o.domain_names || []),
          tags: JSON.stringify(o.tags || []),
          created_at: o.created_at || "",
        };
      }
    },
  });

  // GET /api/v2/search?query=... -> search
  dl.registerTable("zendesk_search", {
    description: "Search Zendesk tickets, users, organizations",
    columns: [
      { name: "id", type: "number" },
      { name: "result_type", type: "string" },
      { name: "subject", type: "string" },
      { name: "name", type: "string" },
      { name: "status", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "url", type: "string" },
    ],
    keyColumns: [{ name: "query", required: "required", operators: ["="] }],
    *list(ctx) {
      const query = ctx.quals.find((q) => q.column === "query")?.value;
      if (!query) return;
      for (const r of zdPaginate(
        ctx,
        `/search?query=${encodeURIComponent(query)}`,
        "results",
      )) {
        yield {
          id: r.id || 0,
          result_type: r.result_type || "",
          subject: r.subject || "",
          name: r.name || "",
          status: r.status || "",
          created_at: r.created_at || "",
          url: r.url || "",
        };
      }
    },
  });
}
