import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

function gfGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const baseUrl = (ctx.connection.config.url || "").replace(/\/$/, "");
  const auth = ctx.connection.config.auth || "";
  const qs = new URLSearchParams(params).toString();
  const url = `${baseUrl}/api${path}${qs ? `?${qs}` : ""}`;

  const headers: Record<string, string> = {};
  // Auth can be "user:password" (basic) or just an API key (bearer)
  if (auth.includes(":")) {
    headers.Authorization = `Basic ${Buffer.from(auth).toString("base64")}`;
  } else {
    headers.Authorization = `Bearer ${auth}`;
  }

  const resp = syncGet(url, headers);
  return resp.status === 200 ? resp.body : null;
}

export default function grafana(dl: DriplinePluginAPI) {
  dl.setName("grafana");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    url: {
      type: "string",
      required: true,
      description: "Grafana instance URL",
      env: "GRAFANA_URL",
    },
    auth: {
      type: "string",
      required: true,
      description: "API key or user:password",
      env: "GRAFANA_AUTH",
    },
  });

  // GET /api/search?type=dash-db -> dashboards (uses search API like steampipe)
  dl.registerTable("grafana_dashboards", {
    description: "Grafana dashboards",
    columns: [
      { name: "id", type: "number" },
      { name: "uid", type: "string" },
      { name: "title", type: "string" },
      { name: "uri", type: "string" },
      { name: "url", type: "string" },
      { name: "dashboard_type", type: "string" },
      { name: "folder_id", type: "number" },
      { name: "folder_title", type: "string" },
      { name: "is_starred", type: "boolean" },
      { name: "tags", type: "json" },
    ],
    *list(ctx) {
      let page = 1;
      while (true) {
        const body = gfGet(ctx, "/search", {
          type: "dash-db",
          limit: "1000",
          page: String(page),
        });
        if (!body || !Array.isArray(body) || body.length === 0) return;
        for (const d of body) {
          yield {
            id: d.id,
            uid: d.uid || "",
            title: d.title || "",
            uri: d.uri || "",
            url: d.url || "",
            dashboard_type: d.type || "",
            folder_id: d.folderId || 0,
            folder_title: d.folderTitle || "",
            is_starred: d.isStarred ? 1 : 0,
            tags: JSON.stringify(d.tags || []),
          };
        }
        if (body.length < 1000) return;
        page++;
      }
    },
  });

  // GET /api/datasources -> data sources
  dl.registerTable("grafana_data_sources", {
    description: "Grafana data sources",
    columns: [
      { name: "id", type: "number" },
      { name: "uid", type: "string" },
      { name: "name", type: "string" },
      { name: "type", type: "string" },
      { name: "url", type: "string" },
      { name: "database", type: "string" },
      { name: "is_default", type: "boolean" },
      { name: "read_only", type: "boolean" },
    ],
    *list(ctx) {
      const body = gfGet(ctx, "/datasources");
      if (!body || !Array.isArray(body)) return;
      for (const ds of body) {
        yield {
          id: ds.id,
          uid: ds.uid || "",
          name: ds.name || "",
          type: ds.type || "",
          url: ds.url || "",
          database: ds.database || "",
          is_default: ds.isDefault ? 1 : 0,
          read_only: ds.readOnly ? 1 : 0,
        };
      }
    },
  });

  // GET /api/orgs -> organizations (admin only)
  dl.registerTable("grafana_organizations", {
    description: "Grafana organizations",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
    ],
    *list(ctx) {
      const body = gfGet(ctx, "/orgs");
      if (!body || !Array.isArray(body)) return;
      for (const o of body) {
        yield { id: o.id, name: o.name || "" };
      }
    },
  });

  // GET /api/org/users -> users in current org
  dl.registerTable("grafana_users", {
    description: "Grafana users in the current organization",
    columns: [
      { name: "user_id", type: "number" },
      { name: "login", type: "string" },
      { name: "email", type: "string" },
      { name: "name", type: "string" },
      { name: "role", type: "string" },
      { name: "last_seen_at", type: "datetime" },
    ],
    *list(ctx) {
      const body = gfGet(ctx, "/org/users");
      if (!body || !Array.isArray(body)) return;
      for (const u of body) {
        yield {
          user_id: u.userId,
          login: u.login || "",
          email: u.email || "",
          name: u.name || "",
          role: u.role || "",
          last_seen_at: u.lastSeenAt || "",
        };
      }
    },
  });

  // GET /api/teams/search -> teams
  dl.registerTable("grafana_teams", {
    description: "Grafana teams",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "email", type: "string" },
      { name: "member_count", type: "number" },
    ],
    *list(ctx) {
      let page = 1;
      while (true) {
        const body = gfGet(ctx, "/teams/search", {
          page: String(page),
          perpage: "1000",
        });
        if (!body?.teams?.length) return;
        for (const t of body.teams) {
          yield {
            id: t.id,
            name: t.name || "",
            email: t.email || "",
            member_count: t.memberCount || 0,
          };
        }
        if (body.teams.length < 1000) return;
        page++;
      }
    },
  });

  // GET /api/folders -> folders
  dl.registerTable("grafana_folders", {
    description: "Grafana dashboard folders",
    columns: [
      { name: "id", type: "number" },
      { name: "uid", type: "string" },
      { name: "title", type: "string" },
      { name: "url", type: "string" },
    ],
    *list(ctx) {
      const body = gfGet(ctx, "/folders");
      if (!body || !Array.isArray(body)) return;
      for (const f of body) {
        yield {
          id: f.id,
          uid: f.uid || "",
          title: f.title || "",
          url: f.url || "",
        };
      }
    },
  });

  // GET /api/v1/provisioning/alert-rules -> alert rules
  dl.registerTable("grafana_alert_rules", {
    description: "Grafana alert rules",
    columns: [
      { name: "id", type: "number" },
      { name: "uid", type: "string" },
      { name: "title", type: "string" },
      { name: "condition", type: "string" },
      { name: "folder_uid", type: "string" },
      { name: "rule_group", type: "string" },
      { name: "no_data_state", type: "string" },
      { name: "exec_err_state", type: "string" },
      { name: "is_paused", type: "boolean" },
    ],
    *list(ctx) {
      const body = gfGet(ctx, "/v1/provisioning/alert-rules");
      if (!body || !Array.isArray(body)) return;
      for (const r of body) {
        yield {
          id: r.id || 0,
          uid: r.uid || "",
          title: r.title || "",
          condition: r.condition || "",
          folder_uid: r.folderUID || "",
          rule_group: r.ruleGroup || "",
          no_data_state: r.noDataState || "",
          exec_err_state: r.execErrState || "",
          is_paused: r.isPaused ? 1 : 0,
        };
      }
    },
  });
}
