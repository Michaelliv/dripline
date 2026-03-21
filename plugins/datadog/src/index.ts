import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

function ddGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const apiUrl = (
    ctx.connection.config.api_url || "https://api.datadoghq.com"
  ).replace(/\/$/, "");
  const apiKey = ctx.connection.config.api_key || "";
  const appKey = ctx.connection.config.app_key || "";
  const qs = new URLSearchParams(params).toString();
  const url = `${apiUrl}/api/v1${path}${qs ? `?${qs}` : ""}`;
  const resp = syncGet(url, {
    "DD-API-KEY": apiKey,
    "DD-APPLICATION-KEY": appKey,
    "Content-Type": "application/json",
  });
  return resp.status === 200 ? resp.body : null;
}

function ddGetV2(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const apiUrl = (
    ctx.connection.config.api_url || "https://api.datadoghq.com"
  ).replace(/\/$/, "");
  const apiKey = ctx.connection.config.api_key || "";
  const appKey = ctx.connection.config.app_key || "";
  const qs = new URLSearchParams(params).toString();
  const url = `${apiUrl}/api/v2${path}${qs ? `?${qs}` : ""}`;
  const resp = syncGet(url, {
    "DD-API-KEY": apiKey,
    "DD-APPLICATION-KEY": appKey,
    "Content-Type": "application/json",
  });
  return resp.status === 200 ? resp.body : null;
}

export default function datadog(dl: DriplinePluginAPI) {
  dl.setName("datadog");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_key: {
      type: "string",
      required: true,
      description: "Datadog API key",
      env: "DD_CLIENT_API_KEY",
    },
    app_key: {
      type: "string",
      required: true,
      description: "Datadog Application key",
      env: "DD_CLIENT_APP_KEY",
    },
    api_url: {
      type: "string",
      required: false,
      description: "Datadog API URL (default: https://api.datadoghq.com)",
      env: "DD_CLIENT_API_URL",
    },
  });

  dl.registerTable("dd_monitors", {
    description: "Datadog monitors",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "type", type: "string" },
      { name: "query", type: "string" },
      { name: "message", type: "string" },
      { name: "overall_state", type: "string" },
      { name: "priority", type: "number" },
      { name: "tags", type: "json" },
      { name: "created", type: "datetime" },
      { name: "modified", type: "datetime" },
    ],
    *list(ctx) {
      const body = ddGet(ctx, "/monitor");
      if (!body || !Array.isArray(body)) return;
      for (const m of body) {
        yield {
          id: m.id,
          name: m.name || "",
          type: m.type || "",
          query: m.query || "",
          message: m.message || "",
          overall_state: m.overall_state || "",
          priority: m.priority || 0,
          tags: JSON.stringify(m.tags || []),
          created: m.created || "",
          modified: m.modified || "",
        };
      }
    },
  });

  dl.registerTable("dd_dashboards", {
    description: "Datadog dashboards",
    columns: [
      { name: "id", type: "string" },
      { name: "title", type: "string" },
      { name: "description", type: "string" },
      { name: "layout_type", type: "string" },
      { name: "author_handle", type: "string" },
      { name: "url", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "modified_at", type: "datetime" },
    ],
    *list(ctx) {
      const body = ddGet(ctx, "/dashboard");
      if (!body?.dashboards) return;
      for (const d of body.dashboards) {
        yield {
          id: d.id,
          title: d.title || "",
          description: d.description || "",
          layout_type: d.layout_type || "",
          author_handle: d.author_handle || "",
          url: d.url || "",
          created_at: d.created_at || "",
          modified_at: d.modified_at || "",
        };
      }
    },
  });

  dl.registerTable("dd_hosts", {
    description: "Datadog hosts",
    columns: [
      { name: "name", type: "string" },
      { name: "id", type: "number" },
      { name: "aliases", type: "json" },
      { name: "apps", type: "json" },
      { name: "is_muted", type: "boolean" },
      { name: "last_reported_time", type: "datetime" },
      { name: "up", type: "boolean" },
      { name: "tags_by_source", type: "json" },
    ],
    *list(ctx) {
      let start = 0;
      while (true) {
        const body = ddGet(ctx, "/hosts", {
          start: String(start),
          count: "1000",
        });
        if (!body?.host_list?.length) return;
        for (const h of body.host_list) {
          yield {
            name: h.name || "",
            id: h.id || 0,
            aliases: JSON.stringify(h.aliases || []),
            apps: JSON.stringify(h.apps || []),
            is_muted: h.is_muted ? 1 : 0,
            last_reported_time: h.last_reported_time
              ? new Date(h.last_reported_time * 1000).toISOString()
              : "",
            up: h.up ? 1 : 0,
            tags_by_source: JSON.stringify(h.tags_by_source || {}),
          };
        }
        start += body.host_list.length;
        if (start >= (body.total_returned || 0)) return;
      }
    },
  });

  dl.registerTable("dd_users", {
    description: "Datadog users",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "email", type: "string" },
      { name: "handle", type: "string" },
      { name: "status", type: "string" },
      { name: "role", type: "string" },
      { name: "disabled", type: "boolean" },
      { name: "verified", type: "boolean" },
    ],
    *list(ctx) {
      let pageNum = 0;
      while (true) {
        const body = ddGetV2(ctx, "/users", {
          "page[size]": "100",
          "page[number]": String(pageNum),
        });
        if (!body?.data?.length) return;
        for (const u of body.data) {
          const a = u.attributes || {};
          yield {
            id: u.id || "",
            name: a.name || "",
            email: a.email || "",
            handle: a.handle || "",
            status: a.status || "",
            role: a.title || "",
            disabled: a.disabled ? 1 : 0,
            verified: a.verified ? 1 : 0,
          };
        }
        if (body.data.length < 100) return;
        pageNum++;
      }
    },
  });

  dl.registerTable("dd_slos", {
    description: "Datadog Service Level Objectives",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "type", type: "string" },
      { name: "target_threshold", type: "number" },
      { name: "tags", type: "json" },
      { name: "created_at", type: "datetime" },
      { name: "modified_at", type: "datetime" },
    ],
    *list(ctx) {
      let offset = 0;
      while (true) {
        const body = ddGet(ctx, "/slo", {
          limit: "100",
          offset: String(offset),
        });
        if (!body?.data?.length) return;
        for (const s of body.data) {
          yield {
            id: s.id,
            name: s.name || "",
            description: s.description || "",
            type: s.type || "",
            target_threshold: s.thresholds?.[0]?.target || 0,
            tags: JSON.stringify(s.tags || []),
            created_at: s.created_at
              ? new Date(s.created_at * 1000).toISOString()
              : "",
            modified_at: s.modified_at
              ? new Date(s.modified_at * 1000).toISOString()
              : "",
          };
        }
        offset += body.data.length;
        if (body.data.length < 100) return;
      }
    },
  });
}
