import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

function cfGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const baseUrl = (ctx.connection.config.base_url || "").replace(/\/$/, "");
  const username = ctx.connection.config.username || "";
  const token = ctx.connection.config.token || "";
  const isDataCenter = ctx.connection.config.deployment_type === "datacenter";

  // Cloud: /wiki/rest/api, Data Center: /rest/api
  const apiPrefix = isDataCenter ? "/rest/api" : "/wiki/rest/api";
  const qs = new URLSearchParams(params).toString();
  const url = `${baseUrl}${apiPrefix}${path}${qs ? `?${qs}` : ""}`;

  const headers: Record<string, string> = { "User-Agent": "dripline/0.1" };
  if (isDataCenter) {
    headers.Authorization = `Bearer ${token}`;
  } else {
    headers.Authorization = `Basic ${Buffer.from(`${username}:${token}`).toString("base64")}`;
  }

  const resp = syncGet(url, headers);
  return resp.status === 200 ? resp.body : null;
}

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

export default function confluence(dl: DriplinePluginAPI) {
  dl.setName("confluence");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    base_url: {
      type: "string",
      required: true,
      description:
        "Confluence instance URL (e.g. https://yoursite.atlassian.net)",
      env: "CONFLUENCE_URL",
    },
    username: {
      type: "string",
      required: false,
      description: "Confluence username (email for Cloud)",
      env: "CONFLUENCE_USERNAME",
    },
    token: {
      type: "string",
      required: true,
      description: "API token (Cloud) or personal access token (Data Center)",
      env: "CONFLUENCE_TOKEN",
    },
    deployment_type: {
      type: "string",
      required: false,
      description: "cloud (default) or datacenter",
      env: "CONFLUENCE_DEPLOYMENT_TYPE",
    },
  });

  // GET /content -> pages/blogposts
  dl.registerTable("confluence_content", {
    description: "Confluence content (pages and blog posts)",
    columns: [
      { name: "id", type: "string" },
      { name: "title", type: "string" },
      { name: "type", type: "string" },
      { name: "status", type: "string" },
      { name: "space_key", type: "string" },
      { name: "version_number", type: "number" },
    ],
    *list(ctx) {
      let start = 0;
      while (true) {
        const body = cfGet(ctx, "/content", {
          limit: "100",
          start: String(start),
          expand: "space,version",
        });
        if (!body?.results?.length) return;
        for (const c of body.results) {
          yield {
            id: c.id || "",
            title: c.title || "",
            type: c.type || "",
            status: c.status || "",
            space_key: c.space?.key || "",
            version_number: c.version?.number || 0,
          };
        }
        start += body.results.length;
        if (
          body.results.length < 100 ||
          start >= (body.size || body.totalSize || Infinity)
        )
          return;
      }
    },
  });

  // GET /space -> spaces
  dl.registerTable("confluence_spaces", {
    description: "Confluence spaces",
    columns: [
      { name: "id", type: "number" },
      { name: "key", type: "string" },
      { name: "name", type: "string" },
      { name: "type", type: "string" },
      { name: "status", type: "string" },
    ],
    *list(ctx) {
      let start = 0;
      while (true) {
        const body = cfGet(ctx, "/space", {
          limit: "100",
          start: String(start),
        });
        if (!body?.results?.length) return;
        for (const s of body.results) {
          yield {
            id: s.id || 0,
            key: s.key || "",
            name: s.name || "",
            type: s.type || "",
            status: s.status || "",
          };
        }
        start += body.results.length;
        if (body.results.length < 100) return;
      }
    },
  });

  // GET /search?cql=... -> search
  dl.registerTable("confluence_search", {
    description: "Search Confluence content using CQL",
    columns: [
      { name: "id", type: "string" },
      { name: "title", type: "string" },
      { name: "type", type: "string" },
      { name: "space_key", type: "string" },
      { name: "url", type: "string" },
      { name: "last_modified", type: "string" },
      { name: "excerpt", type: "string" },
    ],
    keyColumns: [{ name: "cql", required: "required", operators: ["="] }],
    *list(ctx) {
      const cql = getQual(ctx, "cql");
      if (!cql) return;
      let start = 0;
      while (true) {
        const body = cfGet(ctx, "/search", {
          cql,
          limit: "100",
          start: String(start),
        });
        if (!body?.results?.length) return;
        for (const r of body.results) {
          const content = r.content || r;
          yield {
            id: content.id || "",
            title: r.title || content.title || "",
            type: content.type || r.entityType || "",
            space_key:
              content.space?.key || r.resultGlobalContainer?.title || "",
            url: r.url || content._links?.webui || "",
            last_modified: r.lastModified || content.version?.when || "",
            excerpt: r.excerpt || "",
          };
        }
        start += body.results.length;
        if (body.results.length < 100) return;
      }
    },
  });
}
