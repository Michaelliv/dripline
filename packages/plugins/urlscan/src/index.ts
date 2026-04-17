import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// urlscan.io API v1
// Auth: API-Key header (verified from urlscan-go SDK client.go)
// Endpoints: /api/v1/search, /api/v1/result/{uuid}

const API = "https://urlscan.io/api/v1";

function usGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const apiKey = ctx.connection.config.api_key || "";
  const qs = new URLSearchParams(params).toString();
  const url = `${API}${path}${qs ? `?${qs}` : ""}`;
  const headers: Record<string, string> = {};
  if (apiKey) headers["API-Key"] = apiKey;
  const resp = syncGet(url, headers);
  return resp.status === 200 ? resp.body : null;
}

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

export default function urlscan(dl: DriplinePluginAPI) {
  dl.setName("urlscan");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_key: {
      type: "string",
      required: false,
      description:
        "urlscan.io API key (optional for search, required for submissions)",
      env: "URLSCAN_API_KEY",
    },
  });

  // GET /api/v1/search?q={query}&size={size} (SDK: client.Search)
  dl.registerTable("urlscan_search", {
    description: "Search urlscan.io scan results",
    columns: [
      { name: "task_uuid", type: "string" },
      { name: "task_url", type: "string" },
      { name: "task_domain", type: "string" },
      { name: "task_time", type: "datetime" },
      { name: "page_url", type: "string" },
      { name: "page_domain", type: "string" },
      { name: "page_ip", type: "string" },
      { name: "page_server", type: "string" },
      { name: "page_country", type: "string" },
      { name: "page_status", type: "string" },
      { name: "page_title", type: "string" },
      { name: "verdicts_malicious", type: "boolean" },
      { name: "verdicts_score", type: "number" },
      { name: "screenshot", type: "string" },
    ],
    keyColumns: [{ name: "query", required: "required", operators: ["="] }],
    *list(ctx) {
      const query = getQual(ctx, "query");
      if (!query) return;
      const body = usGet(ctx, "/search", { q: query, size: "10000" });
      if (!body?.results) return;
      for (const r of body.results) {
        yield {
          task_uuid: r.task?.uuid || "",
          task_url: r.task?.url || "",
          task_domain: r.task?.domain || "",
          task_time: r.task?.time || "",
          page_url: r.page?.url || "",
          page_domain: r.page?.domain || "",
          page_ip: r.page?.ip || "",
          page_server: r.page?.server || "",
          page_country: r.page?.country || "",
          page_status: r.page?.status ? String(r.page.status) : "",
          page_title: r.page?.title || "",
          verdicts_malicious: r.verdicts?.overall?.malicious ? 1 : 0,
          verdicts_score: r.verdicts?.overall?.score || 0,
          screenshot: r.screenshot || "",
        };
      }
    },
  });

  // GET /api/v1/result/{uuid}/ (SDK: client.get("result/{uuid}"))
  dl.registerTable("urlscan_result", {
    description: "Detailed scan result from urlscan.io",
    columns: [
      { name: "task_uuid", type: "string" },
      { name: "task_url", type: "string" },
      { name: "task_domain", type: "string" },
      { name: "task_time", type: "datetime" },
      { name: "page_url", type: "string" },
      { name: "page_domain", type: "string" },
      { name: "page_ip", type: "string" },
      { name: "page_country", type: "string" },
      { name: "page_title", type: "string" },
      { name: "verdicts_malicious", type: "boolean" },
      { name: "verdicts_score", type: "number" },
      { name: "lists_ips", type: "json" },
      { name: "lists_urls", type: "json" },
      { name: "lists_domains", type: "json" },
    ],
    keyColumns: [{ name: "task_uuid", required: "required", operators: ["="] }],
    *list(ctx) {
      const uuid = getQual(ctx, "task_uuid");
      if (!uuid) return;
      const body = usGet(ctx, `/result/${uuid}/`);
      if (!body) return;
      yield {
        task_uuid: body.task?.uuid || uuid,
        task_url: body.task?.url || "",
        task_domain: body.task?.domain || "",
        task_time: body.task?.time || "",
        page_url: body.page?.url || "",
        page_domain: body.page?.domain || "",
        page_ip: body.page?.ip || "",
        page_country: body.page?.country || "",
        page_title: body.page?.title || "",
        verdicts_malicious: body.verdicts?.overall?.malicious ? 1 : 0,
        verdicts_score: body.verdicts?.overall?.score || 0,
        lists_ips: JSON.stringify(body.lists?.ips || []),
        lists_urls: JSON.stringify((body.lists?.urls || []).slice(0, 100)),
        lists_domains: JSON.stringify(body.lists?.domains || []),
      };
    },
  });
}
