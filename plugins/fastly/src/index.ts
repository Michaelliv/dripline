import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// Fastly REST API: api.fastly.com
// Auth: Fastly-Key header (verified from go-fastly SDK: APIKeyHeader = "Fastly-Key")
// Env: FASTLY_API_KEY

const API = "https://api.fastly.com";

function flGet(ctx: QueryContext, path: string): any {
  const key = ctx.connection.config.api_key || "";
  const resp = syncGet(`${API}${path}`, { "Fastly-Key": key });
  return resp.status === 200 ? resp.body : null;
}

export default function fastly(dl: DriplinePluginAPI) {
  dl.setName("fastly");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_key: {
      type: "string",
      required: true,
      description: "Fastly API key",
      env: "FASTLY_API_KEY",
    },
  });

  dl.registerTable("fastly_services", {
    description: "Fastly services",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "type", type: "string" },
      { name: "active_version", type: "number" },
      { name: "customer_id", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
    ],
    *list(ctx) {
      let page = 1;
      while (true) {
        const body = flGet(ctx, `/service?page=${page}&per_page=100`);
        if (!body || !Array.isArray(body) || body.length === 0) return;
        for (const s of body) {
          yield {
            id: s.id,
            name: s.name || "",
            type: s.type || "",
            active_version:
              s.active_version ||
              s.versions?.find((v: any) => v.active)?.number ||
              0,
            customer_id: s.customer_id || "",
            created_at: s.created_at || "",
            updated_at: s.updated_at || "",
          };
        }
        if (body.length < 100) return;
        page++;
      }
    },
  });

  dl.registerTable("fastly_service_domains", {
    description: "Domains for a Fastly service version",
    columns: [
      { name: "name", type: "string" },
      { name: "service_id", type: "string" },
      { name: "version", type: "number" },
      { name: "comment", type: "string" },
    ],
    keyColumns: [
      { name: "service_id", required: "required", operators: ["="] },
      { name: "version", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const sid = ctx.quals.find((q) => q.column === "service_id")?.value;
      const ver = ctx.quals.find((q) => q.column === "version")?.value;
      if (!sid || !ver) return;
      const body = flGet(ctx, `/service/${sid}/version/${ver}/domain`);
      if (!body || !Array.isArray(body)) return;
      for (const d of body) {
        yield {
          name: d.name || "",
          service_id: d.service_id || sid,
          version: d.version || Number(ver),
          comment: d.comment || "",
        };
      }
    },
  });

  dl.registerTable("fastly_service_backends", {
    description: "Backends for a Fastly service version",
    columns: [
      { name: "name", type: "string" },
      { name: "address", type: "string" },
      { name: "port", type: "number" },
      { name: "service_id", type: "string" },
      { name: "version", type: "number" },
      { name: "ssl_cert_hostname", type: "string" },
    ],
    keyColumns: [
      { name: "service_id", required: "required", operators: ["="] },
      { name: "version", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const sid = ctx.quals.find((q) => q.column === "service_id")?.value;
      const ver = ctx.quals.find((q) => q.column === "version")?.value;
      if (!sid || !ver) return;
      const body = flGet(ctx, `/service/${sid}/version/${ver}/backend`);
      if (!body || !Array.isArray(body)) return;
      for (const b of body) {
        yield {
          name: b.name || "",
          address: b.address || "",
          port: b.port || 0,
          service_id: b.service_id || sid,
          version: b.version || Number(ver),
          ssl_cert_hostname: b.ssl_cert_hostname || "",
        };
      }
    },
  });
}
