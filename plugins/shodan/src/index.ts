import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const API = "https://api.shodan.io";
function q(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}
function sGet(ctx: QueryContext, path: string) {
  const key = ctx.connection.config.api_key || "";
  const sep = path.includes("?") ? "&" : "?";
  const resp = syncGet(`${API}${path}${sep}key=${key}`, {});
  return resp.status === 200 ? resp.body : null;
}

export default function shodan(dl: DriplinePluginAPI) {
  dl.setName("shodan");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_key: {
      type: "string",
      required: true,
      description: "Shodan API key",
      env: "SHODAN_API_KEY",
    },
  });

  dl.registerTable("shodan_host", {
    description: "Shodan host information for an IP",
    columns: [
      { name: "ip_str", type: "string" },
      { name: "hostnames", type: "json" },
      { name: "org", type: "string" },
      { name: "os", type: "string" },
      { name: "ports", type: "json" },
      { name: "vulns", type: "json" },
      { name: "city", type: "string" },
      { name: "country_name", type: "string" },
      { name: "isp", type: "string" },
      { name: "last_update", type: "string" },
    ],
    keyColumns: [{ name: "ip", required: "required", operators: ["="] }],
    *list(ctx) {
      const ip = q(ctx, "ip");
      if (!ip) return;
      const body = sGet(ctx, `/shodan/host/${ip}`) as any;
      if (!body) return;
      yield {
        ip_str: body.ip_str || ip,
        hostnames: JSON.stringify(body.hostnames || []),
        org: body.org || "",
        os: body.os || "",
        ports: JSON.stringify(body.ports || []),
        vulns: JSON.stringify(body.vulns || []),
        city: body.city || "",
        country_name: body.country_name || "",
        isp: body.isp || "",
        last_update: body.last_update || "",
      };
    },
  });

  dl.registerTable("shodan_search", {
    description: "Search Shodan for hosts",
    columns: [
      { name: "ip_str", type: "string" },
      { name: "port", type: "number" },
      { name: "org", type: "string" },
      { name: "hostnames", type: "json" },
      { name: "os", type: "string" },
      { name: "product", type: "string" },
      { name: "version", type: "string" },
      { name: "transport", type: "string" },
      { name: "country_name", type: "string" },
    ],
    keyColumns: [{ name: "query", required: "required", operators: ["="] }],
    *list(ctx) {
      const query = q(ctx, "query");
      if (!query) return;
      const body = sGet(
        ctx,
        `/shodan/host/search?query=${encodeURIComponent(query)}`,
      ) as any;
      if (!body?.matches) return;
      for (const m of body.matches) {
        yield {
          ip_str: m.ip_str || "",
          port: m.port || 0,
          org: m.org || "",
          hostnames: JSON.stringify(m.hostnames || []),
          os: m.os || "",
          product: m.product || "",
          version: m.version || "",
          transport: m.transport || "",
          country_name: m.location?.country_name || "",
        };
      }
    },
  });

  dl.registerTable("shodan_dns_resolve", {
    description: "Resolve hostnames to IPs via Shodan",
    columns: [
      { name: "hostname", type: "string" },
      { name: "ip", type: "string" },
    ],
    keyColumns: [{ name: "hostnames", required: "required", operators: ["="] }],
    *list(ctx) {
      const hostnames = q(ctx, "hostnames");
      if (!hostnames) return;
      const body = sGet(
        ctx,
        `/dns/resolve?hostnames=${encodeURIComponent(hostnames)}`,
      ) as any;
      if (!body) return;
      for (const [hostname, ip] of Object.entries(body)) {
        yield { hostname, ip: ip as string };
      }
    },
  });
}
