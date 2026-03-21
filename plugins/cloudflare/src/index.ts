import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncExec, syncGet } from "dripline";

function getToken(): string | null {
  // 1. Env var
  if (process.env.CLOUDFLARE_API_TOKEN) return process.env.CLOUDFLARE_API_TOKEN;

  // 2. Wrangler OAuth token
  const configPath = join(
    homedir(),
    "Library",
    "Preferences",
    ".wrangler",
    "config",
    "default.toml",
  );
  if (existsSync(configPath)) {
    const content = readFileSync(configPath, "utf-8");
    const match = content.match(/oauth_token\s*=\s*"([^"]+)"/);
    if (match) return match[1];
  }

  // 3. Linux/other wrangler path
  const altPath = join(homedir(), ".wrangler", "config", "default.toml");
  if (existsSync(altPath)) {
    const content = readFileSync(altPath, "utf-8");
    const match = content.match(/oauth_token\s*=\s*"([^"]+)"/);
    if (match) return match[1];
  }

  return null;
}

function getAccountId(): string | null {
  if (process.env.CLOUDFLARE_ACCOUNT_ID)
    return process.env.CLOUDFLARE_ACCOUNT_ID;

  // Get from API
  const token = getToken();
  if (!token) return null;

  const resp = syncGet(
    "https://api.cloudflare.com/client/v4/accounts?per_page=1",
    {
      Authorization: `Bearer ${token}`,
    },
  );
  return resp.body?.result?.[0]?.id ?? null;
}

export default function cloudflare(dl: DriplinePluginAPI) {
  dl.setName("cloudflare");
  dl.setVersion("0.1.0");

  dl.setConnectionSchema({
    api_token: {
      type: "string",
      required: false,
      description: "Cloudflare API token (falls back to wrangler OAuth)",
      env: "CLOUDFLARE_API_TOKEN",
    },
    account_id: {
      type: "string",
      required: false,
      description: "Cloudflare account ID (auto-detected if not set)",
      env: "CLOUDFLARE_ACCOUNT_ID",
    },
  });

  function cfGet(path: string): any[] {
    const token = getToken();
    if (!token) {
      dl.log.warn("No Cloudflare auth found");
      return [];
    }

    const headers = {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    };
    const resp = syncGet(
      `https://api.cloudflare.com/client/v4${path}`,
      headers,
    );
    if (resp.status !== 200) return [];
    return resp.body?.result ?? [];
  }

  function accountPath(path: string): string {
    const accountId = getAccountId();
    if (!accountId) return path;
    return `/accounts/${accountId}${path}`;
  }

  dl.registerTable("cf_workers", {
    description: "Cloudflare Workers scripts",
    columns: [
      { name: "id", type: "string" },
      { name: "created_on", type: "datetime" },
      { name: "modified_on", type: "datetime" },
      { name: "has_modules", type: "boolean" },
      { name: "has_assets", type: "boolean" },
      { name: "logpush", type: "boolean" },
      { name: "tags", type: "json" },
    ],
    *list() {
      const items = cfGet(accountPath("/workers/scripts"));
      for (const w of items) {
        yield {
          id: w.id ?? "",
          created_on: w.created_on ?? "",
          modified_on: w.modified_on ?? "",
          has_modules: w.has_modules ? 1 : 0,
          has_assets: w.has_assets ? 1 : 0,
          logpush: w.logpush ? 1 : 0,
          tags: JSON.stringify(w.tags ?? []),
        };
      }
    },
  });

  dl.registerTable("cf_zones", {
    description: "Cloudflare DNS zones (domains)",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "status", type: "string" },
      { name: "plan", type: "string" },
      { name: "name_servers", type: "json" },
      { name: "created_on", type: "datetime" },
      { name: "modified_on", type: "datetime" },
    ],
    *list() {
      const accountId = getAccountId();
      const items = cfGet(`/zones?account.id=${accountId}&per_page=50`);
      for (const z of items) {
        yield {
          id: z.id ?? "",
          name: z.name ?? "",
          status: z.status ?? "",
          plan: z.plan?.name ?? "",
          name_servers: JSON.stringify(z.name_servers ?? []),
          created_on: z.created_on ?? "",
          modified_on: z.modified_on ?? "",
        };
      }
    },
  });

  dl.registerTable("cf_dns_records", {
    description: "DNS records for a zone. Use WHERE zone_name = 'example.com'",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "type", type: "string" },
      { name: "content", type: "string" },
      { name: "ttl", type: "number" },
      { name: "proxied", type: "boolean" },
      { name: "created_on", type: "datetime" },
      { name: "modified_on", type: "datetime" },
    ],
    keyColumns: [{ name: "zone_name", required: "required", operators: ["="] }],
    *list(ctx) {
      const zoneName = ctx.quals.find((q) => q.column === "zone_name")?.value;
      if (!zoneName) return;

      // First get zone ID
      const accountId = getAccountId();
      const zones = cfGet(`/zones?name=${zoneName}&account.id=${accountId}`);
      if (zones.length === 0) return;
      const zoneId = zones[0].id;

      const records = cfGet(`/zones/${zoneId}/dns_records?per_page=100`);
      for (const r of records) {
        yield {
          id: r.id ?? "",
          name: r.name ?? "",
          type: r.type ?? "",
          content: r.content ?? "",
          ttl: r.ttl ?? null,
          proxied: r.proxied ? 1 : 0,
          created_on: r.created_on ?? "",
          modified_on: r.modified_on ?? "",
        };
      }
    },
  });

  dl.registerTable("cf_pages_projects", {
    description: "Cloudflare Pages projects",
    columns: [
      { name: "name", type: "string" },
      { name: "subdomain", type: "string" },
      { name: "production_branch", type: "string" },
      { name: "domains", type: "json" },
      { name: "created_on", type: "datetime" },
      { name: "source_type", type: "string" },
    ],
    *list() {
      const items = cfGet(accountPath("/pages/projects"));
      for (const p of items) {
        yield {
          name: p.name ?? "",
          subdomain: p.subdomain ?? "",
          production_branch: p.production_branch ?? "",
          domains: JSON.stringify(p.domains ?? []),
          created_on: p.created_on ?? "",
          source_type: p.source?.type ?? "",
        };
      }
    },
  });

  dl.registerTable("cf_pages_deployments", {
    description:
      "Cloudflare Pages deployments. Use WHERE project_name = 'my-project'",
    columns: [
      { name: "id", type: "string" },
      { name: "environment", type: "string" },
      { name: "url", type: "string" },
      { name: "created_on", type: "datetime" },
      { name: "modified_on", type: "datetime" },
      { name: "is_skipped", type: "boolean" },
      { name: "source_hash", type: "string" },
    ],
    keyColumns: [
      { name: "project_name", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const projectName = ctx.quals.find(
        (q) => q.column === "project_name",
      )?.value;
      if (!projectName) return;

      const items = cfGet(
        accountPath(`/pages/projects/${projectName}/deployments`),
      );
      for (const d of items) {
        yield {
          id: d.id ?? "",
          environment: d.environment ?? "",
          url: d.url ?? "",
          created_on: d.created_on ?? "",
          modified_on: d.modified_on ?? "",
          is_skipped: d.is_skipped ? 1 : 0,
          source_hash: d.deployment_trigger?.metadata?.commit_hash ?? "",
        };
      }
    },
  });

  dl.registerTable("cf_d1_databases", {
    description: "Cloudflare D1 databases",
    columns: [
      { name: "uuid", type: "string" },
      { name: "name", type: "string" },
      { name: "version", type: "string" },
      { name: "num_tables", type: "number" },
      { name: "file_size", type: "number" },
      { name: "created_at", type: "datetime" },
    ],
    *list() {
      const items = cfGet(accountPath("/d1/database"));
      for (const db of items) {
        yield {
          uuid: db.uuid ?? "",
          name: db.name ?? "",
          version: db.version ?? "",
          num_tables: db.num_tables ?? null,
          file_size: db.file_size ?? null,
          created_at: db.created_at ?? "",
        };
      }
    },
  });

  dl.registerTable("cf_kv_namespaces", {
    description: "Cloudflare Workers KV namespaces",
    columns: [
      { name: "id", type: "string" },
      { name: "title", type: "string" },
      { name: "supports_url_encoding", type: "boolean" },
    ],
    *list() {
      const items = cfGet(accountPath("/storage/kv/namespaces"));
      for (const ns of items) {
        yield {
          id: ns.id ?? "",
          title: ns.title ?? "",
          supports_url_encoding: ns.supports_url_encoding ? 1 : 0,
        };
      }
    },
  });

  dl.registerTable("cf_r2_buckets", {
    description: "Cloudflare R2 storage buckets",
    columns: [
      { name: "name", type: "string" },
      { name: "creation_date", type: "datetime" },
      { name: "location", type: "string" },
    ],
    *list() {
      const items = cfGet(accountPath("/r2/buckets"));
      for (const b of items) {
        yield {
          name: b.name ?? "",
          creation_date: b.creation_date ?? "",
          location: b.location ?? "",
        };
      }
    },
  });

  dl.registerTable("cf_dns_lookup", {
    description:
      "DNS lookup via Cloudflare 1.1.1.1 (public, no auth needed). Use WHERE domain = 'example.com'",
    columns: [
      { name: "domain", type: "string" },
      { name: "record_type", type: "string" },
      { name: "value", type: "string" },
      { name: "ttl", type: "number" },
    ],
    keyColumns: [
      { name: "domain", required: "required", operators: ["="] },
      { name: "record_type", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const domain = ctx.quals.find((q) => q.column === "domain")?.value;
      if (!domain) return;

      const requestedType = ctx.quals.find(
        (q) => q.column === "record_type",
      )?.value;
      const types = requestedType
        ? [requestedType]
        : ["A", "AAAA", "CNAME", "MX", "TXT", "NS"];

      for (const type of types) {
        const resp = syncGet(
          `https://1.1.1.1/dns-query?name=${encodeURIComponent(domain)}&type=${type}`,
          { Accept: "application/dns-json" },
        );
        if (resp.status !== 200) continue;

        for (const answer of resp.body?.Answer ?? []) {
          yield {
            domain: answer.name ?? domain,
            record_type: dnsTypeToString(answer.type) ?? type,
            value: answer.data ?? "",
            ttl: answer.TTL ?? null,
          };
        }
      }
    },
  });

  dl.registerTable("cf_domain_check", {
    description:
      "Check domain availability via DNS. Use WHERE domain = 'example.com' or pass multiple TLDs with name = 'myapp' AND tlds = 'com,dev,sh,io'",
    columns: [
      { name: "domain", type: "string" },
      { name: "available", type: "boolean" },
      { name: "has_dns", type: "boolean" },
    ],
    keyColumns: [
      { name: "domain", required: "any_of", operators: ["="] },
      { name: "name_prefix", required: "any_of", operators: ["="] },
      { name: "tlds", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const singleDomain = ctx.quals.find((q) => q.column === "domain")?.value;
      const namePrefix = ctx.quals.find(
        (q) => q.column === "name_prefix",
      )?.value;
      const tlds = ctx.quals.find((q) => q.column === "tlds")?.value;

      let domains: string[] = [];
      if (singleDomain) {
        domains = [singleDomain];
      } else if (namePrefix) {
        const tldList = tlds
          ? tlds.split(",").map((t: string) => t.trim())
          : ["com", "dev", "io", "sh", "app", "co", "net", "org"];
        domains = tldList.map((tld: string) => `${namePrefix}.${tld}`);
      }

      for (const domain of domains) {
        const resp = syncGet(
          `https://1.1.1.1/dns-query?name=${encodeURIComponent(domain)}&type=A`,
          { Accept: "application/dns-json" },
        );
        if (resp.status !== 200) continue;

        const status = resp.body?.Status ?? -1;
        const hasAnswers = (resp.body?.Answer ?? []).length > 0;
        // Status 3 = NXDOMAIN (likely available), Status 0 with answers = taken
        // Note: NXDOMAIN doesn't guarantee registrar availability, but is a strong signal
        const available = status === 3;

        yield {
          domain,
          available: available ? 1 : 0,
          has_dns: hasAnswers ? 1 : 0,
        };
      }
    },
  });

  dl.registerTable("cf_queues", {
    description: "Cloudflare Queues (requires auth)",
    columns: [
      { name: "queue_id", type: "string" },
      { name: "queue_name", type: "string" },
      { name: "created_on", type: "datetime" },
      { name: "modified_on", type: "datetime" },
      { name: "producers_total_count", type: "number" },
      { name: "consumers_total_count", type: "number" },
    ],
    *list() {
      const items = cfGet(accountPath("/queues"));
      for (const q of items) {
        yield {
          queue_id: q.queue_id ?? q.id ?? "",
          queue_name: q.queue_name ?? q.name ?? "",
          created_on: q.created_on ?? "",
          modified_on: q.modified_on ?? "",
          producers_total_count: q.producers_total_count ?? null,
          consumers_total_count: q.consumers_total_count ?? null,
        };
      }
    },
  });
}

function dnsTypeToString(type: number): string {
  const types: Record<number, string> = {
    1: "A",
    2: "NS",
    5: "CNAME",
    6: "SOA",
    15: "MX",
    16: "TXT",
    28: "AAAA",
    33: "SRV",
    257: "CAA",
  };
  return types[type] ?? String(type);
}
