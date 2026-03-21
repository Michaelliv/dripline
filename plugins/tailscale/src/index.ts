import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const API = "https://api.tailscale.com/api/v2";

function tsGet(ctx: QueryContext, path: string): any {
  const apiKey = ctx.connection.config.api_key || "";
  // Tailscale API uses Basic auth with API key as username, empty password
  const auth = Buffer.from(`${apiKey}:`).toString("base64");
  const resp = syncGet(`${API}${path}`, { Authorization: `Basic ${auth}` });
  return resp.status === 200 ? resp.body : null;
}

function tailnet(ctx: QueryContext): string {
  return ctx.connection.config.tailnet || "-"; // "-" means the default tailnet for the key
}

export default function tailscale(dl: DriplinePluginAPI) {
  dl.setName("tailscale");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_key: {
      type: "string",
      required: true,
      description: "Tailscale API key",
      env: "TAILSCALE_API_KEY",
    },
    tailnet: {
      type: "string",
      required: false,
      description: "Tailnet name (default: - for key's tailnet)",
      env: "TAILSCALE_TAILNET",
    },
  });

  // GET /tailnet/{tailnet}/devices
  dl.registerTable("tailscale_devices", {
    description: "Devices on the Tailscale tailnet",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "hostname", type: "string" },
      { name: "user", type: "string" },
      { name: "os", type: "string" },
      { name: "client_version", type: "string" },
      { name: "addresses", type: "json" },
      { name: "tags", type: "json" },
      { name: "authorized", type: "boolean" },
      { name: "is_external", type: "boolean" },
      { name: "key_expiry_disabled", type: "boolean" },
      { name: "update_available", type: "boolean" },
      { name: "last_seen", type: "datetime" },
      { name: "created", type: "datetime" },
      { name: "expires", type: "datetime" },
    ],
    *list(ctx) {
      const body = tsGet(ctx, `/tailnet/${tailnet(ctx)}/devices`);
      if (!body?.devices) return;
      for (const d of body.devices) {
        yield {
          id: d.id || "",
          name: d.name || "",
          hostname: d.hostname || "",
          user: d.user || "",
          os: d.os || "",
          client_version: d.clientVersion || "",
          addresses: JSON.stringify(d.addresses || []),
          tags: JSON.stringify(d.tags || []),
          authorized: d.authorized ? 1 : 0,
          is_external: d.isExternal ? 1 : 0,
          key_expiry_disabled: d.keyExpiryDisabled ? 1 : 0,
          update_available: d.updateAvailable ? 1 : 0,
          last_seen: d.lastSeen || "",
          created: d.created || "",
          expires: d.expires || "",
        };
      }
    },
  });

  // GET /tailnet/{tailnet}/keys
  dl.registerTable("tailscale_keys", {
    description: "Tailscale auth keys",
    columns: [
      { name: "id", type: "string" },
      { name: "description", type: "string" },
      { name: "created", type: "datetime" },
      { name: "expires", type: "datetime" },
      { name: "revoked", type: "datetime" },
      { name: "capabilities", type: "json" },
    ],
    *list(ctx) {
      const body = tsGet(ctx, `/tailnet/${tailnet(ctx)}/keys`);
      if (!body?.keys && !Array.isArray(body)) return;
      const keys = body.keys || body;
      for (const k of keys) {
        yield {
          id: k.id || "",
          description: k.description || "",
          created: k.created || "",
          expires: k.expires || "",
          revoked: k.revoked || "",
          capabilities: JSON.stringify(k.capabilities || {}),
        };
      }
    },
  });

  // GET /tailnet/{tailnet}/dns/nameservers + searchpaths
  dl.registerTable("tailscale_dns", {
    description: "Tailscale DNS configuration",
    columns: [
      { name: "type", type: "string" },
      { name: "value", type: "string" },
    ],
    *list(ctx) {
      const ns = tsGet(ctx, `/tailnet/${tailnet(ctx)}/dns/nameservers`);
      if (ns?.dns) {
        for (const n of ns.dns) {
          yield { type: "nameserver", value: n };
        }
      }
      const sp = tsGet(ctx, `/tailnet/${tailnet(ctx)}/dns/searchpaths`);
      if (sp?.searchPaths) {
        for (const p of sp.searchPaths) {
          yield { type: "searchpath", value: p };
        }
      }
    },
  });
}
