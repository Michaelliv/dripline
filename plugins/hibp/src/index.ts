import { createHash } from "node:crypto";
import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const API = "https://haveibeenpwned.com/api/v3";
const PWN_API = "https://api.pwnedpasswords.com";

function hibpGet(ctx: QueryContext, path: string): any {
  const apiKey = ctx.connection.config.api_key || "";
  const headers: Record<string, string> = {
    "User-Agent": "dripline-hibp-plugin",
  };
  if (apiKey) headers["hibp-api-key"] = apiKey;
  const resp = syncGet(`${API}${path}`, headers);
  return resp.status === 200 ? resp.body : null;
}

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

export default function hibp(dl: DriplinePluginAPI) {
  dl.setName("hibp");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_key: {
      type: "string",
      required: false,
      description:
        "HIBP API key (required for breached accounts/pastes, not for breaches list or passwords)",
      env: "HIBP_API_KEY",
    },
  });

  // GET /breaches -> all known breaches (no API key needed)
  dl.registerTable("hibp_breaches", {
    description: "All breaches tracked by Have I Been Pwned",
    columns: [
      { name: "name", type: "string" },
      { name: "title", type: "string" },
      { name: "domain", type: "string" },
      { name: "breach_date", type: "string" },
      { name: "added_date", type: "datetime" },
      { name: "modified_date", type: "datetime" },
      { name: "pwn_count", type: "number" },
      { name: "description", type: "string" },
      { name: "data_classes", type: "json" },
      { name: "is_verified", type: "boolean" },
      { name: "is_sensitive", type: "boolean" },
      { name: "is_retired", type: "boolean" },
    ],
    *list(ctx) {
      const body = hibpGet(ctx, "/breaches");
      if (!body || !Array.isArray(body)) return;
      for (const b of body) {
        yield {
          name: b.Name || "",
          title: b.Title || "",
          domain: b.Domain || "",
          breach_date: b.BreachDate || "",
          added_date: b.AddedDate || "",
          modified_date: b.ModifiedDate || "",
          pwn_count: b.PwnCount || 0,
          description: (b.Description || "").slice(0, 500),
          data_classes: JSON.stringify(b.DataClasses || []),
          is_verified: b.IsVerified ? 1 : 0,
          is_sensitive: b.IsSensitive ? 1 : 0,
          is_retired: b.IsRetired ? 1 : 0,
        };
      }
    },
  });

  // GET /breachedaccount/{email} -> breaches for a specific email (requires API key)
  dl.registerTable("hibp_breached_account", {
    description: "Breaches for a specific email address (requires API key)",
    columns: [
      { name: "email", type: "string" },
      { name: "name", type: "string" },
      { name: "title", type: "string" },
      { name: "domain", type: "string" },
      { name: "breach_date", type: "string" },
      { name: "pwn_count", type: "number" },
      { name: "data_classes", type: "json" },
    ],
    keyColumns: [{ name: "email", required: "required", operators: ["="] }],
    *list(ctx) {
      const email = getQual(ctx, "email");
      if (!email) return;
      const body = hibpGet(
        ctx,
        `/breachedaccount/${encodeURIComponent(email)}?truncateResponse=false`,
      );
      if (!body || !Array.isArray(body)) return;
      for (const b of body) {
        yield {
          email,
          name: b.Name || "",
          title: b.Title || "",
          domain: b.Domain || "",
          breach_date: b.BreachDate || "",
          pwn_count: b.PwnCount || 0,
          data_classes: JSON.stringify(b.DataClasses || []),
        };
      }
    },
  });

  // Password check via k-anonymity (no API key needed)
  // Uses api.pwnedpasswords.com/range/{first5 of SHA-1}
  dl.registerTable("hibp_password", {
    description:
      "Check if a password has been compromised (k-anonymity, no password sent to API)",
    columns: [
      { name: "hash", type: "string" },
      { name: "count", type: "number" },
    ],
    keyColumns: [
      { name: "plaintext", required: "optional", operators: ["="] },
      { name: "hash_prefix", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      let hashHex = "";
      const plaintext = getQual(ctx, "plaintext");
      const hashPrefix = getQual(ctx, "hash_prefix");

      if (plaintext) {
        hashHex = createHash("sha1")
          .update(plaintext)
          .digest("hex")
          .toUpperCase();
      } else if (hashPrefix) {
        hashHex = hashPrefix.toUpperCase();
      } else {
        return;
      }

      const prefix = hashHex.slice(0, 5);
      const resp = syncGet(`${PWN_API}/range/${prefix}`, {
        "User-Agent": "dripline-hibp",
      });
      if (resp.status !== 200) return;
      const text =
        typeof resp.body === "string" ? resp.body : JSON.stringify(resp.body);

      for (const line of text.split("\n")) {
        const [suffix, countStr] = line.trim().split(":");
        if (!suffix) continue;
        const fullHash = prefix + suffix;
        if (fullHash.startsWith(hashHex)) {
          yield { hash: fullHash, count: parseInt(countStr) || 0 };
        }
      }
    },
  });

  // GET /pasteaccount/{email} -> pastes for an email (requires API key)
  dl.registerTable("hibp_pastes", {
    description:
      "Pastes containing a specific email address (requires API key)",
    columns: [
      { name: "email", type: "string" },
      { name: "source", type: "string" },
      { name: "id", type: "string" },
      { name: "title", type: "string" },
      { name: "date", type: "datetime" },
      { name: "email_count", type: "number" },
    ],
    keyColumns: [{ name: "email", required: "required", operators: ["="] }],
    *list(ctx) {
      const email = getQual(ctx, "email");
      if (!email) return;
      const body = hibpGet(ctx, `/pasteaccount/${encodeURIComponent(email)}`);
      if (!body || !Array.isArray(body)) return;
      for (const p of body) {
        yield {
          email,
          source: p.Source || "",
          id: p.Id || "",
          title: p.Title || "",
          date: p.Date || "",
          email_count: p.EmailCount || 0,
        };
      }
    },
  });
}
