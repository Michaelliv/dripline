import type { DriplinePluginAPI, QueryContext } from "dripline";
import { commandExists, syncExec } from "dripline";

// 1Password: steampipe uses Connect SDK (OP_CONNECT_TOKEN + OP_CONNECT_HOST)
// We wrap the `op` CLI which is more accessible (no Connect Server needed)
// op vault list --format json, op item list --format json, etc.

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

export default function onepassword(dl: DriplinePluginAPI) {
  dl.setName("onepassword");
  dl.setVersion("0.1.0");

  // op vault list --format json
  dl.registerTable("op_vaults", {
    description: "1Password vaults (requires op CLI and active session)",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "content_version", type: "number" },
      { name: "items", type: "number" },
      { name: "type", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
    ],
    *list() {
      if (!commandExists("op")) return;
      try {
        const { rows } = syncExec("op", ["vault", "list", "--format", "json"], {
          parser: "json",
        });
        const vaults = Array.isArray(rows[0]) ? rows[0] : rows;
        for (const v of vaults as any[]) {
          yield {
            id: v.id || "",
            name: v.name || "",
            content_version: v.content_version || 0,
            items: v.items || 0,
            type: v.type || "",
            created_at: v.created_at || "",
            updated_at: v.updated_at || "",
          };
        }
      } catch {
        /* op might not be signed in */
      }
    },
  });

  // op item list --format json [--vault {vault}]
  dl.registerTable("op_items", {
    description: "1Password items (requires op CLI and active session)",
    columns: [
      { name: "id", type: "string" },
      { name: "title", type: "string" },
      { name: "category", type: "string" },
      { name: "vault_id", type: "string" },
      { name: "vault_name", type: "string" },
      { name: "favorite", type: "boolean" },
      { name: "tags", type: "json" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
    ],
    keyColumns: [{ name: "vault_id", required: "optional", operators: ["="] }],
    *list(ctx) {
      if (!commandExists("op")) return;
      const vaultId = getQual(ctx, "vault_id");
      const args = ["item", "list", "--format", "json"];
      if (vaultId) args.push("--vault", vaultId);
      try {
        const { rows } = syncExec("op", args, { parser: "json" });
        const items = Array.isArray(rows[0]) ? rows[0] : rows;
        for (const i of items as any[]) {
          yield {
            id: i.id || "",
            title: i.title || "",
            category: i.category || "",
            vault_id: i.vault?.id || "",
            vault_name: i.vault?.name || "",
            favorite: i.favorite ? 1 : 0,
            tags: JSON.stringify(i.tags || []),
            created_at: i.created_at || "",
            updated_at: i.updated_at || "",
          };
        }
      } catch {
        /* op might not be signed in */
      }
    },
  });

  // op user list --format json
  dl.registerTable("op_users", {
    description: "1Password users (requires op CLI and team/business account)",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "email", type: "string" },
      { name: "type", type: "string" },
      { name: "state", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    *list() {
      if (!commandExists("op")) return;
      try {
        const { rows } = syncExec("op", ["user", "list", "--format", "json"], {
          parser: "json",
        });
        const users = Array.isArray(rows[0]) ? rows[0] : rows;
        for (const u of users as any[]) {
          yield {
            id: u.id || "",
            name: u.name || "",
            email: u.email || "",
            type: u.type || "",
            state: u.state || "",
            created_at: u.created_at || "",
          };
        }
      } catch {
        /* might not have permissions */
      }
    },
  });

  // op group list --format json
  dl.registerTable("op_groups", {
    description: "1Password groups (requires op CLI and team/business account)",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "state", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    *list() {
      if (!commandExists("op")) return;
      try {
        const { rows } = syncExec("op", ["group", "list", "--format", "json"], {
          parser: "json",
        });
        const groups = Array.isArray(rows[0]) ? rows[0] : rows;
        for (const g of groups as any[]) {
          yield {
            id: g.id || "",
            name: g.name || "",
            description: g.description || "",
            state: g.state || "",
            created_at: g.created_at || "",
          };
        }
      } catch {
        /* might not have permissions */
      }
    },
  });
}
