import { execSync } from "node:child_process";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";

interface TableSchema {
  plugin: string;
  table: string;
  description: string | null;
  columns: Array<{ name: string; type: string; description: string | null }>;
  keyColumns: Array<{ name: string; required: string }>;
}

function getDriplineTables(cwd: string): TableSchema[] | null {
  try {
    const output = execSync("dripline tables --json", {
      encoding: "utf-8",
      timeout: 30000,
      cwd,
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
    return JSON.parse(output);
  } catch {
    return null;
  }
}

function formatSchema(tables: TableSchema[]): string {
  const lines: string[] = [];

  // Group by plugin
  const byPlugin = new Map<string, TableSchema[]>();
  for (const t of tables) {
    const group = byPlugin.get(t.plugin) ?? [];
    group.push(t);
    byPlugin.set(t.plugin, group);
  }

  for (const [plugin, pluginTables] of byPlugin) {
    const tableEntries = pluginTables.map((t) => {
      const requiredKeys = t.keyColumns
        .filter((k) => k.required === "required")
        .map((k) => k.name);
      const keyHint = requiredKeys.length > 0 ? ` (WHERE: ${requiredKeys.join(", ")})` : "";
      return `- ${t.table}${keyHint}`;
    });
    lines.push(`**${plugin}**: ${tableEntries.join(", ").replace(/- /g, "")}`);
  }

  lines.push("");
  lines.push("Use `dripline tables` to see full column schemas, or `dripline query \"SELECT * FROM <table> LIMIT 1\"` to explore.");

  return lines.join("\n");
}

export default function (pi: ExtensionAPI) {
  pi.on("session_start", async (_event, ctx) => {
    const tables = getDriplineTables(ctx.cwd);
    if (!tables || tables.length === 0) {
      if (ctx.hasUI) {
        ctx.ui.setStatus("dripline", ctx.ui.theme.fg("dim", "dripline: no tables"));
      }
      return;
    }

    const tableCount = tables.length;
    const pluginCount = new Set(tables.map((t) => t.plugin)).size;

    // Check if already injected
    const alreadyInjected = ctx.sessionManager
      .getEntries()
      .some(
        (e) =>
          e.type === "message" &&
          e.message.role === "custom" &&
          (e.message as any).customType === "dripline-context",
      );

    if (!alreadyInjected) {
      const schema = formatSchema(tables);
      ctx.sessionManager.appendCustomMessageEntry(
        "dripline-context",
        `## Dripline tables\nYou have access to dripline (SQL query tool) with ${tableCount} tables from ${pluginCount} plugins. Use \`dripline query "SQL"\` to query any of these tables. Use \`dripline query "SQL" -o json\` for JSON output.\n\n${schema}`,
        true,
      );
    }

    if (ctx.hasUI) {
      const theme = ctx.ui.theme;
      ctx.ui.setStatus(
        "dripline",
        `💧${theme.fg("dim", ` ${tableCount} tables`)}`,
      );
    }
  });
}
