import chalk from "chalk";
import { loadAllPlugins } from "../plugin/loader.js";
import { registry } from "../plugin/registry.js";
import { dim } from "../utils/output.js";

export async function tables(options: { json?: boolean }): Promise<void> {
  await loadAllPlugins();

  const allTables = registry.getAllTables();

  if (options.json) {
    const output = allTables.map(({ plugin, table }) => ({
      plugin,
      table: table.name,
      description: table.description ?? null,
      columns: table.columns.map((c) => ({
        name: c.name,
        type: c.type,
        description: c.description ?? null,
      })),
      keyColumns: (table.keyColumns ?? []).map((k) => ({
        name: k.name,
        required: k.required,
      })),
    }));
    console.log(JSON.stringify(output, null, 2));
    return;
  }

  if (allTables.length === 0) {
    console.log("No tables available. Install a plugin first.");
    return;
  }

  console.log();
  for (const { table } of allTables) {
    const keys = (table.keyColumns ?? [])
      .filter((k) => k.required === "required")
      .map((k) => k.name);
    const keyStr =
      keys.length > 0 ? chalk.dim(` (requires: ${keys.join(", ")})`) : "";
    console.log(`  ${chalk.cyan(table.name)}${keyStr}`);
    if (table.description) console.log(`    ${chalk.dim(table.description)}`);
    for (const col of table.columns) {
      console.log(`      ${col.name.padEnd(25)} ${dim(col.type)}`);
    }
    console.log();
  }
}
