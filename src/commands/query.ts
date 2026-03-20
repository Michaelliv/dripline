import { loadConfig } from "../config/loader.js";
import { loadBuiltinPlugins } from "../plugin/loader.js";
import { registry } from "../plugin/registry.js";
import { createEngine } from "../engine.js";
import { formatTable } from "../utils/table-formatter.js";
import { formatJson, formatCsv, formatLine } from "../utils/formatters.js";
import { error } from "../utils/output.js";

export type OutputFormat = "table" | "json" | "csv" | "line";

export async function query(
  sql: string,
  options: { output?: OutputFormat; json?: boolean; quiet?: boolean },
): Promise<void> {
  await loadBuiltinPlugins();

  const config = loadConfig();
  const engine = createEngine(config, registry);

  try {
    const start = performance.now();
    const rows = engine.query(sql);
    const elapsed = ((performance.now() - start) / 1000).toFixed(3);

    const format = options.json ? "json" : options.output ?? "table";

    switch (format) {
      case "json":
        console.log(formatJson(rows));
        break;
      case "csv":
        console.log(formatCsv(rows));
        break;
      case "line":
        console.log(formatLine(rows));
        break;
      case "table":
      default:
        console.log(formatTable(rows));
        if (!options.quiet) {
          console.log(`Time: ${elapsed}s.`);
        }
        break;
    }
  } catch (e: any) {
    error(e.message);
    process.exit(1);
  } finally {
    engine.close();
  }
}
