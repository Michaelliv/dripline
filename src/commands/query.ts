import { Dripline } from "../sdk.js";
import { loadConfig } from "../config/loader.js";
import { loadBuiltinPlugins } from "../plugin/loader.js";
import { registry } from "../plugin/registry.js";
import { formatTable } from "../utils/table-formatter.js";
import { formatJson, formatCsv, formatLine } from "../utils/formatters.js";
import { error } from "../utils/output.js";
import { startSpinner } from "../utils/spinner.js";

export type OutputFormat = "table" | "json" | "csv" | "line";

export async function query(
  sql: string,
  options: { output?: OutputFormat; json?: boolean; quiet?: boolean },
): Promise<void> {
  await loadBuiltinPlugins();

  const config = loadConfig();
  const dl = new Dripline({
    plugins: registry.listPlugins(),
    connections: config.connections,
    cache: config.cache,
    rateLimits: config.rateLimits,
  });

  try {
    const format = options.json ? "json" : options.output ?? "table";
    const showSpinner = !options.json && !options.quiet && format !== "json";
    const spinner = showSpinner ? startSpinner("Querying...") : null;
    const start = performance.now();
    const rows = dl.query(sql);
    const elapsed = ((performance.now() - start) / 1000).toFixed(3);
    spinner?.stop();

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
    dl.close();
  }
}
