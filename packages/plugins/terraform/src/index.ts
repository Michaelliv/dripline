import type { DriplinePluginAPI, QueryContext } from "dripline";
import { commandExists, syncExec } from "dripline";

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

// Run terraform show -json in a directory to get state
function getTfState(dir: string): any {
  try {
    const { rows } = syncExec("terraform", ["-chdir=" + dir, "show", "-json"], {
      parser: "json",
    });
    return rows[0] || null;
  } catch {
    return null;
  }
}

export default function terraform(dl: DriplinePluginAPI) {
  dl.setName("terraform");
  dl.setVersion("0.1.0");

  // terraform_resource: resources from terraform state (terraform show -json)
  // The steampipe plugin parses HCL directly, but wrapping the CLI is more practical
  dl.registerTable("tf_resources", {
    description: "Terraform resources from state (terraform show -json)",
    columns: [
      { name: "address", type: "string" },
      { name: "type", type: "string" },
      { name: "name", type: "string" },
      { name: "mode", type: "string" },
      { name: "provider_name", type: "string" },
      { name: "schema_version", type: "number" },
      { name: "values", type: "json" },
    ],
    keyColumns: [{ name: "path", required: "required", operators: ["="] }],
    *list(ctx) {
      if (!commandExists("terraform")) return;
      const dir = getQual(ctx, "path");
      if (!dir) return;
      const state = getTfState(dir);
      if (!state?.values?.root_module) return;

      function* walkModule(mod: any): Generator<any> {
        for (const r of mod.resources || []) {
          yield {
            address: r.address || "",
            type: r.type || "",
            name: r.name || "",
            mode: r.mode || "",
            provider_name: r.provider_name || "",
            schema_version: r.schema_version || 0,
            values: JSON.stringify(r.values || {}),
          };
        }
        for (const child of mod.child_modules || []) {
          yield* walkModule(child);
        }
      }

      yield* walkModule(state.values.root_module);
    },
  });

  // terraform_output: outputs from state
  dl.registerTable("tf_outputs", {
    description: "Terraform outputs from state",
    columns: [
      { name: "name", type: "string" },
      { name: "value", type: "string" },
      { name: "type", type: "string" },
      { name: "sensitive", type: "boolean" },
    ],
    keyColumns: [{ name: "path", required: "required", operators: ["="] }],
    *list(ctx) {
      if (!commandExists("terraform")) return;
      const dir = getQual(ctx, "path");
      if (!dir) return;
      const state = getTfState(dir);
      if (!state?.values?.outputs) return;

      for (const [name, output] of Object.entries(state.values.outputs)) {
        const o = output as any;
        yield {
          name,
          value:
            typeof o.value === "string" ? o.value : JSON.stringify(o.value),
          type: o.type || typeof o.value,
          sensitive: o.sensitive ? 1 : 0,
        };
      }
    },
  });

  // terraform_provider: providers from terraform providers command
  dl.registerTable("tf_providers", {
    description: "Terraform providers used in a configuration",
    columns: [
      { name: "name", type: "string" },
      { name: "version_constraint", type: "string" },
    ],
    keyColumns: [{ name: "path", required: "required", operators: ["="] }],
    *list(ctx) {
      if (!commandExists("terraform")) return;
      const dir = getQual(ctx, "path");
      if (!dir) return;
      try {
        const { rows } = syncExec(
          "terraform",
          ["-chdir=" + dir, "providers", "-json"],
          {
            parser: "json",
          },
        );
        // terraform providers -json isn't available in all versions
        // fallback to parsing text output
      } catch {
        // not available
      }

      // Fallback: parse terraform show -json for provider info
      const state = getTfState(dir);
      if (!state?.values?.root_module) return;
      const providers = new Map<string, string>();

      function walkProviders(mod: any) {
        for (const r of mod.resources || []) {
          if (r.provider_name && !providers.has(r.provider_name)) {
            providers.set(r.provider_name, "");
          }
        }
        for (const child of mod.child_modules || []) {
          walkProviders(child);
        }
      }
      walkProviders(state.values.root_module);

      for (const [name, constraint] of providers) {
        yield { name, version_constraint: constraint };
      }
    },
  });

  // terraform_workspace: list workspaces
  dl.registerTable("tf_workspaces", {
    description: "Terraform workspaces",
    columns: [
      { name: "name", type: "string" },
      { name: "is_current", type: "boolean" },
    ],
    keyColumns: [{ name: "path", required: "required", operators: ["="] }],
    *list(ctx) {
      if (!commandExists("terraform")) return;
      const dir = getQual(ctx, "path");
      if (!dir) return;
      try {
        const { rows } = syncExec(
          "terraform",
          ["-chdir=" + dir, "workspace", "list"],
          {
            parser: "lines",
          },
        );
        for (const r of rows) {
          const line = ((r as any).line || "").trim();
          if (!line) continue;
          const isCurrent = line.startsWith("* ");
          const name = line.replace(/^\*\s*/, "").trim();
          if (name) {
            yield { name, is_current: isCurrent ? 1 : 0 };
          }
        }
      } catch {
        // terraform workspace might fail
      }
    },
  });
}
