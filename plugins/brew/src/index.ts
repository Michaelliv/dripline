import type { DriplinePluginAPI } from "dripline";
import { syncExec, commandExists } from "dripline";

export default function brew(dl: DriplinePluginAPI) {
  dl.setName("brew");
  dl.setVersion("0.1.0");

  dl.onInit(() => {
    if (!commandExists("brew")) {
      dl.log.warn("brew not found on PATH — brew tables will be unavailable");
    }
  });

  dl.registerTable("brew_formulae", {
    description: "Installed Homebrew formulae",
    columns: [
      { name: "name", type: "string" },
      { name: "full_name", type: "string" },
      { name: "version", type: "string" },
      { name: "description", type: "string" },
      { name: "homepage", type: "string" },
      { name: "license", type: "string" },
      { name: "tap", type: "string" },
      { name: "outdated", type: "boolean" },
      { name: "pinned", type: "boolean" },
      { name: "installed_on_request", type: "boolean" },
      { name: "dependencies", type: "json" },
    ],
    *list() {
      const { rows: [data] } = syncExec("brew", ["info", "--json=v2", "--installed"], { parser: "json" });
      const formulae: any[] = data?.formulae ?? [];

      for (const f of formulae) {
        const installed = f.installed?.[0] ?? {};
        yield {
          name: f.name ?? "",
          full_name: f.full_name ?? "",
          version: installed.version ?? f.versions?.stable ?? "",
          description: f.desc ?? "",
          homepage: f.homepage ?? "",
          license: f.license ?? "",
          tap: f.tap ?? "",
          outdated: f.outdated ? 1 : 0,
          pinned: f.pinned ? 1 : 0,
          installed_on_request: installed.installed_on_request ? 1 : 0,
          dependencies: JSON.stringify(f.dependencies ?? []),
        };
      }
    },
  });

  dl.registerTable("brew_casks", {
    description: "Installed Homebrew casks",
    columns: [
      { name: "name", type: "string" },
      { name: "full_name", type: "string" },
      { name: "version", type: "string" },
      { name: "installed_version", type: "string" },
      { name: "description", type: "string" },
      { name: "homepage", type: "string" },
      { name: "tap", type: "string" },
      { name: "outdated", type: "boolean" },
      { name: "auto_updates", type: "boolean" },
    ],
    *list() {
      const { rows: [data] } = syncExec("brew", ["info", "--json=v2", "--installed"], { parser: "json" });
      const casks: any[] = data?.casks ?? [];

      for (const c of casks) {
        yield {
          name: c.token ?? "",
          full_name: c.full_token ?? "",
          version: c.version ?? "",
          installed_version: c.installed ?? "",
          description: c.desc ?? "",
          homepage: c.homepage ?? "",
          tap: c.tap ?? "",
          outdated: c.outdated ? 1 : 0,
          auto_updates: c.auto_updates ? 1 : 0,
        };
      }
    },
  });

  dl.registerTable("brew_outdated", {
    description: "Outdated Homebrew formulae and casks",
    columns: [
      { name: "name", type: "string" },
      { name: "type", type: "string" },
      { name: "installed_version", type: "string" },
      { name: "current_version", type: "string" },
      { name: "pinned", type: "boolean" },
    ],
    *list() {
      const { rows: [data] } = syncExec("brew", ["outdated", "--json=v2"], { parser: "json" });

      for (const f of data?.formulae ?? []) {
        yield {
          name: f.name ?? "",
          type: "formula",
          installed_version: f.installed_versions?.[0] ?? "",
          current_version: f.current_version ?? "",
          pinned: f.pinned ? 1 : 0,
        };
      }

      for (const c of data?.casks ?? []) {
        yield {
          name: c.name ?? "",
          type: "cask",
          installed_version: c.installed_versions ?? "",
          current_version: c.current_version ?? "",
          pinned: 0,
        };
      }
    },
  });

  dl.registerTable("brew_services", {
    description: "Homebrew services",
    columns: [
      { name: "name", type: "string" },
      { name: "status", type: "string" },
      { name: "user", type: "string" },
      { name: "file", type: "string" },
      { name: "exit_code", type: "number" },
    ],
    *list() {
      const { rows } = syncExec("brew", ["services", "list", "--json"], { parser: "json" });

      for (const s of rows) {
        yield {
          name: s.name ?? "",
          status: s.status ?? "",
          user: s.user ?? "",
          file: s.file ?? "",
          exit_code: s.exit_code ?? null,
        };
      }
    },
  });
}
