import type { DriplinePluginAPI } from "dripline";
import { commandExists, syncExec } from "dripline";

export default function npm(dl: DriplinePluginAPI) {
  dl.setName("npm");
  dl.setVersion("0.1.0");

  dl.onInit(() => {
    if (!commandExists("npm")) {
      dl.log.warn("npm not found on PATH");
    }
  });

  dl.registerTable("npm_packages", {
    description:
      "Installed npm packages in the current project (or specified directory)",
    columns: [
      { name: "name", type: "string" },
      { name: "version", type: "string" },
      { name: "type", type: "string" },
      { name: "path", type: "string" },
    ],
    keyColumns: [{ name: "dir", required: "optional", operators: ["="] }],
    *list(ctx) {
      const dir = ctx.quals.find((q) => q.column === "dir")?.value;
      const args = ["ls", "--json", "--depth=0", "--long"];
      const opts = dir
        ? { parser: "json" as const, cwd: dir }
        : { parser: "json" as const };

      const {
        rows: [data],
      } = syncExec("npm", args, { ...opts, ignoreExitCode: true });

      for (const [name, info] of Object.entries(data?.dependencies ?? {})) {
        const dep = info as any;
        let type = "dependency";
        if (dep.dev) type = "devDependency";
        if (dep.optional) type = "optionalDependency";
        if (dep.peer) type = "peerDependency";

        yield {
          name,
          version: dep.version ?? "",
          type,
          path: dep.path ?? "",
        };
      }
    },
  });

  dl.registerTable("npm_outdated", {
    description: "Outdated npm packages in the current project",
    columns: [
      { name: "name", type: "string" },
      { name: "current", type: "string" },
      { name: "wanted", type: "string" },
      { name: "latest", type: "string" },
      { name: "type", type: "string" },
      { name: "location", type: "string" },
    ],
    keyColumns: [{ name: "dir", required: "optional", operators: ["="] }],
    *list(ctx) {
      const dir = ctx.quals.find((q) => q.column === "dir")?.value;
      const args = ["outdated", "--json"];
      const opts = dir
        ? { parser: "json" as const, cwd: dir }
        : { parser: "json" as const };

      let data: Record<string, any>;
      try {
        const result = syncExec("npm", args, { ...opts, ignoreExitCode: true });
        data = result.rows[0] ?? {};
      } catch {
        return;
      }

      for (const [name, info] of Object.entries(data)) {
        const dep = info as any;
        yield {
          name,
          current: dep.current ?? "",
          wanted: dep.wanted ?? "",
          latest: dep.latest ?? "",
          type: dep.type ?? "dependencies",
          location: dep.location ?? "",
        };
      }
    },
  });

  dl.registerTable("npm_global", {
    description: "Globally installed npm packages",
    columns: [
      { name: "name", type: "string" },
      { name: "version", type: "string" },
      { name: "path", type: "string" },
    ],
    *list() {
      const {
        rows: [data],
      } = syncExec("npm", ["ls", "--json", "--depth=0", "--global", "--long"], {
        parser: "json",
        ignoreExitCode: true,
      });

      for (const [name, info] of Object.entries(data?.dependencies ?? {})) {
        const dep = info as any;
        yield {
          name,
          version: dep.version ?? "",
          path: dep.path ?? "",
        };
      }
    },
  });

  dl.registerTable("npm_scripts", {
    description: "npm scripts defined in package.json",
    columns: [
      { name: "name", type: "string" },
      { name: "command", type: "string" },
    ],
    keyColumns: [{ name: "dir", required: "optional", operators: ["="] }],
    *list(ctx) {
      const dir = ctx.quals.find((q) => q.column === "dir")?.value;
      const args = ["pkg", "get", "scripts"];
      const opts = dir
        ? { parser: "json" as const, cwd: dir }
        : { parser: "json" as const };

      const {
        rows: [data],
      } = syncExec("npm", args, opts);

      for (const [name, command] of Object.entries(data ?? {})) {
        yield {
          name,
          command: String(command),
        };
      }
    },
  });
}
