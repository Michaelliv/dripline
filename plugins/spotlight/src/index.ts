import { statSync } from "node:fs";
import type { DriplinePluginAPI } from "dripline";
import { syncExec } from "dripline";

export default function spotlight(dl: DriplinePluginAPI) {
  dl.setName("spotlight");
  dl.setVersion("0.1.0");

  dl.registerTable("spotlight_search", {
    description:
      "Search files using macOS Spotlight (mdfind). Use WHERE query = 'search term' or name = '*.ts'",
    columns: [
      { name: "path", type: "string" },
      { name: "name", type: "string" },
      { name: "size_bytes", type: "number" },
      { name: "modified_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "query", required: "any_of", operators: ["="] },
      { name: "filename", required: "any_of", operators: ["="] },
      { name: "dir", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const query = ctx.quals.find((q) => q.column === "query")?.value;
      const name = ctx.quals.find((q) => q.column === "filename")?.value;
      const dir = ctx.quals.find((q) => q.column === "dir")?.value;

      if (!query && !name) return;

      const args: string[] = [];
      if (dir) args.push("-onlyin", dir);

      if (name) {
        // File name search
        args.push(`kMDItemFSName == '${name}'`);
      } else if (query) {
        args.push(query);
      }

      const { raw } = syncExec("mdfind", args, {
        parser: "raw",
        timeout: 30000,
      });
      const paths = raw.trim().split("\n").filter(Boolean);
      const limit = ctx.limit ?? 1000;

      let count = 0;
      for (const filePath of paths) {
        if (count >= limit) break;
        try {
          const stat = statSync(filePath);
          yield {
            path: filePath,
            name: filePath.split("/").pop() ?? "",
            size_bytes: stat.size,
            modified_at: stat.mtime.toISOString(),
          };
          count++;
        } catch {
          // File might have been deleted since indexing
          yield {
            path: filePath,
            name: filePath.split("/").pop() ?? "",
            size_bytes: null,
            modified_at: null,
          };
          count++;
        }
      }
    },
  });

  dl.registerTable("spotlight_apps", {
    description: "Installed applications found by Spotlight",
    columns: [
      { name: "name", type: "string" },
      { name: "path", type: "string" },
      { name: "size_bytes", type: "number" },
      { name: "modified_at", type: "datetime" },
    ],
    *list() {
      const { raw } = syncExec(
        "mdfind",
        ["kMDItemContentType == 'com.apple.application-bundle'"],
        {
          parser: "raw",
          timeout: 30000,
        },
      );

      for (const filePath of raw.trim().split("\n").filter(Boolean)) {
        try {
          const stat = statSync(filePath);
          const name =
            filePath
              .split("/")
              .pop()
              ?.replace(/\.app$/, "") ?? "";
          yield {
            name,
            path: filePath,
            size_bytes: stat.size,
            modified_at: stat.mtime.toISOString(),
          };
        } catch {}
      }
    },
  });

  dl.registerTable("spotlight_recent", {
    description: "Recently modified files found by Spotlight",
    columns: [
      { name: "path", type: "string" },
      { name: "name", type: "string" },
      { name: "size_bytes", type: "number" },
      { name: "modified_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "dir", required: "optional", operators: ["="] },
      { name: "kind", required: "optional", operators: ["="] },
      { name: "days", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const dir = ctx.quals.find((q) => q.column === "dir")?.value;
      const kind = ctx.quals.find((q) => q.column === "kind")?.value;
      const days = parseInt(
        ctx.quals.find((q) => q.column === "days")?.value ?? "7",
        10,
      );

      const date = new Date();
      date.setDate(date.getDate() - days);
      const dateStr = date.toISOString().split("T")[0];

      let query = `kMDItemFSContentChangeDate >= $time.iso(${dateStr})`;
      if (kind) {
        const kindMap: Record<string, string> = {
          pdf: "com.adobe.pdf",
          image: "public.image",
          text: "public.plain-text",
          markdown: "net.daringfireball.markdown",
          typescript: "com.typescriptlang.typescript",
          javascript: "com.netscape.javascript-source",
          json: "public.json",
          python: "public.python-script",
        };
        const contentType = kindMap[kind.toLowerCase()] ?? kind;
        query += ` && kMDItemContentType == '${contentType}'`;
      }

      const args: string[] = [];
      if (dir) args.push("-onlyin", dir);
      args.push(query);

      const { raw } = syncExec("mdfind", args, {
        parser: "raw",
        timeout: 30000,
      });
      const limit = ctx.limit ?? 500;

      let count = 0;
      for (const filePath of raw.trim().split("\n").filter(Boolean)) {
        if (count >= limit) break;
        try {
          const stat = statSync(filePath);
          yield {
            path: filePath,
            name: filePath.split("/").pop() ?? "",
            size_bytes: stat.size,
            modified_at: stat.mtime.toISOString(),
          };
          count++;
        } catch {}
      }
    },
  });
}
