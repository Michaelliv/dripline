import type { DriplinePluginAPI } from "dripline";
import { syncExec } from "dripline";

const SEP = "†";

export default function git(dl: DriplinePluginAPI) {
  dl.setName("git");
  dl.setVersion("0.1.0");

  function gitArgs(ctx: { quals: { column: string; value: any }[] }): string[] {
    const repo = ctx.quals.find((q) => q.column === "repo")?.value;
    return repo ? ["-C", repo] : [];
  }

  dl.registerTable("git_commits", {
    description: "Git commit log",
    columns: [
      { name: "hash", type: "string" },
      { name: "short_hash", type: "string" },
      { name: "author", type: "string" },
      { name: "author_email", type: "string" },
      { name: "date", type: "datetime" },
      { name: "subject", type: "string" },
      { name: "body", type: "string" },
      { name: "refs", type: "string" },
    ],
    keyColumns: [
      { name: "repo", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const format = ["%H", "%h", "%an", "%ae", "%aI", "%s", "%b", "%D"].join(SEP);
      const limit = ctx.limit ?? 500;
      const { raw } = syncExec(
        "git",
        [...gitArgs(ctx), "log", `--format=${format}`, `-${limit}`, "--"],
        { parser: "raw" },
      );

      for (const line of raw.trim().split("\n")) {
        if (!line) continue;
        const [hash, short_hash, author, author_email, date, subject, body, refs] = line.split(SEP);
        yield { hash, short_hash, author, author_email, date, subject, body: body?.trim() ?? "", refs: refs ?? "" };
      }
    },
  });

  dl.registerTable("git_branches", {
    description: "Git branches (local and remote)",
    columns: [
      { name: "name", type: "string" },
      { name: "is_current", type: "boolean" },
      { name: "is_remote", type: "boolean" },
      { name: "hash", type: "string" },
      { name: "upstream", type: "string" },
      { name: "subject", type: "string" },
    ],
    keyColumns: [
      { name: "repo", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const format = ["%(HEAD)", "%(refname:short)", "%(objectname:short)", "%(upstream:short)", "%(subject)"].join(SEP);
      const { raw } = syncExec(
        "git",
        [...gitArgs(ctx), "branch", "-a", `--format=${format}`],
        { parser: "raw" },
      );

      for (const line of raw.trim().split("\n")) {
        if (!line) continue;
        const [head, name, hash, upstream, subject] = line.split(SEP);
        const isRemote = name.startsWith("remotes/") || name.includes("/");
        yield {
          name: name.replace(/^remotes\//, ""),
          is_current: head?.trim() === "*" ? 1 : 0,
          is_remote: isRemote ? 1 : 0,
          hash: hash ?? "",
          upstream: upstream ?? "",
          subject: subject ?? "",
        };
      }
    },
  });

  dl.registerTable("git_tags", {
    description: "Git tags",
    columns: [
      { name: "name", type: "string" },
      { name: "hash", type: "string" },
      { name: "tagger", type: "string" },
      { name: "date", type: "datetime" },
      { name: "subject", type: "string" },
    ],
    keyColumns: [
      { name: "repo", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const format = ["%(refname:short)", "%(objectname:short)", "%(taggername)", "%(creatordate:iso-strict)", "%(subject)"].join(SEP);
      const { raw } = syncExec(
        "git",
        [...gitArgs(ctx), "tag", "-l", `--format=${format}`, "--sort=-creatordate"],
        { parser: "raw" },
      );

      for (const line of raw.trim().split("\n")) {
        if (!line) continue;
        const [name, hash, tagger, date, subject] = line.split(SEP);
        yield { name, hash: hash ?? "", tagger: tagger ?? "", date: date ?? "", subject: subject ?? "" };
      }
    },
  });

  dl.registerTable("git_remotes", {
    description: "Git remotes",
    columns: [
      { name: "name", type: "string" },
      { name: "url", type: "string" },
      { name: "type", type: "string" },
    ],
    keyColumns: [
      { name: "repo", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const { raw } = syncExec("git", [...gitArgs(ctx), "remote", "-v"], { parser: "raw" });

      for (const line of raw.trim().split("\n")) {
        if (!line) continue;
        const match = line.match(/^(\S+)\s+(\S+)\s+\((\w+)\)$/);
        if (match) {
          yield { name: match[1], url: match[2], type: match[3] };
        }
      }
    },
  });

  dl.registerTable("git_status", {
    description: "Git working tree status (modified, staged, untracked files)",
    columns: [
      { name: "path", type: "string" },
      { name: "status", type: "string" },
      { name: "staged", type: "string" },
      { name: "unstaged", type: "string" },
    ],
    keyColumns: [
      { name: "repo", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const { raw } = syncExec(
        "git",
        [...gitArgs(ctx), "status", "--porcelain=v1"],
        { parser: "raw" },
      );

      const statusMap: Record<string, string> = {
        M: "modified", A: "added", D: "deleted", R: "renamed",
        C: "copied", U: "unmerged", "?": "untracked", "!": "ignored",
      };

      for (const line of raw.trim().split("\n")) {
        if (!line) continue;
        const staged = line[0];
        const unstaged = line[1];
        const path = line.slice(3);

        let status = "unknown";
        if (staged === "?" && unstaged === "?") status = "untracked";
        else if (staged !== " " && staged !== "?") status = statusMap[staged] ?? "unknown";
        else if (unstaged !== " ") status = statusMap[unstaged] ?? "unknown";

        yield {
          path,
          status,
          staged: staged === " " || staged === "?" ? "" : statusMap[staged] ?? staged,
          unstaged: unstaged === " " ? "" : statusMap[unstaged] ?? unstaged,
        };
      }
    },
  });
}
