import type { DriplinePluginAPI } from "dripline";
import { syncGet } from "dripline";

const API = "https://skills.sh/api";

export default function skillsSh(dl: DriplinePluginAPI) {
  dl.setName("skills-sh");
  dl.setVersion("0.1.0");

  dl.registerTable("skills_search", {
    description:
      "Search AI agent skills on skills.sh. Use WHERE query = 'search term'",
    columns: [
      { name: "id", type: "string" },
      { name: "skill_id", type: "string" },
      { name: "name", type: "string" },
      { name: "source", type: "string" },
      { name: "installs", type: "number" },
    ],
    keyColumns: [
      { name: "query", required: "required", operators: ["="] },
      { name: "search_limit", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const query = ctx.quals.find((q) => q.column === "query")?.value;
      if (!query || query.length < 2) return;

      const limit =
        ctx.quals.find((q) => q.column === "search_limit")?.value ?? "50";
      const url = `${API}/search?q=${encodeURIComponent(query)}&limit=${limit}`;

      const resp = syncGet(url);
      if (resp.status !== 200) return;

      const skills = resp.body?.skills ?? [];
      for (const s of skills) {
        yield {
          id: s.id ?? "",
          skill_id: s.skillId ?? "",
          name: s.name ?? "",
          source: s.source ?? "",
          installs: s.installs ?? 0,
        };
      }
    },
  });
}
