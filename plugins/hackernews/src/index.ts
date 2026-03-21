import type { DriplinePluginAPI } from "dripline";
import { syncGet } from "dripline";

const API = "https://hacker-news.firebaseio.com/v0";

function getItem(id: number): any {
  const resp = syncGet(`${API}/item/${id}.json`, {});
  return resp.status === 200 ? resp.body : null;
}

function* fetchStories(endpoint: string, limit: number) {
  const resp = syncGet(`${API}/${endpoint}.json`, {});
  if (resp.status !== 200) return;
  const ids = (resp.body as number[]).slice(0, limit);
  for (const id of ids) {
    const item = getItem(id);
    if (!item) continue;
    yield {
      id: item.id,
      title: item.title || "",
      url: item.url || "",
      score: item.score || 0,
      author: item.by || "",
      time: item.time ? new Date(item.time * 1000).toISOString() : "",
      descendants: item.descendants || 0,
      type: item.type || "",
      text: item.text || "",
    };
  }
}

export default function hackernews(dl: DriplinePluginAPI) {
  dl.setName("hackernews");
  dl.setVersion("0.1.0");

  const storyCols = [
    { name: "id", type: "number" as const },
    { name: "title", type: "string" as const },
    { name: "url", type: "string" as const },
    { name: "score", type: "number" as const },
    { name: "author", type: "string" as const },
    { name: "time", type: "datetime" as const },
    { name: "descendants", type: "number" as const },
    { name: "type", type: "string" as const },
    { name: "text", type: "string" as const },
  ];

  for (const [table, endpoint, desc] of [
    ["hn_top", "topstories", "Top stories on Hacker News"],
    ["hn_new", "newstories", "Newest stories on Hacker News"],
    ["hn_best", "beststories", "Best stories on Hacker News"],
    ["hn_ask", "askstories", "Ask HN stories"],
    ["hn_show", "showstories", "Show HN stories"],
    ["hn_job", "jobstories", "Job postings on Hacker News"],
  ] as const) {
    dl.registerTable(table, {
      description: desc,
      columns: storyCols,
      keyColumns: [{ name: "limit", required: "optional", operators: ["="] }],
      *list(ctx) {
        const limit =
          Number(ctx.quals.find((q) => q.column === "limit")?.value) || 30;
        yield* fetchStories(endpoint, limit);
      },
    });
  }

  dl.registerTable("hn_item", {
    description: "Lookup any Hacker News item by ID",
    columns: [
      ...storyCols,
      { name: "parent", type: "number" as const },
      { name: "kids", type: "json" as const },
    ],
    keyColumns: [{ name: "id", required: "required", operators: ["="] }],
    *list(ctx) {
      const id = Number(ctx.quals.find((q) => q.column === "id")?.value);
      if (!id) return;
      const item = getItem(id);
      if (!item) return;
      yield {
        id: item.id,
        title: item.title || "",
        url: item.url || "",
        score: item.score || 0,
        author: item.by || "",
        time: item.time ? new Date(item.time * 1000).toISOString() : "",
        descendants: item.descendants || 0,
        type: item.type || "",
        text: item.text || "",
        parent: item.parent || 0,
        kids: JSON.stringify(item.kids || []),
      };
    },
  });

  dl.registerTable("hn_user", {
    description: "Hacker News user profile",
    columns: [
      { name: "id", type: "string" },
      { name: "karma", type: "number" },
      { name: "created", type: "datetime" },
      { name: "about", type: "string" },
      { name: "submitted", type: "json" },
    ],
    keyColumns: [{ name: "id", required: "required", operators: ["="] }],
    *list(ctx) {
      const id = ctx.quals.find((q) => q.column === "id")?.value;
      if (!id) return;
      const resp = syncGet(`${API}/user/${id}.json`, {});
      if (resp.status !== 200 || !resp.body) return;
      const u = resp.body as any;
      yield {
        id: u.id,
        karma: u.karma || 0,
        created: u.created ? new Date(u.created * 1000).toISOString() : "",
        about: u.about || "",
        submitted: JSON.stringify((u.submitted || []).slice(0, 100)),
      };
    },
  });
}
