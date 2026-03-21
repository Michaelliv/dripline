import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const UA = "dripline:0.1.0 (by /u/dripline)";

// Reddit OAuth2 password grant flow (matching go-reddit SDK's oauthTransport):
// POST https://www.reddit.com/api/v1/access_token
// Basic auth: client_id:client_secret
// Body: grant_type=password&username=X&password=Y
// Then use Bearer token with https://oauth.reddit.com
function getOAuthToken(
  clientId: string,
  clientSecret: string,
  username: string,
  password: string,
): string | null {
  const { execSync } = require("node:child_process");
  try {
    const auth = Buffer.from(`${clientId}:${clientSecret}`).toString("base64");
    const result = execSync(
      `curl -s -X POST "https://www.reddit.com/api/v1/access_token" ` +
        `-H "Authorization: Basic ${auth}" ` +
        `-H "User-Agent: ${UA}" ` +
        `-d "grant_type=password&username=${encodeURIComponent(username)}&password=${encodeURIComponent(password)}"`,
      { encoding: "utf-8", timeout: 15000 },
    );
    const body = JSON.parse(result);
    return body.access_token || null;
  } catch {
    return null;
  }
}

function redditGet(ctx: QueryContext, path: string): any {
  const clientId = ctx.connection.config.client_id || "";
  const clientSecret = ctx.connection.config.client_secret || "";
  const username = ctx.connection.config.username || "";
  const password = ctx.connection.config.password || "";
  const accessToken = ctx.connection.config.access_token || "";

  let token = accessToken;
  if (!token && clientId && clientSecret && username && password) {
    token = getOAuthToken(clientId, clientSecret, username, password) || "";
  }

  if (token) {
    // Authenticated: use oauth.reddit.com
    const resp = syncGet(`https://oauth.reddit.com${path}`, {
      Authorization: `Bearer ${token}`,
      "User-Agent": UA,
    });
    return resp.status === 200 ? resp.body : null;
  }

  // Unauthenticated fallback: use www.reddit.com with .json
  const url = path.includes("?")
    ? `https://www.reddit.com${path}&raw_json=1`
    : `https://www.reddit.com${path}.json?raw_json=1`;
  const resp = syncGet(url, { "User-Agent": UA });
  return resp.status === 200 ? resp.body : null;
}

function ts(epoch: number): string {
  return epoch ? new Date(epoch * 1000).toISOString() : "";
}

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

export default function reddit(dl: DriplinePluginAPI) {
  dl.setName("reddit");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    client_id: {
      type: "string",
      required: false,
      description: "Reddit OAuth app client ID",
      env: "REDDIT_CLIENT_ID",
    },
    client_secret: {
      type: "string",
      required: false,
      description: "Reddit OAuth app client secret",
      env: "REDDIT_CLIENT_SECRET",
    },
    username: {
      type: "string",
      required: false,
      description: "Reddit username",
      env: "REDDIT_USERNAME",
    },
    password: {
      type: "string",
      required: false,
      description: "Reddit password",
      env: "REDDIT_PASSWORD",
    },
    access_token: {
      type: "string",
      required: false,
      description: "Pre-obtained Reddit OAuth access token",
      env: "REDDIT_ACCESS_TOKEN",
    },
  });

  dl.registerTable("reddit_posts", {
    description: "Reddit posts from a subreddit",
    columns: [
      { name: "id", type: "string" },
      { name: "title", type: "string" },
      { name: "author", type: "string" },
      { name: "subreddit", type: "string" },
      { name: "score", type: "number" },
      { name: "upvote_ratio", type: "number" },
      { name: "num_comments", type: "number" },
      { name: "url", type: "string" },
      { name: "selftext", type: "string" },
      { name: "created_utc", type: "datetime" },
      { name: "is_self", type: "boolean" },
      { name: "permalink", type: "string" },
    ],
    keyColumns: [
      { name: "subreddit", required: "required", operators: ["="] },
      { name: "sort", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const sub = getQual(ctx, "subreddit");
      if (!sub) return;
      const sort = getQual(ctx, "sort") || "hot";
      let after = "";
      for (let page = 0; page < 4; page++) {
        const body = redditGet(
          ctx,
          `/r/${sub}/${sort}?limit=100${after ? `&after=${after}` : ""}`,
        );
        if (!body?.data?.children?.length) return;
        for (const child of body.data.children) {
          const p = child.data;
          yield {
            id: p.id,
            title: p.title || "",
            author: p.author || "",
            subreddit: p.subreddit || "",
            score: p.score || 0,
            upvote_ratio: p.upvote_ratio || 0,
            num_comments: p.num_comments || 0,
            url: p.url || "",
            selftext: (p.selftext || "").slice(0, 1000),
            created_utc: ts(p.created_utc),
            is_self: p.is_self ? 1 : 0,
            permalink: `https://reddit.com${p.permalink || ""}`,
          };
        }
        after = body.data.after || "";
        if (!after) return;
      }
    },
  });

  dl.registerTable("reddit_search", {
    description: "Search Reddit posts",
    columns: [
      { name: "id", type: "string" },
      { name: "title", type: "string" },
      { name: "author", type: "string" },
      { name: "subreddit", type: "string" },
      { name: "score", type: "number" },
      { name: "num_comments", type: "number" },
      { name: "url", type: "string" },
      { name: "created_utc", type: "datetime" },
      { name: "permalink", type: "string" },
    ],
    keyColumns: [{ name: "query", required: "required", operators: ["="] }],
    *list(ctx) {
      const query = getQual(ctx, "query");
      if (!query) return;
      const body = redditGet(
        ctx,
        `/search?q=${encodeURIComponent(query)}&limit=100&sort=relevance`,
      );
      if (!body?.data?.children) return;
      for (const child of body.data.children) {
        const p = child.data;
        yield {
          id: p.id,
          title: p.title || "",
          author: p.author || "",
          subreddit: p.subreddit || "",
          score: p.score || 0,
          num_comments: p.num_comments || 0,
          url: p.url || "",
          created_utc: ts(p.created_utc),
          permalink: `https://reddit.com${p.permalink || ""}`,
        };
      }
    },
  });

  dl.registerTable("reddit_subreddit", {
    description: "Subreddit info",
    columns: [
      { name: "name", type: "string" },
      { name: "title", type: "string" },
      { name: "description", type: "string" },
      { name: "subscribers", type: "number" },
      { name: "active_accounts", type: "number" },
      { name: "created_utc", type: "datetime" },
      { name: "over18", type: "boolean" },
      { name: "url", type: "string" },
    ],
    keyColumns: [{ name: "name", required: "required", operators: ["="] }],
    *list(ctx) {
      const name = getQual(ctx, "name");
      if (!name) return;
      const body = redditGet(ctx, `/r/${name}/about`);
      if (!body?.data) return;
      const s = body.data;
      yield {
        name: s.display_name || name,
        title: s.title || "",
        description: (s.public_description || "").slice(0, 500),
        subscribers: s.subscribers || 0,
        active_accounts: s.accounts_active || 0,
        created_utc: ts(s.created_utc),
        over18: s.over18 ? 1 : 0,
        url: `https://reddit.com${s.url || ""}`,
      };
    },
  });

  dl.registerTable("reddit_user", {
    description: "Reddit user profile",
    columns: [
      { name: "name", type: "string" },
      { name: "link_karma", type: "number" },
      { name: "comment_karma", type: "number" },
      { name: "total_karma", type: "number" },
      { name: "created_utc", type: "datetime" },
      { name: "is_gold", type: "boolean" },
      { name: "verified", type: "boolean" },
    ],
    keyColumns: [{ name: "name", required: "required", operators: ["="] }],
    *list(ctx) {
      const name = getQual(ctx, "name");
      if (!name) return;
      const body = redditGet(ctx, `/user/${name}/about`);
      if (!body?.data) return;
      const u = body.data;
      yield {
        name: u.name || name,
        link_karma: u.link_karma || 0,
        comment_karma: u.comment_karma || 0,
        total_karma: u.total_karma || 0,
        created_utc: ts(u.created_utc),
        is_gold: u.is_gold ? 1 : 0,
        verified: u.verified ? 1 : 0,
      };
    },
  });
}
