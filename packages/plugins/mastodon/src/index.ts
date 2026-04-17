import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

function mGet(
  server: string,
  path: string,
  token?: string,
  apiVersion = "v1",
): any {
  const h: Record<string, string> = {};
  if (token) h.Authorization = `Bearer ${token}`;
  const resp = syncGet(`https://${server}/api/${apiVersion}${path}`, h);
  return resp.status === 200 ? resp.body : null;
}

export default function mastodon(dl: DriplinePluginAPI) {
  dl.setName("mastodon");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    server: {
      type: "string",
      required: false,
      description: "Mastodon server (default: mastodon.social)",
      env: "MASTODON_SERVER",
    },
    token: {
      type: "string",
      required: false,
      description: "Access token (optional, for private data)",
      env: "MASTODON_TOKEN",
    },
  });

  const srv = (ctx: QueryContext) =>
    ctx.connection.config.server || "mastodon.social";
  const tok = (ctx: QueryContext) => ctx.connection.config.token;

  dl.registerTable("mastodon_account", {
    description: "Mastodon account lookup",
    columns: [
      { name: "id", type: "string" },
      { name: "username", type: "string" },
      { name: "acct", type: "string" },
      { name: "display_name", type: "string" },
      { name: "note", type: "string" },
      { name: "followers_count", type: "number" },
      { name: "following_count", type: "number" },
      { name: "statuses_count", type: "number" },
      { name: "url", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [{ name: "acct", required: "required", operators: ["="] }],
    *list(ctx) {
      const acct = getQual(ctx, "acct");
      if (!acct) return;
      const body = mGet(
        srv(ctx),
        `/accounts/lookup?acct=${encodeURIComponent(acct)}`,
        tok(ctx),
      );
      if (!body) return;
      yield {
        id: body.id,
        username: body.username || "",
        acct: body.acct || "",
        display_name: body.display_name || "",
        note: body.note || "",
        followers_count: body.followers_count || 0,
        following_count: body.following_count || 0,
        statuses_count: body.statuses_count || 0,
        url: body.url || "",
        created_at: body.created_at || "",
      };
    },
  });

  dl.registerTable("mastodon_toots", {
    description: "Mastodon statuses/toots from an account",
    columns: [
      { name: "id", type: "string" },
      { name: "content", type: "string" },
      { name: "account_acct", type: "string" },
      { name: "reblogs_count", type: "number" },
      { name: "favourites_count", type: "number" },
      { name: "replies_count", type: "number" },
      { name: "url", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "account_id", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const aid = getQual(ctx, "account_id");
      if (!aid) return;
      const body = mGet(
        srv(ctx),
        `/accounts/${aid}/statuses?limit=40`,
        tok(ctx),
      );
      if (!body || !Array.isArray(body)) return;
      for (const s of body) {
        yield {
          id: s.id,
          content: s.content || "",
          account_acct: s.account?.acct || "",
          reblogs_count: s.reblogs_count || 0,
          favourites_count: s.favourites_count || 0,
          replies_count: s.replies_count || 0,
          url: s.url || "",
          created_at: s.created_at || "",
        };
      }
    },
  });

  dl.registerTable("mastodon_search", {
    description: "Search Mastodon",
    columns: [
      { name: "id", type: "string" },
      { name: "type", type: "string" },
      { name: "content", type: "string" },
      { name: "acct", type: "string" },
      { name: "url", type: "string" },
    ],
    keyColumns: [{ name: "query", required: "required", operators: ["="] }],
    *list(ctx) {
      const q = getQual(ctx, "query");
      if (!q) return;
      const body = mGet(
        srv(ctx),
        `/search?q=${encodeURIComponent(q)}&limit=40`,
        tok(ctx),
        "v2",
      );
      if (!body) return;
      for (const a of body.accounts || []) {
        yield {
          id: a.id,
          type: "account",
          content: a.display_name || a.username,
          acct: a.acct || "",
          url: a.url || "",
        };
      }
      for (const s of body.statuses || []) {
        yield {
          id: s.id,
          type: "status",
          content: s.content || "",
          acct: s.account?.acct || "",
          url: s.url || "",
        };
      }
      for (const h of body.hashtags || []) {
        yield {
          id: h.name,
          type: "hashtag",
          content: h.name,
          acct: "",
          url: h.url || "",
        };
      }
    },
  });

  dl.registerTable("mastodon_trending", {
    description: "Trending posts on a Mastodon server",
    columns: [
      { name: "id", type: "string" },
      { name: "content", type: "string" },
      { name: "account_acct", type: "string" },
      { name: "reblogs_count", type: "number" },
      { name: "favourites_count", type: "number" },
      { name: "url", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      const body = mGet(srv(ctx), `/trends/statuses?limit=40`);
      if (!body || !Array.isArray(body)) return;
      for (const s of body) {
        yield {
          id: s.id,
          content: s.content || "",
          account_acct: s.account?.acct || "",
          reblogs_count: s.reblogs_count || 0,
          favourites_count: s.favourites_count || 0,
          url: s.url || "",
          created_at: s.created_at || "",
        };
      }
    },
  });
}
