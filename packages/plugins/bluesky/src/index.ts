import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// AT Protocol session creation (matching indigo xrpc SDK's ServerCreateSession):
// POST {pdsHost}/xrpc/com.atproto.server.createSession
// Body: { identifier: handle, password: appPassword }
// Returns: { accessJwt, refreshJwt, handle, did }
function createSession(
  pdsHost: string,
  handle: string,
  appPassword: string,
): string | null {
  const { execSync } = require("node:child_process");
  try {
    const body = JSON.stringify({ identifier: handle, password: appPassword });
    const result = execSync(
      `curl -s -X POST "${pdsHost}/xrpc/com.atproto.server.createSession" ` +
        `-H "Content-Type: application/json" -d '${body.replace(/'/g, "'\\''")}'`,
      { encoding: "utf-8", timeout: 15000 },
    );
    const resp = JSON.parse(result);
    return resp.accessJwt || null;
  } catch {
    return null;
  }
}

function bskyGet(
  ctx: QueryContext,
  method: string,
  params: Record<string, string> = {},
): any {
  const handle = ctx.connection.config.handle || "";
  const appPassword = ctx.connection.config.app_password || "";
  const pdsHost = ctx.connection.config.pds_host || "https://bsky.social";

  const qs = new URLSearchParams(params).toString();

  if (handle && appPassword) {
    // Authenticated: create session and use PDS host
    const token = createSession(pdsHost, handle, appPassword);
    if (token) {
      const url = `${pdsHost}/xrpc/${method}${qs ? `?${qs}` : ""}`;
      const resp = syncGet(url, { Authorization: `Bearer ${token}` });
      if (resp.status === 200) return resp.body;
    }
  }

  // Fallback: public API (no auth needed for public data)
  const url = `https://public.api.bsky.app/xrpc/${method}${qs ? `?${qs}` : ""}`;
  const resp = syncGet(url, {});
  return resp.status === 200 ? resp.body : null;
}

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

export default function bluesky(dl: DriplinePluginAPI) {
  dl.setName("bluesky");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    handle: {
      type: "string",
      required: false,
      description: "Bluesky handle (e.g. user.bsky.social)",
      env: "BLUESKY_HANDLE",
    },
    app_password: {
      type: "string",
      required: false,
      description: "Bluesky app password",
      env: "BLUESKY_APP_PASSWORD",
    },
    pds_host: {
      type: "string",
      required: false,
      description: "PDS host (default: https://bsky.social)",
      env: "BLUESKY_PDS_HOST",
    },
  });

  // app.bsky.actor.getProfile
  dl.registerTable("bsky_profile", {
    description: "Bluesky user profile",
    columns: [
      { name: "did", type: "string" },
      { name: "handle", type: "string" },
      { name: "display_name", type: "string" },
      { name: "description", type: "string" },
      { name: "followers_count", type: "number" },
      { name: "follows_count", type: "number" },
      { name: "posts_count", type: "number" },
      { name: "avatar", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [{ name: "handle", required: "required", operators: ["="] }],
    *list(ctx) {
      const handle = getQual(ctx, "handle");
      if (!handle) return;
      const body = bskyGet(ctx, "app.bsky.actor.getProfile", { actor: handle });
      if (!body) return;
      yield {
        did: body.did || "",
        handle: body.handle || "",
        display_name: body.displayName || "",
        description: body.description || "",
        followers_count: body.followersCount || 0,
        follows_count: body.followsCount || 0,
        posts_count: body.postsCount || 0,
        avatar: body.avatar || "",
        created_at: body.createdAt || "",
      };
    },
  });

  // app.bsky.feed.getAuthorFeed
  dl.registerTable("bsky_feed", {
    description: "Bluesky posts from a user's feed",
    columns: [
      { name: "uri", type: "string" },
      { name: "cid", type: "string" },
      { name: "author_handle", type: "string" },
      { name: "author_display_name", type: "string" },
      { name: "text", type: "string" },
      { name: "like_count", type: "number" },
      { name: "reply_count", type: "number" },
      { name: "repost_count", type: "number" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [{ name: "handle", required: "required", operators: ["="] }],
    *list(ctx) {
      const handle = getQual(ctx, "handle");
      if (!handle) return;
      let cursor = "";
      for (let page = 0; page < 5; page++) {
        const params: Record<string, string> = { actor: handle, limit: "100" };
        if (cursor) params.cursor = cursor;
        const body = bskyGet(ctx, "app.bsky.feed.getAuthorFeed", params);
        if (!body?.feed?.length) return;
        for (const item of body.feed) {
          const post = item.post;
          yield {
            uri: post.uri || "",
            cid: post.cid || "",
            author_handle: post.author?.handle || "",
            author_display_name: post.author?.displayName || "",
            text: post.record?.text || "",
            like_count: post.likeCount || 0,
            reply_count: post.replyCount || 0,
            repost_count: post.repostCount || 0,
            created_at: post.record?.createdAt || post.indexedAt || "",
          };
        }
        cursor = body.cursor || "";
        if (!cursor) return;
      }
    },
  });

  // app.bsky.graph.getFollowers
  dl.registerTable("bsky_followers", {
    description: "Bluesky followers of a user",
    columns: [
      { name: "did", type: "string" },
      { name: "handle", type: "string" },
      { name: "display_name", type: "string" },
      { name: "description", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [{ name: "actor", required: "required", operators: ["="] }],
    *list(ctx) {
      const actor = getQual(ctx, "actor");
      if (!actor) return;
      let cursor = "";
      for (let page = 0; page < 10; page++) {
        const params: Record<string, string> = { actor, limit: "100" };
        if (cursor) params.cursor = cursor;
        const body = bskyGet(ctx, "app.bsky.graph.getFollowers", params);
        if (!body?.followers?.length) return;
        for (const f of body.followers) {
          yield {
            did: f.did || "",
            handle: f.handle || "",
            display_name: f.displayName || "",
            description: (f.description || "").slice(0, 300),
            created_at: f.createdAt || "",
          };
        }
        cursor = body.cursor || "";
        if (!cursor) return;
      }
    },
  });

  // app.bsky.feed.searchPosts
  dl.registerTable("bsky_search", {
    description: "Search Bluesky posts",
    columns: [
      { name: "uri", type: "string" },
      { name: "author_handle", type: "string" },
      { name: "text", type: "string" },
      { name: "like_count", type: "number" },
      { name: "reply_count", type: "number" },
      { name: "repost_count", type: "number" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [{ name: "query", required: "required", operators: ["="] }],
    *list(ctx) {
      const query = getQual(ctx, "query");
      if (!query) return;
      let cursor = "";
      for (let page = 0; page < 3; page++) {
        const params: Record<string, string> = { q: query, limit: "100" };
        if (cursor) params.cursor = cursor;
        const body = bskyGet(ctx, "app.bsky.feed.searchPosts", params);
        if (!body?.posts?.length) return;
        for (const post of body.posts) {
          yield {
            uri: post.uri || "",
            author_handle: post.author?.handle || "",
            text: post.record?.text || "",
            like_count: post.likeCount || 0,
            reply_count: post.replyCount || 0,
            repost_count: post.repostCount || 0,
            created_at: post.record?.createdAt || post.indexedAt || "",
          };
        }
        cursor = body.cursor || "";
        if (!cursor) return;
      }
    },
  });

  // app.bsky.notification.listNotifications (authenticated only)
  dl.registerTable("bsky_notifications", {
    description: "Bluesky notifications (requires auth)",
    columns: [
      { name: "uri", type: "string" },
      { name: "cid", type: "string" },
      { name: "reason", type: "string" },
      { name: "author_handle", type: "string" },
      { name: "is_read", type: "boolean" },
      { name: "indexed_at", type: "datetime" },
    ],
    *list(ctx) {
      let cursor = "";
      for (let page = 0; page < 5; page++) {
        const params: Record<string, string> = { limit: "100" };
        if (cursor) params.cursor = cursor;
        const body = bskyGet(
          ctx,
          "app.bsky.notification.listNotifications",
          params,
        );
        if (!body?.notifications?.length) return;
        for (const n of body.notifications) {
          yield {
            uri: n.uri || "",
            cid: n.cid || "",
            reason: n.reason || "",
            author_handle: n.author?.handle || "",
            is_read: n.isRead ? 1 : 0,
            indexed_at: n.indexedAt || "",
          };
        }
        cursor = body.cursor || "";
        if (!cursor) return;
      }
    },
  });
}
