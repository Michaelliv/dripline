import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const API = "https://slack.com/api";

function slackGet(
  method: string,
  token: string,
  params: Record<string, string> = {},
): any {
  const qs = new URLSearchParams(params).toString();
  const url = `${API}/${method}${qs ? `?${qs}` : ""}`;
  const resp = syncGet(url, { Authorization: `Bearer ${token}` });
  if (resp.status !== 200) return null;
  const body = resp.body as any;
  if (!body.ok) return null;
  return body;
}

function* slackPaginate(
  method: string,
  token: string,
  key: string,
  params: Record<string, string> = {},
) {
  let cursor = "";
  do {
    const p: Record<string, string> = { ...params, limit: "200" };
    if (cursor) p.cursor = cursor;
    const body = slackGet(method, token, p);
    if (!body || !body[key]) return;
    yield* body[key];
    cursor = body.response_metadata?.next_cursor || "";
  } while (cursor);
}

function getToken(ctx: QueryContext): string {
  return ctx.connection.config.token || "";
}

export default function slack(dl: DriplinePluginAPI) {
  dl.setName("slack");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    token: {
      type: "string",
      required: true,
      description: "Slack Bot User OAuth Token (xoxb-...)",
      env: "SLACK_TOKEN",
    },
  });

  dl.registerTable("slack_users", {
    description: "Slack workspace users",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "real_name", type: "string" },
      { name: "display_name", type: "string" },
      { name: "email", type: "string" },
      { name: "title", type: "string" },
      { name: "is_admin", type: "boolean" },
      { name: "is_bot", type: "boolean" },
      { name: "deleted", type: "boolean" },
      { name: "tz", type: "string" },
      { name: "updated", type: "datetime" },
    ],
    *list(ctx) {
      const token = getToken(ctx);
      for (const u of slackPaginate("users.list", token, "members")) {
        yield {
          id: u.id,
          name: u.name || "",
          real_name: u.profile?.real_name || "",
          display_name: u.profile?.display_name || "",
          email: u.profile?.email || "",
          title: u.profile?.title || "",
          is_admin: u.is_admin ? 1 : 0,
          is_bot: u.is_bot ? 1 : 0,
          deleted: u.deleted ? 1 : 0,
          tz: u.tz || "",
          updated: u.updated ? new Date(u.updated * 1000).toISOString() : "",
        };
      }
    },
  });

  dl.registerTable("slack_conversations", {
    description: "Slack channels and conversations",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "topic", type: "string" },
      { name: "purpose", type: "string" },
      { name: "is_channel", type: "boolean" },
      { name: "is_private", type: "boolean" },
      { name: "is_archived", type: "boolean" },
      { name: "num_members", type: "number" },
      { name: "created", type: "datetime" },
    ],
    *list(ctx) {
      const token = getToken(ctx);
      for (const c of slackPaginate("conversations.list", token, "channels", {
        types: "public_channel,private_channel",
      })) {
        yield {
          id: c.id,
          name: c.name || "",
          topic: c.topic?.value || "",
          purpose: c.purpose?.value || "",
          is_channel: c.is_channel ? 1 : 0,
          is_private: c.is_private ? 1 : 0,
          is_archived: c.is_archived ? 1 : 0,
          num_members: c.num_members || 0,
          created: c.created ? new Date(c.created * 1000).toISOString() : "",
        };
      }
    },
  });

  dl.registerTable("slack_conversation_members", {
    description: "Members of a Slack conversation",
    columns: [
      { name: "channel_id", type: "string" },
      { name: "user_id", type: "string" },
    ],
    keyColumns: [
      { name: "channel_id", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const token = getToken(ctx);
      const channelId = ctx.quals.find((q) => q.column === "channel_id")?.value;
      if (!channelId) return;
      for (const uid of slackPaginate(
        "conversations.members",
        token,
        "members",
        { channel: channelId },
      )) {
        yield { channel_id: channelId, user_id: uid };
      }
    },
  });

  dl.registerTable("slack_messages", {
    description: "Messages in a Slack conversation (requires channel_id)",
    columns: [
      { name: "channel_id", type: "string" },
      { name: "ts", type: "string" },
      { name: "user", type: "string" },
      { name: "text", type: "string" },
      { name: "type", type: "string" },
      { name: "subtype", type: "string" },
      { name: "thread_ts", type: "string" },
      { name: "reply_count", type: "number" },
      { name: "reactions", type: "json" },
    ],
    keyColumns: [
      { name: "channel_id", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const token = getToken(ctx);
      const channelId = ctx.quals.find((q) => q.column === "channel_id")?.value;
      if (!channelId) return;
      // conversations.history doesn't use cursor pagination in the same way
      let cursor = "";
      do {
        const params: Record<string, string> = {
          channel: channelId,
          limit: "200",
        };
        if (cursor) params.cursor = cursor;
        const body = slackGet("conversations.history", token, params);
        if (!body?.messages) return;
        for (const m of body.messages) {
          yield {
            channel_id: channelId,
            ts: m.ts || "",
            user: m.user || "",
            text: m.text || "",
            type: m.type || "",
            subtype: m.subtype || "",
            thread_ts: m.thread_ts || "",
            reply_count: m.reply_count || 0,
            reactions: JSON.stringify(m.reactions || []),
          };
        }
        cursor = body.response_metadata?.next_cursor || "";
        if (!body.has_more) break;
      } while (cursor);
    },
  });

  dl.registerTable("slack_search", {
    description: "Search Slack messages",
    columns: [
      { name: "text", type: "string" },
      { name: "username", type: "string" },
      { name: "channel_name", type: "string" },
      { name: "ts", type: "string" },
      { name: "permalink", type: "string" },
    ],
    keyColumns: [{ name: "query", required: "required", operators: ["="] }],
    *list(ctx) {
      const token = getToken(ctx);
      const query = ctx.quals.find((q) => q.column === "query")?.value;
      if (!query) return;
      let page = 1;
      let totalPages = 1;
      while (page <= totalPages) {
        const body = slackGet("search.messages", token, {
          query,
          count: "100",
          page: String(page),
        });
        if (!body?.messages?.matches) return;
        totalPages = body.messages.paging?.pages || 1;
        for (const m of body.messages.matches) {
          yield {
            text: m.text || "",
            username: m.username || "",
            channel_name: m.channel?.name || "",
            ts: m.ts || "",
            permalink: m.permalink || "",
          };
        }
        page++;
      }
    },
  });

  dl.registerTable("slack_emoji", {
    description: "Custom emoji in the workspace",
    columns: [
      { name: "name", type: "string" },
      { name: "url", type: "string" },
    ],
    *list(ctx) {
      const token = getToken(ctx);
      const body = slackGet("emoji.list", token);
      if (!body?.emoji) return;
      for (const [name, url] of Object.entries(body.emoji)) {
        yield { name, url: url as string };
      }
    },
  });
}
