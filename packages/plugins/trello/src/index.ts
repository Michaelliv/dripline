import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// Trello REST API: api.trello.com/1/
// Auth: key + token query params (verified from adlio/trello SDK: trello.NewClient(apiKey, token))
// Env: TRELLO_API_KEY, TRELLO_TOKEN

const API = "https://api.trello.com/1";

function trGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const key = ctx.connection.config.api_key || "";
  const token = ctx.connection.config.token || "";
  const allParams = { ...params, key, token };
  const qs = new URLSearchParams(allParams).toString();
  const resp = syncGet(`${API}${path}?${qs}`, {});
  return resp.status === 200 ? resp.body : null;
}

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

export default function trello(dl: DriplinePluginAPI) {
  dl.setName("trello");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_key: {
      type: "string",
      required: true,
      description: "Trello API key",
      env: "TRELLO_API_KEY",
    },
    token: {
      type: "string",
      required: true,
      description: "Trello token",
      env: "TRELLO_TOKEN",
    },
  });

  // /members/me/boards (SDK: client.GetMyBoards)
  dl.registerTable("trello_boards", {
    description: "Trello boards for the authenticated user",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "desc", type: "string" },
      { name: "closed", type: "boolean" },
      { name: "url", type: "string" },
      { name: "short_url", type: "string" },
      { name: "id_organization", type: "string" },
      { name: "date_last_activity", type: "datetime" },
    ],
    *list(ctx) {
      const body = trGet(ctx, "/members/me/boards");
      if (!body || !Array.isArray(body)) return;
      for (const b of body) {
        yield {
          id: b.id,
          name: b.name || "",
          desc: b.desc || "",
          closed: b.closed ? 1 : 0,
          url: b.url || "",
          short_url: b.shortUrl || "",
          id_organization: b.idOrganization || "",
          date_last_activity: b.dateLastActivity || "",
        };
      }
    },
  });

  // /boards/{id}/cards (SDK: client.GetCardsOnBoard)
  dl.registerTable("trello_cards", {
    description: "Trello cards on a board",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "desc", type: "string" },
      { name: "closed", type: "boolean" },
      { name: "id_board", type: "string" },
      { name: "id_list", type: "string" },
      { name: "url", type: "string" },
      { name: "due", type: "datetime" },
      { name: "due_complete", type: "boolean" },
      { name: "labels", type: "json" },
      { name: "id_members", type: "json" },
      { name: "date_last_activity", type: "datetime" },
    ],
    keyColumns: [{ name: "id_board", required: "required", operators: ["="] }],
    *list(ctx) {
      const boardId = getQual(ctx, "id_board");
      if (!boardId) return;
      const body = trGet(ctx, `/boards/${boardId}/cards`);
      if (!body || !Array.isArray(body)) return;
      for (const c of body) {
        yield {
          id: c.id,
          name: c.name || "",
          desc: c.desc || "",
          closed: c.closed ? 1 : 0,
          id_board: c.idBoard || boardId,
          id_list: c.idList || "",
          url: c.url || "",
          due: c.due || "",
          due_complete: c.dueComplete ? 1 : 0,
          labels: JSON.stringify((c.labels || []).map((l: any) => l.name)),
          id_members: JSON.stringify(c.idMembers || []),
          date_last_activity: c.dateLastActivity || "",
        };
      }
    },
  });

  // /boards/{id}/lists (SDK: client.GetListsOnBoard)
  dl.registerTable("trello_lists", {
    description: "Trello lists on a board",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "closed", type: "boolean" },
      { name: "id_board", type: "string" },
      { name: "pos", type: "number" },
    ],
    keyColumns: [{ name: "id_board", required: "required", operators: ["="] }],
    *list(ctx) {
      const boardId = getQual(ctx, "id_board");
      if (!boardId) return;
      const body = trGet(ctx, `/boards/${boardId}/lists`);
      if (!body || !Array.isArray(body)) return;
      for (const l of body) {
        yield {
          id: l.id,
          name: l.name || "",
          closed: l.closed ? 1 : 0,
          id_board: l.idBoard || boardId,
          pos: l.pos || 0,
        };
      }
    },
  });

  // /members/me (SDK: client.GetMyMember)
  dl.registerTable("trello_my_member", {
    description: "Current Trello member info",
    columns: [
      { name: "id", type: "string" },
      { name: "username", type: "string" },
      { name: "full_name", type: "string" },
      { name: "email", type: "string" },
      { name: "url", type: "string" },
      { name: "id_organizations", type: "json" },
    ],
    *list(ctx) {
      const body = trGet(ctx, "/members/me");
      if (!body) return;
      yield {
        id: body.id,
        username: body.username || "",
        full_name: body.fullName || "",
        email: body.email || "",
        url: body.url || "",
        id_organizations: JSON.stringify(body.idOrganizations || []),
      };
    },
  });

  // /members/me/organizations
  dl.registerTable("trello_organizations", {
    description: "Trello organizations/workspaces",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "display_name", type: "string" },
      { name: "desc", type: "string" },
      { name: "url", type: "string" },
    ],
    *list(ctx) {
      const body = trGet(ctx, "/members/me/organizations");
      if (!body || !Array.isArray(body)) return;
      for (const o of body) {
        yield {
          id: o.id,
          name: o.name || "",
          display_name: o.displayName || "",
          desc: o.desc || "",
          url: o.url || "",
        };
      }
    },
  });
}
