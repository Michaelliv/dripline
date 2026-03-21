import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

function jiraGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const baseUrl = ctx.connection.config.base_url || "";
  const username = ctx.connection.config.username || "";
  const token = ctx.connection.config.token || "";
  const pat = ctx.connection.config.personal_access_token || "";
  const qs = new URLSearchParams(params).toString();
  const url = `${baseUrl}/rest/api/3${path}${qs ? `?${qs}` : ""}`;

  const headers: Record<string, string> = { Accept: "application/json" };
  if (pat) {
    headers.Authorization = `Bearer ${pat}`;
  } else {
    headers.Authorization = `Basic ${Buffer.from(`${username}:${token}`).toString("base64")}`;
  }

  const resp = syncGet(url, headers);
  return resp.status === 200 ? resp.body : null;
}

function* jiraPaginate(
  ctx: QueryContext,
  path: string,
  key: string,
  extra: Record<string, string> = {},
) {
  let startAt = 0;
  const maxResults = 100;
  while (true) {
    const body = jiraGet(ctx, path, {
      ...extra,
      startAt: String(startAt),
      maxResults: String(maxResults),
    });
    if (!body) return;
    const items = body[key] || body.values || [];
    if (items.length === 0) return;
    yield* items;
    startAt += items.length;
    if (body.total !== undefined && startAt >= body.total) return;
    if (items.length < maxResults) return;
  }
}

export default function jira(dl: DriplinePluginAPI) {
  dl.setName("jira");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    base_url: {
      type: "string",
      required: true,
      description: "Jira instance URL (e.g. https://yoursite.atlassian.net)",
      env: "JIRA_URL",
    },
    username: {
      type: "string",
      required: false,
      description: "Jira username (email for Cloud)",
      env: "JIRA_USER",
    },
    token: {
      type: "string",
      required: false,
      description: "Jira API token (used with username for Basic auth)",
      env: "JIRA_TOKEN",
    },
    personal_access_token: {
      type: "string",
      required: false,
      description:
        "Jira Personal Access Token (Bearer auth, alternative to username+token)",
      env: "JIRA_PERSONAL_ACCESS_TOKEN",
    },
  });

  dl.registerTable("jira_projects", {
    description: "Jira projects",
    columns: [
      { name: "id", type: "string" },
      { name: "key", type: "string" },
      { name: "name", type: "string" },
      { name: "project_type", type: "string" },
      { name: "style", type: "string" },
      { name: "lead_display_name", type: "string" },
    ],
    *list(ctx) {
      for (const p of jiraPaginate(ctx, "/project/search", "values")) {
        yield {
          id: p.id,
          key: p.key,
          name: p.name,
          project_type: p.projectTypeKey || "",
          style: p.style || "",
          lead_display_name: p.lead?.displayName || "",
        };
      }
    },
  });

  dl.registerTable("jira_issues", {
    description: "Jira issues (via JQL search)",
    columns: [
      { name: "id", type: "string" },
      { name: "key", type: "string" },
      { name: "summary", type: "string" },
      { name: "status", type: "string" },
      { name: "issue_type", type: "string" },
      { name: "priority", type: "string" },
      { name: "assignee", type: "string" },
      { name: "reporter", type: "string" },
      { name: "project_key", type: "string" },
      { name: "created", type: "datetime" },
      { name: "updated", type: "datetime" },
      { name: "resolution_date", type: "datetime" },
      { name: "labels", type: "json" },
      { name: "description", type: "string" },
    ],
    keyColumns: [
      { name: "jql", required: "optional", operators: ["="] },
      { name: "project_key", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      let jql = ctx.quals.find((q) => q.column === "jql")?.value || "";
      const projectKey = ctx.quals.find(
        (q) => q.column === "project_key",
      )?.value;
      if (!jql && projectKey) jql = `project = "${projectKey}"`;
      if (!jql) jql = "ORDER BY updated DESC";

      let startAt = 0;
      const maxResults = 100;
      while (true) {
        const body = jiraGet(ctx, "/search", {
          jql,
          startAt: String(startAt),
          maxResults: String(maxResults),
          fields:
            "summary,status,issuetype,priority,assignee,reporter,project,created,updated,resolutiondate,labels,description",
        });
        if (!body?.issues) return;
        for (const i of body.issues) {
          const f = i.fields || {};
          yield {
            id: i.id,
            key: i.key,
            summary: f.summary || "",
            status: f.status?.name || "",
            issue_type: f.issuetype?.name || "",
            priority: f.priority?.name || "",
            assignee: f.assignee?.displayName || "",
            reporter: f.reporter?.displayName || "",
            project_key: f.project?.key || "",
            created: f.created || "",
            updated: f.updated || "",
            resolution_date: f.resolutiondate || "",
            labels: JSON.stringify(f.labels || []),
            description: typeof f.description === "string" ? f.description : "",
          };
        }
        startAt += body.issues.length;
        if (startAt >= (body.total || 0)) return;
        if (body.issues.length < maxResults) return;
      }
    },
  });

  dl.registerTable("jira_sprints", {
    description: "Jira sprints for a board",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "state", type: "string" },
      { name: "board_id", type: "number" },
      { name: "start_date", type: "datetime" },
      { name: "end_date", type: "datetime" },
      { name: "complete_date", type: "datetime" },
      { name: "goal", type: "string" },
    ],
    keyColumns: [{ name: "board_id", required: "required", operators: ["="] }],
    *list(ctx) {
      const boardId = ctx.quals.find((q) => q.column === "board_id")?.value;
      if (!boardId) return;
      const baseUrl = ctx.connection.config.base_url || "";
      const username = ctx.connection.config.username || "";
      const token = ctx.connection.config.token || "";
      const pat = ctx.connection.config.personal_access_token || "";
      const headers: Record<string, string> = { Accept: "application/json" };
      if (pat) {
        headers.Authorization = `Bearer ${pat}`;
      } else {
        headers.Authorization = `Basic ${Buffer.from(`${username}:${token}`).toString("base64")}`;
      }

      let startAt = 0;
      while (true) {
        const url = `${baseUrl}/rest/agile/1.0/board/${boardId}/sprint?startAt=${startAt}&maxResults=50`;
        const resp = syncGet(url, headers);
        if (resp.status !== 200) return;
        const body = resp.body as any;
        if (!body?.values?.length) return;
        for (const s of body.values) {
          yield {
            id: s.id,
            name: s.name || "",
            state: s.state || "",
            board_id: Number(boardId),
            start_date: s.startDate || "",
            end_date: s.endDate || "",
            complete_date: s.completeDate || "",
            goal: s.goal || "",
          };
        }
        if (body.isLast) return;
        startAt += body.values.length;
      }
    },
  });

  dl.registerTable("jira_boards", {
    description: "Jira boards (Scrum/Kanban)",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "board_type", type: "string" },
      { name: "project_key", type: "string" },
    ],
    *list(ctx) {
      const baseUrl = ctx.connection.config.base_url || "";
      const username = ctx.connection.config.username || "";
      const token = ctx.connection.config.token || "";
      const pat = ctx.connection.config.personal_access_token || "";
      const headers2: Record<string, string> = { Accept: "application/json" };
      if (pat) {
        headers2.Authorization = `Bearer ${pat}`;
      } else {
        headers2.Authorization = `Basic ${Buffer.from(`${username}:${token}`).toString("base64")}`;
      }

      let startAt = 0;
      while (true) {
        const url = `${baseUrl}/rest/agile/1.0/board?startAt=${startAt}&maxResults=50`;
        const resp = syncGet(url, headers2);
        if (resp.status !== 200) return;
        const body = resp.body as any;
        if (!body?.values?.length) return;
        for (const b of body.values) {
          yield {
            id: b.id,
            name: b.name || "",
            board_type: b.type || "",
            project_key: b.location?.projectKey || "",
          };
        }
        if (body.isLast) return;
        startAt += body.values.length;
      }
    },
  });

  dl.registerTable("jira_users", {
    description: "Jira users",
    columns: [
      { name: "account_id", type: "string" },
      { name: "display_name", type: "string" },
      { name: "email", type: "string" },
      { name: "active", type: "boolean" },
      { name: "account_type", type: "string" },
    ],
    *list(ctx) {
      let startAt = 0;
      while (true) {
        const body = jiraGet(ctx, "/users/search", {
          startAt: String(startAt),
          maxResults: "100",
        });
        if (!body || !Array.isArray(body) || body.length === 0) return;
        for (const u of body) {
          yield {
            account_id: u.accountId || "",
            display_name: u.displayName || "",
            email: u.emailAddress || "",
            active: u.active ? 1 : 0,
            account_type: u.accountType || "",
          };
        }
        startAt += body.length;
        if (body.length < 100) return;
      }
    },
  });

  dl.registerTable("jira_issue_comments", {
    description: "Comments on a Jira issue",
    columns: [
      { name: "id", type: "string" },
      { name: "issue_key", type: "string" },
      { name: "author", type: "string" },
      { name: "body", type: "string" },
      { name: "created", type: "datetime" },
      { name: "updated", type: "datetime" },
    ],
    keyColumns: [{ name: "issue_key", required: "required", operators: ["="] }],
    *list(ctx) {
      const issueKey = ctx.quals.find((q) => q.column === "issue_key")?.value;
      if (!issueKey) return;
      for (const c of jiraPaginate(
        ctx,
        `/issue/${issueKey}/comment`,
        "comments",
      )) {
        yield {
          id: c.id,
          issue_key: issueKey,
          author: c.author?.displayName || "",
          body: typeof c.body === "string" ? c.body : "",
          created: c.created || "",
          updated: c.updated || "",
        };
      }
    },
  });
}
