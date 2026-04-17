import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

function glGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const baseUrl = (
    ctx.connection.config.base_url || "https://gitlab.com"
  ).replace(/\/$/, "");
  const token = ctx.connection.config.token || "";
  const qs = new URLSearchParams(params).toString();
  const url = `${baseUrl}/api/v4${path}${qs ? `?${qs}` : ""}`;
  const resp = syncGet(url, { "PRIVATE-TOKEN": token });
  return resp.status === 200 ? resp.body : null;
}

function* glPaginate(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
) {
  let page = 1;
  while (true) {
    const body = glGet(ctx, path, {
      ...params,
      per_page: "100",
      page: String(page),
    });
    if (!body || !Array.isArray(body) || body.length === 0) return;
    yield* body;
    if (body.length < 100) return;
    page++;
  }
}

function getQual(ctx: QueryContext, name: string): string | undefined {
  return ctx.quals.find((q) => q.column === name)?.value;
}

function encodeProject(id: string): string {
  return encodeURIComponent(id);
}

export default function gitlab(dl: DriplinePluginAPI) {
  dl.setName("gitlab");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    token: {
      type: "string",
      required: true,
      description: "GitLab personal access token",
      env: "GITLAB_TOKEN",
    },
    base_url: {
      type: "string",
      required: false,
      description: "GitLab instance URL (default: https://gitlab.com)",
      env: "GITLAB_ADDR",
    },
  });

  dl.registerTable("gitlab_projects", {
    description: "GitLab projects",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "path_with_namespace", type: "string" },
      { name: "description", type: "string" },
      { name: "visibility", type: "string" },
      { name: "default_branch", type: "string" },
      { name: "star_count", type: "number" },
      { name: "forks_count", type: "number" },
      { name: "open_issues_count", type: "number" },
      { name: "web_url", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "last_activity_at", type: "datetime" },
      { name: "archived", type: "boolean" },
      { name: "topics", type: "json" },
    ],
    keyColumns: [
      { name: "owner", required: "optional", operators: ["="] },
      { name: "group_id", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const owner = getQual(ctx, "owner");
      const groupId = getQual(ctx, "group_id");
      let path = "/projects?membership=true";
      if (owner) path = `/users/${encodeURIComponent(owner)}/projects`;
      if (groupId) path = `/groups/${encodeURIComponent(groupId)}/projects`;
      for (const p of glPaginate(ctx, path)) {
        yield {
          id: p.id,
          name: p.name,
          path_with_namespace: p.path_with_namespace || "",
          description: p.description || "",
          visibility: p.visibility || "",
          default_branch: p.default_branch || "",
          star_count: p.star_count || 0,
          forks_count: p.forks_count || 0,
          open_issues_count: p.open_issues_count || 0,
          web_url: p.web_url || "",
          created_at: p.created_at || "",
          last_activity_at: p.last_activity_at || "",
          archived: p.archived ? 1 : 0,
          topics: JSON.stringify(p.topics || p.tag_list || []),
        };
      }
    },
  });

  dl.registerTable("gitlab_issues", {
    description: "GitLab issues",
    columns: [
      { name: "id", type: "number" },
      { name: "iid", type: "number" },
      { name: "title", type: "string" },
      { name: "state", type: "string" },
      { name: "author_username", type: "string" },
      { name: "assignee_username", type: "string" },
      { name: "labels", type: "json" },
      { name: "web_url", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
      { name: "closed_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "project_id", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const projectId = getQual(ctx, "project_id");
      if (!projectId) return;
      for (const i of glPaginate(
        ctx,
        `/projects/${encodeProject(projectId)}/issues`,
      )) {
        yield {
          id: i.id,
          iid: i.iid,
          title: i.title || "",
          state: i.state || "",
          author_username: i.author?.username || "",
          assignee_username: i.assignee?.username || "",
          labels: JSON.stringify(i.labels || []),
          web_url: i.web_url || "",
          created_at: i.created_at || "",
          updated_at: i.updated_at || "",
          closed_at: i.closed_at || "",
        };
      }
    },
  });

  dl.registerTable("gitlab_merge_requests", {
    description: "GitLab merge requests",
    columns: [
      { name: "id", type: "number" },
      { name: "iid", type: "number" },
      { name: "title", type: "string" },
      { name: "state", type: "string" },
      { name: "author_username", type: "string" },
      { name: "source_branch", type: "string" },
      { name: "target_branch", type: "string" },
      { name: "web_url", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "merged_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "project_id", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const projectId = getQual(ctx, "project_id");
      if (!projectId) return;
      for (const mr of glPaginate(
        ctx,
        `/projects/${encodeProject(projectId)}/merge_requests`,
      )) {
        yield {
          id: mr.id,
          iid: mr.iid,
          title: mr.title || "",
          state: mr.state || "",
          author_username: mr.author?.username || "",
          source_branch: mr.source_branch || "",
          target_branch: mr.target_branch || "",
          web_url: mr.web_url || "",
          created_at: mr.created_at || "",
          merged_at: mr.merged_at || "",
        };
      }
    },
  });

  dl.registerTable("gitlab_pipelines", {
    description: "GitLab CI/CD pipelines",
    columns: [
      { name: "id", type: "number" },
      { name: "iid", type: "number" },
      { name: "status", type: "string" },
      { name: "source", type: "string" },
      { name: "ref", type: "string" },
      { name: "sha", type: "string" },
      { name: "web_url", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "project_id", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const projectId = getQual(ctx, "project_id");
      if (!projectId) return;
      for (const p of glPaginate(
        ctx,
        `/projects/${encodeProject(projectId)}/pipelines`,
      )) {
        yield {
          id: p.id,
          iid: p.iid || 0,
          status: p.status || "",
          source: p.source || "",
          ref: p.ref || "",
          sha: p.sha || "",
          web_url: p.web_url || "",
          created_at: p.created_at || "",
          updated_at: p.updated_at || "",
        };
      }
    },
  });

  dl.registerTable("gitlab_branches", {
    description: "GitLab project branches",
    columns: [
      { name: "name", type: "string" },
      { name: "merged", type: "boolean" },
      { name: "protected", type: "boolean" },
      { name: "default", type: "boolean" },
      { name: "commit_sha", type: "string" },
      { name: "commit_message", type: "string" },
      { name: "commit_date", type: "datetime" },
    ],
    keyColumns: [
      { name: "project_id", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const projectId = getQual(ctx, "project_id");
      if (!projectId) return;
      for (const b of glPaginate(
        ctx,
        `/projects/${encodeProject(projectId)}/repository/branches`,
      )) {
        yield {
          name: b.name || "",
          merged: b.merged ? 1 : 0,
          protected: b.protected ? 1 : 0,
          default: b.default ? 1 : 0,
          commit_sha: b.commit?.id || "",
          commit_message: b.commit?.message || "",
          commit_date: b.commit?.committed_date || "",
        };
      }
    },
  });

  dl.registerTable("gitlab_commits", {
    description: "GitLab project commits",
    columns: [
      { name: "id", type: "string" },
      { name: "short_id", type: "string" },
      { name: "title", type: "string" },
      { name: "author_name", type: "string" },
      { name: "author_email", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "message", type: "string" },
      { name: "web_url", type: "string" },
    ],
    keyColumns: [
      { name: "project_id", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const projectId = getQual(ctx, "project_id");
      if (!projectId) return;
      for (const c of glPaginate(
        ctx,
        `/projects/${encodeProject(projectId)}/repository/commits`,
      )) {
        yield {
          id: c.id || "",
          short_id: c.short_id || "",
          title: c.title || "",
          author_name: c.author_name || "",
          author_email: c.author_email || "",
          created_at: c.created_at || "",
          message: c.message || "",
          web_url: c.web_url || "",
        };
      }
    },
  });

  dl.registerTable("gitlab_groups", {
    description: "GitLab groups",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "path", type: "string" },
      { name: "full_path", type: "string" },
      { name: "description", type: "string" },
      { name: "visibility", type: "string" },
      { name: "web_url", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const g of glPaginate(ctx, "/groups")) {
        yield {
          id: g.id,
          name: g.name || "",
          path: g.path || "",
          full_path: g.full_path || "",
          description: g.description || "",
          visibility: g.visibility || "",
          web_url: g.web_url || "",
          created_at: g.created_at || "",
        };
      }
    },
  });

  dl.registerTable("gitlab_users", {
    description: "GitLab users",
    columns: [
      { name: "id", type: "number" },
      { name: "username", type: "string" },
      { name: "name", type: "string" },
      { name: "state", type: "string" },
      { name: "email", type: "string" },
      { name: "is_admin", type: "boolean" },
      { name: "web_url", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const u of glPaginate(ctx, "/users")) {
        yield {
          id: u.id,
          username: u.username || "",
          name: u.name || "",
          state: u.state || "",
          email: u.email || "",
          is_admin: u.is_admin ? 1 : 0,
          web_url: u.web_url || "",
          created_at: u.created_at || "",
        };
      }
    },
  });
}
