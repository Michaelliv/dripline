import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet, syncGetPaginated } from "dripline";

const API = "https://api.github.com";

export default function github(dl: DriplinePluginAPI) {
  dl.setName("github");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    token: {
      type: "string",
      required: false,
      description: "GitHub personal access token",
      env: "GITHUB_TOKEN",
    },
  });

  function authHeaders(ctx: QueryContext): Record<string, string> {
    const h: Record<string, string> = {
      Accept: "application/vnd.github+json",
      "User-Agent": "dripline",
    };
    if (ctx.connection.config.token) {
      h.Authorization = `Bearer ${ctx.connection.config.token}`;
    }
    return h;
  }

  function getQual(ctx: QueryContext, name: string): string | undefined {
    return ctx.quals.find((q) => q.column === name)?.value;
  }

  dl.registerTable("github_repos", {
    description: "GitHub repositories for a user or organization",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "full_name", type: "string" },
      { name: "description", type: "string" },
      { name: "stargazers_count", type: "number" },
      { name: "forks_count", type: "number" },
      { name: "language", type: "string" },
      { name: "html_url", type: "string" },
      { name: "private", type: "boolean" },
      { name: "archived", type: "boolean" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
      { name: "topics", type: "json" },
    ],
    keyColumns: [{ name: "owner", required: "required", operators: ["="] }],
    *list(ctx) {
      const owner = getQual(ctx, "owner");
      if (!owner) return;
      const data = syncGetPaginated(
        `${API}/users/${owner}/repos?per_page=100`,
        authHeaders(ctx),
      );
      for (const r of data) {
        yield {
          id: r.id,
          name: r.name,
          full_name: r.full_name,
          description: r.description || "",
          stargazers_count: r.stargazers_count,
          forks_count: r.forks_count,
          language: r.language || "",
          html_url: r.html_url,
          private: r.private ? 1 : 0,
          archived: r.archived ? 1 : 0,
          created_at: r.created_at,
          updated_at: r.updated_at,
          topics: JSON.stringify(r.topics || []),
        };
      }
    },
    get(ctx) {
      const owner = getQual(ctx, "owner");
      const name = getQual(ctx, "name");
      if (!owner || !name) return null;
      const resp = syncGet(`${API}/repos/${owner}/${name}`, authHeaders(ctx));
      if (resp.status !== 200) return null;
      const r = resp.body;
      return {
        id: r.id,
        name: r.name,
        full_name: r.full_name,
        description: r.description || "",
        stargazers_count: r.stargazers_count,
        forks_count: r.forks_count,
        language: r.language || "",
        html_url: r.html_url,
        private: r.private ? 1 : 0,
        archived: r.archived ? 1 : 0,
        created_at: r.created_at,
        updated_at: r.updated_at,
        topics: JSON.stringify(r.topics || []),
      };
    },
  });

  dl.registerTable("github_issues", {
    description: "GitHub issues for a repository",
    columns: [
      { name: "id", type: "number" },
      { name: "number", type: "number" },
      { name: "title", type: "string" },
      { name: "state", type: "string" },
      { name: "user_login", type: "string" },
      { name: "labels", type: "json" },
      { name: "body", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
      { name: "html_url", type: "string" },
    ],
    keyColumns: [
      { name: "owner", required: "required", operators: ["="] },
      { name: "repo", required: "required", operators: ["="] },
      { name: "issue_state", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const owner = getQual(ctx, "owner");
      const repo = getQual(ctx, "repo");
      if (!owner || !repo) return;
      const state = getQual(ctx, "issue_state") || "open";
      const data = syncGetPaginated(
        `${API}/repos/${owner}/${repo}/issues?per_page=100&state=${state}`,
        authHeaders(ctx),
      );
      for (const r of data) {
        if (r.pull_request) continue;
        yield {
          id: r.id,
          number: r.number,
          title: r.title,
          state: r.state,
          user_login: r.user?.login || "",
          labels: JSON.stringify(r.labels?.map((l: any) => l.name) || []),
          body: r.body || "",
          created_at: r.created_at,
          updated_at: r.updated_at,
          html_url: r.html_url,
        };
      }
    },
  });

  dl.registerTable("github_pull_requests", {
    description: "GitHub pull requests for a repository",
    columns: [
      { name: "id", type: "number" },
      { name: "number", type: "number" },
      { name: "title", type: "string" },
      { name: "state", type: "string" },
      { name: "user_login", type: "string" },
      { name: "head_ref", type: "string" },
      { name: "base_ref", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "merged_at", type: "datetime" },
      { name: "html_url", type: "string" },
    ],
    keyColumns: [
      { name: "owner", required: "required", operators: ["="] },
      { name: "repo", required: "required", operators: ["="] },
      { name: "pr_state", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const owner = getQual(ctx, "owner");
      const repo = getQual(ctx, "repo");
      if (!owner || !repo) return;
      const state = getQual(ctx, "pr_state") || "open";
      const data = syncGetPaginated(
        `${API}/repos/${owner}/${repo}/pulls?per_page=100&state=${state}`,
        authHeaders(ctx),
      );
      for (const r of data) {
        yield {
          id: r.id,
          number: r.number,
          title: r.title,
          state: r.state,
          user_login: r.user?.login || "",
          head_ref: r.head?.ref || "",
          base_ref: r.base?.ref || "",
          created_at: r.created_at,
          merged_at: r.merged_at || "",
          html_url: r.html_url,
        };
      }
    },
  });

  dl.registerTable("github_stargazers", {
    description: "GitHub stargazers for a repository",
    columns: [
      { name: "login", type: "string" },
      { name: "starred_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "owner", required: "required", operators: ["="] },
      { name: "repo", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const owner = getQual(ctx, "owner");
      const repo = getQual(ctx, "repo");
      if (!owner || !repo) return;
      const headers = {
        ...authHeaders(ctx),
        Accept: "application/vnd.github.star+json",
      };
      const data = syncGetPaginated(
        `${API}/repos/${owner}/${repo}/stargazers?per_page=100`,
        headers,
      );
      for (const r of data) {
        yield {
          login: r.user?.login || r.login || "",
          starred_at: r.starred_at || "",
        };
      }
    },
  });
}
