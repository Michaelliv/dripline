import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const API = "https://hub.docker.com/v2";

// Docker Hub JWT auth (matching hub-tool SDK's Login):
// POST /v2/users/login?refresh_token=true with {username, password}
// Returns {token}
function getDockerHubToken(username: string, password: string): string | null {
  const { execSync } = require("node:child_process");
  try {
    const body = JSON.stringify({ username, password });
    const result = execSync(
      `curl -s -X POST "${API}/users/login?refresh_token=true" ` +
        `-H "Content-Type: application/json" -d '${body.replace(/'/g, "'\\''")}'`,
      { encoding: "utf-8", timeout: 15000 },
    );
    const resp = JSON.parse(result);
    return resp.token || null;
  } catch {
    return null;
  }
}

function dhGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const username = ctx.connection.config.username || "";
  const password = ctx.connection.config.password || "";

  const qs = new URLSearchParams(params).toString();
  const url = `${API}${path}${qs ? (path.includes("?") ? "&" : "?") + qs : ""}`;

  const headers: Record<string, string> = {};
  if (username && password) {
    const token = getDockerHubToken(username, password);
    if (token) headers.Authorization = `Bearer ${token}`;
  }

  const resp = syncGet(url, headers);
  return resp.status === 200 ? resp.body : null;
}

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

export default function dockerhub(dl: DriplinePluginAPI) {
  dl.setName("dockerhub");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    username: {
      type: "string",
      required: false,
      description: "Docker Hub username",
      env: "DOCKER_HUB_USERNAME",
    },
    password: {
      type: "string",
      required: false,
      description: "Docker Hub password or PAT",
      env: "DOCKER_HUB_PASSWORD",
    },
  });

  // GET /v2/repositories/{namespace}/ (matches hub-tool SDK's GetRepositories)
  dl.registerTable("dockerhub_repos", {
    description: "Docker Hub repositories for a user/org",
    columns: [
      { name: "name", type: "string" },
      { name: "namespace", type: "string" },
      { name: "description", type: "string" },
      { name: "star_count", type: "number" },
      { name: "pull_count", type: "number" },
      { name: "is_private", type: "boolean" },
      { name: "last_updated", type: "datetime" },
    ],
    keyColumns: [{ name: "namespace", required: "required", operators: ["="] }],
    *list(ctx) {
      const ns = getQual(ctx, "namespace");
      if (!ns) return;
      let page = 1;
      while (true) {
        const body = dhGet(ctx, `/repositories/${ns}/`, {
          page: String(page),
          page_size: "100",
        });
        if (!body?.results?.length) return;
        for (const r of body.results) {
          yield {
            name: r.name,
            namespace: r.namespace || ns,
            description: (r.description || "").slice(0, 200),
            star_count: r.star_count || 0,
            pull_count: r.pull_count || 0,
            is_private: r.is_private ? 1 : 0,
            last_updated: r.last_updated || "",
          };
        }
        if (!body.next) return;
        page++;
      }
    },
  });

  // GET /v2/repositories/{namespace}/{repo}/tags/ (matches hub-tool SDK)
  dl.registerTable("dockerhub_tags", {
    description: "Tags for a Docker Hub repository",
    columns: [
      { name: "name", type: "string" },
      { name: "full_size", type: "number" },
      { name: "last_updated", type: "datetime" },
      { name: "digest", type: "string" },
    ],
    keyColumns: [
      { name: "namespace", required: "required", operators: ["="] },
      { name: "repo", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const ns = getQual(ctx, "namespace");
      const repo = getQual(ctx, "repo");
      if (!ns || !repo) return;
      let page = 1;
      while (true) {
        const body = dhGet(ctx, `/repositories/${ns}/${repo}/tags`, {
          page: String(page),
          page_size: "100",
        });
        if (!body?.results?.length) return;
        for (const t of body.results) {
          yield {
            name: t.name,
            full_size: t.full_size || 0,
            last_updated: t.last_updated || "",
            digest: t.images?.[0]?.digest || t.digest || "",
          };
        }
        if (!body.next) return;
        page++;
      }
    },
  });

  // GET /v2/search/repositories/ (public search, no auth needed)
  dl.registerTable("dockerhub_search", {
    description: "Search Docker Hub",
    columns: [
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "star_count", type: "number" },
      { name: "pull_count", type: "number" },
      { name: "is_official", type: "boolean" },
    ],
    keyColumns: [{ name: "query", required: "required", operators: ["="] }],
    *list(ctx) {
      const q = getQual(ctx, "query");
      if (!q) return;
      const body = dhGet(ctx, `/search/repositories/`, {
        query: q,
        page_size: "100",
      });
      if (!body?.results) return;
      for (const r of body.results) {
        yield {
          name: r.repo_name || r.name || "",
          description: (r.short_description || "").slice(0, 200),
          star_count: r.star_count || 0,
          pull_count: r.pull_count || 0,
          is_official: r.is_official ? 1 : 0,
        };
      }
    },
  });

  // GET /v2/users/{username} (authenticated - user info)
  dl.registerTable("dockerhub_user", {
    description: "Docker Hub user info (requires auth for full details)",
    columns: [
      { name: "id", type: "string" },
      { name: "username", type: "string" },
      { name: "full_name", type: "string" },
      { name: "location", type: "string" },
      { name: "company", type: "string" },
      { name: "date_joined", type: "datetime" },
      { name: "type", type: "string" },
    ],
    keyColumns: [{ name: "username", required: "required", operators: ["="] }],
    *list(ctx) {
      const username = getQual(ctx, "username");
      if (!username) return;
      const body = dhGet(ctx, `/users/${username}`);
      if (!body) return;
      yield {
        id: body.id || "",
        username: body.username || "",
        full_name: body.full_name || "",
        location: body.location || "",
        company: body.company || "",
        date_joined: body.date_joined || "",
        type: body.type || "",
      };
    },
  });
}
