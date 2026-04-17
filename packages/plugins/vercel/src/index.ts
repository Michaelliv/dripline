import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import type { DriplinePluginAPI } from "dripline";
import { syncGet } from "dripline";

const API = "https://api.vercel.com";

function getToken(): string | null {
  if (process.env.VERCEL_TOKEN) return process.env.VERCEL_TOKEN;

  // macOS
  const macPath = join(
    homedir(),
    "Library",
    "Application Support",
    "com.vercel.cli",
    "auth.json",
  );
  if (existsSync(macPath)) {
    try {
      return JSON.parse(readFileSync(macPath, "utf-8")).token;
    } catch {
      /* ignore */
    }
  }

  // Linux / XDG
  const xdgPath = join(
    process.env.XDG_CONFIG_HOME ?? join(homedir(), ".config"),
    "com.vercel.cli",
    "auth.json",
  );
  if (existsSync(xdgPath)) {
    try {
      return JSON.parse(readFileSync(xdgPath, "utf-8")).token;
    } catch {
      /* ignore */
    }
  }

  // Legacy
  const legacyPath = join(
    homedir(),
    ".local",
    "share",
    "com.vercel.cli",
    "auth.json",
  );
  if (existsSync(legacyPath)) {
    try {
      return JSON.parse(readFileSync(legacyPath, "utf-8")).token;
    } catch {
      /* ignore */
    }
  }

  return null;
}

export default function vercel(dl: DriplinePluginAPI) {
  dl.setName("vercel");
  dl.setVersion("0.1.0");

  dl.setConnectionSchema({
    token: {
      type: "string",
      required: false,
      description: "Vercel API token (auto-detected from vercel CLI auth)",
      env: "VERCEL_TOKEN",
    },
  });

  function vGet(path: string): any {
    const token = getToken();
    if (!token) {
      dl.log.warn("No Vercel auth found");
      return null;
    }

    const resp = syncGet(`${API}${path}`, {
      Authorization: `Bearer ${token}`,
    });
    if (resp.status !== 200) return null;
    return resp.body;
  }

  dl.registerTable("vercel_projects", {
    description: "Vercel projects",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "framework", type: "string" },
      { name: "node_version", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
    ],
    *list() {
      const data = vGet("/v9/projects?limit=100");
      for (const p of data?.projects ?? []) {
        yield {
          id: p.id ?? "",
          name: p.name ?? "",
          framework: p.framework ?? "",
          node_version: p.nodeVersion ?? "",
          created_at: p.createdAt ? new Date(p.createdAt).toISOString() : "",
          updated_at: p.updatedAt ? new Date(p.updatedAt).toISOString() : "",
        };
      }
    },
  });

  dl.registerTable("vercel_deployments", {
    description: "Vercel deployments. Optionally filter by project_name",
    columns: [
      { name: "uid", type: "string" },
      { name: "name", type: "string" },
      { name: "url", type: "string" },
      { name: "state", type: "string" },
      { name: "target", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "ready_at", type: "datetime" },
      { name: "source", type: "string" },
      { name: "git_branch", type: "string" },
      { name: "git_commit_sha", type: "string" },
      { name: "git_commit_message", type: "string" },
    ],
    keyColumns: [
      { name: "project_name", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const projectName = ctx.quals.find(
        (q) => q.column === "project_name",
      )?.value;
      const limit = ctx.limit ?? 20;
      let path = `/v6/deployments?limit=${limit}`;
      if (projectName) {
        // Get project ID first
        const projects = vGet("/v9/projects?limit=100");
        const project = (projects?.projects ?? []).find(
          (p: any) => p.name === projectName,
        );
        if (project) path += `&projectId=${project.id}`;
      }

      const data = vGet(path);
      for (const d of data?.deployments ?? []) {
        const meta = d.meta ?? {};
        yield {
          uid: d.uid ?? "",
          name: d.name ?? "",
          url: d.url ?? "",
          state: d.state ?? d.readyState ?? "",
          target: d.target ?? "",
          created_at: d.createdAt ? new Date(d.createdAt).toISOString() : "",
          ready_at: d.ready ? new Date(d.ready).toISOString() : "",
          source: d.source ?? "",
          git_branch: meta.githubCommitRef ?? meta.gitlabCommitRef ?? "",
          git_commit_sha: meta.githubCommitSha ?? meta.gitlabCommitSha ?? "",
          git_commit_message:
            meta.githubCommitMessage ?? meta.gitlabCommitMessage ?? "",
        };
      }
    },
  });

  dl.registerTable("vercel_domains", {
    description: "Vercel domains",
    columns: [
      { name: "name", type: "string" },
      { name: "verified", type: "boolean" },
      { name: "nameservers", type: "json" },
      { name: "bought_at", type: "datetime" },
      { name: "created_at", type: "datetime" },
      { name: "expires_at", type: "datetime" },
      { name: "transferred_at", type: "datetime" },
    ],
    *list() {
      const data = vGet("/v5/domains?limit=100");
      for (const d of data?.domains ?? []) {
        yield {
          name: d.name ?? "",
          verified: d.verified ? 1 : 0,
          nameservers: JSON.stringify(
            d.intendedNameservers ?? d.nameservers ?? [],
          ),
          bought_at: d.boughtAt ? new Date(d.boughtAt).toISOString() : "",
          created_at: d.createdAt ? new Date(d.createdAt).toISOString() : "",
          expires_at: d.expiresAt ? new Date(d.expiresAt).toISOString() : "",
          transferred_at: d.transferredAt
            ? new Date(d.transferredAt).toISOString()
            : "",
        };
      }
    },
  });

  dl.registerTable("vercel_env_vars", {
    description:
      "Environment variables for a project. Use WHERE project_name = '...'",
    columns: [
      { name: "key", type: "string" },
      { name: "target", type: "json" },
      { name: "type", type: "string" },
      { name: "updated_at", type: "datetime" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "project_name", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const projectName = ctx.quals.find(
        (q) => q.column === "project_name",
      )?.value;
      if (!projectName) return;

      const data = vGet(`/v9/projects/${projectName}/env`);
      for (const e of data?.envs ?? []) {
        yield {
          key: e.key ?? "",
          target: JSON.stringify(e.target ?? []),
          type: e.type ?? "",
          updated_at: e.updatedAt ? new Date(e.updatedAt).toISOString() : "",
          created_at: e.createdAt ? new Date(e.createdAt).toISOString() : "",
        };
      }
    },
  });
}
