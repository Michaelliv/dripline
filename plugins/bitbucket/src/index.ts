import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const API = "https://api.bitbucket.org/2.0";
function q(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

function bbGet(ctx: QueryContext, path: string): any {
  const user = ctx.connection.config.username || "";
  const pass = ctx.connection.config.password || "";
  const auth = Buffer.from(`${user}:${pass}`).toString("base64");
  const resp = syncGet(`${API}${path}`, { Authorization: `Basic ${auth}` });
  return resp.status === 200 ? resp.body : null;
}

function* bbPaginate(ctx: QueryContext, path: string) {
  let url = `${API}${path}`;
  const user = ctx.connection.config.username || "";
  const pass = ctx.connection.config.password || "";
  const auth = Buffer.from(`${user}:${pass}`).toString("base64");
  while (url) {
    const resp = syncGet(url, { Authorization: `Basic ${auth}` });
    if (resp.status !== 200) return;
    const body = resp.body as any;
    if (!body?.values?.length) return;
    yield* body.values;
    url = body.next || "";
  }
}

export default function bitbucket(dl: DriplinePluginAPI) {
  dl.setName("bitbucket");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    username: {
      type: "string",
      required: true,
      description: "Bitbucket username",
      env: "BITBUCKET_USERNAME",
    },
    password: {
      type: "string",
      required: true,
      description: "Bitbucket app password",
      env: "BITBUCKET_PASSWORD",
    },
  });

  dl.registerTable("bitbucket_repos", {
    description: "Bitbucket repositories",
    columns: [
      { name: "slug", type: "string" },
      { name: "name", type: "string" },
      { name: "full_name", type: "string" },
      { name: "description", type: "string" },
      { name: "is_private", type: "boolean" },
      { name: "language", type: "string" },
      { name: "created_on", type: "datetime" },
      { name: "updated_on", type: "datetime" },
    ],
    keyColumns: [{ name: "workspace", required: "required", operators: ["="] }],
    *list(ctx) {
      const ws = q(ctx, "workspace");
      if (!ws) return;
      for (const r of bbPaginate(ctx, `/repositories/${ws}`)) {
        yield {
          slug: r.slug,
          name: r.name || "",
          full_name: r.full_name || "",
          description: r.description || "",
          is_private: r.is_private ? 1 : 0,
          language: r.language || "",
          created_on: r.created_on || "",
          updated_on: r.updated_on || "",
        };
      }
    },
  });

  dl.registerTable("bitbucket_pull_requests", {
    description: "Bitbucket pull requests",
    columns: [
      { name: "id", type: "number" },
      { name: "title", type: "string" },
      { name: "state", type: "string" },
      { name: "author", type: "string" },
      { name: "source_branch", type: "string" },
      { name: "destination_branch", type: "string" },
      { name: "created_on", type: "datetime" },
      { name: "updated_on", type: "datetime" },
    ],
    keyColumns: [
      { name: "workspace", required: "required", operators: ["="] },
      { name: "repo_slug", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const ws = q(ctx, "workspace");
      const repo = q(ctx, "repo_slug");
      if (!ws || !repo) return;
      for (const pr of bbPaginate(
        ctx,
        `/repositories/${ws}/${repo}/pullrequests`,
      )) {
        yield {
          id: pr.id,
          title: pr.title || "",
          state: pr.state || "",
          author: pr.author?.display_name || "",
          source_branch: pr.source?.branch?.name || "",
          destination_branch: pr.destination?.branch?.name || "",
          created_on: pr.created_on || "",
          updated_on: pr.updated_on || "",
        };
      }
    },
  });

  dl.registerTable("bitbucket_issues", {
    description: "Bitbucket issues",
    columns: [
      { name: "id", type: "number" },
      { name: "title", type: "string" },
      { name: "state", type: "string" },
      { name: "priority", type: "string" },
      { name: "kind", type: "string" },
      { name: "assignee", type: "string" },
      { name: "reporter", type: "string" },
      { name: "created_on", type: "datetime" },
    ],
    keyColumns: [
      { name: "workspace", required: "required", operators: ["="] },
      { name: "repo_slug", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const ws = q(ctx, "workspace");
      const repo = q(ctx, "repo_slug");
      if (!ws || !repo) return;
      for (const i of bbPaginate(ctx, `/repositories/${ws}/${repo}/issues`)) {
        yield {
          id: i.id,
          title: i.title || "",
          state: i.state || "",
          priority: i.priority || "",
          kind: i.kind || "",
          assignee: i.assignee?.display_name || "",
          reporter: i.reporter?.display_name || "",
          created_on: i.created_on || "",
        };
      }
    },
  });

  dl.registerTable("bitbucket_pipelines", {
    description: "Bitbucket pipeline runs",
    columns: [
      { name: "uuid", type: "string" },
      { name: "state_name", type: "string" },
      { name: "result_name", type: "string" },
      { name: "target_ref_name", type: "string" },
      { name: "creator", type: "string" },
      { name: "created_on", type: "datetime" },
      { name: "completed_on", type: "datetime" },
      { name: "duration_in_seconds", type: "number" },
    ],
    keyColumns: [
      { name: "workspace", required: "required", operators: ["="] },
      { name: "repo_slug", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const ws = q(ctx, "workspace");
      const repo = q(ctx, "repo_slug");
      if (!ws || !repo) return;
      for (const p of bbPaginate(
        ctx,
        `/repositories/${ws}/${repo}/pipelines/?sort=-created_on`,
      )) {
        yield {
          uuid: p.uuid || "",
          state_name: p.state?.name || "",
          result_name: p.state?.result?.name || "",
          target_ref_name: p.target?.ref_name || "",
          creator: p.creator?.display_name || "",
          created_on: p.created_on || "",
          completed_on: p.completed_on || "",
          duration_in_seconds: p.duration_in_seconds || 0,
        };
      }
    },
  });
}
