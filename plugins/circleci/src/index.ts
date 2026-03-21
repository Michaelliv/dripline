import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const API = "https://circleci.com/api/v2";

function ccGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const token = ctx.connection.config.api_token || "";
  const qs = new URLSearchParams(params).toString();
  const url = `${API}/${path}${qs ? `?${qs}` : ""}`;
  const resp = syncGet(url, {
    "Circle-Token": token,
    Accept: "application/json",
  });
  return resp.status === 200 ? resp.body : null;
}

function* ccPaginate(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
) {
  let pageToken = "";
  while (true) {
    const p = { ...params };
    if (pageToken) p["page-token"] = pageToken;
    const body = ccGet(ctx, path, p);
    if (!body?.items?.length) return;
    yield* body.items;
    if (!body.next_page_token) return;
    pageToken = body.next_page_token;
  }
}

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

export default function circleci(dl: DriplinePluginAPI) {
  dl.setName("circleci");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_token: {
      type: "string",
      required: true,
      description: "CircleCI API token",
      env: "CIRCLECI_TOKEN",
    },
  });

  // GET /me/collaborations -> list of orgs
  dl.registerTable("circleci_organizations", {
    description: "CircleCI organizations the current user belongs to",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "slug", type: "string" },
      { name: "vcs_type", type: "string" },
    ],
    *list(ctx) {
      const body = ccGet(ctx, "me/collaborations");
      if (!body || !Array.isArray(body)) return;
      for (const o of body) {
        yield {
          id: o.id || "",
          name: o.name || "",
          slug: o.slug || "",
          vcs_type: o.vcs_type || "",
        };
      }
    },
  });

  // GET /pipeline?org-slug={slug} -> pipelines for an org
  dl.registerTable("circleci_pipelines", {
    description: "CircleCI pipelines for a project",
    columns: [
      { name: "id", type: "string" },
      { name: "project_slug", type: "string" },
      { name: "number", type: "number" },
      { name: "state", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
      { name: "trigger_type", type: "string" },
      { name: "trigger_actor", type: "string" },
      { name: "vcs_branch", type: "string" },
      { name: "vcs_revision", type: "string" },
      { name: "vcs_commit_subject", type: "string" },
    ],
    keyColumns: [
      { name: "project_slug", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const slug = getQual(ctx, "project_slug");
      if (!slug) return;
      // Extract org-slug from project_slug (first two parts: vcs/org)
      const parts = slug.split("/");
      if (parts.length < 2) return;
      const orgSlug = `${parts[0]}/${parts[1]}`;

      for (const p of ccPaginate(ctx, "pipeline", { "org-slug": orgSlug })) {
        // Filter to matching project_slug if full slug provided
        if (parts.length >= 3 && p.project_slug !== slug) continue;
        yield {
          id: p.id || "",
          project_slug: p.project_slug || "",
          number: p.number || 0,
          state: p.state || "",
          created_at: p.created_at || "",
          updated_at: p.updated_at || "",
          trigger_type: p.trigger?.type || "",
          trigger_actor: p.trigger?.actor?.login || "",
          vcs_branch: p.vcs?.branch || "",
          vcs_revision: p.vcs?.revision || "",
          vcs_commit_subject: p.vcs?.commit?.subject || "",
        };
      }
    },
  });

  // GET /pipeline/{id}/workflow -> workflows for a pipeline
  dl.registerTable("circleci_workflows", {
    description: "CircleCI workflows for a pipeline",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "pipeline_id", type: "string" },
      { name: "pipeline_number", type: "number" },
      { name: "project_slug", type: "string" },
      { name: "status", type: "string" },
      { name: "started_by", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "stopped_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "pipeline_id", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const pipelineId = getQual(ctx, "pipeline_id");
      if (!pipelineId) return;
      for (const w of ccPaginate(ctx, `pipeline/${pipelineId}/workflow`)) {
        yield {
          id: w.id || "",
          name: w.name || "",
          pipeline_id: w.pipeline_id || pipelineId,
          pipeline_number: w.pipeline_number || 0,
          project_slug: w.project_slug || "",
          status: w.status || "",
          started_by: w.started_by || "",
          created_at: w.created_at || "",
          stopped_at: w.stopped_at || "",
        };
      }
    },
  });

  // GET /context?owner-slug={slug} -> contexts for an org
  dl.registerTable("circleci_contexts", {
    description: "CircleCI contexts for an organization",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "organization_slug", type: "string" },
      { name: "created_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "organization_slug", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const orgSlug = getQual(ctx, "organization_slug");
      if (!orgSlug) return;
      for (const c of ccPaginate(ctx, "context", { "owner-slug": orgSlug })) {
        yield {
          id: c.id || "",
          name: c.name || "",
          organization_slug: orgSlug,
          created_at: c.created_at || "",
        };
      }
    },
  });

  // GET /insights/{project_slug}/workflows/{workflow_name} -> workflow run insights
  dl.registerTable("circleci_insights_workflow_runs", {
    description: "CircleCI workflow run insights",
    columns: [
      { name: "id", type: "string" },
      { name: "workflow_name", type: "string" },
      { name: "project_slug", type: "string" },
      { name: "branch", type: "string" },
      { name: "status", type: "string" },
      { name: "duration", type: "number" },
      { name: "credits_used", type: "number" },
      { name: "created_at", type: "datetime" },
      { name: "stopped_at", type: "datetime" },
    ],
    keyColumns: [
      { name: "project_slug", required: "required", operators: ["="] },
      { name: "workflow_name", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const slug = getQual(ctx, "project_slug");
      const wfName = getQual(ctx, "workflow_name");
      if (!slug || !wfName) return;
      for (const r of ccPaginate(ctx, `insights/${slug}/workflows/${wfName}`)) {
        yield {
          id: r.id || "",
          workflow_name: r.workflow_name || wfName,
          project_slug: slug,
          branch: r.branch || "",
          status: r.status || "",
          duration: r.duration || 0,
          credits_used: r.credits_used || 0,
          created_at: r.created_at || "",
          stopped_at: r.stopped_at || "",
        };
      }
    },
  });
}
