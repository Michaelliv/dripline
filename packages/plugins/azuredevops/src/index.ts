import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// Azure DevOps REST API: dev.azure.com/{org}/_apis/ or {org_url}/_apis/
// Auth: Basic with PAT (empty username, PAT as password)
// Verified from steampipe: azure-devops-go-api NewPatConnection, env AZDO_ORG_SERVICE_URL, AZDO_PERSONAL_ACCESS_TOKEN

function adoGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const orgUrl = (ctx.connection.config.organization_url || "").replace(
    /\/$/,
    "",
  );
  const pat = ctx.connection.config.personal_access_token || "";
  const allParams = { ...params, "api-version": "7.0" };
  const qs = new URLSearchParams(allParams).toString();
  const url = `${orgUrl}/_apis${path}${qs ? `?${qs}` : ""}`;
  const auth = Buffer.from(`:${pat}`).toString("base64");
  const resp = syncGet(url, { Authorization: `Basic ${auth}` });
  return resp.status === 200 ? resp.body : null;
}

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

export default function azuredevops(dl: DriplinePluginAPI) {
  dl.setName("azuredevops");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    organization_url: {
      type: "string",
      required: true,
      description: "Azure DevOps org URL (e.g. https://dev.azure.com/myorg)",
      env: "AZDO_ORG_SERVICE_URL",
    },
    personal_access_token: {
      type: "string",
      required: true,
      description: "Personal access token",
      env: "AZDO_PERSONAL_ACCESS_TOKEN",
    },
  });

  // GET /_apis/projects
  dl.registerTable("ado_projects", {
    description: "Azure DevOps projects",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "state", type: "string" },
      { name: "visibility", type: "string" },
      { name: "last_update_time", type: "datetime" },
    ],
    *list(ctx) {
      const body = adoGet(ctx, "/projects");
      if (!body?.value) return;
      for (const p of body.value) {
        yield {
          id: p.id,
          name: p.name || "",
          description: p.description || "",
          state: p.state || "",
          visibility: p.visibility || "",
          last_update_time: p.lastUpdateTime || "",
        };
      }
    },
  });

  // GET /{project}/_apis/git/repositories
  dl.registerTable("ado_repositories", {
    description: "Azure DevOps Git repositories",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "project_name", type: "string" },
      { name: "default_branch", type: "string" },
      { name: "size", type: "number" },
      { name: "web_url", type: "string" },
    ],
    keyColumns: [
      { name: "project_name", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const project = getQual(ctx, "project_name");
      if (!project) return;
      const orgUrl = (ctx.connection.config.organization_url || "").replace(
        /\/$/,
        "",
      );
      const pat = ctx.connection.config.personal_access_token || "";
      const auth = Buffer.from(`:${pat}`).toString("base64");
      const resp = syncGet(
        `${orgUrl}/${encodeURIComponent(project)}/_apis/git/repositories?api-version=7.0`,
        { Authorization: `Basic ${auth}` },
      );
      if (resp.status !== 200) return;
      const body = resp.body as any;
      if (!body?.value) return;
      for (const r of body.value) {
        yield {
          id: r.id,
          name: r.name || "",
          project_name: r.project?.name || project,
          default_branch: r.defaultBranch || "",
          size: r.size || 0,
          web_url: r.webUrl || "",
        };
      }
    },
  });

  // GET /_apis/build/builds
  dl.registerTable("ado_builds", {
    description: "Azure DevOps builds",
    columns: [
      { name: "id", type: "number" },
      { name: "build_number", type: "string" },
      { name: "status", type: "string" },
      { name: "result", type: "string" },
      { name: "project_name", type: "string" },
      { name: "definition_name", type: "string" },
      { name: "source_branch", type: "string" },
      { name: "requested_by", type: "string" },
      { name: "start_time", type: "datetime" },
      { name: "finish_time", type: "datetime" },
    ],
    keyColumns: [
      { name: "project_name", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const project = getQual(ctx, "project_name");
      if (!project) return;
      const orgUrl = (ctx.connection.config.organization_url || "").replace(
        /\/$/,
        "",
      );
      const pat = ctx.connection.config.personal_access_token || "";
      const auth = Buffer.from(`:${pat}`).toString("base64");
      const resp = syncGet(
        `${orgUrl}/${encodeURIComponent(project)}/_apis/build/builds?api-version=7.0&$top=100`,
        { Authorization: `Basic ${auth}` },
      );
      if (resp.status !== 200) return;
      const body = resp.body as any;
      if (!body?.value) return;
      for (const b of body.value) {
        yield {
          id: b.id,
          build_number: b.buildNumber || "",
          status: b.status || "",
          result: b.result || "",
          project_name: b.project?.name || project,
          definition_name: b.definition?.name || "",
          source_branch: b.sourceBranch || "",
          requested_by: b.requestedBy?.displayName || "",
          start_time: b.startTime || "",
          finish_time: b.finishTime || "",
        };
      }
    },
  });

  // GET /_apis/pipelines
  dl.registerTable("ado_pipelines", {
    description: "Azure DevOps pipelines",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "project_name", type: "string" },
      { name: "folder", type: "string" },
      { name: "url", type: "string" },
    ],
    keyColumns: [
      { name: "project_name", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const project = getQual(ctx, "project_name");
      if (!project) return;
      const orgUrl = (ctx.connection.config.organization_url || "").replace(
        /\/$/,
        "",
      );
      const pat = ctx.connection.config.personal_access_token || "";
      const auth = Buffer.from(`:${pat}`).toString("base64");
      const resp = syncGet(
        `${orgUrl}/${encodeURIComponent(project)}/_apis/pipelines?api-version=7.0`,
        { Authorization: `Basic ${auth}` },
      );
      if (resp.status !== 200) return;
      const body = resp.body as any;
      if (!body?.value) return;
      for (const p of body.value) {
        yield {
          id: p.id,
          name: p.name || "",
          project_name: project,
          folder: p.folder || "",
          url: p.url || "",
        };
      }
    },
  });
}
