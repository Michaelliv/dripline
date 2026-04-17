import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const API = "https://sentry.io/api/0";

function sentryGet(ctx: QueryContext, path: string): any {
  const token = ctx.connection.config.auth_token || "";
  const resp = syncGet(`${API}${path}`, { Authorization: `Bearer ${token}` });
  return resp.status === 200 ? resp.body : null;
}

function* sentryPaginate(ctx: QueryContext, path: string) {
  let url = `${API}${path}`;
  const token = ctx.connection.config.auth_token || "";
  while (url) {
    const resp = syncGet(url, { Authorization: `Bearer ${token}` });
    if (resp.status !== 200 || !resp.body) return;
    const body = Array.isArray(resp.body) ? resp.body : [];
    yield* body;
    // Sentry uses Link headers for pagination - we'll just check if we got a full page
    if (body.length < 100) return;
    // Simple cursor: use the last item's dateCreated or just stop at 100 pages
    url = "";
  }
}

function getQual(ctx: QueryContext, name: string): string | undefined {
  return ctx.quals.find((q) => q.column === name)?.value;
}

export default function sentry(dl: DriplinePluginAPI) {
  dl.setName("sentry");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    auth_token: {
      type: "string",
      required: true,
      description: "Sentry auth token",
      env: "SENTRY_AUTH_TOKEN",
    },
  });

  dl.registerTable("sentry_organizations", {
    description: "Sentry organizations",
    columns: [
      { name: "id", type: "string" },
      { name: "slug", type: "string" },
      { name: "name", type: "string" },
      { name: "status", type: "string" },
      { name: "date_created", type: "datetime" },
    ],
    *list(ctx) {
      const body = sentryGet(ctx, "/organizations/?member=true");
      if (!body || !Array.isArray(body)) return;
      for (const o of body) {
        yield {
          id: o.id,
          slug: o.slug || "",
          name: o.name || "",
          status: o.status?.id || "",
          date_created: o.dateCreated || "",
        };
      }
    },
  });

  dl.registerTable("sentry_projects", {
    description: "Sentry projects",
    columns: [
      { name: "id", type: "string" },
      { name: "slug", type: "string" },
      { name: "name", type: "string" },
      { name: "organization_slug", type: "string" },
      { name: "platform", type: "string" },
      { name: "status", type: "string" },
      { name: "date_created", type: "datetime" },
    ],
    keyColumns: [
      { name: "organization_slug", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const org = getQual(ctx, "organization_slug");
      if (!org) return;
      const body = sentryGet(ctx, `/organizations/${org}/projects/`);
      if (!body || !Array.isArray(body)) return;
      for (const p of body) {
        yield {
          id: p.id,
          slug: p.slug || "",
          name: p.name || "",
          organization_slug: org,
          platform: p.platform || "",
          status: p.status || "",
          date_created: p.dateCreated || "",
        };
      }
    },
  });

  dl.registerTable("sentry_issues", {
    description: "Sentry issues for a project",
    columns: [
      { name: "id", type: "string" },
      { name: "short_id", type: "string" },
      { name: "title", type: "string" },
      { name: "culprit", type: "string" },
      { name: "level", type: "string" },
      { name: "status", type: "string" },
      { name: "count", type: "number" },
      { name: "user_count", type: "number" },
      { name: "first_seen", type: "datetime" },
      { name: "last_seen", type: "datetime" },
      { name: "permalink", type: "string" },
    ],
    keyColumns: [
      { name: "organization_slug", required: "required", operators: ["="] },
      { name: "project_slug", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const org = getQual(ctx, "organization_slug");
      const project = getQual(ctx, "project_slug");
      if (!org || !project) return;
      const body = sentryGet(
        ctx,
        `/projects/${org}/${project}/issues/?query=is:unresolved`,
      );
      if (!body || !Array.isArray(body)) return;
      for (const i of body) {
        yield {
          id: i.id,
          short_id: i.shortId || "",
          title: i.title || "",
          culprit: i.culprit || "",
          level: i.level || "",
          status: i.status || "",
          count: Number(i.count) || 0,
          user_count: i.userCount || 0,
          first_seen: i.firstSeen || "",
          last_seen: i.lastSeen || "",
          permalink: i.permalink || "",
        };
      }
    },
  });

  dl.registerTable("sentry_teams", {
    description: "Sentry teams",
    columns: [
      { name: "id", type: "string" },
      { name: "slug", type: "string" },
      { name: "name", type: "string" },
      { name: "member_count", type: "number" },
      { name: "date_created", type: "datetime" },
    ],
    keyColumns: [
      { name: "organization_slug", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const org = getQual(ctx, "organization_slug");
      if (!org) return;
      const body = sentryGet(ctx, `/organizations/${org}/teams/`);
      if (!body || !Array.isArray(body)) return;
      for (const t of body) {
        yield {
          id: t.id,
          slug: t.slug || "",
          name: t.name || "",
          member_count: t.memberCount || 0,
          date_created: t.dateCreated || "",
        };
      }
    },
  });

  dl.registerTable("sentry_members", {
    description: "Sentry organization members",
    columns: [
      { name: "id", type: "string" },
      { name: "email", type: "string" },
      { name: "name", type: "string" },
      { name: "role", type: "string" },
      { name: "expired", type: "boolean" },
      { name: "pending", type: "boolean" },
      { name: "date_created", type: "datetime" },
    ],
    keyColumns: [
      { name: "organization_slug", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const org = getQual(ctx, "organization_slug");
      if (!org) return;
      const body = sentryGet(ctx, `/organizations/${org}/members/`);
      if (!body || !Array.isArray(body)) return;
      for (const m of body) {
        yield {
          id: m.id,
          email: m.email || "",
          name: m.name || "",
          role: m.role || "",
          expired: m.expired ? 1 : 0,
          pending: m.pending ? 1 : 0,
          date_created: m.dateCreated || "",
        };
      }
    },
  });
}
