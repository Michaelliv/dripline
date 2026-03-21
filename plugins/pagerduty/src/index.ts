import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const API = "https://api.pagerduty.com";

function pdGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const token = ctx.connection.config.token || "";
  const qs = new URLSearchParams(params).toString();
  const url = `${API}${path}${qs ? `?${qs}` : ""}`;
  const resp = syncGet(url, {
    Authorization: `Token token=${token}`,
    "Content-Type": "application/json",
  });
  return resp.status === 200 ? resp.body : null;
}

function* pdPaginate(
  ctx: QueryContext,
  path: string,
  key: string,
  params: Record<string, string> = {},
) {
  let offset = 0;
  while (true) {
    const body = pdGet(ctx, path, {
      ...params,
      limit: "100",
      offset: String(offset),
    });
    if (!body?.[key]?.length) return;
    yield* body[key];
    if (!body.more) return;
    offset += body[key].length;
  }
}

export default function pagerduty(dl: DriplinePluginAPI) {
  dl.setName("pagerduty");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    token: {
      type: "string",
      required: true,
      description: "PagerDuty API token",
      env: "PAGERDUTY_TOKEN",
    },
  });

  dl.registerTable("pd_services", {
    description: "PagerDuty services",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "status", type: "string" },
      { name: "escalation_policy_name", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "html_url", type: "string" },
    ],
    *list(ctx) {
      for (const s of pdPaginate(ctx, "/services", "services")) {
        yield {
          id: s.id,
          name: s.name || "",
          description: s.description || "",
          status: s.status || "",
          escalation_policy_name: s.escalation_policy?.summary || "",
          created_at: s.created_at || "",
          html_url: s.html_url || "",
        };
      }
    },
  });

  dl.registerTable("pd_incidents", {
    description: "PagerDuty incidents",
    columns: [
      { name: "id", type: "string" },
      { name: "incident_number", type: "number" },
      { name: "title", type: "string" },
      { name: "status", type: "string" },
      { name: "urgency", type: "string" },
      { name: "service_name", type: "string" },
      { name: "assigned_to", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "resolved_at", type: "datetime" },
      { name: "html_url", type: "string" },
    ],
    *list(ctx) {
      for (const i of pdPaginate(ctx, "/incidents", "incidents", {
        sort_by: "created_at:desc",
      })) {
        yield {
          id: i.id,
          incident_number: i.incident_number || 0,
          title: i.title || "",
          status: i.status || "",
          urgency: i.urgency || "",
          service_name: i.service?.summary || "",
          assigned_to: i.assignments?.[0]?.assignee?.summary || "",
          created_at: i.created_at || "",
          resolved_at: i.resolved_at || "",
          html_url: i.html_url || "",
        };
      }
    },
  });

  dl.registerTable("pd_users", {
    description: "PagerDuty users",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "email", type: "string" },
      { name: "role", type: "string" },
      { name: "job_title", type: "string" },
      { name: "time_zone", type: "string" },
      { name: "html_url", type: "string" },
    ],
    *list(ctx) {
      for (const u of pdPaginate(ctx, "/users", "users")) {
        yield {
          id: u.id,
          name: u.name || "",
          email: u.email || "",
          role: u.role || "",
          job_title: u.job_title || "",
          time_zone: u.time_zone || "",
          html_url: u.html_url || "",
        };
      }
    },
  });

  dl.registerTable("pd_teams", {
    description: "PagerDuty teams",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "html_url", type: "string" },
    ],
    *list(ctx) {
      for (const t of pdPaginate(ctx, "/teams", "teams")) {
        yield {
          id: t.id,
          name: t.name || "",
          description: t.description || "",
          html_url: t.html_url || "",
        };
      }
    },
  });

  dl.registerTable("pd_oncalls", {
    description: "PagerDuty on-call schedules (who is on call now)",
    columns: [
      { name: "user_name", type: "string" },
      { name: "user_email", type: "string" },
      { name: "escalation_policy", type: "string" },
      { name: "escalation_level", type: "number" },
      { name: "schedule_name", type: "string" },
      { name: "start", type: "datetime" },
      { name: "end", type: "datetime" },
    ],
    *list(ctx) {
      for (const o of pdPaginate(ctx, "/oncalls", "oncalls")) {
        yield {
          user_name: o.user?.summary || "",
          user_email: o.user?.email || "",
          escalation_policy: o.escalation_policy?.summary || "",
          escalation_level: o.escalation_level || 0,
          schedule_name: o.schedule?.summary || "",
          start: o.start || "",
          end: o.end || "",
        };
      }
    },
  });

  dl.registerTable("pd_escalation_policies", {
    description: "PagerDuty escalation policies",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "num_loops", type: "number" },
      { name: "html_url", type: "string" },
    ],
    *list(ctx) {
      for (const ep of pdPaginate(
        ctx,
        "/escalation_policies",
        "escalation_policies",
      )) {
        yield {
          id: ep.id,
          name: ep.name || "",
          description: ep.description || "",
          num_loops: ep.num_loops || 0,
          html_url: ep.html_url || "",
        };
      }
    },
  });
}
