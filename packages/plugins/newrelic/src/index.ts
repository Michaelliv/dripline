import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// New Relic REST API v2
function nrGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const apiKey = ctx.connection.config.api_key || "";
  const region = ctx.connection.config.region || "us";
  const baseUrl =
    region === "eu"
      ? "https://api.eu.newrelic.com"
      : "https://api.newrelic.com";
  const qs = new URLSearchParams(params).toString();
  const url = `${baseUrl}/v2${path}.json${qs ? `?${qs}` : ""}`;
  const resp = syncGet(url, { "Api-Key": apiKey });
  return resp.status === 200 ? resp.body : null;
}

function* nrPaginate(ctx: QueryContext, path: string, key: string) {
  let page = 1;
  while (true) {
    const body = nrGet(ctx, path, { page: String(page) });
    if (!body?.[key]?.length) return;
    yield* body[key];
    if (body[key].length < 200) return;
    page++;
  }
}

export default function newrelic(dl: DriplinePluginAPI) {
  dl.setName("newrelic");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_key: {
      type: "string",
      required: true,
      description: "New Relic User API key",
      env: "NEW_RELIC_API_KEY",
    },
    region: {
      type: "string",
      required: false,
      description: "Region: us (default) or eu",
      env: "NEW_RELIC_REGION",
    },
  });

  // GET /v2/applications.json
  dl.registerTable("nr_applications", {
    description: "New Relic APM applications",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "language", type: "string" },
      { name: "health_status", type: "string" },
      { name: "reporting", type: "boolean" },
      { name: "response_time", type: "number" },
      { name: "throughput", type: "number" },
      { name: "error_rate", type: "number" },
      { name: "last_reported_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const a of nrPaginate(ctx, "/applications", "applications")) {
        yield {
          id: a.id,
          name: a.name || "",
          language: a.language || "",
          health_status: a.health_status || "",
          reporting: a.reporting ? 1 : 0,
          response_time: a.application_summary?.response_time || 0,
          throughput: a.application_summary?.throughput || 0,
          error_rate: a.application_summary?.error_rate || 0,
          last_reported_at: a.last_reported_at || "",
        };
      }
    },
  });

  // GET /v2/alerts_policies.json
  dl.registerTable("nr_alert_policies", {
    description: "New Relic alert policies",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "incident_preference", type: "string" },
      { name: "created_at", type: "datetime" },
      { name: "updated_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const p of nrPaginate(ctx, "/alerts_policies", "policies")) {
        yield {
          id: p.id,
          name: p.name || "",
          incident_preference: p.incident_preference || "",
          created_at: p.created_at ? new Date(p.created_at).toISOString() : "",
          updated_at: p.updated_at ? new Date(p.updated_at).toISOString() : "",
        };
      }
    },
  });

  // GET /v2/alerts_conditions.json?policy_id=
  dl.registerTable("nr_alert_conditions", {
    description: "New Relic alert conditions for a policy",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "type", type: "string" },
      { name: "enabled", type: "boolean" },
      { name: "policy_id", type: "number" },
      { name: "metric", type: "string" },
    ],
    keyColumns: [{ name: "policy_id", required: "required", operators: ["="] }],
    *list(ctx) {
      const policyId = ctx.quals.find((q) => q.column === "policy_id")?.value;
      if (!policyId) return;
      const body = nrGet(ctx, "/alerts_conditions", { policy_id: policyId });
      if (!body?.conditions) return;
      for (const c of body.conditions) {
        yield {
          id: c.id,
          name: c.name || "",
          type: c.type || "",
          enabled: c.enabled ? 1 : 0,
          policy_id: Number(policyId),
          metric: c.metric || "",
        };
      }
    },
  });

  // GET /v2/alerts_incidents.json
  dl.registerTable("nr_alert_incidents", {
    description: "New Relic alert incidents",
    columns: [
      { name: "id", type: "number" },
      { name: "policy_name", type: "string" },
      { name: "condition_name", type: "string" },
      { name: "opened_at", type: "datetime" },
      { name: "closed_at", type: "datetime" },
      { name: "entity_type", type: "string" },
      { name: "entity_name", type: "string" },
    ],
    *list(ctx) {
      for (const i of nrPaginate(ctx, "/alerts_incidents", "incidents")) {
        yield {
          id: i.id,
          policy_name: i.links?.policy_name || "",
          condition_name: i.links?.condition_name || "",
          opened_at: i.opened_at
            ? new Date(i.opened_at * 1000).toISOString()
            : "",
          closed_at: i.closed_at
            ? new Date(i.closed_at * 1000).toISOString()
            : "",
          entity_type: i.entity?.type || "",
          entity_name: i.entity?.name || "",
        };
      }
    },
  });

  // GET /v2/alerts_channels.json (legacy REST endpoint; steampipe uses NerdGraph)
  dl.registerTable("nr_alert_channels", {
    description: "New Relic alert notification channels (legacy API)",
    columns: [
      { name: "id", type: "number" },
      { name: "name", type: "string" },
      { name: "type", type: "string" },
      { name: "configuration", type: "json" },
    ],
    *list(ctx) {
      for (const c of nrPaginate(ctx, "/alerts_channels", "channels")) {
        yield {
          id: c.id,
          name: c.name || "",
          type: c.type || "",
          configuration: JSON.stringify(c.configuration || {}),
        };
      }
    },
  });
}
