import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const API = "https://api.openai.com/v1";

function oaiGet(ctx: QueryContext, path: string): any {
  const token = ctx.connection.config.api_key || "";
  const resp = syncGet(`${API}${path}`, {
    Authorization: `Bearer ${token}`,
  });
  return resp.status === 200 ? resp.body : null;
}

function* oaiPaginate(ctx: QueryContext, path: string, key: string) {
  let after = "";
  while (true) {
    const sep = path.includes("?") ? "&" : "?";
    const url = `${path}${after ? `${sep}after=${after}` : ""}`;
    const body = oaiGet(ctx, url);
    if (!body?.[key]?.length) return;
    yield* body[key];
    if (!body.has_more) return;
    after = body[key][body[key].length - 1]?.id || "";
    if (!after) return;
  }
}

export default function openai(dl: DriplinePluginAPI) {
  dl.setName("openai");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_key: {
      type: "string",
      required: true,
      description: "OpenAI API key",
      env: "OPENAI_API_KEY",
    },
  });

  dl.registerTable("openai_models", {
    description: "Available OpenAI models",
    columns: [
      { name: "id", type: "string" },
      { name: "object", type: "string" },
      { name: "created", type: "datetime" },
      { name: "owned_by", type: "string" },
    ],
    *list(ctx) {
      const body = oaiGet(ctx, "/models");
      if (!body?.data) return;
      for (const m of body.data) {
        yield {
          id: m.id,
          object: m.object || "",
          created: m.created ? new Date(m.created * 1000).toISOString() : "",
          owned_by: m.owned_by || "",
        };
      }
    },
  });

  dl.registerTable("openai_files", {
    description: "Files uploaded to OpenAI",
    columns: [
      { name: "id", type: "string" },
      { name: "filename", type: "string" },
      { name: "purpose", type: "string" },
      { name: "bytes", type: "number" },
      { name: "created_at", type: "datetime" },
      { name: "status", type: "string" },
    ],
    *list(ctx) {
      const body = oaiGet(ctx, "/files");
      if (!body?.data) return;
      for (const f of body.data) {
        yield {
          id: f.id,
          filename: f.filename || "",
          purpose: f.purpose || "",
          bytes: f.bytes || 0,
          created_at: f.created_at
            ? new Date(f.created_at * 1000).toISOString()
            : "",
          status: f.status || "",
        };
      }
    },
  });

  dl.registerTable("openai_assistants", {
    description: "OpenAI assistants",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "model", type: "string" },
      { name: "instructions", type: "string" },
      { name: "tools", type: "json" },
      { name: "created_at", type: "datetime" },
    ],
    *list(ctx) {
      for (const a of oaiPaginate(ctx, "/assistants?limit=100", "data")) {
        yield {
          id: a.id,
          name: a.name || "",
          description: a.description || "",
          model: a.model || "",
          instructions: a.instructions || "",
          tools: JSON.stringify(a.tools || []),
          created_at: a.created_at
            ? new Date(a.created_at * 1000).toISOString()
            : "",
        };
      }
    },
  });

  dl.registerTable("openai_usage", {
    description: "OpenAI API usage by date (costs endpoint)",
    columns: [
      { name: "date", type: "string" },
      { name: "organization_id", type: "string" },
      { name: "total_tokens", type: "number" },
      { name: "total_cost_usd", type: "number" },
      { name: "requests", type: "number" },
    ],
    keyColumns: [
      { name: "start_date", required: "required", operators: ["="] },
      { name: "end_date", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const startDate = ctx.quals.find((q) => q.column === "start_date")?.value;
      if (!startDate) return;
      const endDate =
        ctx.quals.find((q) => q.column === "end_date")?.value || startDate;
      // The /usage endpoint is on the old dashboard API
      const token = ctx.connection.config.api_key || "";
      const resp = syncGet(
        `https://api.openai.com/v1/organization/usage?start_date=${startDate}&end_date=${endDate}`,
        { Authorization: `Bearer ${token}` },
      );
      if (resp.status !== 200 || !resp.body) return;
      const body = resp.body as any;
      // If the API returns usage data, yield it
      if (body.data) {
        for (const d of body.data) {
          yield {
            date: d.aggregation_timestamp
              ? new Date(d.aggregation_timestamp * 1000)
                  .toISOString()
                  .slice(0, 10)
              : "",
            organization_id: d.organization_id || "",
            total_tokens:
              (d.n_context_tokens_total || 0) +
              (d.n_generated_tokens_total || 0),
            total_cost_usd: 0, // Cost not directly in this endpoint
            requests: d.n_requests || 0,
          };
        }
      }
    },
  });

  dl.registerTable("openai_completion", {
    description: "Generate a completion from OpenAI (query with prompt)",
    columns: [
      { name: "completion", type: "string" },
      { name: "model", type: "string" },
      { name: "prompt_tokens", type: "number" },
      { name: "completion_tokens", type: "number" },
    ],
    keyColumns: [
      { name: "prompt", required: "required", operators: ["="] },
      { name: "model", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const prompt = ctx.quals.find((q) => q.column === "prompt")?.value;
      if (!prompt) return;
      const model =
        ctx.quals.find((q) => q.column === "model")?.value || "gpt-4o-mini";
      const token = ctx.connection.config.api_key || "";

      const { execSync } = require("node:child_process");
      const body = JSON.stringify({
        model,
        messages: [{ role: "user", content: prompt }],
      });
      const result = execSync(
        `curl -s -X POST "${API}/chat/completions" -H "Authorization: Bearer ${token}" -H "Content-Type: application/json" -d '${body.replace(/'/g, "'\\''")}'`,
        { encoding: "utf-8", timeout: 60000 },
      );
      try {
        const resp = JSON.parse(result);
        yield {
          completion: resp.choices?.[0]?.message?.content || "",
          model: resp.model || model,
          prompt_tokens: resp.usage?.prompt_tokens || 0,
          completion_tokens: resp.usage?.completion_tokens || 0,
        };
      } catch {
        return;
      }
    },
  });
}
