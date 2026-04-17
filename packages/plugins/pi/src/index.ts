import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { homedir } from "node:os";
import { basename, join } from "node:path";
import type { DriplinePluginAPI } from "dripline";
import { commandExists, syncExec } from "dripline";

interface SessionRecord {
  type: string;
  id?: string;
  parentId?: string;
  timestamp?: string;
  message?: {
    role?: string;
    content?: any[];
    usage?: {
      input?: number;
      output?: number;
      cacheRead?: number;
      cacheWrite?: number;
      totalTokens?: number;
      cost?: {
        input?: number;
        output?: number;
        cacheRead?: number;
        cacheWrite?: number;
        total?: number;
      };
    };
    model?: string;
    provider?: string;
    stopReason?: string;
  };
  provider?: string;
  modelId?: string;
  cwd?: string;
  version?: number;
  thinkingLevel?: string;
}

function getSessionsDir(): string {
  return join(homedir(), ".pi", "agent", "sessions");
}

function parseProjectName(dirName: string): string {
  return dirName.replace(/^--/, "/").replace(/--/g, "/").replace(/-$/, "");
}

function readSessionFile(filePath: string): SessionRecord[] {
  try {
    const content = readFileSync(filePath, "utf-8");
    return content
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      })
      .filter(Boolean) as SessionRecord[];
  } catch {
    return [];
  }
}

function* iterateSessions(): Generator<{
  dir: string;
  file: string;
  filePath: string;
  project: string;
}> {
  const sessionsDir = getSessionsDir();
  if (!existsSync(sessionsDir)) return;

  for (const dir of readdirSync(sessionsDir)) {
    const dirPath = join(sessionsDir, dir);
    if (!statSync(dirPath).isDirectory()) continue;
    const project = parseProjectName(dir);

    for (const file of readdirSync(dirPath)) {
      if (!file.endsWith(".jsonl")) continue;
      yield { dir, file, filePath: join(dirPath, file), project };
    }
  }
}

export default function pi(dl: DriplinePluginAPI) {
  dl.setName("pi");
  dl.setVersion("0.1.0");

  dl.registerTable("pi_sessions", {
    description: "pi coding agent sessions",
    columns: [
      { name: "id", type: "string" },
      { name: "project", type: "string" },
      { name: "started_at", type: "datetime" },
      { name: "model", type: "string" },
      { name: "provider", type: "string" },
      { name: "thinking_level", type: "string" },
      { name: "message_count", type: "number" },
      { name: "user_messages", type: "number" },
      { name: "assistant_messages", type: "number" },
      { name: "tool_calls", type: "number" },
      { name: "total_tokens", type: "number" },
      { name: "total_cost", type: "number" },
      { name: "file", type: "string" },
    ],
    *list() {
      for (const { file, filePath, project } of iterateSessions()) {
        const records = readSessionFile(filePath);
        if (records.length === 0) continue;

        const sessionRec = records.find((r) => r.type === "session");
        const modelChange = records.find((r) => r.type === "model_change");
        const thinkingChange = records.find(
          (r) => r.type === "thinking_level_change",
        );

        let messageCount = 0;
        let userMessages = 0;
        let assistantMessages = 0;
        let toolCalls = 0;
        let totalTokens = 0;
        let totalCost = 0;

        for (const rec of records) {
          if (rec.type !== "message") continue;
          const role = rec.message?.role;
          messageCount++;
          if (role === "user") userMessages++;
          if (role === "assistant") {
            assistantMessages++;
            totalTokens += rec.message?.usage?.totalTokens ?? 0;
            totalCost += rec.message?.usage?.cost?.total ?? 0;
            const content = rec.message?.content ?? [];
            if (Array.isArray(content)) {
              toolCalls += content.filter(
                (c: any) => c.type === "toolCall",
              ).length;
            }
          }
        }

        yield {
          id: sessionRec?.id ?? "",
          project,
          started_at: sessionRec?.timestamp ?? "",
          model: modelChange?.modelId ?? "",
          provider: modelChange?.provider ?? "",
          thinking_level: thinkingChange?.thinkingLevel ?? "",
          message_count: messageCount,
          user_messages: userMessages,
          assistant_messages: assistantMessages,
          tool_calls: toolCalls,
          total_tokens: totalTokens,
          total_cost: Math.round(totalCost * 10000) / 10000,
          file: basename(filePath),
        };
      }
    },
  });

  dl.registerTable("pi_messages", {
    description: "Individual messages from pi sessions",
    columns: [
      { name: "session_id", type: "string" },
      { name: "project", type: "string" },
      { name: "message_id", type: "string" },
      { name: "role", type: "string" },
      { name: "timestamp", type: "datetime" },
      { name: "model", type: "string" },
      { name: "provider", type: "string" },
      { name: "text", type: "string" },
      { name: "input_tokens", type: "number" },
      { name: "output_tokens", type: "number" },
      { name: "total_tokens", type: "number" },
      { name: "cost", type: "number" },
      { name: "stop_reason", type: "string" },
    ],
    *list() {
      for (const { filePath, project } of iterateSessions()) {
        const records = readSessionFile(filePath);
        const sessionRec = records.find((r) => r.type === "session");
        const sessionId = sessionRec?.id ?? "";

        for (const rec of records) {
          if (rec.type !== "message") continue;
          const msg = rec.message;
          if (!msg) continue;
          const role = msg.role ?? "";
          if (role === "toolResult") continue;

          let text = "";
          const content = msg.content;
          if (typeof content === "string") {
            text = content;
          } else if (Array.isArray(content)) {
            text = content
              .filter((c: any) => c.type === "text")
              .map((c: any) => c.text ?? "")
              .join("\n")
              .slice(0, 500);
          }

          yield {
            session_id: sessionId,
            project,
            message_id: rec.id ?? "",
            role,
            timestamp:
              (rec.timestamp ?? (msg as any).timestamp)
                ? new Date(
                    (msg as any).timestamp ?? rec.timestamp ?? 0,
                  ).toISOString()
                : "",
            model: (msg as any).model ?? "",
            provider: (msg as any).provider ?? "",
            text,
            input_tokens: msg.usage?.input ?? null,
            output_tokens: msg.usage?.output ?? null,
            total_tokens: msg.usage?.totalTokens ?? null,
            cost: msg.usage?.cost?.total
              ? Math.round(msg.usage.cost.total * 10000) / 10000
              : null,
            stop_reason: (msg as any).stopReason ?? "",
          };
        }
      }
    },
  });

  dl.registerTable("pi_tool_calls", {
    description: "Tool calls made by pi across all sessions",
    columns: [
      { name: "session_id", type: "string" },
      { name: "project", type: "string" },
      { name: "message_id", type: "string" },
      { name: "tool_call_id", type: "string" },
      { name: "tool_name", type: "string" },
      { name: "arguments", type: "json" },
      { name: "timestamp", type: "datetime" },
      { name: "model", type: "string" },
    ],
    *list() {
      for (const { filePath, project } of iterateSessions()) {
        const records = readSessionFile(filePath);
        const sessionRec = records.find((r) => r.type === "session");
        const sessionId = sessionRec?.id ?? "";

        for (const rec of records) {
          if (rec.type !== "message") continue;
          const msg = rec.message;
          if (!msg || msg.role !== "assistant") continue;
          const content = msg.content;
          if (!Array.isArray(content)) continue;

          for (const block of content) {
            if (block.type !== "toolCall") continue;
            yield {
              session_id: sessionId,
              project,
              message_id: rec.id ?? "",
              tool_call_id: block.id ?? "",
              tool_name: block.name ?? "",
              arguments: JSON.stringify(block.arguments ?? {}),
              timestamp: rec.timestamp ?? "",
              model: (msg as any).model ?? "",
            };
          }
        }
      }
    },
  });

  dl.registerTable("pi_costs", {
    description: "Aggregated cost per session",
    columns: [
      { name: "session_id", type: "string" },
      { name: "project", type: "string" },
      { name: "started_at", type: "datetime" },
      { name: "model", type: "string" },
      { name: "provider", type: "string" },
      { name: "input_tokens", type: "number" },
      { name: "output_tokens", type: "number" },
      { name: "cache_read_tokens", type: "number" },
      { name: "cache_write_tokens", type: "number" },
      { name: "total_tokens", type: "number" },
      { name: "input_cost", type: "number" },
      { name: "output_cost", type: "number" },
      { name: "cache_read_cost", type: "number" },
      { name: "cache_write_cost", type: "number" },
      { name: "total_cost", type: "number" },
    ],
    *list() {
      for (const { filePath, project } of iterateSessions()) {
        const records = readSessionFile(filePath);
        const sessionRec = records.find((r) => r.type === "session");
        const modelChange = records.find((r) => r.type === "model_change");

        let inputTokens = 0;
        let outputTokens = 0;
        let cacheReadTokens = 0;
        let cacheWriteTokens = 0;
        let totalTokens = 0;
        let inputCost = 0;
        let outputCost = 0;
        let cacheReadCost = 0;
        let cacheWriteCost = 0;
        let totalCost = 0;

        for (const rec of records) {
          if (rec.type !== "message") continue;
          const usage = rec.message?.usage;
          if (!usage) continue;

          inputTokens += usage.input ?? 0;
          outputTokens += usage.output ?? 0;
          cacheReadTokens += usage.cacheRead ?? 0;
          cacheWriteTokens += usage.cacheWrite ?? 0;
          totalTokens += usage.totalTokens ?? 0;
          inputCost += usage.cost?.input ?? 0;
          outputCost += usage.cost?.output ?? 0;
          cacheReadCost += usage.cost?.cacheRead ?? 0;
          cacheWriteCost += usage.cost?.cacheWrite ?? 0;
          totalCost += usage.cost?.total ?? 0;
        }

        if (totalTokens === 0) continue;

        yield {
          session_id: sessionRec?.id ?? "",
          project,
          started_at: sessionRec?.timestamp ?? "",
          model: modelChange?.modelId ?? "",
          provider: modelChange?.provider ?? "",
          input_tokens: inputTokens,
          output_tokens: outputTokens,
          cache_read_tokens: cacheReadTokens,
          cache_write_tokens: cacheWriteTokens,
          total_tokens: totalTokens,
          input_cost: round4(inputCost),
          output_cost: round4(outputCost),
          cache_read_cost: round4(cacheReadCost),
          cache_write_cost: round4(cacheWriteCost),
          total_cost: round4(totalCost),
        };
      }
    },
  });

  dl.registerTable("pi_prompt", {
    description:
      "Send a prompt to pi and get a response. Use WHERE prompt = 'your question'",
    columns: [
      { name: "prompt", type: "string" },
      { name: "response", type: "string" },
      { name: "model", type: "string" },
      { name: "provider", type: "string" },
    ],
    keyColumns: [
      { name: "prompt", required: "required", operators: ["="] },
      { name: "model", required: "optional", operators: ["="] },
      { name: "provider", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const prompt = ctx.quals.find((q) => q.column === "prompt")?.value;
      if (!prompt) return;

      const model = ctx.quals.find((q) => q.column === "model")?.value;
      const provider = ctx.quals.find((q) => q.column === "provider")?.value;

      const args = buildPiArgs({ model, provider });
      args.push(prompt);

      const { raw } = syncExec("pi", args, { parser: "raw", timeout: 120000 });

      yield {
        prompt,
        response: raw.trim(),
        model: model ?? "",
        provider: provider ?? "",
      };
    },
  });

  dl.registerTable("pi_generate", {
    description:
      "Generate structured data with pi. Use WHERE prompt = 'description' AND columns = 'name:string,age:number'",
    columns: [{ name: "data", type: "json" }],
    keyColumns: [
      { name: "prompt", required: "required", operators: ["="] },
      { name: "model", required: "optional", operators: ["="] },
      { name: "provider", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const prompt = ctx.quals.find((q) => q.column === "prompt")?.value;
      if (!prompt) return;

      const model = ctx.quals.find((q) => q.column === "model")?.value;
      const provider = ctx.quals.find((q) => q.column === "provider")?.value;

      const systemPrompt =
        "You are a data generator. Return ONLY a JSON array of objects. No markdown fences, no explanation, no extra text. Just the raw JSON array.";
      const args = buildPiArgs({ model, provider, systemPrompt });
      args.push(prompt);

      const { raw } = syncExec("pi", args, { parser: "raw", timeout: 120000 });

      // Try to extract JSON array from response
      const trimmed = raw.trim();
      let parsed: any[];
      try {
        parsed = JSON.parse(trimmed);
        if (!Array.isArray(parsed)) parsed = [parsed];
      } catch {
        // Try to find JSON array in the response
        const match = trimmed.match(/\[[\s\S]*\]/);
        if (match) {
          try {
            parsed = JSON.parse(match[0]);
          } catch {
            parsed = [{ data: trimmed }];
          }
        } else {
          parsed = [{ data: trimmed }];
        }
      }

      for (const row of parsed) {
        yield { data: JSON.stringify(row) };
      }
    },
  });
}

function buildPiArgs(opts: {
  model?: string;
  provider?: string;
  systemPrompt?: string;
}): string[] {
  const args = [
    "-p",
    "--mode",
    "text",
    "--no-tools",
    "--no-extensions",
    "--no-skills",
    "--no-prompt-templates",
    "--no-session",
  ];
  if (opts.model) args.push("--model", opts.model);
  if (opts.provider) args.push("--provider", opts.provider);
  if (opts.systemPrompt) args.push("--system-prompt", opts.systemPrompt);
  return args;
}

function round4(n: number): number {
  return Math.round(n * 10000) / 10000;
}
