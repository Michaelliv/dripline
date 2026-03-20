import { type ExecFileSyncOptions, execFileSync } from "node:child_process";

export type OutputParser =
  | "json"
  | "jsonlines"
  | "csv"
  | "tsv"
  | "lines"
  | "raw"
  | "kv";

export interface ExecOptions {
  /** How to parse stdout. Default: "json" */
  parser?: OutputParser;
  /** For "kv" parser: delimiter between key and value. Default: "=" */
  kvDelimiter?: string;
  /** For "csv"/"tsv": use first line as headers. Default: true */
  headers?: boolean;
  /** Custom field separator for CSV-like parsing */
  separator?: string;
  /** Working directory */
  cwd?: string;
  /** Environment variables (merged with process.env) */
  env?: Record<string, string>;
  /** Max buffer in bytes. Default: 50MB */
  maxBuffer?: number;
  /** Timeout in ms */
  timeout?: number;
  /** Stdin to pipe to the command */
  input?: string;
  /** If true, don't throw on non-zero exit. Default: false */
  ignoreExitCode?: boolean;
}

export interface ExecResult {
  rows: Record<string, any>[];
  raw: string;
}

/**
 * Execute a CLI command synchronously and parse its output into rows.
 *
 * @example
 * // JSON output
 * const { rows } = syncExec("docker", ["ps", "--format", "json"], { parser: "jsonlines" });
 *
 * // Key-value pairs
 * const { rows } = syncExec("brew", ["info", "--json=v2", "node"], { parser: "json" });
 *
 * // TSV with headers
 * const { rows } = syncExec("ps", ["aux"], { parser: "lines" });
 */
export function syncExec(
  command: string,
  args: string[] = [],
  options: ExecOptions = {},
): ExecResult {
  const {
    parser = "json",
    cwd,
    env,
    maxBuffer = 50 * 1024 * 1024,
    timeout,
    input,
    ignoreExitCode = false,
  } = options;

  const execOpts: ExecFileSyncOptions = {
    encoding: "utf-8",
    maxBuffer,
    stdio: [input ? "pipe" : "ignore", "pipe", "pipe"],
  };

  if (cwd) execOpts.cwd = cwd;
  if (timeout) execOpts.timeout = timeout;
  if (env) execOpts.env = { ...process.env, ...env };
  if (input) execOpts.input = input;

  let raw: string;
  try {
    raw = execFileSync(command, args, execOpts) as string;
  } catch (e: any) {
    if (ignoreExitCode && e.stdout) {
      raw =
        typeof e.stdout === "string" ? e.stdout : e.stdout.toString("utf-8");
    } else {
      const stderr = e.stderr
        ? (typeof e.stderr === "string"
            ? e.stderr
            : e.stderr.toString("utf-8")
          ).trim()
        : "";
      const code = e.status ?? "unknown";
      throw new Error(
        `Command failed: ${command} ${args.join(" ")} (exit ${code})${stderr ? `\n${stderr}` : ""}`,
      );
    }
  }

  const rows = parseOutput(raw, options);
  return { rows, raw };
}

/**
 * Check if a command exists on the system.
 */
export function commandExists(command: string): boolean {
  try {
    execFileSync("which", [command], { stdio: "pipe", encoding: "utf-8" });
    return true;
  } catch {
    return false;
  }
}

function parseOutput(raw: string, options: ExecOptions): Record<string, any>[] {
  const parser = options.parser ?? "json";

  switch (parser) {
    case "json":
      return parseJson(raw);
    case "jsonlines":
      return parseJsonLines(raw);
    case "csv":
      return parseDelimited(
        raw,
        options.separator ?? ",",
        options.headers ?? true,
      );
    case "tsv":
      return parseDelimited(
        raw,
        options.separator ?? "\t",
        options.headers ?? true,
      );
    case "lines":
      return parseLines(raw);
    case "kv":
      return parseKeyValue(raw, options.kvDelimiter ?? "=");
    case "raw":
      return [{ output: raw.trim() }];
    default:
      return [{ output: raw.trim() }];
  }
}

function parseJson(raw: string): Record<string, any>[] {
  const trimmed = raw.trim();
  if (!trimmed) return [];

  const parsed = JSON.parse(trimmed);
  if (Array.isArray(parsed)) return parsed;
  return [parsed];
}

function parseJsonLines(raw: string): Record<string, any>[] {
  const lines = raw.trim().split("\n").filter(Boolean);
  const rows: Record<string, any>[] = [];
  for (const line of lines) {
    try {
      rows.push(JSON.parse(line));
    } catch {
      // skip non-JSON lines
    }
  }
  return rows;
}

function parseDelimited(
  raw: string,
  separator: string,
  hasHeaders: boolean,
): Record<string, any>[] {
  const lines = raw.trim().split("\n").filter(Boolean);
  if (lines.length === 0) return [];

  if (hasHeaders) {
    const headers = splitRow(lines[0], separator);
    return lines.slice(1).map((line) => {
      const values = splitRow(line, separator);
      const row: Record<string, any> = {};
      for (let i = 0; i < headers.length; i++) {
        row[headers[i]] = coerce(values[i] ?? "");
      }
      return row;
    });
  }

  return lines.map((line) => {
    const values = splitRow(line, separator);
    const row: Record<string, any> = {};
    for (let i = 0; i < values.length; i++) {
      row[`col${i}`] = coerce(values[i]);
    }
    return row;
  });
}

function splitRow(line: string, separator: string): string[] {
  // Simple CSV-aware split: handle quoted fields for comma separator
  if (separator === "," && line.includes('"')) {
    const fields: string[] = [];
    let current = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"') {
        if (inQuotes && line[i + 1] === '"') {
          current += '"';
          i++;
        } else {
          inQuotes = !inQuotes;
        }
      } else if (ch === separator && !inQuotes) {
        fields.push(current.trim());
        current = "";
      } else {
        current += ch;
      }
    }
    fields.push(current.trim());
    return fields;
  }
  return line.split(separator).map((s) => s.trim());
}

function parseLines(raw: string): Record<string, any>[] {
  return raw
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line, i) => ({ line_number: i + 1, line: line }));
}

function parseKeyValue(raw: string, delimiter: string): Record<string, any>[] {
  const row: Record<string, any> = {};
  for (const line of raw.trim().split("\n")) {
    const idx = line.indexOf(delimiter);
    if (idx > 0) {
      const key = line.slice(0, idx).trim();
      const value = line.slice(idx + delimiter.length).trim();
      row[key] = coerce(value);
    }
  }
  return Object.keys(row).length > 0 ? [row] : [];
}

function coerce(value: string): any {
  if (value === "true") return true;
  if (value === "false") return false;
  if (value === "null" || value === "") return null;
  const num = Number(value);
  if (!Number.isNaN(num) && value !== "") return num;
  return value;
}
