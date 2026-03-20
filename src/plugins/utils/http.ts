import { execFileSync } from "node:child_process";

export interface HttpResponse {
  status: number;
  body: any;
  headers: Record<string, string>;
}

export function syncGet(
  url: string,
  headers?: Record<string, string>,
): HttpResponse {
  const args = ["-s", "-w", "\n%{http_code}", "-D", "-"];

  if (headers) {
    for (const [k, v] of Object.entries(headers)) {
      args.push("-H", `${k}: ${v}`);
    }
  }

  args.push(url);

  const raw = execFileSync("curl", args, {
    maxBuffer: 50 * 1024 * 1024,
    encoding: "utf-8",
  });

  // Response has: headers\r\n\r\nbody\nstatus_code
  const headerEnd = raw.indexOf("\r\n\r\n");
  const headerSection = raw.slice(0, headerEnd);
  const rest = raw.slice(headerEnd + 4);

  // Parse response headers
  const parsedHeaders: Record<string, string> = {};
  for (const line of headerSection.split("\r\n")) {
    const colonIdx = line.indexOf(":");
    if (colonIdx > 0) {
      parsedHeaders[line.slice(0, colonIdx).toLowerCase().trim()] = line
        .slice(colonIdx + 1)
        .trim();
    }
  }

  // Last line is status code
  const lines = rest.trimEnd().split("\n");
  const statusCode = parseInt(lines.pop() || "0", 10);
  const bodyStr = lines.join("\n");

  let body: any;
  try {
    body = JSON.parse(bodyStr);
  } catch {
    body = bodyStr;
  }

  return { status: statusCode, body, headers: parsedHeaders };
}

export function syncGetPaginated(
  url: string,
  headers?: Record<string, string>,
): any[] {
  const results: any[] = [];
  let nextUrl: string | null = url;

  while (nextUrl) {
    const resp = syncGet(nextUrl, headers);

    if (resp.status >= 400) {
      throw new Error(
        `HTTP ${resp.status}: ${typeof resp.body === "string" ? resp.body : JSON.stringify(resp.body)}`,
      );
    }

    if (Array.isArray(resp.body)) {
      results.push(...resp.body);
    } else {
      results.push(resp.body);
    }

    // Parse Link header for next page
    nextUrl = null;
    const link = resp.headers.link;
    if (link) {
      const match = link.match(/<([^>]+)>;\s*rel="next"/);
      if (match) {
        nextUrl = match[1];
      }
    }
  }

  return results;
}
