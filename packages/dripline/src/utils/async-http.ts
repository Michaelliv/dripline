import type { HttpResponse } from "./http.js";

/** Async HTTP GET using native fetch. */
export async function asyncGet(
  url: string,
  headers?: Record<string, string>,
): Promise<HttpResponse> {
  const resp = await fetch(url, { headers });

  const parsedHeaders: Record<string, string> = {};
  resp.headers.forEach((v, k) => {
    parsedHeaders[k.toLowerCase()] = v;
  });

  let body: any;
  const text = await resp.text();
  try {
    body = JSON.parse(text);
  } catch {
    body = text;
  }

  return { status: resp.status, body, headers: parsedHeaders };
}

/** Async paginated GET — follows Link: <url>; rel="next" headers. */
export async function asyncGetPaginated(
  url: string,
  headers?: Record<string, string>,
): Promise<any[]> {
  const results: any[] = [];
  let nextUrl: string | null = url;

  while (nextUrl) {
    const resp = await asyncGet(nextUrl, headers);

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
