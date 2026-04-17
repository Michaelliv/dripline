import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// SVGL API: api.svgl.app (Hono on Cloudflare Workers)
// No auth needed. Rate limited to 5 req/5s.
// Source: github.com/pheralb/svgl/api-routes/src/index.ts
// Endpoints:
//   GET / — all SVGs (?limit=N, ?search=term)
//   GET /categories — category list with counts
//   GET /category/:name — SVGs by category
//   GET /svg/:filename — raw SVG code

const API = "https://api.svgl.app";

function svglGet(path: string): any {
  const resp = syncGet(`${API}${path}`, {});
  return resp.status === 200 ? resp.body : null;
}

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

function normalizeRoute(route: any): { light: string; dark: string } | string {
  if (typeof route === "string") return route;
  if (route && typeof route === "object")
    return { light: route.light || "", dark: route.dark || "" };
  return "";
}

export default function svgl(dl: DriplinePluginAPI) {
  dl.setName("svgl");
  dl.setVersion("0.1.0");

  // GET / or GET /?search=term or GET /category/:name
  dl.registerTable("svgl_logos", {
    description: "SVG logos from svgl.app (603+ logos, 40 categories)",
    columns: [
      { name: "id", type: "number" },
      { name: "title", type: "string" },
      { name: "category", type: "json" },
      { name: "route_light", type: "string" },
      { name: "route_dark", type: "string" },
      { name: "wordmark_light", type: "string" },
      { name: "wordmark_dark", type: "string" },
      { name: "url", type: "string" },
    ],
    keyColumns: [
      { name: "search", required: "optional", operators: ["="] },
      { name: "category_name", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const search = getQual(ctx, "search");
      const category = getQual(ctx, "category_name");

      let body: any;
      if (search) {
        body = svglGet(`/?search=${encodeURIComponent(search)}`);
      } else if (category) {
        body = svglGet(`/category/${encodeURIComponent(category)}`);
      } else {
        body = svglGet("/");
      }

      if (!body || !Array.isArray(body)) return;
      for (const svg of body) {
        const route = normalizeRoute(svg.route);
        const wordmark = normalizeRoute(svg.wordmark);
        yield {
          id: svg.id ?? 0,
          title: svg.title || "",
          category: JSON.stringify(
            Array.isArray(svg.category) ? svg.category : [svg.category || ""],
          ),
          route_light: typeof route === "string" ? route : route.light,
          route_dark: typeof route === "string" ? "" : route.dark,
          wordmark_light:
            typeof wordmark === "string"
              ? wordmark
              : wordmark
                ? (wordmark as any).light || ""
                : "",
          wordmark_dark:
            typeof wordmark === "string"
              ? ""
              : wordmark
                ? (wordmark as any).dark || ""
                : "",
          url: svg.url || "",
        };
      }
    },
  });

  // GET /categories
  dl.registerTable("svgl_categories", {
    description: "SVGL logo categories with counts",
    columns: [
      { name: "category", type: "string" },
      { name: "total", type: "number" },
    ],
    *list() {
      const body = svglGet("/categories");
      if (!body || !Array.isArray(body)) return;
      for (const c of body) {
        yield { category: c.category || "", total: c.total || 0 };
      }
    },
  });
}
