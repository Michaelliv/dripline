import type { DriplinePluginAPI } from "dripline";
import { syncGet } from "dripline";

// Minimal XML tag extractor - no dependency needed
function extractTag(xml: string, tag: string): string {
  // Handle CDATA
  const cdataRe = new RegExp(
    `<${tag}[^>]*><!\\[CDATA\\[([\\s\\S]*?)\\]\\]></${tag}>`,
    "i",
  );
  const cdataMatch = xml.match(cdataRe);
  if (cdataMatch) return cdataMatch[1].trim();

  const re = new RegExp(`<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, "i");
  const m = xml.match(re);
  return m ? m[1].trim() : "";
}

function extractAttr(xml: string, tag: string, attr: string): string {
  const re = new RegExp(`<${tag}[^>]*${attr}=["']([^"']*)["']`, "i");
  const m = xml.match(re);
  return m ? m[1] : "";
}

function parseItems(xml: string): any[] {
  const items: any[] = [];

  // RSS 2.0: <item>...</item>
  const rssItems = xml.match(/<item[\s>][\s\S]*?<\/item>/gi) || [];
  for (const item of rssItems) {
    items.push({
      title: extractTag(item, "title"),
      link: extractTag(item, "link") || extractAttr(item, "link", "href"),
      description: extractTag(item, "description"),
      author: extractTag(item, "author") || extractTag(item, "dc:creator"),
      published:
        extractTag(item, "pubDate") ||
        extractTag(item, "published") ||
        extractTag(item, "dc:date"),
      guid: extractTag(item, "guid") || extractTag(item, "id"),
      categories: (item.match(/<category[^>]*>([^<]*)<\/category>/gi) || [])
        .map((c: string) => c.replace(/<[^>]*>/g, "").trim())
        .filter(Boolean),
    });
  }

  // Atom: <entry>...</entry>
  if (items.length === 0) {
    const atomEntries = xml.match(/<entry[\s>][\s\S]*?<\/entry>/gi) || [];
    for (const entry of atomEntries) {
      items.push({
        title: extractTag(entry, "title"),
        link: extractAttr(entry, "link", "href") || extractTag(entry, "link"),
        description:
          extractTag(entry, "summary") || extractTag(entry, "content"),
        author: extractTag(entry, "name"),
        published:
          extractTag(entry, "published") || extractTag(entry, "updated"),
        guid: extractTag(entry, "id"),
        categories: (entry.match(/<category[^>]*term=["']([^"']*)["']/gi) || [])
          .map((c: string) => {
            const m = c.match(/term=["']([^"']*)/);
            return m ? m[1] : "";
          })
          .filter(Boolean),
      });
    }
  }

  return items;
}

function parseFeedMeta(xml: string): any {
  // Try RSS channel
  const channel =
    xml.match(/<channel[\s>][\s\S]*?(?=<item[\s>]|$)/i)?.[0] || xml;
  return {
    title: extractTag(channel, "title"),
    link: extractTag(channel, "link") || extractAttr(channel, "link", "href"),
    description:
      extractTag(channel, "description") || extractTag(channel, "subtitle"),
    language: extractTag(channel, "language"),
    feed_type: xml.includes("<rss")
      ? "rss"
      : xml.includes("<feed")
        ? "atom"
        : "unknown",
  };
}

export default function rss(dl: DriplinePluginAPI) {
  dl.setName("rss");
  dl.setVersion("0.1.0");

  dl.registerTable("rss_channel", {
    description: "RSS/Atom feed channel metadata",
    columns: [
      { name: "title", type: "string" },
      { name: "link", type: "string" },
      { name: "description", type: "string" },
      { name: "language", type: "string" },
      { name: "feed_type", type: "string" },
    ],
    keyColumns: [{ name: "feed_url", required: "required", operators: ["="] }],
    *list(ctx) {
      const url = ctx.quals.find((q) => q.column === "feed_url")?.value;
      if (!url) return;
      const resp = syncGet(url, { "User-Agent": "dripline/0.1" });
      if (resp.status !== 200) return;
      const xml =
        typeof resp.body === "string" ? resp.body : JSON.stringify(resp.body);
      const meta = parseFeedMeta(xml);
      yield { ...meta, feed_url: url };
    },
  });

  dl.registerTable("rss_item", {
    description: "RSS/Atom feed items",
    columns: [
      { name: "title", type: "string" },
      { name: "link", type: "string" },
      { name: "description", type: "string" },
      { name: "author", type: "string" },
      { name: "published", type: "string" },
      { name: "guid", type: "string" },
      { name: "categories", type: "json" },
    ],
    keyColumns: [{ name: "feed_url", required: "required", operators: ["="] }],
    *list(ctx) {
      const url = ctx.quals.find((q) => q.column === "feed_url")?.value;
      if (!url) return;
      const resp = syncGet(url, { "User-Agent": "dripline/0.1" });
      if (resp.status !== 200) return;
      const xml =
        typeof resp.body === "string" ? resp.body : JSON.stringify(resp.body);
      for (const item of parseItems(xml)) {
        yield {
          ...item,
          categories: JSON.stringify(item.categories),
          feed_url: url,
        };
      }
    },
  });
}
