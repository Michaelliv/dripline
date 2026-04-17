import type { DriplinePluginAPI, QueryContext } from "dripline";
import { commandExists, syncExec } from "dripline";

function mongosh(ctx: QueryContext, script: string): any[] {
  const uri = ctx.connection.config.uri || "";
  const { rows } = syncExec(
    "mongosh",
    [uri, "--quiet", "--json=relaxed", "--eval", script],
    { parser: "json" },
  );
  return Array.isArray(rows) ? rows : [rows];
}

export default function mongodb(dl: DriplinePluginAPI) {
  dl.setName("mongodb");
  dl.setVersion("0.1.0");

  dl.onInit(() => {
    if (!commandExists("mongosh")) {
      dl.log.warn("mongosh not found on PATH — mongodb tables will be unavailable");
    }
  });

  dl.setConnectionSchema({
    uri: {
      type: "string",
      description: "MongoDB connection URI",
      required: true,
      env: "MONGODB_URI",
    },
  });

  dl.registerTable("mongo_collections", {
    description: "Collections in the connected database",
    columns: [
      { name: "name", type: "string" },
      { name: "type", type: "string" },
    ],
    *list(ctx) {
      const results = mongosh(ctx, `db.getCollectionInfos().map(c => ({name: c.name, type: c.type}))`);
      for (const r of results) yield r;
    },
  });

  dl.registerTable("mongo_query", {
    description: "Run a MongoDB find query. Use collection and filter key columns.",
    columns: [
      { name: "collection", type: "string" },
      { name: "filter", type: "string" },
      { name: "doc", type: "json" },
    ],
    keyColumns: [
      { name: "collection", required: "required", operators: ["="] },
      { name: "filter", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const collection = ctx.quals.find((q) => q.column === "collection")?.value as string;
      if (!collection) return;
      const filter = (ctx.quals.find((q) => q.column === "filter")?.value as string) || "{}";
      const script = `db.getCollection("${collection}").find(${filter}).limit(1000).toArray()`;
      const results = mongosh(ctx, script);
      for (const doc of results) {
        yield { collection, filter, doc: JSON.stringify(doc) };
      }
    },
  });

  dl.registerTable("mongo_aggregate", {
    description: "Run a MongoDB aggregation pipeline. Use collection and pipeline key columns.",
    columns: [
      { name: "collection", type: "string" },
      { name: "pipeline", type: "string" },
      { name: "doc", type: "json" },
    ],
    keyColumns: [
      { name: "collection", required: "required", operators: ["="] },
      { name: "pipeline", required: "required", operators: ["="] },
    ],
    *list(ctx) {
      const collection = ctx.quals.find((q) => q.column === "collection")?.value as string;
      const pipeline = ctx.quals.find((q) => q.column === "pipeline")?.value as string;
      if (!collection || !pipeline) return;
      const script = `db.getCollection("${collection}").aggregate(${pipeline}).toArray()`;
      const results = mongosh(ctx, script);
      for (const doc of results) {
        yield { collection, pipeline, doc: JSON.stringify(doc) };
      }
    },
  });

  dl.registerTable("mongo_stats", {
    description: "Database statistics",
    columns: [
      { name: "db", type: "string" },
      { name: "collections", type: "number" },
      { name: "objects", type: "number" },
      { name: "data_size", type: "number" },
      { name: "storage_size", type: "number" },
      { name: "indexes", type: "number" },
      { name: "index_size", type: "number" },
    ],
    *list(ctx) {
      const results = mongosh(ctx, `db.stats()`);
      const s = results[0] || results;
      yield {
        db: s.db ?? "",
        collections: s.collections ?? 0,
        objects: s.objects ?? 0,
        data_size: s.dataSize ?? 0,
        storage_size: s.storageSize ?? 0,
        indexes: s.indexes ?? 0,
        index_size: s.indexSize ?? 0,
      };
    },
  });
}
