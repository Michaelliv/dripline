import { describe, it, beforeEach, afterEach } from "node:test";
import { strict as assert } from "node:assert";
import { QueryEngine } from "../engine.js";
import { PluginRegistry } from "../plugin/registry.js";
import { QueryCache } from "../cache.js";
import { RateLimiter } from "../rate-limiter.js";
import type { PluginDef, QueryContext } from "../plugin/types.js";

// ── Mock Data ──

const USERS = [
  { id: 1, name: "Alice", email: "alice@test.com", role: "admin", active: 1 },
  { id: 2, name: "Bob", email: "bob@test.com", role: "user", active: 1 },
  { id: 3, name: "Charlie", email: "charlie@test.com", role: "user", active: 0 },
  { id: 4, name: "Diana", email: "diana@test.com", role: "admin", active: 1 },
  { id: 5, name: "Eve", email: "eve@test.com", role: "viewer", active: 1 },
  { id: 6, name: "Frank", email: null, role: "user", active: 0 },
  { id: 7, name: "Grace", email: "grace@test.com", role: "admin", active: 1 },
  { id: 8, name: "Hank", email: "hank@test.com", role: "user", active: 1 },
  { id: 9, name: "Ivy", email: "ivy@test.com", role: "viewer", active: 0 },
  { id: 10, name: "Jack", email: "jack@test.com", role: "user", active: 1 },
];

const ORDERS = Array.from({ length: 20 }, (_, i) => ({
  id: i + 1,
  user_id: (i % 10) + 1,
  amount: Math.round((i + 1) * 9.99 * 100) / 100,
  status: i % 3 === 0 ? "completed" : i % 3 === 1 ? "pending" : "cancelled",
  created_at: `2024-01-${String(i + 1).padStart(2, "0")} 10:00:00`,
  metadata: JSON.stringify({ source: i % 2 === 0 ? "web" : "api" }),
}));

let listCallCount = 0;

const mockPlugin: PluginDef = {
  name: "mock",
  version: "0.1.0",
  tables: [
    {
      name: "mock_users",
      description: "Mock users table",
      columns: [
        { name: "id", type: "number" },
        { name: "name", type: "string" },
        { name: "email", type: "string" },
        { name: "active", type: "boolean" },
      ],
      keyColumns: [{ name: "role", required: "optional" }],
      *list(ctx) {
        listCallCount++;
        const role = ctx.quals.find((q) => q.column === "role")?.value;
        for (const u of USERS) {
          if (role && u.role !== role) continue;
          yield { id: u.id, name: u.name, email: u.email, active: u.active };
        }
      },
    },
    {
      name: "mock_orders",
      description: "Mock orders table",
      columns: [
        { name: "id", type: "number" },
        { name: "user_id", type: "number" },
        { name: "amount", type: "number" },
        { name: "created_at", type: "datetime" },
        { name: "metadata", type: "json" },
      ],
      keyColumns: [
        { name: "status", required: "optional" },
      ],
      *list(ctx) {
        listCallCount++;
        const status = ctx.quals.find((q) => q.column === "status")?.value;
        for (const o of ORDERS) {
          if (status && o.status !== status) continue;
          yield {
            id: o.id,
            user_id: o.user_id,
            amount: o.amount,
            created_at: o.created_at,
            metadata: o.metadata,
          };
        }
      },
    },
    {
      name: "mock_empty",
      description: "Always empty table",
      columns: [{ name: "id", type: "number" }],
      *list() {
        // yields nothing
      },
    },
    {
      name: "mock_error",
      description: "Always throws",
      columns: [{ name: "id", type: "number" }],
      keyColumns: [{ name: "should_error", required: "optional" }],
      *list(ctx) {
        const shouldError = ctx.quals.find((q) => q.column === "should_error")?.value;
        if (shouldError === "yes") throw new Error("API rate limit exceeded");
        yield { id: 1 };
      },
    },
  ],
};

// ── Test Setup ──

let engine: QueryEngine;
let cache: QueryCache;
let rl: RateLimiter;

function setup() {
  const reg = new PluginRegistry();
  reg.register(mockPlugin);
  cache = new QueryCache({ enabled: true, ttl: 300, maxSize: 100 });
  rl = new RateLimiter();
  engine = new QueryEngine(reg, cache, rl);
  engine.initialize({
    connections: [],
    cache: { enabled: true, ttl: 300, maxSize: 100 },
    rateLimits: {},
  });
  listCallCount = 0;
}

function teardown() {
  engine.close();
}

// ── Tests ──

describe("Basic queries", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("SELECT * FROM mock_users returns all 10", () => {
    const rows = engine.query("SELECT * FROM mock_users");
    assert.equal(rows.length, 10);
  });

  it("WHERE on key column filters at plugin level", () => {
    const rows = engine.query("SELECT * FROM mock_users WHERE role = 'admin'");
    assert.equal(rows.length, 3);
    for (const row of rows) {
      // role is a parameter, not in visible columns
    }
  });

  it("WHERE on non-key column filtered by SQLite", () => {
    const rows = engine.query("SELECT * FROM mock_users WHERE name = 'Alice'");
    assert.equal(rows.length, 1);
    assert.equal((rows[0] as any).name, "Alice");
  });
});

describe("JOINs", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("JOIN mock_users and mock_orders", () => {
    const rows = engine.query(`
      SELECT u.name, o.amount, o.created_at
      FROM mock_users u
      JOIN mock_orders o ON u.id = o.user_id
      WHERE u.name = 'Alice'
    `);
    assert.equal(rows.length, 2); // user_id 1 appears twice in 20 orders
  });

  it("LEFT JOIN with empty table", () => {
    const rows = engine.query(`
      SELECT u.name, e.id as eid
      FROM mock_users u
      LEFT JOIN mock_empty e ON u.id = e.id
    `);
    assert.equal(rows.length, 10);
    assert.equal((rows[0] as any).eid, null);
  });
});

describe("Aggregation", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("GROUP BY with COUNT", () => {
    const rows = engine.query(`
      SELECT active, COUNT(*) as cnt
      FROM mock_users
      GROUP BY active
    `);
    assert.equal(rows.length, 2);
  });

  it("SUM aggregation", () => {
    const rows = engine.query("SELECT SUM(amount) as total FROM mock_orders");
    assert.equal(rows.length, 1);
    assert.ok((rows[0] as any).total > 0);
  });

  it("HAVING clause", () => {
    const rows = engine.query(`
      SELECT active, COUNT(*) as cnt
      FROM mock_users
      GROUP BY active
      HAVING cnt > 3
    `);
    assert.ok(rows.length >= 1);
  });

  it("DISTINCT", () => {
    const rows = engine.query("SELECT DISTINCT active FROM mock_users");
    assert.equal(rows.length, 2);
  });
});

describe("Subqueries", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("subquery in WHERE", () => {
    const rows = engine.query(`
      SELECT * FROM mock_users
      WHERE id IN (SELECT user_id FROM mock_orders WHERE amount > 100)
    `);
    assert.ok(rows.length > 0);
  });

  it("derived table", () => {
    const rows = engine.query(`
      SELECT * FROM (
        SELECT active, COUNT(*) as cnt FROM mock_users GROUP BY active
      ) sub WHERE sub.cnt > 0
    `);
    assert.equal(rows.length, 2);
  });
});

describe("LIMIT and OFFSET", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("LIMIT", () => {
    const rows = engine.query("SELECT * FROM mock_users LIMIT 3");
    assert.equal(rows.length, 3);
  });

  it("LIMIT with OFFSET", () => {
    const rows = engine.query("SELECT * FROM mock_users LIMIT 2 OFFSET 5");
    assert.equal(rows.length, 2);
  });

  it("LIMIT 0", () => {
    const rows = engine.query("SELECT * FROM mock_users LIMIT 0");
    assert.equal(rows.length, 0);
  });
});

describe("ORDER BY", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("ORDER BY ASC", () => {
    const rows = engine.query("SELECT * FROM mock_users ORDER BY id ASC") as any[];
    assert.equal(rows[0].id, 1);
  });

  it("ORDER BY DESC", () => {
    const rows = engine.query("SELECT * FROM mock_users ORDER BY id DESC") as any[];
    assert.equal(rows[0].id, 10);
  });
});

describe("NULL handling", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("IS NULL", () => {
    const rows = engine.query("SELECT * FROM mock_users WHERE email IS NULL");
    assert.equal(rows.length, 1);
    assert.equal((rows[0] as any).name, "Frank");
  });

  it("COALESCE", () => {
    const rows = engine.query(
      "SELECT COALESCE(email, 'none') as email FROM mock_users WHERE name = 'Frank'",
    ) as any[];
    assert.equal(rows[0].email, "none");
  });

  it("COUNT(col) vs COUNT(*)", () => {
    const rows = engine.query(
      "SELECT COUNT(email) as c, COUNT(*) as total FROM mock_users",
    ) as any[];
    assert.equal(rows[0].c, 9);
    assert.equal(rows[0].total, 10);
  });
});

describe("Cache", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("cache hit on second query", () => {
    engine.query("SELECT * FROM mock_users");
    const before = cache.stats().hits;
    engine.query("SELECT * FROM mock_users");
    assert.equal(cache.stats().hits, before + 1);
  });
});

describe("JSON handling", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("json_extract on JSON column", () => {
    const rows = engine.query(
      "SELECT json_extract(metadata, '$.source') as source FROM mock_orders WHERE id = 1",
    ) as any[];
    assert.equal(rows[0].source, "web");
  });
});

describe("Empty results", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("empty table", () => {
    const rows = engine.query("SELECT * FROM mock_empty");
    assert.equal(rows.length, 0);
  });

  it("no matching rows", () => {
    const rows = engine.query("SELECT * FROM mock_users WHERE name = 'Nobody'");
    assert.equal(rows.length, 0);
  });
});

describe("Error handling", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("plugin error propagates", () => {
    assert.throws(
      () => engine.query("SELECT * FROM mock_error WHERE should_error = 'yes'"),
      /rate limit/,
    );
  });

  it("plugin success when no error", () => {
    const rows = engine.query("SELECT * FROM mock_error WHERE should_error = 'no'");
    assert.equal(rows.length, 1);
  });
});

describe("CTE", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("WITH clause works", () => {
    const rows = engine.query(`
      WITH active_users AS (
        SELECT * FROM mock_users WHERE active = 1
      )
      SELECT COUNT(*) as cnt FROM active_users
    `) as any[];
    assert.equal(rows[0].cnt, 7);
  });
});

describe("UNION", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("UNION across tables", () => {
    const rows = engine.query(`
      SELECT name FROM mock_users WHERE id <= 2
      UNION
      SELECT name FROM mock_users WHERE id = 2
    `);
    assert.equal(rows.length, 2); // UNION deduplicates
  });
});
