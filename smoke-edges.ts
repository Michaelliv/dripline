import Database from "better-sqlite3";

const db = new Database(":memory:");
let passed = 0;
let failed = 0;

function test(name: string, fn: () => void) {
  try {
    fn();
    console.log(`  ✅ ${name}`);
    passed++;
  } catch (e: any) {
    console.log(`  ❌ ${name}: ${e.message}`);
    failed++;
  }
}

function assert(condition: boolean, msg: string) {
  if (!condition) throw new Error(msg);
}

// === Setup: mock plugin tables ===

// Table with all data types
db.table("typed_table", {
  columns: ["id", "name", "score", "active", "metadata", "created_at", "ip_addr", "tags"],
  *rows() {
    yield { id: 1, name: "alice", score: 99.5, active: 1, metadata: '{"role":"admin"}', created_at: "2024-01-15 10:30:00", ip_addr: "192.168.1.1", tags: '["a","b"]' };
    yield { id: 2, name: "bob", score: 45.2, active: 0, metadata: '{"role":"user"}', created_at: "2024-06-20 14:00:00", ip_addr: "10.0.0.1", tags: '["c"]' };
    yield { id: 3, name: null, score: null, active: null, metadata: null, created_at: null, ip_addr: null, tags: null };
  },
});

// Table with required parameters (like steampipe key columns)
db.table("api_resources", {
  columns: ["id", "name", "type", "status"],
  parameters: ["region", "account_id"],
  *rows(region: string, account_id: string) {
    if (!region) return; // required param not provided
    const data: Record<string, any[]> = {
      "us-east-1": [
        { id: "i-1", name: "web-1", type: "t3.micro", status: "running" },
        { id: "i-2", name: "web-2", type: "t3.small", status: "stopped" },
      ],
      "eu-west-1": [
        { id: "i-3", name: "api-1", type: "m5.large", status: "running" },
      ],
    };
    for (const row of data[region] || []) yield row;
  },
});

// Large dataset for pagination/limit testing
db.table("big_table", {
  columns: ["idx", "value"],
  *rows() {
    for (let i = 0; i < 10000; i++) {
      yield { idx: i, value: `row-${i}` };
    }
  },
});

// Table that throws errors
db.table("error_table", {
  columns: ["id"],
  parameters: ["should_error"],
  *rows(should_error: string) {
    if (should_error === "yes") throw new Error("API rate limit exceeded");
    yield { id: 1 };
  },
});

// Empty table
db.table("empty_table", {
  columns: ["id", "name"],
  *rows() {
    // yields nothing
  },
});

// Table with duplicate column values for GROUP BY testing
db.table("events", {
  columns: ["id", "type", "severity", "timestamp"],
  *rows() {
    yield { id: 1, type: "login", severity: "info", timestamp: "2024-01-01 00:00:00" };
    yield { id: 2, type: "login", severity: "info", timestamp: "2024-01-01 01:00:00" };
    yield { id: 3, type: "error", severity: "high", timestamp: "2024-01-01 02:00:00" };
    yield { id: 4, type: "error", severity: "critical", timestamp: "2024-01-01 03:00:00" };
    yield { id: 5, type: "login", severity: "warn", timestamp: "2024-01-02 00:00:00" };
  },
});

// === Edge Case Tests ===

console.log("\n🔍 1. NULL handling");
test("SELECT with NULL values", () => {
  const rows = db.prepare("SELECT * FROM typed_table WHERE name IS NULL").all();
  assert(rows.length === 1, `expected 1 row, got ${rows.length}`);
});
test("NULL in aggregation", () => {
  const r: any = db.prepare("SELECT COUNT(name) as c, COUNT(*) as total FROM typed_table").get();
  assert(r.c === 2 && r.total === 3, `count(name)=${r.c}, count(*)=${r.total}`);
});
test("COALESCE with NULLs", () => {
  const r: any = db.prepare("SELECT COALESCE(name, 'unknown') as name FROM typed_table WHERE id = 3").get();
  assert(r.name === "unknown", `expected 'unknown', got '${r.name}'`);
});

console.log("\n🔍 2. Required parameters (key columns)");
test("query with required param works", () => {
  const rows = db.prepare("SELECT * FROM api_resources WHERE region = 'us-east-1'").all();
  assert(rows.length === 2, `expected 2, got ${rows.length}`);
});
test("query without required param returns empty", () => {
  const rows = db.prepare("SELECT * FROM api_resources").all();
  assert(rows.length === 0, `expected 0, got ${rows.length}`);
});
test("query with multiple params", () => {
  const rows = db.prepare("SELECT * FROM api_resources WHERE region = 'us-east-1' AND account_id = '12345'").all();
  assert(rows.length === 2, `expected 2, got ${rows.length}`);
});
test("query with non-existent region", () => {
  const rows = db.prepare("SELECT * FROM api_resources WHERE region = 'ap-south-1'").all();
  assert(rows.length === 0, `expected 0, got ${rows.length}`);
});

console.log("\n🔍 3. LIMIT and OFFSET");
test("LIMIT works", () => {
  const rows = db.prepare("SELECT * FROM big_table LIMIT 5").all();
  assert(rows.length === 5, `expected 5, got ${rows.length}`);
});
test("LIMIT with OFFSET", () => {
  const rows: any[] = db.prepare("SELECT * FROM big_table LIMIT 3 OFFSET 10").all();
  assert(rows.length === 3, `expected 3, got ${rows.length}`);
  assert(rows[0].idx === 10, `expected idx=10, got ${rows[0].idx}`);
});
test("LIMIT 0", () => {
  const rows = db.prepare("SELECT * FROM big_table LIMIT 0").all();
  assert(rows.length === 0, `expected 0, got ${rows.length}`);
});

console.log("\n🔍 4. Operators in WHERE");
test("equality", () => {
  const rows = db.prepare("SELECT * FROM typed_table WHERE id = 1").all();
  assert(rows.length === 1, `expected 1, got ${rows.length}`);
});
test("greater than", () => {
  const rows = db.prepare("SELECT * FROM typed_table WHERE score > 50").all();
  assert(rows.length === 1, `expected 1, got ${rows.length}`);
});
test("LIKE", () => {
  const rows = db.prepare("SELECT * FROM typed_table WHERE name LIKE 'a%'").all();
  assert(rows.length === 1, `expected 1, got ${rows.length}`);
});
test("IN clause", () => {
  const rows = db.prepare("SELECT * FROM typed_table WHERE id IN (1, 2)").all();
  assert(rows.length === 2, `expected 2, got ${rows.length}`);
});
test("BETWEEN", () => {
  const rows = db.prepare("SELECT * FROM typed_table WHERE score BETWEEN 40 AND 100").all();
  assert(rows.length === 2, `expected 2, got ${rows.length}`);
});
test("NOT equal", () => {
  const rows = db.prepare("SELECT * FROM typed_table WHERE name != 'alice'").all();
  assert(rows.length === 1, `expected 1, got ${rows.length}`);
});

console.log("\n🔍 5. JOINs across virtual tables");
test("JOIN two virtual tables", () => {
  const rows = db.prepare(`
    SELECT t.name, e.type, e.severity 
    FROM typed_table t 
    JOIN events e ON t.id = e.id
  `).all();
  assert(rows.length === 3, `expected 3, got ${rows.length}`);
});
test("LEFT JOIN with empty table", () => {
  const rows = db.prepare(`
    SELECT t.name, e.name as ename 
    FROM typed_table t 
    LEFT JOIN empty_table e ON t.id = e.id
  `).all();
  assert(rows.length === 3, `expected 3, got ${rows.length}`);
});

console.log("\n🔍 6. Aggregation");
test("GROUP BY with COUNT", () => {
  const rows: any[] = db.prepare("SELECT type, COUNT(*) as cnt FROM events GROUP BY type").all();
  assert(rows.length === 2, `expected 2 groups, got ${rows.length}`);
});
test("HAVING clause", () => {
  const rows = db.prepare("SELECT type, COUNT(*) as cnt FROM events GROUP BY type HAVING cnt > 2").all();
  assert(rows.length === 1, `expected 1, got ${rows.length}`);
});
test("DISTINCT", () => {
  const rows = db.prepare("SELECT DISTINCT type FROM events").all();
  assert(rows.length === 2, `expected 2, got ${rows.length}`);
});

console.log("\n🔍 7. Subqueries");
test("subquery in WHERE", () => {
  const rows = db.prepare(`
    SELECT * FROM typed_table 
    WHERE id IN (SELECT id FROM events WHERE severity = 'high')
  `).all();
  assert(rows.length === 1, `expected 1, got ${rows.length}`);
});
test("subquery in FROM (derived table)", () => {
  const rows = db.prepare(`
    SELECT * FROM (SELECT type, COUNT(*) as cnt FROM events GROUP BY type) sub
    WHERE sub.cnt >= 2
  `).all();
  assert(rows.length === 2, `expected 2, got ${rows.length}`);
});

console.log("\n🔍 8. JSON handling");
test("json_extract on JSON column", () => {
  const r: any = db.prepare("SELECT json_extract(metadata, '$.role') as role FROM typed_table WHERE id = 1").get();
  assert(r.role === "admin", `expected 'admin', got '${r.role}'`);
});
test("json_each for array expansion", () => {
  const rows = db.prepare(`
    SELECT t.id, j.value as tag 
    FROM typed_table t, json_each(t.tags) j 
    WHERE t.id = 1
  `).all();
  assert(rows.length === 2, `expected 2 tags, got ${rows.length}`);
});

console.log("\n🔍 9. Error handling");
test("API error propagates", () => {
  try {
    db.prepare("SELECT * FROM error_table WHERE should_error = 'yes'").all();
    assert(false, "should have thrown");
  } catch (e: any) {
    assert(e.message.includes("rate limit"), `unexpected error: ${e.message}`);
  }
});
test("API success when no error", () => {
  const rows = db.prepare("SELECT * FROM error_table WHERE should_error = 'no'").all();
  assert(rows.length === 1, `expected 1, got ${rows.length}`);
});

console.log("\n🔍 10. Empty results");
test("empty table returns empty array", () => {
  const rows = db.prepare("SELECT * FROM empty_table").all();
  assert(rows.length === 0, `expected 0, got ${rows.length}`);
});
test("WHERE with no matches", () => {
  const rows = db.prepare("SELECT * FROM typed_table WHERE name = 'nonexistent'").all();
  assert(rows.length === 0, `expected 0, got ${rows.length}`);
});

console.log("\n🔍 11. ORDER BY on virtual tables");
test("ORDER BY ASC", () => {
  const rows: any[] = db.prepare("SELECT * FROM typed_table ORDER BY id ASC").all();
  assert(rows[0].id === 1, `expected first id=1, got ${rows[0].id}`);
});
test("ORDER BY DESC", () => {
  const rows: any[] = db.prepare("SELECT * FROM typed_table ORDER BY id DESC").all();
  assert(rows[0].id === 3, `expected first id=3, got ${rows[0].id}`);
});
test("ORDER BY with NULLs", () => {
  const rows: any[] = db.prepare("SELECT * FROM typed_table ORDER BY score ASC").all();
  // SQLite: NULLs come first in ASC
  assert(rows[0].score === null, `expected null first, got ${rows[0].score}`);
});

console.log("\n🔍 12. Large result sets (generator streaming)");
test("10k rows without OOM", () => {
  const r: any = db.prepare("SELECT COUNT(*) as cnt FROM big_table").get();
  assert(r.cnt === 10000, `expected 10000, got ${r.cnt}`);
});
test("aggregate on 10k rows", () => {
  const r: any = db.prepare("SELECT SUM(idx) as total FROM big_table").get();
  assert(r.total === 49995000, `expected 49995000, got ${r.total}`);
});

console.log("\n🔍 13. UNION across virtual tables");
test("UNION two virtual tables", () => {
  const rows = db.prepare(`
    SELECT name FROM typed_table WHERE name IS NOT NULL
    UNION
    SELECT type as name FROM events
  `).all();
  assert(rows.length === 4, `expected 4 unique, got ${rows.length}`);
});

console.log("\n🔍 14. CTE (WITH clause)");
test("CTE works with virtual tables", () => {
  const rows = db.prepare(`
    WITH high_events AS (
      SELECT * FROM events WHERE severity IN ('high', 'critical')
    )
    SELECT COUNT(*) as cnt FROM high_events
  `).all();
  assert((rows[0] as any).cnt === 2, `expected 2, got ${(rows[0] as any).cnt}`);
});

console.log("\n🔍 15. Multiple scans of same virtual table");
test("self-join on virtual table", () => {
  const rows = db.prepare(`
    SELECT a.id, b.id 
    FROM typed_table a, typed_table b 
    WHERE a.id < b.id AND a.name IS NOT NULL AND b.name IS NOT NULL
  `).all();
  assert(rows.length === 1, `expected 1 pair, got ${rows.length}`);
});

// === Summary ===
console.log(`\n${"=".repeat(40)}`);
console.log(`Results: ${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
console.log("✅ All edge case tests passed\n");
