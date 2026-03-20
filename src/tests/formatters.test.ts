import { describe, it } from "node:test";
import { strict as assert } from "node:assert";
import { formatTable } from "../utils/table-formatter.js";
import { formatJson, formatCsv, formatLine } from "../utils/formatters.js";

describe("formatTable", () => {
  it("empty array returns No results.", () => {
    assert.equal(formatTable([]), "No results.");
  });

  it("one row has borders, header, data", () => {
    const out = formatTable([{ id: 1, name: "alice" }]);
    assert.ok(out.includes("┌"));
    assert.ok(out.includes("┘"));
    assert.ok(out.includes("id"));
    assert.ok(out.includes("name"));
    assert.ok(out.includes("alice"));
  });

  it("multiple rows", () => {
    const out = formatTable([{ a: 1 }, { a: 2 }, { a: 3 }]);
    assert.ok(out.includes("1"));
    assert.ok(out.includes("2"));
    assert.ok(out.includes("3"));
  });

  it("null values display as <null>", () => {
    const out = formatTable([{ a: null }]);
    assert.ok(out.includes("<null>"));
  });

  it("boolean values", () => {
    const out = formatTable([{ a: true, b: false }]);
    assert.ok(out.includes("true"));
    assert.ok(out.includes("false"));
  });

  it("long strings truncated", () => {
    const long = "x".repeat(200);
    const out = formatTable([{ a: long }], { maxWidth: 30 });
    assert.ok(out.includes("…"));
    assert.ok(!out.includes(long)); // full string should not appear
  });

  it("shows row count", () => {
    const out = formatTable([{ a: 1 }, { a: 2 }]);
    assert.ok(out.includes("2 rows."));
  });

  it("1 row shows singular", () => {
    const out = formatTable([{ a: 1 }]);
    assert.ok(out.includes("1 row."));
  });
});

describe("formatJson", () => {
  it("returns pretty-printed JSON array", () => {
    const out = formatJson([{ id: 1 }]);
    assert.equal(out, '[\n  {\n    "id": 1\n  }\n]');
  });

  it("empty array", () => {
    assert.equal(formatJson([]), "[]");
  });

  it("nested objects", () => {
    const out = formatJson([{ a: { b: 1 } }]);
    assert.ok(out.includes('"b": 1'));
  });
});

describe("formatCsv", () => {
  it("header + data rows", () => {
    const out = formatCsv([{ id: 1, name: "alice" }]);
    const lines = out.split("\n");
    assert.equal(lines[0], "id,name");
    assert.equal(lines[1], "1,alice");
  });

  it("empty array returns empty string", () => {
    assert.equal(formatCsv([]), "");
  });

  it("escapes fields with commas", () => {
    const out = formatCsv([{ a: "hello, world" }]);
    assert.ok(out.includes('"hello, world"'));
  });

  it("escapes fields with quotes", () => {
    const out = formatCsv([{ a: 'say "hi"' }]);
    assert.ok(out.includes('"say ""hi"""'));
  });

  it("escapes fields with newlines", () => {
    const out = formatCsv([{ a: "line1\nline2" }]);
    assert.ok(out.includes('"line1\nline2"'));
  });

  it("null/undefined as empty", () => {
    const out = formatCsv([{ a: null, b: undefined }]);
    const lines = out.split("\n");
    assert.equal(lines[1], ",");
  });
});

describe("formatLine", () => {
  it("empty array returns No results.", () => {
    assert.equal(formatLine([]), "No results.");
  });

  it("one record per block", () => {
    const out = formatLine([{ id: 1, name: "alice" }]);
    assert.ok(out.includes("id"));
    assert.ok(out.includes("alice"));
    assert.ok(out.includes("|"));
  });

  it("records separated by blank line", () => {
    const out = formatLine([{ a: 1 }, { a: 2 }]);
    assert.ok(out.includes("\n\n"));
  });

  it("null values as <null>", () => {
    const out = formatLine([{ a: null }]);
    assert.ok(out.includes("<null>"));
  });

  it("pads column names to align", () => {
    const out = formatLine([{ short: 1, longername: 2 }]);
    const lines = out.split("\n");
    // Both pipes should be at the same column
    const pipe1 = lines[0].indexOf("|");
    const pipe2 = lines[1].indexOf("|");
    assert.equal(pipe1, pipe2);
  });
});
