import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { commandExists, syncExec } from "../utils/cli.js";

describe("syncExec", () => {
  it("parses JSON output", () => {
    const { rows } = syncExec("echo", ['[{"a":1},{"a":2}]'], {
      parser: "json",
    });
    assert.deepStrictEqual(rows, [{ a: 1 }, { a: 2 }]);
  });

  it("parses JSON object as single-row array", () => {
    const { rows } = syncExec("echo", ['{"x":"y"}'], { parser: "json" });
    assert.deepStrictEqual(rows, [{ x: "y" }]);
  });

  it("parses JSON lines", () => {
    const { rows } = syncExec("printf", ['{"a":1}\n{"a":2}\n'], {
      parser: "jsonlines",
    });
    assert.equal(rows.length, 2);
    assert.equal(rows[0].a, 1);
    assert.equal(rows[1].a, 2);
  });

  it("skips invalid JSON lines", () => {
    const { rows } = syncExec("printf", ['{"a":1}\nnot json\n{"a":2}\n'], {
      parser: "jsonlines",
    });
    assert.equal(rows.length, 2);
  });

  it("parses CSV with headers", () => {
    const { rows } = syncExec("printf", ["name,age\nalice,30\nbob,25\n"], {
      parser: "csv",
    });
    assert.equal(rows.length, 2);
    assert.equal(rows[0].name, "alice");
    assert.equal(rows[0].age, 30);
  });

  it("parses TSV with headers", () => {
    const { rows } = syncExec("printf", ["name\tage\nalice\t30\n"], {
      parser: "tsv",
    });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].name, "alice");
  });

  it("parses CSV without headers", () => {
    const { rows } = syncExec("printf", ["alice,30\nbob,25\n"], {
      parser: "csv",
      headers: false,
    });
    assert.equal(rows.length, 2);
    assert.equal(rows[0].col0, "alice");
    assert.equal(rows[0].col1, 30);
  });

  it("parses lines", () => {
    const { rows } = syncExec("printf", ["hello\nworld\n"], {
      parser: "lines",
    });
    assert.equal(rows.length, 2);
    assert.equal(rows[0].line, "hello");
    assert.equal(rows[0].line_number, 1);
  });

  it("parses key-value", () => {
    const { rows } = syncExec("printf", ["name=alice\nage=30\n"], {
      parser: "kv",
    });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].name, "alice");
    assert.equal(rows[0].age, 30);
  });

  it("returns raw output", () => {
    const { rows } = syncExec("echo", ["hello world"], { parser: "raw" });
    assert.equal(rows[0].output, "hello world");
  });

  it("preserves raw string", () => {
    const { raw } = syncExec("echo", ["test"], { parser: "raw" });
    assert.ok(raw.includes("test"));
  });

  it("throws on non-zero exit", () => {
    assert.throws(() => syncExec("false", []), /Command failed/);
  });

  it("ignores exit code when told to", () => {
    const { rows } = syncExec("sh", ["-c", "echo '{\"ok\":true}'; exit 1"], {
      parser: "json",
      ignoreExitCode: true,
    });
    assert.deepStrictEqual(rows, [{ ok: true }]);
  });

  it("passes input via stdin", () => {
    const { rows } = syncExec("cat", [], { parser: "raw", input: "hello" });
    assert.equal(rows[0].output, "hello");
  });

  it("coerces boolean and null values in CSV", () => {
    const { rows } = syncExec("printf", ["a,b,c\ntrue,false,null\n"], {
      parser: "csv",
    });
    assert.equal(rows[0].a, true);
    assert.equal(rows[0].b, false);
    assert.equal(rows[0].c, null);
  });

  it("handles empty output", () => {
    const { rows } = syncExec("printf", [""], { parser: "jsonlines" });
    assert.equal(rows.length, 0);
  });
});

describe("commandExists", () => {
  it("returns true for echo", () => {
    assert.equal(commandExists("echo"), true);
  });

  it("returns false for nonexistent command", () => {
    assert.equal(commandExists("definitely_not_a_real_command_xyz"), false);
  });
});
