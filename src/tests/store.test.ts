import { describe, it, beforeEach, afterEach } from "node:test";
import { strict as assert } from "node:assert";
import { mkdtempSync, mkdirSync, existsSync, realpathSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  findRoot,
  initStore,
  newId,
  writeRecord,
  readOne,
  readAll,
  deleteRecord,
} from "../store.js";

let origCwd: string;
let tmpDir: string;

function setup() {
  origCwd = process.cwd();
  tmpDir = realpathSync(mkdtempSync(join(tmpdir(), "dripline-store-test-")));
  process.chdir(tmpDir);
}

function teardown() {
  process.chdir(origCwd);
}

describe("Store", () => {
  beforeEach(() => setup());
  afterEach(() => teardown());

  it("findRoot returns null when no .dripline", () => {
    assert.equal(findRoot(), null);
  });

  it("findRoot finds .dripline in current dir", () => {
    mkdirSync(join(tmpDir, ".dripline"));
    assert.equal(findRoot(), join(tmpDir, ".dripline"));
  });

  it("findRoot walks up parent dirs", () => {
    mkdirSync(join(tmpDir, ".dripline"));
    const sub = join(tmpDir, "a", "b");
    mkdirSync(sub, { recursive: true });
    process.chdir(sub);
    assert.equal(findRoot(), join(tmpDir, ".dripline"));
  });

  it("initStore creates .dripline/ and collection dirs", () => {
    const root = initStore(["items", "users"]);
    assert.ok(existsSync(root));
    assert.ok(existsSync(join(root, "items")));
    assert.ok(existsSync(join(root, "users")));
  });

  it("initStore is idempotent", () => {
    initStore(["items"]);
    initStore(["items"]); // should not throw
  });

  it("newId returns 8-char string", () => {
    const id = newId();
    assert.equal(typeof id, "string");
    assert.equal(id.length, 8);
  });

  it("newId returns unique values", () => {
    const ids = new Set(Array.from({ length: 100 }, () => newId()));
    assert.equal(ids.size, 100);
  });

  it("writeRecord creates a JSON file", () => {
    const root = initStore(["items"]);
    writeRecord(root, "items", { id: "abc", name: "test" });
    assert.ok(existsSync(join(root, "items", "abc.json")));
  });

  it("readOne returns written record", () => {
    const root = initStore(["items"]);
    writeRecord(root, "items", { id: "abc", name: "test" });
    const rec = readOne<any>(root, "items", "abc");
    assert.equal(rec?.name, "test");
  });

  it("readOne returns null for non-existent", () => {
    const root = initStore(["items"]);
    assert.equal(readOne(root, "items", "nope"), null);
  });

  it("readAll returns all records", () => {
    const root = initStore(["items"]);
    writeRecord(root, "items", { id: "a", v: 1 });
    writeRecord(root, "items", { id: "b", v: 2 });
    const all = readAll<any>(root, "items");
    assert.equal(all.length, 2);
  });

  it("readAll returns empty for non-existent collection", () => {
    const root = initStore(["items"]);
    assert.deepEqual(readAll(root, "nope"), []);
  });

  it("deleteRecord removes and returns true", () => {
    const root = initStore(["items"]);
    writeRecord(root, "items", { id: "abc", v: 1 });
    assert.equal(deleteRecord(root, "items", "abc"), true);
    assert.equal(readOne(root, "items", "abc"), null);
  });

  it("deleteRecord returns false for non-existent", () => {
    const root = initStore(["items"]);
    assert.equal(deleteRecord(root, "items", "nope"), false);
  });

  it("writeRecord overwrites existing", () => {
    const root = initStore(["items"]);
    writeRecord(root, "items", { id: "abc", v: 1 });
    writeRecord(root, "items", { id: "abc", v: 2 });
    const rec = readOne<any>(root, "items", "abc");
    assert.equal(rec?.v, 2);
  });
});
