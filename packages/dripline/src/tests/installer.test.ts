import { strict as assert } from "node:assert";
import { describe, it } from "node:test";
import { parsePluginSource } from "../plugin/installer.js";

describe("parsePluginSource", () => {
  it("npm with version", () => {
    const r = parsePluginSource("npm:@dripline/github@1.0.0");
    assert.equal(r.type, "npm");
    assert.equal(r.name, "@dripline/github");
    assert.equal(r.ref, "1.0.0");
  });

  it("npm without version", () => {
    const r = parsePluginSource("npm:dripline-plugin-aws");
    assert.equal(r.type, "npm");
    assert.equal(r.name, "dripline-plugin-aws");
    assert.equal(r.ref, undefined);
  });

  it("npm scoped without version", () => {
    const r = parsePluginSource("npm:@scope/pkg");
    assert.equal(r.type, "npm");
    assert.equal(r.name, "@scope/pkg");
    assert.equal(r.ref, undefined);
  });

  it("git shorthand", () => {
    const r = parsePluginSource("git:github.com/user/repo@v1");
    assert.equal(r.type, "git");
    assert.equal(r.name, "repo");
    assert.equal(r.url, "https://github.com/user/repo.git");
    assert.equal(r.ref, "v1");
  });

  it("git shorthand without ref", () => {
    const r = parsePluginSource("git:github.com/user/repo");
    assert.equal(r.type, "git");
    assert.equal(r.ref, undefined);
  });

  it("git https URL", () => {
    const r = parsePluginSource("https://github.com/user/repo");
    assert.equal(r.type, "git");
    assert.equal(r.url, "https://github.com/user/repo.git");
  });

  it("git https URL with ref", () => {
    const r = parsePluginSource("https://github.com/user/repo@v2");
    assert.equal(r.type, "git");
    assert.equal(r.ref, "v2");
  });

  it("local relative path", () => {
    const r = parsePluginSource("./test.ts");
    assert.equal(r.type, "local");
    assert.ok(r.path?.endsWith("/test.ts"));
    assert.ok(r.path?.startsWith("/"));
    assert.equal(r.name, "test");
  });

  it("local absolute path", () => {
    const r = parsePluginSource("/tmp/my-plugin.ts");
    assert.equal(r.type, "local");
    assert.equal(r.path, "/tmp/my-plugin.ts");
    assert.equal(r.name, "my-plugin");
  });

  it("local directory path", () => {
    const r = parsePluginSource("/tmp/my-plugin");
    assert.equal(r.type, "local");
    assert.equal(r.name, "my-plugin");
  });

  it("git with subpath", () => {
    const r = parsePluginSource("git:github.com/user/dripline#plugins/docker");
    assert.equal(r.type, "git");
    assert.equal(r.name, "docker");
    assert.equal(r.url, "https://github.com/user/dripline.git");
    assert.equal(r.subpath, "plugins/docker");
    assert.equal(r.ref, undefined);
  });

  it("git with subpath and ref", () => {
    const r = parsePluginSource(
      "git:github.com/user/dripline@main#plugins/github",
    );
    assert.equal(r.type, "git");
    assert.equal(r.name, "github");
    assert.equal(r.ref, "main");
    assert.equal(r.subpath, "plugins/github");
  });

  it("https URL with subpath", () => {
    const r = parsePluginSource("https://github.com/user/repo#plugins/brew");
    assert.equal(r.type, "git");
    assert.equal(r.name, "brew");
    assert.equal(r.subpath, "plugins/brew");
  });
});
