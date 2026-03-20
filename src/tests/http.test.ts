import { describe, it } from "node:test";
import { strict as assert } from "node:assert";
import { syncGet, syncGetPaginated } from "../plugins/utils/http.js";

describe("syncGet", () => {
  it("returns status, body, headers for 200", () => {
    const res = syncGet("https://httpbin.org/get");
    assert.equal(res.status, 200);
    assert.ok(res.body);
    assert.ok(res.headers);
    assert.equal(typeof res.body, "object");
  });

  it("parses JSON body", () => {
    const res = syncGet("https://httpbin.org/get");
    assert.ok(res.body.url);
    assert.equal(res.body.url, "https://httpbin.org/get");
  });

  it("sends custom headers", () => {
    const res = syncGet("https://httpbin.org/headers", {
      "X-Custom": "test-value",
    });
    assert.equal(res.status, 200);
    assert.equal(res.body.headers["X-Custom"], "test-value");
  });

  it("handles 404", () => {
    const res = syncGet("https://httpbin.org/status/404");
    assert.equal(res.status, 404);
  });

  it("handles non-JSON response", () => {
    const res = syncGet("https://httpbin.org/html");
    assert.equal(res.status, 200);
    assert.equal(typeof res.body, "string");
  });
});

describe("syncGetPaginated", () => {
  it("single page (no Link header) returns data", () => {
    const data = syncGetPaginated("https://httpbin.org/get");
    assert.ok(data.length >= 1);
    assert.ok(data[0].url);
  });

  it("throws on HTTP error", () => {
    assert.throws(
      () => syncGetPaginated("https://httpbin.org/status/500"),
      /500/,
    );
  });
});
