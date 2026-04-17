/**
 * Unit tests for `dripline remote set / show`.
 *
 * No backend. Each test gets a fresh temp `.dripline/` dir; commands
 * read/write `config.json` directly. Error paths throw
 * RemoteConfigError and are asserted as thrown exceptions.
 */

import { strict as assert } from "node:assert";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import {
  RemoteConfigError,
  remoteSet,
  remoteShow,
} from "../commands/remote.js";

let tmpDir: string;
let prevCwd: string;
const logs: string[] = [];
const origLog = console.log;
const origErr = console.error;

function configPath(): string {
  return join(tmpDir, ".dripline", "config.json");
}

function readConfig(): Record<string, any> {
  return JSON.parse(readFileSync(configPath(), "utf-8"));
}

beforeEach(() => {
  tmpDir = mkdtempSync(join(tmpdir(), "dripline-remote-cmd-"));
  mkdirSync(join(tmpDir, ".dripline"), { recursive: true });
  writeFileSync(
    configPath(),
    JSON.stringify({ connections: [], cache: {}, rateLimits: {}, lanes: {} }),
  );
  prevCwd = process.cwd();
  process.chdir(tmpDir);
  logs.length = 0;
  console.log = (msg?: unknown) => {
    logs.push(String(msg ?? ""));
  };
  console.error = (msg?: unknown) => {
    logs.push(String(msg ?? ""));
  };
});

afterEach(() => {
  console.log = origLog;
  console.error = origErr;
  process.chdir(prevCwd);
  rmSync(tmpDir, { recursive: true, force: true });
});

describe("remote set", () => {
  it("writes a minimal env-var-backed remote", async () => {
    await remoteSet("https://example.r2.cloudflarestorage.com", {
      bucket: "wh",
      accessKeyEnv: "KEY",
      secretKeyEnv: "SECRET",
      json: true,
    });
    const cfg = readConfig();
    assert.deepEqual(cfg.remote, {
      endpoint: "https://example.r2.cloudflarestorage.com",
      bucket: "wh",
      accessKeyEnv: "KEY",
      secretKeyEnv: "SECRET",
    });
  });

  it("writes all optional fields when provided", async () => {
    await remoteSet("http://localhost:9100", {
      bucket: "b",
      prefix: "prod",
      region: "us-east-1",
      secretType: "S3",
      accessKey: "inline",
      secretKey: "secret",
      json: true,
    });
    const cfg = readConfig();
    assert.deepEqual(cfg.remote, {
      endpoint: "http://localhost:9100",
      bucket: "b",
      prefix: "prod",
      region: "us-east-1",
      secretType: "S3",
      accessKeyId: "inline",
      secretAccessKey: "secret",
    });
  });

  it("overwrites an existing remote", async () => {
    await remoteSet("http://a", {
      bucket: "one",
      accessKeyEnv: "K",
      secretKeyEnv: "S",
      json: true,
    });
    await remoteSet("http://b", {
      bucket: "two",
      accessKeyEnv: "K",
      secretKeyEnv: "S",
      json: true,
    });
    assert.equal(readConfig().remote.bucket, "two");
    assert.equal(readConfig().remote.endpoint, "http://b");
  });

  it("rejects missing endpoint", async () => {
    await assert.rejects(
      () =>
        remoteSet("", {
          bucket: "b",
          accessKeyEnv: "K",
          secretKeyEnv: "S",
        }),
      RemoteConfigError,
    );
  });

  it("rejects missing --bucket", async () => {
    await assert.rejects(
      () =>
        remoteSet("http://a", {
          bucket: "" as unknown as string,
          accessKeyEnv: "K",
          secretKeyEnv: "S",
        }),
      /bucket/,
    );
  });

  it("rejects mixing inline and env access key", async () => {
    await assert.rejects(
      () =>
        remoteSet("http://a", {
          bucket: "b",
          accessKey: "k",
          accessKeyEnv: "K",
          secretKeyEnv: "S",
        }),
      /access-key/,
    );
  });

  it("rejects mixing inline and env secret key", async () => {
    await assert.rejects(
      () =>
        remoteSet("http://a", {
          bucket: "b",
          accessKeyEnv: "K",
          secretKey: "s",
          secretKeyEnv: "S",
        }),
      /secret-key/,
    );
  });

  it("rejects missing credentials entirely", async () => {
    await assert.rejects(
      () => remoteSet("http://a", { bucket: "b" }),
      /credentials/,
    );
  });

  it("emits JSON on --json", async () => {
    await remoteSet("http://a", {
      bucket: "b",
      accessKeyEnv: "K",
      secretKeyEnv: "S",
      json: true,
    });
    const payload = JSON.parse(logs[0]);
    assert.equal(payload.success, true);
    assert.equal(payload.remote.bucket, "b");
  });
});

describe("remote show", () => {
  it("reports when no remote is configured (human)", async () => {
    await remoteShow({});
    assert.ok(logs.some((l) => /No remote configured/.test(l)));
  });

  it("reports null in JSON mode when no remote is configured", async () => {
    await remoteShow({ json: true });
    assert.deepEqual(JSON.parse(logs[0]), { remote: null });
  });

  it("redacts inline secrets in JSON mode", async () => {
    await remoteSet("http://a", {
      bucket: "b",
      accessKey: "REAL_KEY",
      secretKey: "REAL_SECRET",
      json: true,
    });
    logs.length = 0;
    await remoteShow({ json: true });
    const payload = JSON.parse(logs[0]);
    assert.equal(payload.remote.accessKeyId, "***");
    assert.equal(payload.remote.secretAccessKey, "***");
    assert.equal(payload.remote.bucket, "b");
  });

  it("shows env-var references verbatim in JSON mode", async () => {
    await remoteSet("http://a", {
      bucket: "b",
      accessKeyEnv: "MY_KEY",
      secretKeyEnv: "MY_SECRET",
      json: true,
    });
    logs.length = 0;
    await remoteShow({ json: true });
    const payload = JSON.parse(logs[0]);
    assert.equal(payload.remote.accessKeyEnv, "MY_KEY");
    assert.equal(payload.remote.secretKeyEnv, "MY_SECRET");
    assert.equal(payload.remote.accessKeyId, undefined);
  });

  it("prints a human-readable block", async () => {
    await remoteSet("http://a", {
      bucket: "b",
      prefix: "p",
      accessKeyEnv: "K",
      secretKeyEnv: "S",
      json: true,
    });
    logs.length = 0;
    await remoteShow({});
    const out = logs.join("\n");
    assert.match(out, /endpoint/);
    assert.match(out, /http:\/\/a/);
    assert.match(out, /bucket/);
    assert.match(out, /env:K/);
  });
});
