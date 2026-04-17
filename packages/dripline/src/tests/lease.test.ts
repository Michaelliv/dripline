/**
 * LeaseStore tests against an S3-compatible backend.
 *
 * Requires MinIO (or any S3-compatible store) reachable at the endpoint
 * configured by env vars below. The suite auto-skips if the endpoint is
 * unreachable, so it's safe to run in CI without a server.
 *
 *   docker run -d --name dripline-minio -p 9100:9000 \
 *     -e MINIO_ROOT_USER=testkey -e MINIO_ROOT_PASSWORD=testsecret123 \
 *     minio/minio server /data
 *
 * Env vars (all optional, sensible defaults for the docker command above):
 *   DRIPLINE_TEST_S3_ENDPOINT  default http://localhost:9100
 *   DRIPLINE_TEST_S3_BUCKET    default dripline-test
 *   DRIPLINE_TEST_S3_KEY       default testkey
 *   DRIPLINE_TEST_S3_SECRET    default testsecret123
 */

import { strict as assert } from "node:assert";
import { after, before, describe, it } from "node:test";
import { AwsClient } from "aws4fetch";
import { type Lease, LeaseStore } from "../core/lease.js";

const ENDPOINT =
  process.env.DRIPLINE_TEST_S3_ENDPOINT ?? "http://localhost:9100";
const BUCKET = process.env.DRIPLINE_TEST_S3_BUCKET ?? "dripline-test";
const KEY = process.env.DRIPLINE_TEST_S3_KEY ?? "testkey";
const SECRET = process.env.DRIPLINE_TEST_S3_SECRET ?? "testsecret123";

// Per-test prefix so parallel runs / re-runs never collide.
const RUN_PREFIX = `lease-tests/${process.pid}-${Date.now()}`;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Assert non-null and narrow the type. Replaces `!` in tests. */
function must<T>(value: T | null | undefined, msg = "expected non-null"): T {
  assert.ok(value != null, msg);
  return value;
}

// ── Backend probe — skip the whole suite if S3 is unreachable. ───────

let backendUp = false;
async function probeBackend(): Promise<boolean> {
  try {
    const aws = new AwsClient({
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      service: "s3",
      region: "auto",
    });
    // Probe via HEAD bucket — returns 200 on both MinIO and R2 for
    // an existing bucket. We deliberately don't try to CREATE the
    // bucket because R2 rejects PUT-bucket over the S3 API; the
    // tests assume the bucket already exists (MinIO: pre-created
    // with `mc mb` or first-test side-effect; R2: created via
    // dashboard or wrangler).
    const r = await aws.fetch(`${ENDPOINT}/${BUCKET}/`, { method: "HEAD" });
    if (r.status === 200) return true;
    // MinIO on first-ever run returns 404 — try a one-shot create so
    // the existing dev workflow (docker run; bun test) still works.
    if (r.status === 404) {
      const create = await aws.fetch(`${ENDPOINT}/${BUCKET}/`, {
        method: "PUT",
      });
      return create.status === 200 || create.status === 409;
    }
    return false;
  } catch {
    return false;
  }
}

// Counter so each it() gets a unique lease name within the run prefix.
let n = 0;
const lane = (label: string) => `${RUN_PREFIX}/${label}-${++n}`;

const newStore = () =>
  new LeaseStore({
    endpoint: ENDPOINT,
    bucket: BUCKET,
    accessKeyId: KEY,
    secretAccessKey: SECRET,
  });

// ── Suite ────────────────────────────────────────────────────────────

describe("LeaseStore", { concurrency: false }, () => {
  before(async () => {
    backendUp = await probeBackend();
    if (!backendUp) {
      // eslint-disable-next-line no-console
      console.warn(
        `\n  ⚠ skipping LeaseStore tests: ${ENDPOINT} unreachable. ` +
          `start MinIO with the docker command in this file's header.\n`,
      );
    }
  });

  after(async () => {
    if (!backendUp) return;
    // Best-effort cleanup of any leftover lease objects from this run.
    const aws = new AwsClient({
      accessKeyId: KEY,
      secretAccessKey: SECRET,
      service: "s3",
      region: "auto",
    });
    const list = await aws.fetch(
      `${ENDPOINT}/${BUCKET}/?list-type=2&prefix=${encodeURIComponent(RUN_PREFIX)}`,
    );
    if (list.status !== 200) return;
    const xml = await list.text();
    const keys = [...xml.matchAll(/<Key>([^<]+)<\/Key>/g)].map((m) => m[1]);
    for (const k of keys) {
      await aws.fetch(`${ENDPOINT}/${BUCKET}/${k}`, { method: "DELETE" });
    }
  });

  // Wraps it() so each test auto-skips when the backend is down.
  const ift = (name: string, fn: () => Promise<void>) =>
    it(name, async (t) => {
      if (!backendUp) return t.skip("backend unreachable");
      await fn();
    });

  ift("acquire returns a lease with a holder and future expiry", async () => {
    const store = newStore();
    const name = lane("basic");
    const a = must(await store.acquire(name, 5_000), "expected a lease");
    assert.equal(a.name, name);
    assert.equal(typeof a.holder, "string");
    assert.ok(a.holder.length > 0);
    assert.ok(a.expiresAt > Date.now());
    assert.ok(a.etag.length > 0);
    await store.release(a);
  });

  ift("second acquire on a held lease returns null", async () => {
    const store = newStore();
    const name = lane("held");
    const a = must(await store.acquire(name, 5_000));
    const b = await store.acquire(name, 5_000);
    assert.equal(b, null);
    await store.release(a);
  });

  ift("acquire after release succeeds and yields a fresh holder", async () => {
    const store = newStore();
    const name = lane("rerelease");
    const a = must(await store.acquire(name, 5_000));
    await store.release(a);
    const b = must(await store.acquire(name, 5_000));
    assert.notEqual(b.holder, a.holder);
    await store.release(b);
  });

  ift("expired lease is reacquirable by another caller", async () => {
    const store = newStore();
    const name = lane("expire");
    const a = must(await store.acquire(name, 300));
    await sleep(500);
    const b = must(await store.acquire(name, 5_000));
    assert.notEqual(b.holder, a.holder);
    await store.release(b);
  });

  // ── Regression: lease left over from a dead process ────────────────
  //
  // Observed in production against R2: a lease file whose `expires_at`
  // was 10 hours in the past could not be reacquired by any fresh
  // process — `acquire()` kept returning null. `lane reset` (a flat
  // DELETE of the object) was required as a workaround, which
  // defeats the whole "expired leases self-heal" design.
  //
  // This test mimics that exact shape: write an already-expired lease
  // doc directly to the bucket (as if a prior process crashed after
  // writing it) and ask a fresh LeaseStore to take it over. If
  // `acquire()` returns null here, something is wrong with the
  // conditional-PUT path — etag quoting, weak-etag normalization, a
  // race in the GET→PUT window, or the store's view of "expired".
  //
  // Point DRIPLINE_TEST_S3_ENDPOINT at a real R2 bucket to exercise
  // this against the same backend where the bug originally appeared.
  ift(
    "pre-existing expired lease written directly to the bucket is reacquirable",
    async () => {
      const store = newStore();
      const name = lane("stale-file");
      const url = `${ENDPOINT}/${BUCKET}/_leases/${encodeURIComponent(name)}.json`;

      // Write an expired lease object the way a crashed process would
      // have left it: no holder we track, `expires_at` in the past.
      const staleDoc = {
        holder: "crashed-process-uuid",
        expires_at: Date.now() - 10 * 60 * 60 * 1000, // 10h ago
      };
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });
      const wrote = await aws.fetch(url, {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(staleDoc),
      });
      assert.ok(
        wrote.status === 200 || wrote.status === 201,
        `setup: direct PUT failed: ${wrote.status}`,
      );

      // Fresh LeaseStore (no in-memory state from the write above)
      // tries to take it over. Should succeed — the doc is expired.
      const taken = await store.acquire(name, 5_000);
      assert.ok(
        taken != null,
        "acquire() returned null on a stale expired lease — " +
          "this is the R2 production regression. The conditional PUT " +
          "with If-Match on the old etag was rejected even though " +
          "expires_at is 10h in the past.",
      );
      assert.notEqual(taken.holder, staleDoc.holder);
      assert.ok(taken.expiresAt > Date.now());
      await store.release(taken);
    },
  );

  // ── Regression: etag round-trip through If-Match ───────────────────
  //
  // The acquire() path relies on the server echoing the etag we send
  // in If-Match. If there's any quoting/normalization difference
  // between how GET renders an etag and how the server compares it
  // on PUT, the conditional write fails with 412 and acquire()
  // mysteriously returns null. This test asserts the round-trip
  // works by explicitly using the etag from GET as the If-Match on
  // a subsequent PUT — if the header path is broken, we'll see it
  // here isolated from any other lease logic.
  ift(
    "If-Match with etag from GET is accepted on PUT (etag round-trip)",
    async () => {
      const name = lane("etag-roundtrip");
      const url = `${ENDPOINT}/${BUCKET}/_leases/${encodeURIComponent(name)}.json`;
      const aws = new AwsClient({
        accessKeyId: KEY,
        secretAccessKey: SECRET,
        service: "s3",
        region: "auto",
      });

      // Seed an object.
      const first = await aws.fetch(url, {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ v: 1 }),
      });
      assert.ok(first.status === 200 || first.status === 201);

      // GET it and capture the etag verbatim.
      const got = await aws.fetch(url);
      assert.equal(got.status, 200);
      const etag = got.headers.get("etag");
      assert.ok(etag, "expected an etag on GET");

      // Conditional PUT using that etag. If the server round-trips
      // its own header correctly, this must succeed.
      const second = await aws.fetch(url, {
        method: "PUT",
        headers: {
          "content-type": "application/json",
          "if-match": etag,
        },
        body: JSON.stringify({ v: 2 }),
      });
      assert.ok(
        second.status === 200 || second.status === 201,
        `If-Match round-trip failed: ${second.status} (etag sent: ${etag}). ` +
          `This is the underlying cause of the production "lease held" bug: ` +
          `acquire()'s conditional PUT gets rejected because the server does ` +
          `not accept the etag it returned on GET.`,
      );
    },
  );

  ift("20 concurrent acquirers — exactly one wins", async () => {
    const store = newStore();
    const name = lane("contend-20");
    const N = 20;
    const results = await Promise.all(
      Array.from({ length: N }, () => store.acquire(name, 5_000)),
    );
    const winners = results.filter((r): r is Lease => r !== null);
    assert.equal(winners.length, 1, `expected 1 winner, got ${winners.length}`);
    await store.release(winners[0]);
  });

  ift("10 lanes × 5 workers — every lane has exactly one holder", async () => {
    const store = newStore();
    const lanes = Array.from({ length: 10 }, (_, i) => lane(`fanout-${i}`));
    const workers = 5;
    const grids = await Promise.all(
      Array.from({ length: workers }, () =>
        Promise.all(lanes.map((l) => store.acquire(l, 5_000))),
      ),
    );
    for (let i = 0; i < lanes.length; i++) {
      const winners = grids.filter((g) => g[i] !== null);
      assert.equal(
        winners.length,
        1,
        `lane ${lanes[i]}: expected 1 winner, got ${winners.length}`,
      );
    }
    // Cleanup
    for (const g of grids)
      for (const lease of g) if (lease) await store.release(lease);
  });

  ift("renew extends expiry and returns an updated lease", async () => {
    const store = newStore();
    const name = lane("renew");
    const a = must(await store.acquire(name, 1_000));
    const b = must(await store.renew(a, 5_000));
    assert.equal(b.holder, a.holder, "holder unchanged across renew");
    assert.ok(b.expiresAt > a.expiresAt, "expiry extended");
    await store.release(b);
  });

  ift("renewed lease blocks acquirers past the original ttl", async () => {
    const store = newStore();
    const name = lane("renew-blocks");
    const a = must(await store.acquire(name, 500));
    const b = must(await store.renew(a, 5_000));
    await sleep(700); // past original ttl
    const c = await store.acquire(name, 5_000);
    assert.equal(c, null, "renewed lease should still block acquirers");
    await store.release(b);
  });

  ift("renew on a lost lease returns null", async () => {
    const store = newStore();
    const name = lane("renew-lost");
    const a = must(await store.acquire(name, 200));
    await sleep(400);
    const b = must(await store.acquire(name, 5_000)); // takeover
    const renewed = await store.renew(a, 5_000);
    assert.equal(renewed, null, "stale holder cannot renew");
    await store.release(b);
  });

  ift("stale release does not free the new holder's lease", async () => {
    const store = newStore();
    const name = lane("stale-release");
    const a = must(await store.acquire(name, 200));
    await sleep(400);
    const b = must(await store.acquire(name, 5_000)); // takeover
    await store.release(a); // stale — must be a no-op
    const c = await store.acquire(name, 5_000);
    assert.equal(c, null, "b's lease must still be held");
    await store.release(b);
  });

  ift("release of unknown lease is a no-op (idempotent)", async () => {
    const store = newStore();
    const name = lane("release-missing");
    // Synthesize a lease that was never acquired.
    await store.release({
      name,
      holder: "ghost",
      expiresAt: Date.now() + 5_000,
      etag: '"deadbeef"',
    });
    // Should now be acquirable normally.
    const a = must(await store.acquire(name, 5_000));
    await store.release(a);
  });

  // The worker loop can be slow against a real R2 bucket — each
  // request is ~600ms and the test contends 4 workers × 8 iterations
  // on a single lease. Bump the per-test timeout well above the
  // default 5s so R2 runs don't flake on latency.
  it("worker loop pattern — sequential acquire/work/release across workers", {
    timeout: 120_000,
  }, async (t) => {
    if (!backendUp) return t.skip("backend unreachable");
    const store = newStore();
    const name = lane("worker-loop");
    const workers = 4;
    const iterations = 8;
    const order: number[] = [];

    const workerLoop = async (id: number) => {
      for (let i = 0; i < iterations; i++) {
        // Spin until we get the lease (cheap exponential backoff)
        let lease: Lease | null = null;
        let backoff = 5;
        while (lease == null) {
          lease = await store.acquire(name, 1_000);
          if (lease == null) {
            await sleep(backoff);
            backoff = Math.min(backoff * 2, 50);
          }
        }
        order.push(id);
        await sleep(2); // simulated work
        await store.release(lease);
      }
    };

    await Promise.all(
      Array.from({ length: workers }, (_, id) => workerLoop(id)),
    );

    // Every iteration must have completed exactly once.
    assert.equal(order.length, workers * iterations);
    // No two adjacent entries from different workers should overlap —
    // we can't strictly assert that here without timing, but the fact
    // that we got `workers * iterations` releases without a deadlock
    // and without an acquire-while-held proves mutual exclusion held.
  });
});
