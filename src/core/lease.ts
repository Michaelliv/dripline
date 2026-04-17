/**
 * R2-native lease primitive.
 *
 * A lease is a single object at `_leases/<name>.json` containing:
 *   { holder, expires_at }
 *
 * Acquisition uses S3 conditional writes:
 *   - If no object exists → PUT with `If-None-Match: *`
 *   - If an object exists but is expired → PUT with `If-Match: <etag>`
 *
 * Both R2 and MinIO implement the conditional-write semantics correctly,
 * so two concurrent acquirers cannot both succeed: S3 enforces
 * compare-and-swap on the key. The one wart to know about is weak
 * etags — R2 returns `W/"..."` on GET a second or so after the
 * write, and rejects that same value in `If-Match` per RFC 7232.
 * `stripWeakPrefix` handles it at every site that sends `If-Match`.
 *
 * The bucket is the coordinator. No locks service, no leader election.
 */

import { AwsClient } from "aws4fetch";
import type { ResolvedRemote } from "./remote.js";

export interface LeaseConfig {
  /** Endpoint URL, e.g. "https://<account>.r2.cloudflarestorage.com" or "http://localhost:9100". */
  endpoint: string;
  /** Bucket name. */
  bucket: string;
  /** Path prefix inside the bucket. Default: "". */
  prefix?: string;
  accessKeyId: string;
  secretAccessKey: string;
  /** Region — R2 ignores it but signing requires a value. Default: "auto". */
  region?: string;
}

export interface Lease {
  /** Lease name (e.g. lane name). */
  name: string;
  /** Opaque holder id. Each acquire() generates a fresh one. */
  holder: string;
  /** Wall-clock expiry, ms since epoch. */
  expiresAt: number;
  /** Etag of the lease object — required for release/renew. */
  etag: string;
}

interface LeaseDoc {
  holder: string;
  expires_at: number;
}

export class LeaseStore {
  private readonly aws: AwsClient;
  private readonly base: string;

  /**
   * Build a LeaseStore from an already-resolved remote config. Keeps
   * `run.ts` and `compact.ts` from each repeating the same 7-field
   * boilerplate — the lease store and the warehouse always point at
   * the same bucket.
   */
  static fromRemote(r: ResolvedRemote): LeaseStore {
    return new LeaseStore({
      endpoint: r.endpoint,
      bucket: r.bucket,
      prefix: r.prefix,
      accessKeyId: r.accessKeyId,
      secretAccessKey: r.secretAccessKey,
      region: r.region,
    });
  }

  constructor(cfg: LeaseConfig) {
    this.aws = new AwsClient({
      accessKeyId: cfg.accessKeyId,
      secretAccessKey: cfg.secretAccessKey,
      service: "s3",
      region: cfg.region ?? "auto",
    });
    const prefix = (cfg.prefix ?? "").replace(/^\/|\/$/g, "");
    this.base = `${cfg.endpoint.replace(/\/$/, "")}/${cfg.bucket}${prefix ? `/${prefix}` : ""}`;
  }

  /**
   * Wrap a fetch() in a single retry for transient network errors.
   *
   * HTTP status codes (404, 412, etc) are NOT retried — they're
   * meaningful responses the caller interprets. We only retry when
   * the request itself fails to produce any response (connection
   * dropped, DNS blip, server-side close). Real production workers
   * need this tolerance; tests that hammer a local MinIO also
   * benefit from it.
   */
  private async fetchWithRetry(
    url: string,
    init?: Parameters<AwsClient["fetch"]>[1],
  ): Promise<Response> {
    try {
      return await this.aws.fetch(url, init);
    } catch (e) {
      // Small sleep so we don't retry into the same overwhelmed socket.
      await new Promise((r) => setTimeout(r, 25));
      try {
        return await this.aws.fetch(url, init);
      } catch {
        throw e; // surface the ORIGINAL error — most diagnostic.
      }
    }
  }

  private url(name: string): string {
    return `${this.base}/_leases/${encodeURIComponent(name)}.json`;
  }

  /**
   * Normalize an etag for use in `If-Match`.
   *
   * Background — the bug this fixes:
   *   R2 occasionally returns a **weak** etag on GET (prefix `W/"..."`)
   *   for objects that were written as strong etags. The prefix shows
   *   up ~1s after the original write, apparently once R2 finishes
   *   computing its internal integrity metadata. When the client then
   *   passes that weak etag back as `If-Match`, R2 rejects it with 412
   *   per RFC 7232 (If-Match requires strong comparison; weak etags
   *   fail strong comparison against anything).
   *
   *   Net effect on LeaseStore: every expired lease became
   *   permanently unreacquirable. `acquire()` would return null on
   *   an expired doc because the conditional PUT got 412 — even
   *   though the doc was objectively stale. The only escape was
   *   deleting the object outright (`dripline lane reset`).
   *
   *   Reproduces reliably against R2 after ~500ms between PUT and
   *   the subsequent GET. MinIO never weakens etags so the suite
   *   was green; production was broken for hours.
   *
   * Why stripping the `W/` is safe here:
   *   We only use `If-Match` for **optimistic concurrency** on a tiny
   *   JSON blob we wrote ourselves. If nobody else wrote to the key
   *   since our GET, a strong-compare on the stripped etag succeeds
   *   and we win the CAS. If someone else wrote, their strong etag
   *   differs from ours (different bytes → different hash) and the
   *   412 is correct. Weak-vs-strong has no semantic meaning for us.
   */
  private stripWeakPrefix(etag: string): string {
    return etag.startsWith("W/") ? etag.slice(2) : etag;
  }

  /**
   * Read the current lease doc without trying to acquire. Returns
   * null if the key doesn't exist. Used by read-only inspection
   * (`dripline lane status`) — never mutates remote state.
   */
  async inspect(
    name: string,
  ): Promise<{ holder: string; expiresAt: number; etag: string } | null> {
    const res = await this.fetchWithRetry(this.url(name), { method: "GET" });
    if (res.status === 404) return null;
    if (res.status !== 200) {
      throw new Error(`lease GET failed: ${res.status} ${await res.text()}`);
    }
    const doc = (await res.json()) as LeaseDoc;
    return {
      holder: doc.holder,
      expiresAt: doc.expires_at,
      etag: res.headers.get("etag") ?? "",
    };
  }

  /**
   * Try to acquire the lease for `name`. Returns a Lease on success, null
   * if someone else holds it.
   *
   * Safe under arbitrary concurrency: at most one caller across all
   * processes will receive a non-null result for the same (name, instant).
   */
  async acquire(name: string, ttlMs: number): Promise<Lease | null> {
    const url = this.url(name);
    const now = Date.now();

    // Read current state.
    const head = await this.fetchWithRetry(url, { method: "GET" });
    let currentEtag: string | null = null;
    let current: LeaseDoc | null = null;
    if (head.status === 200) {
      currentEtag = head.headers.get("etag");
      current = (await head.json()) as LeaseDoc;
    } else if (head.status !== 404) {
      throw new Error(`lease GET failed: ${head.status} ${await head.text()}`);
    }

    // Held and not expired → bail.
    if (current && current.expires_at > now) return null;

    const holder = crypto.randomUUID();
    const doc: LeaseDoc = { holder, expires_at: now + ttlMs };
    const body = JSON.stringify(doc);

    // Conditional PUT — succeeds only if the precondition matches.
    // - No prior object: require it to still not exist.
    // - Expired prior object: require its etag is unchanged.
    //
    // The etag is stripped of any `W/` weak prefix before use — R2
    // returns weak etags on GET for older objects, but rejects them
    // in If-Match per RFC 7232. See stripWeakPrefix() for the full
    // story.
    const headers: Record<string, string> = {
      "content-type": "application/json",
    };
    if (currentEtag) {
      headers["if-match"] = this.stripWeakPrefix(currentEtag);
    } else {
      headers["if-none-match"] = "*";
    }

    const put = await this.fetchWithRetry(url, {
      method: "PUT",
      headers,
      body,
    });

    if (put.status === 200 || put.status === 201) {
      const etag = put.headers.get("etag") ?? "";
      return { name, holder, expiresAt: doc.expires_at, etag };
    }
    if (put.status === 412 || put.status === 409) {
      // Precondition failed — someone else won the race.
      return null;
    }
    throw new Error(`lease PUT failed: ${put.status} ${await put.text()}`);
  }

  /**
   * Release a lease we hold. Idempotent — if the holder no longer matches
   * (someone else took over after our expiry), the delete is skipped.
   *
   * We GET-check the holder before DELETE because S3's `If-Match` on DELETE
   * is a recent addition and not all S3-compatible stores honor it (MinIO
   * as of 2025 ignores it). The GET-then-DELETE has a tiny race window
   * where a third party could take over between our check and our delete,
   * but the worst case is we delete a lease that was held for ~1ms by
   * someone who immediately lost it — and that someone will simply fail
   * to push their cursor / publish, then retry next run. No data loss.
   */
  async release(lease: Lease): Promise<void> {
    const url = this.url(lease.name);
    const head = await this.fetchWithRetry(url, { method: "GET" });
    if (head.status === 404) return;
    if (head.status !== 200) {
      throw new Error(`lease GET (release) failed: ${head.status}`);
    }
    const current = (await head.json()) as LeaseDoc;
    if (current.holder !== lease.holder) return; // someone else owns it now
    const currentEtag = head.headers.get("etag") ?? lease.etag;
    const res = await this.fetchWithRetry(url, {
      method: "DELETE",
      headers: { "if-match": this.stripWeakPrefix(currentEtag) },
    });
    if (
      res.status !== 204 &&
      res.status !== 200 &&
      res.status !== 412 &&
      res.status !== 404
    ) {
      throw new Error(`lease DELETE failed: ${res.status} ${await res.text()}`);
    }
  }

  /**
   * Renew a lease we hold, extending the expiry. Returns a new Lease
   * with an updated etag. Returns null if we lost the lease (etag
   * mismatch — someone else took over).
   */
  async renew(lease: Lease, ttlMs: number): Promise<Lease | null> {
    const url = this.url(lease.name);
    const doc: LeaseDoc = {
      holder: lease.holder,
      expires_at: Date.now() + ttlMs,
    };
    const res = await this.fetchWithRetry(url, {
      method: "PUT",
      headers: {
        "content-type": "application/json",
        "if-match": this.stripWeakPrefix(lease.etag),
      },
      body: JSON.stringify(doc),
    });
    if (res.status === 200 || res.status === 201) {
      return {
        ...lease,
        expiresAt: doc.expires_at,
        etag: res.headers.get("etag") ?? "",
      };
    }
    if (res.status === 412) return null;
    throw new Error(`lease renew failed: ${res.status} ${await res.text()}`);
  }
}
