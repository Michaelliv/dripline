/**
 * RemoteFS — low-level S3 object operations.
 *
 * Pulled out of `remote.ts` so the warehouse class can focus on the
 * dripline-shaped primitives (attach, hydrate, publish, compact,
 * manifest) without the bucket-listing / bulk-delete plumbing
 * cluttering it. Pure S3, no DuckDB, no engine.
 *
 * Every method here is a thin wrapper around `aws4fetch` + the S3
 * REST API. The XML parsing is the same regex-with-entity-decode
 * approach used elsewhere in dripline — small, dep-free, sufficient
 * for the keys we actually emit and consume.
 */

import { createHash } from "node:crypto";
import type { AwsClient } from "aws4fetch";

export interface RemoteFSConfig {
  aws: AwsClient;
  endpoint: string;
  bucket: string;
  /** Path prefix inside the bucket. Already stripped of leading/trailing slashes. */
  prefix: string;
}

export class RemoteFS {
  private readonly aws: AwsClient;
  private readonly endpoint: string;
  private readonly bucket: string;
  private readonly prefix: string;

  constructor(cfg: RemoteFSConfig) {
    this.aws = cfg.aws;
    this.endpoint = cfg.endpoint;
    this.bucket = cfg.bucket;
    this.prefix = cfg.prefix;
  }

  /** HTTPS URL for a key inside the configured prefix. */
  http(key: string): string {
    const k = this.prefix ? `${this.prefix}/${key}` : key;
    return `${this.endpoint}/${this.bucket}/${k.replace(/^\//, "")}`;
  }

  /** Count objects under a prefix. Used to decide if a parquet glob
   *  is safe to hand to DuckDB (which errors on empty globs). */
  async countObjects(prefix: string): Promise<number> {
    const fullPrefix = this.prefix ? `${this.prefix}/${prefix}` : prefix;
    const url = `${this.endpoint}/${this.bucket}/?list-type=2&max-keys=1&prefix=${encodeURIComponent(fullPrefix)}`;
    const res = await this.aws.fetch(url);
    if (res.status !== 200) {
      throw new Error(`list failed: ${res.status} ${await res.text()}`);
    }
    const xml = await res.text();
    const match = xml.match(/<KeyCount>(\d+)<\/KeyCount>/);
    return match ? Number(match[1]) : 0;
  }

  /**
   * List every object key under a prefix, following continuation
   * tokens. Returns keys relative to the bucket (the full key as
   * stored in S3), not relative to `this.prefix`.
   */
  async listObjects(prefix: string): Promise<string[]> {
    const fullPrefix = this.prefix ? `${this.prefix}/${prefix}` : prefix;
    const keys: string[] = [];
    let continuationToken: string | undefined;
    do {
      const params = new URLSearchParams({
        "list-type": "2",
        prefix: fullPrefix,
      });
      if (continuationToken)
        params.set("continuation-token", continuationToken);
      const url = `${this.endpoint}/${this.bucket}/?${params.toString()}`;
      const res = await this.aws.fetch(url);
      if (res.status !== 200) {
        throw new Error(`list failed: ${res.status} ${await res.text()}`);
      }
      const xml = await res.text();
      for (const m of xml.matchAll(/<Key>([^<]*)<\/Key>/g))
        keys.push(unescapeXml(m[1]));
      const truncated = /<IsTruncated>true<\/IsTruncated>/.test(xml);
      if (truncated) {
        const tokenMatch = xml.match(
          /<NextContinuationToken>([^<]+)<\/NextContinuationToken>/,
        );
        continuationToken = tokenMatch ? tokenMatch[1] : undefined;
      } else {
        continuationToken = undefined;
      }
    } while (continuationToken);
    return keys;
  }

  /**
   * Delete a batch of object keys. Uses the S3 DeleteObjects API
   * (1 request per 1000 keys) so cleanup is O(ceil(N / 1000))
   * requests instead of O(N).
   */
  async deleteObjects(keys: string[]): Promise<void> {
    const CHUNK = 1000;
    for (let i = 0; i < keys.length; i += CHUNK) {
      const chunk = keys.slice(i, i + CHUNK);
      const xml =
        `<?xml version="1.0" encoding="UTF-8"?>` +
        `<Delete>${chunk
          .map((k) => `<Object><Key>${escapeXml(k)}</Key></Object>`)
          .join("")}<Quiet>true</Quiet></Delete>`;
      // Content-MD5 is REQUIRED by MinIO for multi-object delete and
      // is part of the original S3 spec; we send it on every request.
      const md5 = createHash("md5").update(xml).digest("base64");
      const url = `${this.endpoint}/${this.bucket}/?delete`;
      const res = await this.aws.fetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/xml",
          "content-md5": md5,
        },
        body: xml,
      });
      if (res.status !== 200 && res.status !== 204) {
        throw new Error(
          `bulk delete failed: ${res.status} ${await res.text()}`,
        );
      }
    }
  }
}

/** Inverse of escapeXml — decode the five predefined XML entities that
 *  S3 uses to encode object keys in ListObjectsV2 responses. Numeric
 *  character references (&#NN;) are also handled since some S3 stores
 *  emit them for non-ASCII bytes. */
function unescapeXml(s: string): string {
  return s
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Number(n)))
    .replace(/&#x([0-9a-fA-F]+);/g, (_, n) =>
      String.fromCodePoint(Number.parseInt(n, 16)),
    )
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&");
}

/** Minimal XML entity escape for <Key> values in DeleteObjects bodies. */
function escapeXml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}
