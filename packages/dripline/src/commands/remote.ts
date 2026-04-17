/**
 * `dripline remote set` / `dripline remote show` — manage the warehouse
 * target in `.dripline/config.json`.
 *
 * The stored shape is `RemoteConfig` from `src/config/types.ts`. We
 * keep the command surface small: set (write/overwrite) and show
 * (print, redacting inline secrets). No edit, no remove — delete the
 * block by hand if you really need to.
 */

import chalk from "chalk";
import { loadConfig, saveConfig } from "../config/loader.js";
import type { RemoteConfig } from "../config/types.js";
import { bold, dim, success, warn } from "../utils/output.js";

/**
 * Thrown for any user-facing config error. `main.ts` catches this and
 * converts it to `process.exit(1)` with a clean message; tests catch
 * it as an ordinary exception. Same pattern as `LaneConfigError`.
 */
export class RemoteConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RemoteConfigError";
  }
}

export interface RemoteSetOptions {
  bucket: string;
  prefix?: string;
  secretType?: "R2" | "S3";
  region?: string;
  accessKey?: string;
  secretKey?: string;
  accessKeyEnv?: string;
  secretKeyEnv?: string;
  json?: boolean;
}

export async function remoteSet(
  endpoint: string,
  options: RemoteSetOptions,
): Promise<void> {
  if (!endpoint) {
    throw new RemoteConfigError("endpoint is required");
  }
  if (!options.bucket) {
    throw new RemoteConfigError("--bucket is required");
  }

  // Enforce exactly one credential path per field. Mixing inline and
  // env-var for the same field is almost always a misconfiguration.
  if (options.accessKey && options.accessKeyEnv) {
    throw new RemoteConfigError(
      "pass either --access-key or --access-key-env, not both",
    );
  }
  if (options.secretKey && options.secretKeyEnv) {
    throw new RemoteConfigError(
      "pass either --secret-key or --secret-key-env, not both",
    );
  }
  const hasAccess = Boolean(options.accessKey || options.accessKeyEnv);
  const hasSecret = Boolean(options.secretKey || options.secretKeyEnv);
  if (!hasAccess || !hasSecret) {
    throw new RemoteConfigError(
      "credentials required: pass --access-key/--secret-key or --access-key-env/--secret-key-env",
    );
  }

  const remote: RemoteConfig = {
    endpoint,
    bucket: options.bucket,
    ...(options.prefix ? { prefix: options.prefix } : {}),
    ...(options.region ? { region: options.region } : {}),
    ...(options.secretType ? { secretType: options.secretType } : {}),
    ...(options.accessKey ? { accessKeyId: options.accessKey } : {}),
    ...(options.secretKey ? { secretAccessKey: options.secretKey } : {}),
    ...(options.accessKeyEnv ? { accessKeyEnv: options.accessKeyEnv } : {}),
    ...(options.secretKeyEnv ? { secretKeyEnv: options.secretKeyEnv } : {}),
  };

  const config = loadConfig();
  config.remote = remote;
  saveConfig(config);

  if (options.accessKey || options.secretKey) {
    warn("inline credentials are stored in plaintext in .dripline/config.json");
    warn("prefer --access-key-env / --secret-key-env to reference env vars");
  }

  if (options.json) {
    console.log(JSON.stringify({ success: true, remote }));
  } else {
    success(`Remote set: ${bold(options.bucket)} @ ${endpoint}`);
  }
}

export async function remoteShow(options: {
  json?: boolean;
}): Promise<void> {
  const config = loadConfig();
  if (!config.remote) {
    if (options.json) {
      console.log(JSON.stringify({ remote: null }));
      return;
    }
    console.log("No remote configured.");
    console.log(
      dim(
        "  Set one: dripline remote set <endpoint> --bucket <name> --access-key-env <VAR> --secret-key-env <VAR>",
      ),
    );
    return;
  }

  // Redact any inline secrets before printing. Env-var references are
  // safe to show verbatim — they're just names.
  const redacted: RemoteConfig = {
    ...config.remote,
    ...(config.remote.accessKeyId ? { accessKeyId: "***" } : {}),
    ...(config.remote.secretAccessKey ? { secretAccessKey: "***" } : {}),
  };

  if (options.json) {
    console.log(JSON.stringify({ remote: redacted }, null, 2));
    return;
  }

  console.log();
  console.log(`  ${chalk.cyan("endpoint")}  ${redacted.endpoint}`);
  console.log(`  ${chalk.cyan("bucket")}    ${redacted.bucket}`);
  if (redacted.prefix)
    console.log(`  ${chalk.cyan("prefix")}    ${redacted.prefix}`);
  if (redacted.region)
    console.log(`  ${chalk.cyan("region")}    ${redacted.region}`);
  if (redacted.secretType)
    console.log(`  ${chalk.cyan("type")}      ${redacted.secretType}`);
  if (redacted.accessKeyEnv)
    console.log(
      `  ${chalk.cyan("key")}       ${dim(`env:${redacted.accessKeyEnv}`)}`,
    );
  if (redacted.accessKeyId)
    console.log(`  ${chalk.cyan("key")}       ${dim(redacted.accessKeyId)}`);
  if (redacted.secretKeyEnv)
    console.log(
      `  ${chalk.cyan("secret")}    ${dim(`env:${redacted.secretKeyEnv}`)}`,
    );
  if (redacted.secretAccessKey)
    console.log(
      `  ${chalk.cyan("secret")}    ${dim(redacted.secretAccessKey)}`,
    );
  console.log();
}
