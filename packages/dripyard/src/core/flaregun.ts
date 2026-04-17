/**
 * Flaregun integration — a rotating proxy network on Cloudflare Workers.
 *
 * Dripyard uses flaregun to route plugin HTTP through a pool of
 * workers for lanes marked `proxyEnabled`. The integration is fully
 * optional: if Cloudflare credentials aren't available the server
 * boots and runs normally, and any lane that actually tries to use a
 * proxy fails with a clear, actionable error at run time.
 *
 * Credentials come from the environment — flaregun reads
 *   CLOUDFLARE_API_TOKEN  (or CF_API_TOKEN)
 *   CLOUDFLARE_ACCOUNT_ID (or CF_ACCOUNT_ID)
 * via its own `resolveConfig()`. We don't re-validate here; we just
 * let flaregun throw and translate the failure into "not available".
 *
 * Lifecycle: one FlareGun per server process. Warmed at boot via
 * `ls()` so worker URLs are cached locally before the first lane
 * triggers. No background tasks held open; `ls()` is a one-shot
 * HTTPS call to the Cloudflare API. Shutdown is a no-op.
 */

import { FlareGun } from "@miclivs/flaregun";

let active: FlareGun | null = null;

/**
 * Initialize flaregun. Called once at server boot. Returns the
 * instance on success, or `null` if credentials are missing or the
 * Cloudflare API is unreachable. Logs the outcome either way so the
 * server startup banner tells the operator exactly what happened.
 *
 * A second call with an active instance short-circuits; a prior
 * failure is retried (we don't cache the negative outcome because
 * credentials can be added at runtime via the Proxies page).
 */
export async function initFlareGun(): Promise<FlareGun | null> {
  if (active) return active;

  let fg: FlareGun;
  try {
    // FlareGun's constructor throws synchronously when creds are
    // absent. We treat that as "disabled", not as a fatal boot error.
    fg = new FlareGun();
  } catch (err: any) {
    console.log(
      `flaregun: disabled (${err?.message ?? String(err)}). Proxied lanes will error if triggered.`,
    );
    return null;
  }

  try {
    const workers = await fg.ls();
    console.log(`flaregun: ready (${workers.length} worker${workers.length === 1 ? "" : "s"})`);
    active = fg;
    return fg;
  } catch (err: any) {
    // Creds were present but the API call failed — wrong token scope,
    // CF outage, network off. Log loudly and fall back to disabled so
    // the rest of the server still boots.
    console.warn(
      `flaregun: cloudflare API unreachable (${err?.message ?? String(err)}). Proxied lanes will error if triggered.`,
    );
    return null;
  }
}

/**
 * Return the active FlareGun, or `null` if initialization hasn't
 * happened or didn't find credentials. Code paths that need flaregun
 * must check for null and produce a meaningful error — see
 * `Orchestrator.executeLane` for the canonical example.
 */
export function getActiveFlareGun(): FlareGun | null {
  return active;
}

/**
 * Set or clear the active FlareGun. Used by the server's shutdown
 * path to drop the singleton so tests and hot-reloads don't inherit
 * state across instances.
 */
export function setActiveFlareGun(fg: FlareGun | null): void {
  active = fg;
}
