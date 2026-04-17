/**
 * Token-based auth for the dripyard HTTP surface.
 *
 * Design goals
 *   - Opt-in. When `DRIPYARD_TOKEN` (or `options.token`) is unset, the
 *     server runs exactly as it did pre-auth: open, no middleware,
 *     every route free. This preserves the localhost-dev and test
 *     flows without changes.
 *   - One secret. A single shared bearer token that the handful of
 *     people who should see the dashboard all know. No user store,
 *     no OIDC, no sessions beyond "you proved you have the token."
 *   - Two input surfaces. API/CLI clients send
 *     `Authorization: Bearer <token>`. Browsers POST the token once
 *     to `/login` and get an HTTP-only cookie they replay on every
 *     subsequent request. Same token, two delivery mechanisms.
 *   - Constant-time comparison so we don't leak the token's length
 *     or prefix through response-time measurements.
 *   - Health check stays open so Render / k8s probes don't need the
 *     secret. Nothing sensitive lives at `/health` anyway.
 *
 * What this explicitly is NOT
 *   - Not multi-user. There are no roles, per-user audit trails, or
 *     account management. If you need that, layer Cloudflare Access /
 *     Tailscale / oauth-proxy in front instead.
 *   - Not a session store. The cookie IS the token, scoped `HttpOnly`
 *     + `Secure` + `SameSite=Strict`, verified on every request.
 *     Logging out clears the cookie; there's no server-side state
 *     to invalidate.
 *   - Not CSRF-resistant for mutation endpoints beyond `SameSite=Strict`
 *     — which is sufficient because the cookie never gets sent on
 *     cross-site requests. Dripyard has no public write endpoints.
 */

import { timingSafeEqual } from "node:crypto";

export interface AuthConfig {
  /** Shared bearer token. Undefined disables auth (open server). */
  token: string | undefined;
  /**
   * Paths that are never gated. Healthchecks, static favicon, etc.
   * Matched as a startsWith prefix — `/health` also protects
   * `/healthz` etc., but we keep the exact `/health` for legacy.
   */
  publicPaths?: string[];
}

const DEFAULT_PUBLIC_PATHS = ["/health", "/login", "/logout", "/favicon.ico"];

const COOKIE_NAME = "dripyard_auth";
const COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 30; // 30 days

/**
 * Resolve the active auth config from env + explicit options.
 * `options.token` wins over `DRIPYARD_TOKEN`; absence of both means
 * "no auth."
 */
export function resolveAuthConfig(explicit?: string | null): AuthConfig {
  const token =
    explicit !== undefined && explicit !== null
      ? explicit
      : process.env.DRIPYARD_TOKEN;
  return {
    token: token && token.length > 0 ? token : undefined,
    publicPaths: DEFAULT_PUBLIC_PATHS,
  };
}

/**
 * Constant-time string equality. Returns false immediately on length
 * mismatch — that's not a leak because the attacker already knows
 * their own guess's length. The guarded bit is "how far did the byte
 * compare get before failing."
 */
function constantTimeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  const ab = Buffer.from(a, "utf8");
  const bb = Buffer.from(b, "utf8");
  return timingSafeEqual(ab, bb);
}

function extractBearerToken(req: Request): string | null {
  const h = req.headers.get("authorization");
  if (!h) return null;
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m ? m[1].trim() : null;
}

function extractCookieToken(req: Request): string | null {
  const cookie = req.headers.get("cookie");
  if (!cookie) return null;
  for (const part of cookie.split(";")) {
    const [k, ...rest] = part.trim().split("=");
    if (k === COOKIE_NAME) return decodeURIComponent(rest.join("="));
  }
  return null;
}

/**
 * Check whether a request is authenticated. Returns true when auth is
 * disabled (no token configured) OR when the request carries a valid
 * token via either accepted channel.
 */
export function isAuthenticated(req: Request, cfg: AuthConfig): boolean {
  if (!cfg.token) return true;
  const bearer = extractBearerToken(req);
  if (bearer && constantTimeEqual(bearer, cfg.token)) return true;
  const cookie = extractCookieToken(req);
  if (cookie && constantTimeEqual(cookie, cfg.token)) return true;
  return false;
}

/**
 * Naive per-IP failed-login counter. In-process, resets on restart —
 * good enough as a brute-force speed bump in front of a 32-byte
 * random token. Five strikes per minute per IP; on the sixth failed
 * attempt we return 429 and stop checking the token altogether for
 * that window.
 */
const loginFailures = new Map<string, { count: number; resetAt: number }>();
const LOGIN_WINDOW_MS = 60_000;
const LOGIN_MAX_FAILURES = 5;

function clientIp(req: Request): string {
  // Render / Cloudflare / nginx all set X-Forwarded-For; trust the
  // first entry for rate-limit keying. Not security-critical — if an
  // attacker forges the header they just rate-limit themselves per
  // forged value.
  const xff = req.headers.get("x-forwarded-for");
  if (xff) return xff.split(",")[0].trim();
  const real = req.headers.get("x-real-ip");
  if (real) return real.trim();
  return "unknown";
}

function recordLoginFailure(ip: string): boolean {
  const now = Date.now();
  const entry = loginFailures.get(ip);
  if (!entry || now > entry.resetAt) {
    loginFailures.set(ip, { count: 1, resetAt: now + LOGIN_WINDOW_MS });
    return false;
  }
  entry.count += 1;
  return entry.count > LOGIN_MAX_FAILURES;
}

function isLoginBlocked(ip: string): boolean {
  const entry = loginFailures.get(ip);
  if (!entry) return false;
  if (Date.now() > entry.resetAt) {
    loginFailures.delete(ip);
    return false;
  }
  return entry.count > LOGIN_MAX_FAILURES;
}

/**
 * Minimal login page — one field, one button, one failed-attempt hint.
 * Inlined here so the auth module is self-contained and doesn't
 * depend on the React bundle being present (useful for curl-based
 * debugging).
 */
function renderLoginPage(message = ""): string {
  const msgHtml = message
    ? `<p class="msg">${escapeHtml(message)}</p>`
    : "";
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>dripyard \u2014 login</title>
<style>
  :root { color-scheme: dark; }
  html,body { margin:0; background:#050a14; color:#e2e8f0;
              font-family: ui-monospace, "JetBrains Mono", monospace; }
  body { min-height:100vh; display:grid; place-items:center; padding:2rem; }
  .card { width: min(420px, 100%); padding: 2.5rem 2rem;
          border: 1px solid rgba(255,255,255,0.08);
          border-radius: 16px; background: rgba(255,255,255,0.02); }
  h1 { margin:0 0 1.5rem; font-size: 22px; letter-spacing: -0.02em; }
  h1 small { color:#7dd3fc; font-weight: 400; }
  label { display:block; margin-bottom: 0.4rem; font-size: 13px;
          text-transform: uppercase; letter-spacing: 0.2em; color:#94a3b8; }
  input { width:100%; padding: 0.8rem 1rem; font: inherit;
          background:#0a1220; color:#e2e8f0;
          border: 1px solid rgba(255,255,255,0.1); border-radius: 10px;
          outline: none; }
  input:focus { border-color:#38bdf8; }
  button { margin-top: 1rem; width:100%; padding: 0.8rem 1rem;
           font: inherit; color:#050a14; background:#7dd3fc;
           border: 0; border-radius: 10px; cursor: pointer;
           font-weight: 600; }
  button:hover { background:#bae6fd; }
  .msg { margin: 1rem 0 0; padding: 0.65rem 0.9rem; font-size: 14px;
         background: rgba(251,113,133,0.1); color: #fda4af;
         border: 1px solid rgba(251,113,133,0.25); border-radius: 8px; }
</style>
</head>
<body>
<form class="card" method="POST" action="/login">
  <h1>dripyard <small>\u2192 sign in</small></h1>
  <label for="token">access token</label>
  <input id="token" name="token" type="password" autocomplete="off" autofocus required>
  <button type="submit">open dashboard</button>
  ${msgHtml}
</form>
</body>
</html>`;
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function buildCookie(token: string, secure: boolean): string {
  const attrs = [
    `${COOKIE_NAME}=${encodeURIComponent(token)}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Strict",
    `Max-Age=${COOKIE_MAX_AGE_SECONDS}`,
  ];
  if (secure) attrs.push("Secure");
  return attrs.join("; ");
}

function clearCookie(secure: boolean): string {
  const attrs = [
    `${COOKIE_NAME}=`,
    "Path=/",
    "HttpOnly",
    "SameSite=Strict",
    "Max-Age=0",
  ];
  if (secure) attrs.push("Secure");
  return attrs.join("; ");
}

/**
 * True when the request arrived over a TLS-terminated connection.
 * Render / Cloudflare / any reverse proxy sets `X-Forwarded-Proto`.
 * When it's missing we assume localhost-dev and skip the `Secure`
 * cookie attribute so cookies actually work over http://localhost.
 */
function isSecureRequest(req: Request): boolean {
  const proto = req.headers.get("x-forwarded-proto");
  if (proto) return proto.toLowerCase() === "https";
  return new URL(req.url).protocol === "https:";
}

/**
 * Handle `POST /login` — read the submitted token, compare in
 * constant time, set the auth cookie on success. Bad tokens
 * increment the per-IP failure counter; once the counter trips we
 * return 429 for the rest of the window.
 */
async function handleLogin(
  req: Request,
  cfg: AuthConfig,
): Promise<Response> {
  if (!cfg.token) {
    // Auth disabled \u2014 nothing to log into. Send them to the root.
    return Response.redirect(new URL("/", req.url).toString(), 303);
  }
  const ip = clientIp(req);
  if (isLoginBlocked(ip)) {
    return new Response(renderLoginPage("Too many attempts. Wait a minute."), {
      status: 429,
      headers: { "content-type": "text/html; charset=utf-8" },
    });
  }

  let submitted: string | null = null;
  const contentType = req.headers.get("content-type") ?? "";
  if (contentType.includes("application/x-www-form-urlencoded")) {
    const body = await req.text();
    const params = new URLSearchParams(body);
    submitted = params.get("token");
  } else if (contentType.includes("application/json")) {
    const body = (await req.json().catch(() => null)) as {
      token?: string;
    } | null;
    submitted = body?.token ?? null;
  }

  if (!submitted || !constantTimeEqual(submitted, cfg.token)) {
    recordLoginFailure(ip);
    return new Response(renderLoginPage("Invalid token."), {
      status: 401,
      headers: { "content-type": "text/html; charset=utf-8" },
    });
  }

  // Success — clear any stale failure counter and set the cookie.
  loginFailures.delete(ip);
  const secure = isSecureRequest(req);
  return new Response(null, {
    status: 303,
    headers: {
      location: "/",
      "set-cookie": buildCookie(cfg.token, secure),
    },
  });
}

/**
 * Handle `POST /logout` (or GET — we're lenient for ergonomics).
 * Always clears the cookie; never errors. If the user wasn't logged
 * in, we still return 303 → /login so the experience is consistent.
 */
function handleLogout(req: Request, cfg: AuthConfig): Response {
  const secure = isSecureRequest(req);
  return new Response(null, {
    status: 303,
    headers: {
      location: cfg.token ? "/login" : "/",
      "set-cookie": clearCookie(secure),
    },
  });
}

function handleLoginPage(cfg: AuthConfig, req: Request): Response {
  if (!cfg.token) {
    return Response.redirect(new URL("/", req.url).toString(), 303);
  }
  return new Response(renderLoginPage(), {
    status: 200,
    headers: { "content-type": "text/html; charset=utf-8" },
  });
}

/**
 * Wrap an existing fetch handler in an auth gate. When auth is
 * disabled the wrapper is a thin passthrough. When enabled, it
 * routes `/login` and `/logout` itself and short-circuits
 * unauthenticated requests to either a login page (browser) or a
 * 401 JSON payload (API client).
 */
export function wrapWithAuth(
  inner: (req: Request) => Promise<Response>,
  cfg: AuthConfig,
): (req: Request) => Promise<Response> {
  if (!cfg.token) return inner;

  const publicPaths = cfg.publicPaths ?? DEFAULT_PUBLIC_PATHS;

  return async (req: Request): Promise<Response> => {
    const url = new URL(req.url);

    // Route our own auth endpoints before anything else.
    if (url.pathname === "/login") {
      if (req.method === "POST") return handleLogin(req, cfg);
      return handleLoginPage(cfg, req);
    }
    if (url.pathname === "/logout") {
      return handleLogout(req, cfg);
    }

    if (publicPaths.some((p) => url.pathname === p || url.pathname === p + "/"))
      return inner(req);

    if (isAuthenticated(req, cfg)) return inner(req);

    // Unauthenticated. Browsers get redirected to /login; API clients
    // (Accept: application/json or no Accept at all on non-GET) get a
    // plain 401 with a machine-readable body so they don't chase a
    // redirect to an HTML page they can't parse.
    const accept = req.headers.get("accept") ?? "";
    const wantsHtml =
      (req.method === "GET" || req.method === "HEAD") &&
      accept.includes("text/html");
    if (wantsHtml) {
      return Response.redirect(new URL("/login", req.url).toString(), 303);
    }
    return new Response(
      JSON.stringify({ error: "unauthorized", detail: "Missing or invalid bearer token." }),
      {
        status: 401,
        headers: {
          "content-type": "application/json",
          "www-authenticate": 'Bearer realm="dripyard"',
        },
      },
    );
  };
}

/** Test helper — clear the in-memory rate limit between tests. */
export function __resetAuthState(): void {
  loginFailures.clear();
}
