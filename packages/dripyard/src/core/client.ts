import type { Vex } from "vex-core";

/**
 * Transport-agnostic interface between orchestration code (runs in both
 * the dashboard's embedded worker and standalone worker processes) and
 * dripyard's state layer (SQLite living on the dashboard).
 *
 * Two implementations:
 *   - LocalVexClient  — wraps an in-process Vex directly, zero overhead
 *   - SocketVexClient — same /vex/* API over a unix domain socket
 *
 * Both speak the same vex-core query/mutate protocol, so orchestrator
 * code is identical regardless of which side of the socket it runs on.
 * An HttpVexClient for cross-machine workers is a trivial addition but
 * not built yet — we don't need it for the +1-worker flow.
 */
export interface VexClient {
  query<T = any>(name: string, args?: Record<string, any>): Promise<T>;
  mutate<T = any>(name: string, args?: Record<string, any>): Promise<T>;
}

/** Direct in-process binding. Used by `dripyard serve` single-process mode. */
export class LocalVexClient implements VexClient {
  constructor(private readonly vex: Vex) {}

  query<T = any>(name: string, args: Record<string, any> = {}): Promise<T> {
    return this.vex.query(name, args) as Promise<T>;
  }

  mutate<T = any>(name: string, args: Record<string, any> = {}): Promise<T> {
    return this.vex.mutate(name, args) as Promise<T>;
  }
}

/**
 * Unix-socket client for standalone worker processes on the same host.
 * Speaks the dashboard's /vex/query + /vex/mutate endpoints unchanged —
 * the dashboard binds the same handler on TCP (for the UI) and on the
 * socket (for local workers).
 *
 * Bun's fetch supports `unix: <path>` natively, so the wire protocol is
 * literally HTTP over a unix socket. No custom framing, no serialization
 * quirks, no new code to maintain on either side.
 */
export class SocketVexClient implements VexClient {
  constructor(private readonly socketPath: string) {}

  async query<T = any>(
    name: string,
    args: Record<string, any> = {},
  ): Promise<T> {
    return this.call<T>("/vex/query", name, args);
  }

  async mutate<T = any>(
    name: string,
    args: Record<string, any> = {},
  ): Promise<T> {
    return this.call<T>("/vex/mutate", name, args);
  }

  private async call<T>(
    path: string,
    name: string,
    args: Record<string, any>,
  ): Promise<T> {
    const res = await fetch(`http://dripyard${path}`, {
      // biome-ignore lint/suspicious/noExplicitAny: Bun-specific option
      unix: this.socketPath,
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, args }),
    } as any);
    const data = (await res.json()) as { data?: T; error?: string };
    if (data.error) throw new Error(data.error);
    return data.data as T;
  }
}
