/**
 * Worker spawner — the pluggable piece behind the UI's "+1" button.
 *
 * Dashboard exposes a `workers.spawn` mutation; when called, it invokes
 * the configured spawner which is responsible for actually starting a
 * new worker process (locally, via fly.io API, by scaling a k8s
 * Deployment, etc).
 *
 * Only the local ForkSpawner is built today. Cloud spawners are a
 * matter of implementing the same interface against each provider's
 * API — zero change to dashboard or worker code.
 */

export interface Spawner {
  /** Start a new worker. Returns an opaque handle naming the instance. */
  spawn(): Promise<{ handle: string }>;

  /** Stop a worker by handle. May be no-op for platforms where the worker
   *  exits via SIGTERM delivered by the platform (fly, k8s). */
  stop?(handle: string): Promise<void>;
}

export interface ForkSpawnerOptions {
  /** Path to the script that boots a worker (typically `src/main.ts worker`). */
  command: string[];
  /** Unix socket path the new worker should connect to. */
  socketPath: string;
  /** Extra env vars to pass through. */
  env?: Record<string, string>;
}

/**
 * Local-process spawner. Forks a `bun run ...` (or equivalent) process
 * that boots a standalone worker pointed at the dashboard's unix socket.
 * Good for local dev, single-VM deployments, and demos of the +1 flow.
 */
export class ForkSpawner implements Spawner {
  private children = new Map<string, ReturnType<typeof Bun.spawn>>();
  private counter = 0;

  constructor(private readonly opts: ForkSpawnerOptions) {}

  async spawn(): Promise<{ handle: string }> {
    const handle = `fork-${++this.counter}-${Date.now()}`;
    const child = Bun.spawn({
      cmd: this.opts.command,
      env: {
        ...process.env,
        DRIPYARD_SOCKET: this.opts.socketPath,
        // Pass the handle so the child can include it in its register()
        // call. Links the workers row back to this spawner's child map
        // so UI's "stop" button can kill the right process.
        DRIPYARD_SPAWN_HANDLE: handle,
        ...(this.opts.env ?? {}),
      },
      stdout: "inherit",
      stderr: "inherit",
    });
    this.children.set(handle, child);

    // Clean up the map when the child exits so stop() doesn't leak.
    child.exited.then(() => {
      this.children.delete(handle);
    });

    return { handle };
  }

  async stop(handle: string): Promise<void> {
    const child = this.children.get(handle);
    if (!child) return;
    child.kill("SIGTERM");
    await child.exited.catch(() => {
      /* already dead */
    });
    this.children.delete(handle);
  }

  /** For tests: list handles of currently-running children. */
  listHandles(): string[] {
    return [...this.children.keys()];
  }
}
