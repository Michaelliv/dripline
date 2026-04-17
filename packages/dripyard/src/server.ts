import { existsSync, unlinkSync } from "node:fs";
import { hostname, loadavg, tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { registry } from "dripline";
import { duckdbAdapter, sqliteAdapter, Vex } from "vex-core";
import {
  accessLog,
  bearerAuth,
  cors,
  createRouter,
  errorBoundary,
  requestId,
  staticFiles,
  vexHandler,
} from "vex-core/http";
import { LocalVexClient } from "./core/client.js";
import { Compactor } from "./core/compactor.js";
import {
  getActiveFlareGun,
  initFlareGun,
  setActiveFlareGun,
} from "./core/flaregun.js";
import { Orchestrator, type OrchestratorOptions } from "./core/orchestrator.js";
import { Scheduler } from "./core/scheduler.js";
import { ForkSpawner, type Spawner } from "./core/spawner.js";
import {
  hydrateLanes,
  loadWorkspace,
  setActiveWorkspace,
  type Workspace,
} from "./core/workspace.js";
import { lanesPlugin, setLanesOrchestrator } from "./plugins/lanes.js";
import { proxiesPlugin } from "./plugins/proxies.js";
import { runsPlugin } from "./plugins/runs.js";
import { setWorkersSpawner, workersPlugin } from "./plugins/workers.js";
import { workspacePlugin } from "./plugins/workspace.js";

export interface ServerOptions {
  port?: number;
  /** Unix socket path for local worker RPC. Default: <tmpdir>/dripyard-<port>.sock. */
  socketPath?: string;
  dbPath?: string;
  /**
   * Path to a dripline workspace (directory containing `.dripline/`).
   * When set, the server loads plugins, hydrates lanes from the
   * workspace's config.json, and serves as a reactive UI for that
   * project. When absent, the server runs empty — useful for tests
   * and for programmatic users who manage lanes via mutations.
   */
  workspace?: string;
  orchestratorOptions?: OrchestratorOptions;
  /**
   * Run with an embedded worker (scheduler + telemetry). Default: true.
   * Set false for `dripyard dashboard` mode where only external workers
   * execute lanes.
   */
  embeddedWorker?: boolean;
  /**
   * Spawner behind the UI's "+1 worker" button. Default: a ForkSpawner
   * that runs `bun run src/main.ts worker --socket <socketPath>`. Pass
   * a cloud spawner for fly.io / k8s / docker deployments.
   */
  spawner?: Spawner;
  /**
   * Shared bearer token. When present (explicitly or via
   * `DRIPYARD_TOKEN`), every HTTP request must carry
   * `Authorization: Bearer <token>` or a `dripyard_auth` cookie set
   * by the built-in `/login` page. When absent, the server runs
   * open — the right default for localhost dev and tests.
   */
  token?: string | null;
  /** Enable CORS. Default: true, origin `*` (dashboards typically on same origin). */
  cors?: boolean;
  /** Log each request to stdout. Default: true. */
  log?: boolean;
}

export async function startServer(options: ServerOptions = {}) {
  const port = options.port ?? Number(process.env.DRIPYARD_PORT ?? 3457);
  const socketPath =
    options.socketPath ??
    process.env.DRIPYARD_SOCKET ??
    join(tmpdir(), `dripyard-${port}.sock`);
  const dbPath = options.dbPath ?? process.env.DRIPYARD_DB ?? ":memory:";
  const embeddedWorker = options.embeddedWorker ?? true;
  const token =
    options.token === undefined
      ? process.env.DRIPYARD_TOKEN || null
      : options.token;
  const enableCors = options.cors ?? true;
  const enableLog = options.log ?? true;

  const vex = await Vex.create({
    plugins: [
      lanesPlugin,
      runsPlugin,
      workersPlugin,
      workspacePlugin,
      proxiesPlugin,
    ],
    transactional: sqliteAdapter(dbPath === ":memory:" ? ":memory:" : dbPath),
    analytical: await duckdbAdapter(),
  });

  // Load the workspace — plugins land in dripline's shared registry,
  // lanes get hydrated after vex is up.
  let workspace: Workspace | null = null;
  if (options.workspace) {
    workspace = await loadWorkspace(resolve(options.workspace));
    setActiveWorkspace(workspace);
    console.log(
      `workspace: ${workspace.path} (${workspace.plugins.length} plugins, ${Object.keys(workspace.config.lanes ?? {}).length} lanes)`,
    );
  }

  // Try to bring up flaregun. No-op when creds are absent — the server
  // still boots fully; only lanes with proxyEnabled will error when
  // they actually run. Callers can pass an explicit FlareGun via
  // options.orchestratorOptions.flaregun to bypass env-based init
  // (tests, cloud deployments with pre-constructed clients).
  const flaregun =
    options.orchestratorOptions?.flaregun ??
    (await initFlareGun()) ??
    undefined;
  if (flaregun) setActiveFlareGun(flaregun);

  const client = new LocalVexClient(vex);
  const orchestratorOpts: OrchestratorOptions = {
    ...options.orchestratorOptions,
    flaregun,
    // Default resolver reads from dripline's shared registry, which the
    // workspace loader populated above. Falls back to the caller's
    // resolver if they passed one.
    resolvePlugin:
      options.orchestratorOptions?.resolvePlugin ??
      ((name: string) => {
        const plugin = registry.getPlugin(name);
        if (!plugin)
          throw new Error(
            `Plugin not registered: ${name}. Declared in .dripline/plugins.json?`,
          );
        return plugin;
      }),
  };
  const orchestrator = new Orchestrator(client, orchestratorOpts);
  const scheduler = new Scheduler(vex, orchestrator);
  // Compactor: one Vex job per compactable table (registered after
  // the workspace loads its plugins). Lease-protected per table so
  // multiple workers safely divide work; on dashboard-only mode it's
  // a no-op since no worker ever ticks the jobs.
  const compactor = new Compactor(vex, workspace?.remote ?? null);
  // Wire the orchestrator up to the lanes plugin so UI's "Run now"
  // button can dispatch through the same path as scheduled runs.
  // workerId is set below once the embedded worker registers; until
  // then runNow runs without a workerId attribution, which is fine —
  // the runs table just stores null for that column.
  setLanesOrchestrator(orchestrator);

  // Hydrate lanes from the workspace config. Done after vex is up
  // but before the scheduler starts so jobs register on real lanes.
  if (workspace) await hydrateLanes(vex, workspace);

  // Seed the flaregun_workers table from the CF API so the Proxies
  // page has data on first paint. Fire-and-forget: a cold CF API call
  // can take a few hundred ms, and server boot shouldn't block on it.
  // Until the mutation lands, the page shows an empty worker list
  // — which is the correct representation of "we don't know yet."
  if (flaregun) {
    vex.mutate("proxies.refresh", {}).catch((err: any) => {
      console.warn(
        `flaregun: initial worker sync failed (${err?.message ?? String(err)}). Proxies page will show empty until the next mutation.`,
      );
    });
  }

  let workerId: string | null = null;
  if (embeddedWorker) {
    // Register this process as a worker and start the scheduler. Workers
    // connecting over the unix socket will register themselves via the
    // same workers.register mutation and show up alongside this one.
    const workerName = process.env.DRIPYARD_WORKER ?? `worker-${hostname()}`;
    workerId = (await vex.mutate("workers.register", {
      name: workerName,
      host: hostname(),
    })) as string;
    scheduler.setWorkerId(workerId);
    setLanesOrchestrator(orchestrator, workerId);
    await scheduler.start();
    await compactor.start();

    // Per-worker telemetry — cheap sample every 10s for UI sparklines.
    // Doubles as the embedded worker's heartbeat: receiving a sample
    // proves the process is alive, so we bump lastHeartbeat in the
    // same tick. Keeps the workers row live without a second job.
    const wid = workerId;
    await vex.addJob("telemetry.sample", {
      schedule: "every 10s",
      description: "Worker telemetry + heartbeat",
      async handler() {
        const mem = process.memoryUsage();
        const [avg1, avg5] = loadavg();
        await vex.mutate("workers.sampleRecord", {
          workerId: wid,
          heapUsedMb: Math.round((mem.heapUsed / 1024 / 1024) * 10) / 10,
          rssMb: Math.round((mem.rss / 1024 / 1024) * 10) / 10,
          externalMb: Math.round((mem.external / 1024 / 1024) * 10) / 10,
          loadAvg1m: Math.round(avg1 * 100) / 100,
          loadAvg5m: Math.round(avg5 * 100) / 100,
          uptimeS: Math.round(process.uptime()),
        });
        await vex.mutate("workers.heartbeat", { id: wid });
      },
    });

    // Rolling flaregun stats for the Proxies page sparklines. `stats()`
    // is an in-memory read (no CF API call), so 30s is plenty often.
    // Only registered when flaregun is available; otherwise we'd just
    // be inserting zero-value rows every half minute.
    if (flaregun) {
      const fg = flaregun;
      await vex.addJob("proxies.sample", {
        schedule: "every 30s",
        description: "Flaregun rotation stats",
        async handler() {
          const stats = fg.stats();
          await vex.mutate("proxies.sampleRecord", {
            workerCount: stats.workers,
            totalRequests: stats.totalRequests,
            perWorker: stats.perWorker,
          });
        },
      });
    }
  }

  // Locate the built UI bundle. Two candidates cover both layouts:
  // running from source (src/server.ts → ../dist/app/ui) and the
  // published package (dist/server.js → ./app/ui). dist must win
  // over source — src/app/ui/ contains the Vite entry index.html
  // but no built /assets/, and serving that would 404 every hashed
  // script.
  const here = dirname(fileURLToPath(import.meta.url));
  const uiCandidates = [
    resolve(here, "../dist/app/ui"),
    resolve(here, "app/ui"),
  ];
  const uiDir = uiCandidates.find((p) => existsSync(p)) ?? null;
  if (!uiDir)
    console.warn(
      `ui: no built bundle found (looked in ${uiCandidates.join(", ")}). Run \`bun --filter dripyard build\`. UI requests will 404.`,
    );

  // Compose the HTTP surface. Order matters: every layer above
  // `vexHandler` + `staticFiles` gets a shot at the request first.
  //   errorBoundary  → catches anything thrown and renders a Response
  //   requestId      → X-Request-Id header in, out, and on ctx.state
  //   accessLog      → one line per request once the response is known
  //   cors           → preflight + ACL headers
  //   bearerAuth     → /login + /logout + token gate (when configured)
  //   vexHandler     → /query | /mutate | /subscribe | /webhook/*
  //   staticFiles    → React bundle + SPA fallback
  const app = createRouter().use(errorBoundary());
  app.use(requestId());
  if (enableLog)
    app.use(accessLog({ skipPaths: ["/health", "/favicon.ico"] }));
  if (enableCors) app.use(cors());
  app.get("/health", () => new Response("ok"));
  if (token) app.use(bearerAuth({ token, brand: "dripyard" }));
  app.mount("", vexHandler(vex));
  if (uiDir) app.use(staticFiles({ dir: uiDir }));

  const fetchHandler = (req: Request) => app.handle(req);

  const tcpServer = Bun.serve({ port, fetch: fetchHandler });

  // Unix socket for local worker RPC. Bun won't bind if the file already
  // exists, so we unlink a stale socket from a previous crash.
  if (existsSync(socketPath)) unlinkSync(socketPath);
  const socketServer = Bun.serve({ unix: socketPath, fetch: fetchHandler });

  // Wire up the worker spawner so the UI's +1 button works. Default
  // spawner forks a `bun run src/main.ts worker` pointed at our socket.
  const spawner =
    options.spawner ??
    new ForkSpawner({
      command: [
        process.execPath,
        "run",
        new URL("./main.ts", import.meta.url).pathname,
        "worker",
        "--socket",
        socketPath,
      ],
      socketPath,
    });
  setWorkersSpawner(spawner);

  console.log(`dripyard running on http://localhost:${tcpServer.port}`);
  console.log(`local worker socket: ${socketPath}`);
  if (workerId) console.log(`embedded worker: ${workerId}`);

  return {
    server: tcpServer,
    socketServer,
    socketPath,
    vex,
    orchestrator,
    scheduler,
    workerId,
    async close() {
      await compactor.stop();
      await scheduler.stop();
      tcpServer.stop(true);
      socketServer.stop(true);
      if (existsSync(socketPath)) {
        try {
          unlinkSync(socketPath);
        } catch {
          /* best effort */
        }
      }
      await vex.close();
      // Clear module-level singletons so a subsequent startServer()
      // in the same process (tests, hot-reload) doesn't inherit stale
      // state from this instance.
      setActiveWorkspace(null);
      setWorkersSpawner(null);
      setLanesOrchestrator(null);
      setActiveFlareGun(null);
    },
  };
}

// Re-export so plugins and tests can reach the module-level singleton
// without pulling the server module itself.
export { getActiveFlareGun };
