import {
  Activity,
  Database,
  HardDrive,
  Server,
  TrendingUp,
  Zap,
} from "lucide-react";
import { type ReactNode, useMemo } from "react";
import { PageHeader } from "@/components/ui/page-header";
import { Sparkline } from "@/components/ui/sparkline";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useSubscription } from "@/hooks/use-subscription";
import { formatRelativeTime } from "@/lib/api";

/**
 * Bloomberg-style overview: at-a-glance density, live-updating numbers,
 * color-coded status, sparklines everywhere. Single screen answers:
 *   - is the warehouse alive (runs, rows, errors, workers)?
 *   - which lanes are healthy? failing? due?
 *   - are workers coping? any drift?
 *   - is compaction keeping up?
 */

type Lane = {
  _id: string;
  name: string;
  schedule: string;
  enabled: boolean;
  sourcePlugin: string;
  tables: Array<unknown>;
};

type Run = {
  _id: string;
  laneId: string;
  status: "running" | "ok" | "error" | "skipped";
  startedAt: number;
  finishedAt: number | null;
  rowsSynced: number;
  filesPublished: number;
  durationMs: number | null;
};

type Worker = {
  _id: string;
  name: string;
  status: "idle" | "running" | "offline" | "draining";
  lastHeartbeat: number;
  currentLaneId: string | null;
  startedAt: number;
};

type Sample = {
  workerId: string;
  timestamp: number;
  rssMb: number;
  heapUsedMb: number;
  loadAvg1m: number;
  uptimeS: number;
};

type WarehouseTable = {
  plugin: string;
  table: string;
  rows: number | null;
  rawFiles: number;
  curatedFiles: number;
  lastCompactAt: number | null;
};

type Warehouse = {
  remote: { bucket: string; endpoint: string } | null;
  tables: WarehouseTable[];
};

const DAY_MS = 86_400_000;
const HEARTBEAT_STALE_MS = 90_000;

export function OverviewPage() {
  const lanes = useSubscription<Lane[]>("lanes.list") ?? [];
  const runs = useSubscription<Run[]>("runs.list", { limit: 500 }) ?? [];
  const workers = useSubscription<Worker[]>("workers.list") ?? [];
  const samples =
    useSubscription<Sample[]>("workers.samplesRecent", {}) ?? [];
  const warehouse = useSubscription<Warehouse>("workspace.warehouse");

  const now = Date.now();

  // ── KPI derivations ──────────────────────────────────────────────
  const stats = useMemo(() => {
    const dayAgo = now - DAY_MS;
    const runs24h = runs.filter((r) => r.startedAt >= dayAgo);
    const ok24h = runs24h.filter((r) => r.status === "ok").length;
    const err24h = runs24h.filter((r) => r.status === "error").length;
    const active = runs.filter((r) => r.status === "running").length;
    const rows24h = runs24h.reduce((s, r) => s + (r.rowsSynced ?? 0), 0);
    const completed = runs.filter(
      (r) => r.status === "ok" && r.durationMs != null,
    );
    const recentCompleted = completed.slice(0, 20);
    const rowsPerSec =
      recentCompleted.length > 0
        ? Math.round(
            recentCompleted.reduce(
              (s, r) => s + r.rowsSynced / (r.durationMs! / 1000),
              0,
            ) / recentCompleted.length,
          )
        : 0;

    const healthy = workers.filter(
      (w) =>
        w.status !== "offline" &&
        w.status !== "draining" &&
        now - w.lastHeartbeat < HEARTBEAT_STALE_MS,
    ).length;
    const draining = workers.filter((w) => w.status === "draining").length;
    const offline = workers.length - healthy - draining;

    const whRows =
      warehouse?.tables.reduce((s, t) => s + (t.rows ?? 0), 0) ?? 0;
    const rawPending =
      warehouse?.tables.reduce((s, t) => s + t.rawFiles, 0) ?? 0;
    const compacts = (warehouse?.tables ?? [])
      .map((t) => t.lastCompactAt)
      .filter((x): x is number => x != null);
    const newestCompact = compacts.length > 0 ? Math.max(...compacts) : null;
    const compactLag = newestCompact ? now - newestCompact : null;

    return {
      runs24h,
      ok24h,
      err24h,
      active,
      rows24h,
      rowsPerSec,
      healthy,
      draining,
      offline,
      whRows,
      rawPending,
      compactLag,
      tableCount: warehouse?.tables.length ?? 0,
    };
  }, [now, runs, workers, warehouse]);

  // Per-lane aggregation for the status table
  const perLane = useMemo(() => {
    const dayAgo = now - DAY_MS;
    const map = new Map<
      string,
      {
        ok: number;
        err: number;
        runs: Run[];
        rows: number;
        durs: number[];
        last: Run | null;
      }
    >();
    for (const r of runs) {
      if (r.startedAt < dayAgo) continue;
      const entry = map.get(r.laneId) ?? {
        ok: 0,
        err: 0,
        runs: [],
        rows: 0,
        durs: [],
        last: null,
      };
      if (r.status === "ok") entry.ok++;
      if (r.status === "error") entry.err++;
      entry.rows += r.rowsSynced ?? 0;
      if (r.durationMs) entry.durs.push(r.durationMs);
      entry.runs.push(r);
      if (!entry.last || r.startedAt > entry.last.startedAt) entry.last = r;
      map.set(r.laneId, entry);
    }
    return map;
  }, [now, runs]);

  // Heartbeat: this tick updates every second so "5s ago" labels tick live.
  // React re-renders on subscription updates anyway; this just ensures the
  // countdown/age displays don't feel frozen between mutations.
  // (Skipped — relying on subscription invalidation keeps code simpler; if
  // staleness matters the numbers are already sparkline-shaped.)

  return (
    <div className="flex flex-col h-full min-h-0 p-4 gap-3 overflow-hidden">
      <PageHeader
        title="Overview"
        description={`${lanes.length} lanes · ${workers.length} workers · ${stats.whRows.toLocaleString()} warehouse rows`}
      />

      <div className="flex-1 min-h-0 overflow-auto flex flex-col gap-3">
      {/* KPI STRIP */}
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2 flex-none">
        <KPI
          icon={Activity}
          label="Runs 24h"
          value={String(stats.runs24h.length)}
          sub={`${stats.ok24h} ok · ${stats.err24h} err`}
          tone={stats.err24h > stats.ok24h && stats.runs24h.length > 0 ? "error" : "ok"}
        />
        <KPI
          icon={TrendingUp}
          label="Rows 24h"
          value={stats.rows24h.toLocaleString()}
          sub={stats.rowsPerSec > 0 ? `~${stats.rowsPerSec.toLocaleString()}/s` : "—"}
        />
        <KPI
          icon={Zap}
          label="Active"
          value={String(stats.active)}
          sub={stats.active === 0 ? "idle" : "running"}
          tone={stats.active > 0 ? "warning" : undefined}
        />
        <KPI
          icon={Server}
          label="Workers"
          value={String(stats.healthy)}
          sub={
            stats.draining > 0
              ? `${stats.draining} draining`
              : stats.offline > 0
                ? `${stats.offline} offline`
                : "all healthy"
          }
          tone={
            stats.healthy === 0 && workers.length > 0
              ? "error"
              : stats.draining > 0
                ? "warning"
                : "ok"
          }
        />
        <KPI
          icon={Database}
          label="Warehouse"
          value={stats.whRows.toLocaleString()}
          sub={`${stats.tableCount} tables`}
        />
        <KPI
          icon={HardDrive}
          label="Compact"
          value={stats.compactLag ? formatAge(stats.compactLag) : "—"}
          sub={stats.rawPending > 0 ? `${stats.rawPending} raw pending` : "clean"}
          tone={stats.rawPending > 10 ? "warning" : "ok"}
        />
      </div>

      {/* RUN STREAM */}
      <Panel
        title="Run stream"
        sub={
          runs.length === 0
            ? "no runs yet"
            : `last ${Math.min(200, runs.length)} · newest left`
        }
      >
        <RunStream runs={runs.slice(0, 200)} lanes={lanes} />
      </Panel>

      {/* MAIN GRID */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-3">
        <div className="lg:col-span-2">
          <Panel title="Lane status" sub="24h window">
            <LaneTable lanes={lanes} perLane={perLane} now={now} />
          </Panel>
        </div>
        <Panel
          title="Worker fleet"
          sub={workers.length === 0 ? "none" : `${workers.length} registered`}
        >
          <WorkerList workers={workers} samples={samples} now={now} />
        </Panel>
      </div>

      {/* WAREHOUSE ACTIVITY */}
      <Panel
        title="Warehouse activity"
        sub={warehouse?.remote?.bucket ?? "no remote"}
      >
        <WarehouseGrid tables={warehouse?.tables ?? []} now={now} />
      </Panel>
      </div>
    </div>
  );
}

// ────────────────────────────────────────────────────────────────────
// KPI tile
// ────────────────────────────────────────────────────────────────────

function KPI({
  icon: Icon,
  label,
  value,
  sub,
  tone,
}: {
  icon: typeof Activity;
  label: string;
  value: string;
  sub?: string;
  tone?: "ok" | "warning" | "error";
}) {
  const toneClass =
    tone === "error"
      ? "text-shift-error"
      : tone === "warning"
        ? "text-shift-warning"
        : tone === "ok"
          ? "text-shift-ok"
          : "";
  return (
    <div className="rounded border border-[var(--border-muted)] bg-[var(--panel)]/40 p-2 min-w-0">
      <div className="flex items-center justify-between text-[10px] uppercase tracking-wider text-shift-muted">
        <span className="truncate">{label}</span>
        <Icon className="size-3 flex-none" />
      </div>
      <div
        className={`mt-1 text-xl font-semibold tabular-nums truncate ${toneClass}`}
      >
        {value}
      </div>
      {sub && (
        <div className="text-[10px] text-shift-muted truncate">{sub}</div>
      )}
    </div>
  );
}

// ────────────────────────────────────────────────────────────────────
// Panel wrapper
// ────────────────────────────────────────────────────────────────────

function Panel({
  title,
  sub,
  children,
}: {
  title: string;
  sub?: string;
  children: ReactNode;
}) {
  return (
    <section className="rounded border border-[var(--border-muted)] bg-[var(--panel)]/20">
      <header className="flex items-baseline justify-between gap-2 px-3 py-1.5 border-b border-[var(--border-muted)]/60">
        <span className="text-[11px] uppercase tracking-wider font-medium">
          {title}
        </span>
        {sub && (
          <span className="text-[10px] text-shift-muted truncate">{sub}</span>
        )}
      </header>
      <div className="p-2">{children}</div>
    </section>
  );
}

// ────────────────────────────────────────────────────────────────────
// Run stream — colored squares, one per run, hover = details
// ────────────────────────────────────────────────────────────────────

function RunStream({ runs, lanes }: { runs: Run[]; lanes: Lane[] }) {
  const laneById = useMemo(() => {
    const m = new Map<string, string>();
    for (const l of lanes) m.set(l._id, l.name);
    return m;
  }, [lanes]);

  if (runs.length === 0) {
    return (
      <div className="text-xs text-shift-muted px-2 py-3">
        No runs yet. Lanes will record runs here when they execute.
      </div>
    );
  }

  return (
    <div className="flex flex-wrap gap-[3px]">
      {runs.map((r) => {
        const color =
          r.status === "ok"
            ? "bg-shift-ok/70 hover:bg-shift-ok"
            : r.status === "error"
              ? "bg-shift-error/70 hover:bg-shift-error"
              : r.status === "running"
                ? "bg-shift-warning/70 hover:bg-shift-warning animate-pulse"
                : "bg-shift-muted/40 hover:bg-shift-muted";
        const lane = laneById.get(r.laneId) ?? r.laneId;
        const title = `${lane} · ${r.status} · ${r.rowsSynced.toLocaleString()} rows · ${formatRelativeTime(r.startedAt)}${r.durationMs ? ` · ${formatMs(r.durationMs)}` : ""}`;
        return (
          <a
            key={r._id}
            href={`/runs/${r._id}`}
            title={title}
            className={`size-3 rounded-sm transition-colors ${color}`}
          />
        );
      })}
    </div>
  );
}

// ────────────────────────────────────────────────────────────────────
// Lane status table
// ────────────────────────────────────────────────────────────────────

function LaneTable({
  lanes,
  perLane,
  now,
}: {
  lanes: Lane[];
  perLane: Map<
    string,
    { ok: number; err: number; runs: Run[]; rows: number; durs: number[]; last: Run | null }
  >;
  now: number;
}) {
  if (lanes.length === 0) {
    return (
      <div className="text-xs text-shift-muted px-2 py-3">
        No lanes configured.
      </div>
    );
  }

  const sorted = [...lanes].sort((a, b) => a.name.localeCompare(b.name));

  return (
    <Table className="table-fixed">
      <colgroup>
        <col />
        <col className="w-[80px]" />
        <col className="w-[70px]" />
        <col className="w-[80px]" />
        <col className="w-[80px]" />
        <col className="w-[80px]" />
        <col className="w-[60px]" />
      </colgroup>
      <TableHeader>
        <TableRow>
          <TableHead>Lane</TableHead>
          <TableHead>Cadence</TableHead>
          <TableHead className="text-right">24h</TableHead>
          <TableHead className="text-right">Rows</TableHead>
          <TableHead className="text-right">Avg dur</TableHead>
          <TableHead>Last</TableHead>
          <TableHead className="text-right">OK%</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {sorted.map((l) => {
          const s = perLane.get(l._id);
          const runs = s?.runs.length ?? 0;
          const okPct = runs > 0 ? Math.round((s!.ok / runs) * 100) : null;
          const avgDur =
            s && s.durs.length > 0
              ? s.durs.reduce((a, b) => a + b, 0) / s.durs.length
              : 0;
          const last = s?.last;
          return (
            <TableRow
              key={l._id}
              className={!l.enabled ? "opacity-50" : undefined}
            >
              <TableCell>
                <div className="min-w-0">
                  <span
                    className="text-xs truncate block"
                    title={l.name}
                  >
                    {l.name}{" "}
                    <span className="text-shift-muted">{l.sourcePlugin}</span>
                  </span>
                </div>
              </TableCell>
              <TableCell className="text-xs text-shift-muted font-mono">
                {l.schedule}
              </TableCell>
              <TableCell className="text-right text-xs font-mono tabular-nums">
                {runs > 0 ? (
                  <>
                    <span className="text-shift-ok">{s!.ok}</span>
                    {s!.err > 0 && (
                      <>
                        <span className="text-shift-muted">/</span>
                        <span className="text-shift-error">{s!.err}</span>
                      </>
                    )}
                  </>
                ) : (
                  <span className="text-shift-muted">—</span>
                )}
              </TableCell>
              <TableCell className="text-right text-xs font-mono tabular-nums">
                {s && s.rows > 0 ? formatRows(s.rows) : "—"}
              </TableCell>
              <TableCell className="text-right text-xs font-mono tabular-nums text-shift-muted">
                {avgDur > 0 ? formatMs(avgDur) : "—"}
              </TableCell>
              <TableCell className="text-xs text-shift-muted">
                {last ? formatAge(now - last.startedAt) : "never"}
              </TableCell>
              <TableCell
                className={`text-right text-xs font-mono tabular-nums ${okPct == null ? "text-shift-muted" : okPct === 100 ? "text-shift-ok" : okPct >= 80 ? "text-shift-warning" : "text-shift-error"}`}
              >
                {okPct == null ? "—" : `${okPct}%`}
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

// ────────────────────────────────────────────────────────────────────
// Worker fleet — sparkline-heavy
// ────────────────────────────────────────────────────────────────────

function WorkerList({
  workers,
  samples,
  now,
}: {
  workers: Worker[];
  samples: Sample[];
  now: number;
}) {
  if (workers.length === 0) {
    return (
      <div className="text-xs text-shift-muted px-2 py-3">
        No workers registered.
      </div>
    );
  }
  return (
    <div className="space-y-1.5">
      {workers.map((w) => {
        const mine = samples.filter((s) => s.workerId === w._id);
        const rss = mine.map((s) => s.rssMb);
        const load = mine.map((s) => s.loadAvg1m);
        const latest = mine[mine.length - 1];
        // Uptime: prefer the process-reported value from the latest
        // telemetry sample; fall back to (now - startedAt) so newly
        // registered workers don't render "—" while we wait for their
        // first sample (up to 10s window).
        const uptimeMs = latest
          ? latest.uptimeS * 1000
          : Math.max(0, now - w.startedAt);
        const stale = now - w.lastHeartbeat > HEARTBEAT_STALE_MS;
        const statusTone =
          stale || w.status === "offline"
            ? "text-shift-error"
            : w.status === "draining"
              ? "text-shift-warning"
              : w.status === "running"
                ? "text-shift-warning"
                : "text-shift-ok";
        return (
          <div
            key={w._id}
            className="flex items-center gap-2 text-xs px-1"
          >
            <div className="flex-1 min-w-0">
              <div className="flex items-baseline gap-1.5 min-w-0">
                <span
                  className={`size-1.5 rounded-full flex-none ${statusTone.replace("text-", "bg-")}`}
                />
                <span className="truncate font-medium" title={w.name}>
                  {w.name}
                </span>
              </div>
              <div className="text-[10px] text-shift-muted truncate">
                up {formatAge(uptimeMs)}
                {w.currentLaneId && " · running"}
              </div>
            </div>
            <div className="flex items-center gap-0.5">
              <Sparkline
                values={rss}
                className="text-shift-accent"
                fill="rgb(138 190 183 / 0.15)"
                width={60}
                height={18}
              />
              <span className="font-mono tabular-nums text-[10px] text-shift-muted w-[40px] text-right">
                {latest ? `${latest.rssMb.toFixed(0)}M` : "—"}
              </span>
            </div>
            <div className="flex items-center gap-0.5">
              <Sparkline
                values={load}
                className="text-shift-warning"
                width={40}
                height={18}
                yMin={0}
              />
              <span className="font-mono tabular-nums text-[10px] text-shift-muted w-[32px] text-right">
                {latest ? latest.loadAvg1m.toFixed(1) : "—"}
              </span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ────────────────────────────────────────────────────────────────────
// Warehouse grid — sorted by freshness, colored by compact age
// ────────────────────────────────────────────────────────────────────

function WarehouseGrid({
  tables,
  now,
}: {
  tables: WarehouseTable[];
  now: number;
}) {
  if (tables.length === 0) {
    return (
      <div className="text-xs text-shift-muted px-2 py-3">
        No warehouse tables.
      </div>
    );
  }
  const sorted = [...tables].sort((a, b) => {
    // Tables with raw pending compaction surface first.
    if (a.rawFiles !== b.rawFiles) return b.rawFiles - a.rawFiles;
    const ar = a.rows ?? 0;
    const br = b.rows ?? 0;
    return br - ar;
  });

  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6 gap-1.5">
      {sorted.map((t) => {
        const age = t.lastCompactAt ? now - t.lastCompactAt : null;
        const rawBad = t.rawFiles > 0;
        const borderTone = rawBad
          ? "border-shift-warning/40"
          : age && age > 2 * DAY_MS
            ? "border-shift-muted/30"
            : "border-[var(--border-muted)]";
        return (
          <div
            key={`${t.plugin}.${t.table}`}
            className={`rounded border ${borderTone} p-1.5 min-w-0`}
            title={`${t.plugin} · curated: ${t.curatedFiles} · raw: ${t.rawFiles}`}
          >
            <div
              className="text-[10px] truncate text-shift-accent"
              title={t.plugin}
            >
              {t.plugin}
            </div>
            <div
              className="text-xs truncate font-medium"
              title={t.table}
            >
              {t.table}
            </div>
            <div className="flex items-baseline justify-between text-[10px] mt-0.5">
              <span className="font-mono tabular-nums">
                {t.rows != null ? formatRows(t.rows) : "—"}
              </span>
              <span
                className={
                  rawBad ? "text-shift-warning" : "text-shift-muted"
                }
              >
                {rawBad
                  ? `+${t.rawFiles} raw`
                  : age
                    ? formatAge(age)
                    : "never"}
              </span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ────────────────────────────────────────────────────────────────────
// Formatters
// ────────────────────────────────────────────────────────────────────

function formatAge(ms: number): string {
  if (ms < 60_000) return `${Math.round(ms / 1000)}s`;
  if (ms < 3600_000) return `${Math.round(ms / 60_000)}m`;
  if (ms < DAY_MS) return `${Math.round(ms / 3600_000)}h`;
  return `${Math.round(ms / DAY_MS)}d`;
}

function formatMs(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  const m = Math.floor(ms / 60_000);
  const s = Math.floor((ms % 60_000) / 1000);
  return `${m}m${s > 0 ? `${s}s` : ""}`;
}

function formatRows(n: number): string {
  if (n < 1000) return String(n);
  if (n < 1_000_000) return `${(n / 1000).toFixed(1)}K`;
  if (n < 1_000_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  return `${(n / 1_000_000_000).toFixed(1)}B`;
}
