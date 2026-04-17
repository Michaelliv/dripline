import {
  CheckCircle,
  Loader2,
  Plus,
  Server,
  X,
  XCircle,
} from "lucide-react";
import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
import { formatRelativeTime, mutate } from "@/lib/api";

type Worker = {
  _id: string;
  name: string;
  status: "idle" | "running" | "offline" | "draining";
  host: string;
  lastHeartbeat: number;
  currentLaneId: string | null;
  currentRunId: string | null;
  startedAt: number;
  spawnHandle: string | null;
};

type Sample = {
  _id: string;
  workerId: string;
  timestamp: number;
  heapUsedMb: number;
  rssMb: number;
  loadAvg1m: number;
  uptimeS: number;
};

export function WorkersPage() {
  const workers = useSubscription<Worker[]>("workers.list") ?? [];
  const samples = useSubscription<Sample[]>("workers.samplesRecent", {}) ?? [];
  const [spawning, setSpawning] = useState(false);
  const [spawnError, setSpawnError] = useState<string | null>(null);

  const handleSpawn = async () => {
    setSpawning(true);
    setSpawnError(null);
    try {
      await mutate("workers.spawn", {});
    } catch (e: any) {
      setSpawnError(e?.message ?? "Failed to spawn worker");
    } finally {
      setSpawning(false);
    }
  };

  return (
    <div className="flex flex-col h-full min-h-0 p-6 gap-4 overflow-hidden">
      <PageHeader
        title="Workers"
        description={
          workers.length === 0
            ? "None registered"
            : `${workers.length} registered · auto-claim lanes via R2 leases`
        }
        actions={
          <>
            {spawnError && (
              <span
                className="text-xs text-shift-error truncate max-w-[240px]"
                title={spawnError}
              >
                {spawnError}
              </span>
            )}
            <Button
              size="sm"
              onClick={handleSpawn}
              disabled={spawning}
              className="gap-1"
            >
              <Plus className="size-3.5" />
              {spawning ? "Spawning…" : "+1 worker"}
            </Button>
          </>
        }
      />

      {workers.length === 0 ? (
        <div className="flex flex-1 items-center justify-center">
          <div className="text-center">
            <Server className="mx-auto size-8 text-shift-muted opacity-40" />
            <p className="mt-3 text-sm text-shift-muted">
              No workers registered
            </p>
            <p className="mt-1 text-xs text-shift-muted">
              Start a worker with{" "}
              <code className="text-shift-accent">dripyard serve</code>
            </p>
          </div>
        </div>
      ) : (
        <div className="flex-1 min-h-0 overflow-auto">
        <Table className="table-fixed">
          <colgroup>
            <col className="w-[260px]" />
            <col />
            <col className="w-[110px]" />
            <col className="w-[160px]" />
            <col className="w-[150px]" />
            <col className="w-[130px]" />
            <col className="w-[120px]" />
            <col className="w-[60px]" />
          </colgroup>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Host</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Current work</TableHead>
              <TableHead>Memory</TableHead>
              <TableHead>Load</TableHead>
              <TableHead>Last heartbeat</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            {workers.map((w) => {
              const mine = samples.filter((s) => s.workerId === w._id);
              const rssSeries = mine.map((s) => s.rssMb);
              const loadSeries = mine.map((s) => s.loadAvg1m);
              const latest = mine[mine.length - 1];

              return (
                <TableRow key={w._id}>
                  <TableCell>
                    <div className="min-w-0">
                      <span
                        className="font-medium text-sm truncate block"
                        title={w.name}
                      >
                        {w.name}
                      </span>
                      <span className="text-xs text-shift-muted truncate block">
                        {latest ? `up ${formatUptime(latest.uptimeS)}` : "—"}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell>
                    <code
                      className="text-xs text-shift-muted truncate block"
                      title={w.host}
                    >
                      {w.host}
                    </code>
                  </TableCell>
                  <TableCell>
                    <StatusBadge
                      status={w.status}
                      lastHeartbeat={w.lastHeartbeat}
                    />
                  </TableCell>
                  <TableCell>
                    {w.currentLaneId ? (
                      <code
                        className="text-xs text-shift-accent truncate block"
                        title={w.currentLaneId}
                      >
                        {w.currentLaneId}
                      </code>
                    ) : (
                      <span className="text-xs text-shift-muted">—</span>
                    )}
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-2">
                      <Sparkline
                        values={rssSeries}
                        className="text-shift-accent"
                        fill="rgb(138 190 183 / 0.15)"
                      />
                      <span className="text-xs text-shift-muted font-mono tabular-nums">
                        {latest ? `${latest.rssMb.toFixed(0)}MB` : "—"}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-2">
                      <Sparkline
                        values={loadSeries}
                        className="text-shift-warning"
                        yMin={0}
                      />
                      <span className="text-xs text-shift-muted font-mono tabular-nums">
                        {latest ? latest.loadAvg1m.toFixed(2) : "—"}
                      </span>
                    </div>
                  </TableCell>
                  <TableCell className="text-sm text-shift-muted">
                    {formatRelativeTime(w.lastHeartbeat)}
                  </TableCell>
                  <TableCell>
                    {w.spawnHandle ? (
                      <button
                        type="button"
                        title="Stop this worker (SIGTERM + deregister)"
                        onClick={() =>
                          mutate("workers.kill", { id: w._id }).catch((e) =>
                            setSpawnError(e?.message ?? "Failed to stop"),
                          )
                        }
                        className="p-1 rounded hover:bg-sidebar-accent transition-colors text-shift-muted hover:text-shift-error"
                      >
                        <X className="size-3.5" />
                      </button>
                    ) : (
                      <span
                        className="text-shift-muted/40 text-xs cursor-help"
                        title="Not spawned by this dashboard — managed externally (embedded, systemd, k8s, etc). Stop it through its own process manager."
                      >
                        —
                      </span>
                    )}
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
        </div>
      )}
    </div>
  );
}

function StatusBadge({
  status,
  lastHeartbeat,
}: {
  status: "idle" | "running" | "offline" | "draining";
  lastHeartbeat: number;
}) {
  // Draining is a legitimate alive state — don't let the stale-heartbeat
  // check mask it. A draining worker keeps heart-beating through the
  // grace window; if the heartbeat DOES fall stale mid-drain the worker
  // has crashed, and "Offline" is the correct signal.
  const stale = Date.now() - lastHeartbeat > 90_000;
  if (status === "draining" && !stale) {
    return (
      <Badge
        variant="outline"
        className="text-shift-warning border-shift-warning/30"
      >
        <Loader2 className="size-3 mr-1 animate-spin" />
        Draining
      </Badge>
    );
  }
  if (stale || status === "offline") {
    return (
      <Badge
        variant="outline"
        className="text-shift-error border-shift-error/30"
      >
        <XCircle className="size-3 mr-1" />
        Offline
      </Badge>
    );
  }
  if (status === "running") {
    return (
      <Badge
        variant="outline"
        className="text-shift-warning border-shift-warning/30"
      >
        Running
      </Badge>
    );
  }
  return (
    <Badge variant="outline" className="text-shift-ok border-shift-ok/30">
      <CheckCircle className="size-3 mr-1" />
      Idle
    </Badge>
  );
}

function formatUptime(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
  return `${Math.floor(seconds / 86400)}d`;
}
