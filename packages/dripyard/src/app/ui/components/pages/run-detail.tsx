import { useParams, Link } from "react-router-dom";
import { ArrowLeft, CheckCircle, CircleDashed, XCircle } from "lucide-react";
import { Badge } from "@/components/ui/badge";
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

type Run = {
  _id: string;
  laneId: string;
  status: "running" | "ok" | "error" | "skipped";
  reason: string | null;
  startedAt: number;
  finishedAt: number | null;
  rowsSynced: number;
  filesPublished: number;
  error: string | null;
  workerId: string | null;
  durationMs: number | null;
  tableResults?: Array<{
    _id: string;
    tableName: string;
    rowsInserted: number;
    cursor: string | null;
    durationMs: number;
    error: string | null;
  }>;
};

type Progress = {
  _id: string;
  runId: string;
  tableName: string;
  rowsInserted: number;
  cursor: string | null;
  elapsedMs: number;
  rate: number;
  timestamp: number;
};

type Lane = { _id: string; name: string };

export function RunDetailPage() {
  const { runId } = useParams<{ runId: string }>();
  const run = useSubscription<Run>("runs.get", { id: runId ?? "" });
  const progress =
    useSubscription<Progress[]>("runs.progress", { runId: runId ?? "" }) ?? [];
  const lanes = useSubscription<Lane[]>("lanes.list") ?? [];
  const laneName =
    run && run.laneId ? lanes.find((l) => l._id === run.laneId)?.name : null;

  if (!run) {
    return (
      <div className="flex flex-col h-full min-h-0 p-6 gap-4 overflow-hidden">
        <PageHeader
          title="Run"
          description="Loading…"
          back={{ to: "/runs", label: "All runs" }}
        />
        <div className="text-sm text-shift-muted">Loading run…</div>
      </div>
    );
  }

  // Group progress events by table for per-table sparklines
  const byTable = new Map<string, Progress[]>();
  for (const p of progress) {
    if (!byTable.has(p.tableName)) byTable.set(p.tableName, []);
    byTable.get(p.tableName)!.push(p);
  }

  const runLabel = `${run._id}${run.laneId ? ` · lane ${laneName ?? run.laneId}` : ""}`;

  return (
    <div className="flex flex-col h-full min-h-0 p-6 gap-4 overflow-hidden">
      <PageHeader
        title="Run detail"
        description={runLabel}
        back={{ to: "/runs", label: "All runs" }}
      />

      <div className="flex-1 min-h-0 overflow-auto space-y-4">

      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <SummaryTile label="Status" value={<StatusBadge run={run} />} />
        <SummaryTile
          label="Rows synced"
          value={
            <span className="text-2xl font-bold tabular-nums">
              {run.rowsSynced.toLocaleString()}
            </span>
          }
        />
        <SummaryTile
          label="Files published"
          value={
            <span className="text-2xl font-bold tabular-nums">
              {run.filesPublished}
            </span>
          }
        />
        <SummaryTile
          label="Duration"
          value={
            <span className="text-2xl font-bold tabular-nums">
              {run.durationMs != null
                ? formatDuration(run.durationMs)
                : `${formatDuration(Date.now() - run.startedAt)} so far`}
            </span>
          }
        />
      </div>

      {run.error && (
        <div className="rounded bg-sidebar-accent p-3">
          <p className="text-xs text-shift-muted mb-1">Error</p>
          <pre className="text-xs font-mono text-shift-error whitespace-pre-wrap">
            {run.error}
          </pre>
        </div>
      )}

      {/* Live progress per table */}
      {byTable.size > 0 && (
        <div className="space-y-2">
          <h3 className="text-sm font-medium">Live progress</h3>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Table</TableHead>
                <TableHead>Trend</TableHead>
                <TableHead className="text-right">Rows</TableHead>
                <TableHead className="text-right">Rate</TableHead>
                <TableHead>Cursor</TableHead>
                <TableHead>Last update</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {[...byTable.entries()].map(([tableName, events]) => {
                const latest = events[events.length - 1];
                const rows = events.map((e) => e.rowsInserted);
                return (
                  <TableRow key={tableName}>
                    <TableCell>
                      <code className="text-xs">{tableName}</code>
                    </TableCell>
                    <TableCell>
                      <Sparkline
                        values={rows}
                        className="text-shift-accent"
                        fill="rgb(138 190 183 / 0.15)"
                        width={120}
                        yMin={0}
                      />
                    </TableCell>
                    <TableCell className="text-right text-sm font-mono tabular-nums">
                      {latest.rowsInserted.toLocaleString()}
                    </TableCell>
                    <TableCell className="text-right text-sm font-mono tabular-nums text-shift-muted">
                      {latest.rate.toLocaleString()}/s
                    </TableCell>
                    <TableCell>
                      {latest.cursor && (
                        <code className="text-xs text-shift-muted">
                          {latest.cursor.length > 24
                            ? `${latest.cursor.slice(0, 24)}…`
                            : latest.cursor}
                        </code>
                      )}
                    </TableCell>
                    <TableCell className="text-xs text-shift-muted">
                      {formatRelativeTime(latest.timestamp)}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      )}

      {/* Finalized table results (populated on runs.finish) */}
      {run.tableResults && run.tableResults.length > 0 && (
        <div className="space-y-2">
          <h3 className="text-sm font-medium">Final table results</h3>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Table</TableHead>
                <TableHead className="text-right">Rows inserted</TableHead>
                <TableHead className="text-right">Duration</TableHead>
                <TableHead>Cursor</TableHead>
                <TableHead>Error</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {run.tableResults.map((r) => (
                <TableRow key={r._id}>
                  <TableCell>
                    <code className="text-xs">{r.tableName}</code>
                  </TableCell>
                  <TableCell className="text-right text-sm font-mono tabular-nums">
                    {r.rowsInserted.toLocaleString()}
                  </TableCell>
                  <TableCell className="text-right text-sm text-shift-muted font-mono">
                    {r.durationMs}ms
                  </TableCell>
                  <TableCell>
                    {r.cursor && (
                      <code className="text-xs text-shift-muted">
                        {r.cursor.length > 24
                          ? `${r.cursor.slice(0, 24)}…`
                          : r.cursor}
                      </code>
                    )}
                  </TableCell>
                  <TableCell>
                    {r.error && (
                      <span
                        className="text-xs text-shift-error truncate block max-w-[300px]"
                        title={r.error}
                      >
                        {r.error}
                      </span>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
      </div>
    </div>
  );
}

function SummaryTile({
  label,
  value,
}: {
  label: string;
  value: React.ReactNode;
}) {
  return (
    <div className="rounded border border-sidebar-border p-3">
      <p className="text-xs text-shift-muted mb-1">{label}</p>
      <div>{value}</div>
    </div>
  );
}

function StatusBadge({ run }: { run: Run }) {
  if (run.status === "running") {
    return (
      <Badge
        variant="outline"
        className="text-shift-warning border-shift-warning/30"
      >
        <CircleDashed className="size-3 mr-1 animate-spin" />
        Running
      </Badge>
    );
  }
  if (run.status === "skipped") {
    return (
      <Badge variant="outline" className="text-shift-muted">
        Skipped{run.reason ? ` — ${run.reason}` : ""}
      </Badge>
    );
  }
  if (run.status === "ok") {
    return (
      <Badge variant="outline" className="text-shift-ok border-shift-ok/30">
        <CheckCircle className="size-3 mr-1" />
        OK
      </Badge>
    );
  }
  return (
    <Badge variant="outline" className="text-shift-error border-shift-error/30">
      <XCircle className="size-3 mr-1" />
      Error
    </Badge>
  );
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  const m = Math.floor(ms / 60_000);
  const s = Math.floor((ms % 60_000) / 1000);
  return `${m}m ${s}s`;
}
