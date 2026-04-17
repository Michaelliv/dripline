import { Activity, CheckCircle, CircleDashed, XCircle } from "lucide-react";
import { useMemo } from "react";
import { Link } from "react-router-dom";
import { Badge } from "@/components/ui/badge";
import { PageHeader } from "@/components/ui/page-header";
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
  startedAt: number;
  finishedAt: number | null;
  rowsSynced: number;
  filesPublished: number;
  error: string | null;
  durationMs: number | null;
};

type Lane = { _id: string; name: string };

export function RunsPage() {
  const runs = useSubscription<Run[]>("runs.list", { limit: 100 }) ?? [];
  const lanes = useSubscription<Lane[]>("lanes.list") ?? [];
  const laneNameById = useMemo(() => {
    const m = new Map<string, string>();
    for (const l of lanes) m.set(l._id, l.name);
    return m;
  }, [lanes]);

  const ok = runs.filter((r) => r.status === "ok").length;
  const err = runs.filter((r) => r.status === "error").length;
  const running = runs.filter((r) => r.status === "running").length;

  return (
    <div className="flex flex-col h-full min-h-0 p-6 gap-4 overflow-hidden">
      <PageHeader
        title="Runs"
        description={
          runs.length === 0
            ? "No runs yet"
            : `${runs.length} recent · ${ok} ok · ${err} error · ${running} running`
        }
      />

      {runs.length === 0 ? (
        <div className="flex flex-1 items-center justify-center">
          <div className="text-center">
            <Activity className="mx-auto size-8 text-shift-muted opacity-40" />
            <p className="mt-3 text-sm text-shift-muted">No runs yet</p>
            <p className="mt-1 text-xs text-shift-muted">
              Lanes will record runs here when they execute
            </p>
          </div>
        </div>
      ) : (
        <div className="flex-1 min-h-0 overflow-auto">
        <Table className="table-fixed">
          <colgroup>
            <col className="w-[110px]" />
            <col className="w-[200px]" />
            <col className="w-[140px]" />
            <col className="w-[100px]" />
            <col className="w-[80px]" />
            <col className="w-[100px]" />
            <col />
          </colgroup>
          <TableHeader>
            <TableRow>
              <TableHead>Status</TableHead>
              <TableHead>Lane</TableHead>
              <TableHead>Started</TableHead>
              <TableHead className="text-right">Rows</TableHead>
              <TableHead className="text-right">Files</TableHead>
              <TableHead className="text-right">Duration</TableHead>
              <TableHead>Error</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {runs.map((r) => (
              <TableRow key={r._id} className="group">
                <TableCell>
                  <Link
                    to={`/runs/${r._id}`}
                    className="inline-block"
                    title="View run detail"
                  >
                    <StatusBadge status={r.status} />
                  </Link>
                </TableCell>
                <TableCell>
                  <span
                    className="text-sm truncate block"
                    title={r.laneId}
                  >
                    {laneNameById.get(r.laneId) ?? (
                      <code className="text-xs text-shift-muted">
                        {r.laneId}
                      </code>
                    )}
                  </span>
                </TableCell>
                <TableCell className="text-sm text-shift-muted">
                  {formatRelativeTime(r.startedAt)}
                </TableCell>
                <TableCell className="text-right text-sm font-mono">
                  {r.rowsSynced.toLocaleString()}
                </TableCell>
                <TableCell className="text-right text-sm font-mono">
                  {r.filesPublished}
                </TableCell>
                <TableCell className="text-right text-sm text-shift-muted font-mono">
                  {r.durationMs != null ? `${r.durationMs}ms` : "—"}
                </TableCell>
                <TableCell>
                  {r.error && (
                    <span
                      className="text-xs text-shift-error truncate block"
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
  );
}

function StatusBadge({
  status,
}: {
  status: "running" | "ok" | "error" | "skipped";
}) {
  if (status === "skipped") {
    return (
      <Badge variant="outline" className="text-shift-muted">
        Skipped
      </Badge>
    );
  }
  if (status === "running") {
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
  if (status === "ok") {
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
