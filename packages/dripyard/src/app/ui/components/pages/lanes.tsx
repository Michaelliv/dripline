import { CheckCircle, Database, Play, Power } from "lucide-react";
import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { ProxiedBadge } from "./proxies";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
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
import { formatRelativeTime, mutate } from "@/lib/api";

type Lane = {
  _id: string;
  name: string;
  sourcePlugin: string;
  schedule: string;
  sinkType: string;
  enabled: boolean;
  proxyEnabled: boolean;
  createdAt: number;
  updatedAt: number;
};

export function LanesPage() {
  const lanes = useSubscription<Lane[]>("lanes.list") ?? [];
  const [errorDialog, setErrorDialog] = useState<{
    name: string;
    error: string;
  } | null>(null);

  const handleToggle = async (id: string, name: string) => {
    try {
      await mutate("lanes.toggle", { id });
    } catch (e: any) {
      setErrorDialog({
        name,
        error: e.message || "Failed to toggle lane",
      });
    }
  };

  const handleRun = async (id: string, name: string) => {
    try {
      // lanes.runNow dispatches through the orchestrator — actually
      // acquires the R2 lease, runs the sync, releases. runs.start
      // alone would just create an orphan row with no work attached.
      await mutate("lanes.runNow", { id });
    } catch (e: any) {
      setErrorDialog({ name, error: e.message || "Failed to start run" });
    }
  };

  return (
    <div className="flex flex-col h-full min-h-0 p-6 gap-4 overflow-hidden">
      <PageHeader
        title="Lanes"
        description={
          lanes.length === 0
            ? "None configured"
            : `${lanes.length} configured · ${lanes.filter((l) => l.enabled).length} enabled`
        }
      />

      {lanes.length === 0 ? (
        <div className="flex flex-1 items-center justify-center">
          <div className="text-center">
            <Database className="mx-auto size-8 text-shift-muted opacity-40" />
            <p className="mt-3 text-sm text-shift-muted">
              No lanes configured
            </p>
            <p className="mt-1 text-xs text-shift-muted">
              Create one with{" "}
              <code className="text-shift-accent">
                dripyard lane create
              </code>
            </p>
          </div>
        </div>
      ) : (
        <div className="flex-1 min-h-0 overflow-auto">
        <Table className="table-fixed">
          <colgroup>
            <col />
            <col className="w-[120px]" />
            <col className="w-[80px]" />
            <col className="w-[120px]" />
            <col className="w-[120px]" />
            <col className="w-[140px]" />
            <col className="w-[90px]" />
          </colgroup>
          <TableHeader>
            <TableRow>
              <TableHead>Name</TableHead>
              <TableHead>Source</TableHead>
              <TableHead>Sink</TableHead>
              <TableHead>Schedule</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Updated</TableHead>
              <TableHead className="text-right">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {lanes.map((p) => (
              <TableRow key={p._id}>
                <TableCell>
                  <div className="min-w-0">
                    <span
                      className="font-medium text-sm truncate inline-flex items-center gap-2 max-w-full"
                      title={p.name}
                    >
                      <span className="truncate">{p.name}</span>
                      {p.proxyEnabled && <ProxiedBadge />}
                    </span>
                  </div>
                </TableCell>
                <TableCell>
                  <code className="text-xs text-shift-muted">
                    {p.sourcePlugin}
                  </code>
                </TableCell>
                <TableCell>
                  <code className="text-xs text-shift-muted">
                    {p.sinkType}
                  </code>
                </TableCell>
                <TableCell>
                  <code className="text-xs text-shift-muted">
                    {p.schedule}
                  </code>
                </TableCell>
                <TableCell>
                  {p.enabled ? (
                    <Badge
                      variant="outline"
                      className="text-shift-ok border-shift-ok/30"
                    >
                      <CheckCircle className="size-3 mr-1" />
                      Enabled
                    </Badge>
                  ) : (
                    <Badge variant="outline" className="text-shift-muted">
                      Disabled
                    </Badge>
                  )}
                </TableCell>
                <TableCell className="text-sm text-shift-muted">
                  {formatRelativeTime(p.updatedAt)}
                </TableCell>
                <TableCell>
                  <div className="flex items-center justify-end gap-1">
                    <button
                      type="button"
                      onClick={() => handleToggle(p._id, p.name)}
                      className={`p-1 rounded hover:bg-sidebar-accent transition-colors ${p.enabled ? "text-shift-ok" : "text-shift-muted"}`}
                      title={p.enabled ? "Disable" : "Enable"}
                    >
                      <Power className="size-3.5" />
                    </button>
                    <button
                      type="button"
                      onClick={() => handleRun(p._id, p.name)}
                      className="p-1 rounded hover:bg-sidebar-accent transition-colors text-shift-muted hover:text-shift-text"
                      title="Run now"
                    >
                      <Play className="size-3.5" />
                    </button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
        </div>
      )}

      <Dialog
        open={!!errorDialog}
        onOpenChange={(open) => {
          if (!open) setErrorDialog(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Error: {errorDialog?.name}</DialogTitle>
            <DialogDescription>Operation failed with:</DialogDescription>
          </DialogHeader>
          <pre className="mt-2 rounded bg-sidebar-accent p-3 text-xs font-mono text-shift-error overflow-auto max-h-[300px]">
            {errorDialog?.error}
          </pre>
          <DialogFooter>
            <Button variant="outline" onClick={() => setErrorDialog(null)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
