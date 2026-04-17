import { AlertCircle, HardDrive, Layers } from "lucide-react";
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

type WarehouseTable = {
  plugin: string;
  table: string;
  rawFiles: number;
  curatedFiles: number;
  rows: number | null;
  lastCompactAt: number | null;
  partitions: string[];
};

type WarehouseData = {
  remote: {
    endpoint: string;
    bucket: string;
    prefix: string | null;
    secretType?: string;
    error?: string | null;
  } | null;
  tables: WarehouseTable[];
};

export function WarehousePage() {
  const data = useSubscription<WarehouseData>("workspace.warehouse");

  if (!data) {
    return (
      <PageShell description="Loading…">
        <div className="text-sm text-shift-muted">Loading warehouse…</div>
      </PageShell>
    );
  }

  if (!data.remote) {
    return (
      <PageShell description="No remote configured">
        <div className="flex flex-1 items-center justify-center">
          <div className="text-center">
            <HardDrive className="mx-auto size-8 text-shift-muted opacity-40" />
            <p className="mt-3 text-sm text-shift-muted">
              No remote configured
            </p>
            <p className="mt-1 text-xs text-shift-muted">
              Run <code className="text-shift-accent">dripline remote set …</code>{" "}
              to attach an R2/S3 bucket
            </p>
          </div>
        </div>
      </PageShell>
    );
  }

  if (data.remote.error) {
    return (
      <PageShell description={`${data.remote.bucket} · unreachable`}>
        <div className="rounded border border-shift-error/30 bg-shift-error/5 p-3 flex gap-2 items-start">
          <AlertCircle className="size-4 text-shift-error mt-0.5 flex-none" />
          <div className="min-w-0">
            <p className="text-sm text-shift-error">
              Remote configured but unreachable
            </p>
            <pre className="text-xs font-mono text-shift-muted mt-1 whitespace-pre-wrap break-all">
              {data.remote.error}
            </pre>
            <p className="text-xs text-shift-muted mt-2 truncate">
              {data.remote.endpoint} · bucket {data.remote.bucket}
            </p>
          </div>
        </div>
      </PageShell>
    );
  }

  const totalRaw = data.tables.reduce((s, t) => s + t.rawFiles, 0);
  const totalCurated = data.tables.reduce((s, t) => s + t.curatedFiles, 0);
  const totalRows = data.tables.reduce((s, t) => s + (t.rows ?? 0), 0);
  const stale = data.tables.filter((t) => t.rawFiles > 0).length;

  const bucketLabel = data.remote.prefix
    ? `${data.remote.bucket} / ${data.remote.prefix}`
    : data.remote.bucket;

  return (
    <PageShell
      description={`${bucketLabel} · ${data.remote.endpoint.replace(/^https?:\/\//, "")}`}
    >
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 flex-none">
        <Tile label="Rows (curated)" value={totalRows.toLocaleString()} />
        <Tile label="Curated files" value={totalCurated.toLocaleString()} />
        <Tile label="Raw uncompacted" value={totalRaw.toLocaleString()} />
        <Tile
          label="Need compact"
          value={String(stale)}
          hint={stale === 0 ? "All caught up" : `${stale} with new raw`}
          tone={stale > 0 ? "warning" : "ok"}
        />
      </div>

      <div className="flex-1 min-h-0 overflow-auto">
        <Table className="table-fixed">
          <colgroup>
            <col className="w-[120px]" />
            <col />
            <col className="w-[100px]" />
            <col className="w-[100px]" />
            <col className="w-[100px]" />
            <col className="w-[180px]" />
            <col className="w-[120px]" />
          </colgroup>
          <TableHeader>
            <TableRow>
              <TableHead>Plugin</TableHead>
              <TableHead>Table</TableHead>
              <TableHead className="text-right">Rows</TableHead>
              <TableHead className="text-right">Curated</TableHead>
              <TableHead className="text-right">Raw</TableHead>
              <TableHead>Partitions</TableHead>
              <TableHead>Compacted</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.tables.map((t) => (
              <TableRow key={`${t.plugin}.${t.table}`}>
                <TableCell>
                  <code className="text-xs text-shift-accent truncate block">
                    {t.plugin}
                  </code>
                </TableCell>
                <TableCell>
                  <code className="text-xs truncate block" title={t.table}>
                    {t.table}
                  </code>
                </TableCell>
                <TableCell className="text-right text-xs font-mono tabular-nums">
                  {t.rows != null ? t.rows.toLocaleString() : "—"}
                </TableCell>
                <TableCell className="text-right text-xs font-mono tabular-nums text-shift-muted">
                  {t.curatedFiles || "—"}
                </TableCell>
                <TableCell className="text-right text-xs font-mono tabular-nums">
                  {t.rawFiles > 0 ? (
                    <span className="text-shift-warning">{t.rawFiles}</span>
                  ) : (
                    <span className="text-shift-muted">0</span>
                  )}
                </TableCell>
                <TableCell>
                  {t.partitions.length === 0 ? (
                    <span className="text-xs text-shift-muted">—</span>
                  ) : (
                    <div className="flex flex-wrap gap-1">
                      {t.partitions.map((p) => (
                        <Badge
                          key={p}
                          variant="outline"
                          className="text-xs text-shift-muted"
                        >
                          <Layers className="size-2.5 mr-0.5" />
                          {p}
                        </Badge>
                      ))}
                    </div>
                  )}
                </TableCell>
                <TableCell className="text-xs text-shift-muted truncate">
                  {t.lastCompactAt
                    ? formatRelativeTime(t.lastCompactAt)
                    : "never"}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </PageShell>
  );
}

function PageShell({
  children,
  description,
}: {
  children: React.ReactNode;
  description?: string;
}) {
  return (
    <div className="flex flex-col h-full min-h-0 p-6 gap-4 overflow-hidden">
      <PageHeader title="Warehouse" description={description} />
      {children}
    </div>
  );
}

function Tile({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string;
  hint?: string;
  tone?: "ok" | "warning";
}) {
  const toneClass =
    tone === "warning"
      ? "text-shift-warning"
      : tone === "ok"
        ? "text-shift-ok"
        : "";
  return (
    <div className="rounded border border-sidebar-border p-3 min-w-0">
      <p className="text-xs text-shift-muted mb-1 truncate">{label}</p>
      <p className={`text-xl font-bold tabular-nums truncate ${toneClass}`}>
        {value}
      </p>
      {hint && (
        <p className="text-xs text-shift-muted mt-1 truncate">{hint}</p>
      )}
    </div>
  );
}
