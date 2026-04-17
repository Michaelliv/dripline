import { Key, KeyRound, Package } from "lucide-react";
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

type Plugin = {
  name: string;
  version: string;
  description: string | null;
  tableCount: number;
  tables: Array<{
    name: string;
    description: string | null;
    columnCount: number;
    hasCursor: boolean;
    cursor: string | null;
    hasPrimaryKey: boolean;
    primaryKey: string[];
    keyColumns: Array<{ name: string; required: string }>;
  }>;
  connections: string[];
};

export function PluginsPage() {
  const plugins = useSubscription<Plugin[]>("workspace.plugins") ?? [];

  const tableTotal = plugins.reduce((s, p) => s + p.tableCount, 0);

  return (
    <div className="flex flex-col h-full min-h-0 p-6 gap-4 overflow-hidden">
      <PageHeader
        title="Plugins"
        description={
          plugins.length === 0
            ? "None loaded"
            : `${plugins.length} loaded · ${tableTotal} tables`
        }
      />

      {plugins.length === 0 ? (
        <EmptyState />
      ) : (
        <div className="flex-1 min-h-0 overflow-auto space-y-6">
          {plugins.map((p) => (
            <PluginSection key={p.name} plugin={p} />
          ))}
        </div>
      )}
    </div>
  );
}

function EmptyState() {
  return (
    <div className="flex flex-1 items-center justify-center">
      <div className="text-center">
        <Package className="mx-auto size-8 text-shift-muted opacity-40" />
        <p className="mt-3 text-sm text-shift-muted">No plugins loaded</p>
        <p className="mt-1 text-xs text-shift-muted">
          Add entries to{" "}
          <code className="text-shift-accent">.dripline/plugins.json</code>,
          then restart
        </p>
      </div>
    </div>
  );
}

function PluginSection({ plugin: p }: { plugin: Plugin }) {
  return (
    <section className="space-y-2">
      <div className="flex items-baseline justify-between gap-4">
        <h2 className="text-sm font-semibold flex items-center gap-2 min-w-0">
          <Package className="size-3.5 text-shift-accent flex-none" />
          <span className="truncate">{p.name}</span>
          <span className="text-xs font-normal text-shift-muted flex-none">
            v{p.version}
          </span>
        </h2>
        <div className="flex gap-3 text-xs text-shift-muted flex-none">
          <span>{p.tableCount} tables</span>
          <span className="truncate max-w-[240px]">
            {p.connections.length > 0
              ? `conn: ${p.connections.join(", ")}`
              : "no connection"}
          </span>
        </div>
      </div>

      <Table className="table-fixed">
        <colgroup>
          <col className="w-[220px]" />
          <col />
          <col className="w-[220px]" />
          <col className="w-[180px]" />
          <col className="w-[60px]" />
        </colgroup>
        <TableHeader>
          <TableRow>
            <TableHead>Table</TableHead>
            <TableHead>Description</TableHead>
            <TableHead>Key columns</TableHead>
            <TableHead>PK / Cursor</TableHead>
            <TableHead className="text-right">Cols</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {p.tables.map((t) => (
            <TableRow key={t.name}>
              <TableCell>
                <code className="text-xs truncate block" title={t.name}>
                  {t.name}
                </code>
              </TableCell>
              <TableCell className="text-xs text-shift-muted">
                <span
                  className="block truncate"
                  title={t.description ?? undefined}
                >
                  {t.description ?? "—"}
                </span>
              </TableCell>
              <TableCell className="whitespace-normal">
                {t.keyColumns.length === 0 ? (
                  <span className="text-xs text-shift-muted">—</span>
                ) : (
                  <div className="flex flex-wrap gap-1 min-w-0">
                    {t.keyColumns.map((k) => (
                      <Badge
                        key={k.name}
                        variant="outline"
                        className={`text-xs max-w-full ${k.required === "required" ? "text-shift-accent" : "text-shift-muted"}`}
                        title={k.name}
                      >
                        <KeyRound className="size-2.5 mr-0.5 flex-none" />
                        <span className="truncate">{k.name}</span>
                      </Badge>
                    ))}
                  </div>
                )}
              </TableCell>
              <TableCell className="whitespace-normal">
                <div className="flex flex-wrap gap-1 min-w-0">
                  {t.hasPrimaryKey && (
                    <Badge
                      variant="outline"
                      className="text-xs text-shift-ok max-w-full"
                      title={`PK: ${t.primaryKey.join(", ")}`}
                    >
                      <Key className="size-2.5 mr-0.5 flex-none" />
                      <span className="truncate">
                        {t.primaryKey.join("+")}
                      </span>
                    </Badge>
                  )}
                  {t.hasCursor && (
                    <Badge
                      variant="outline"
                      className="text-xs text-shift-warning max-w-full"
                      title={`incremental cursor: ${t.cursor}`}
                    >
                      <span className="truncate">↻ {t.cursor}</span>
                    </Badge>
                  )}
                  {!t.hasPrimaryKey && !t.hasCursor && (
                    <span className="text-xs text-shift-muted">—</span>
                  )}
                </div>
              </TableCell>
              <TableCell className="text-right text-xs font-mono tabular-nums text-shift-muted">
                {t.columnCount}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </section>
  );
}
