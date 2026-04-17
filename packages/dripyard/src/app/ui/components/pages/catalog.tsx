import { Database, Key, KeyRound } from "lucide-react";
import { useMemo, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
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

type CatalogEntry = {
  plugin: string;
  table: string;
  description: string | null;
  columns: Array<{ name: string; type: string }>;
  primaryKey: string[];
  cursor: string | null;
  keyColumns: Array<{ name: string; required: string }>;
  usedByLanes: Array<{ lane: string; params: Record<string, unknown> }>;
};

export function CatalogPage() {
  const entries = useSubscription<CatalogEntry[]>("workspace.catalog") ?? [];
  const [filter, setFilter] = useState("");

  const filtered = useMemo(() => {
    if (!filter) return entries;
    const q = filter.toLowerCase();
    return entries.filter(
      (e) =>
        e.table.toLowerCase().includes(q) ||
        e.plugin.toLowerCase().includes(q) ||
        (e.description ?? "").toLowerCase().includes(q) ||
        e.columns.some((c) => c.name.toLowerCase().includes(q)),
    );
  }, [entries, filter]);

  const pluginCount = useMemo(
    () => new Set(entries.map((e) => e.plugin)).size,
    [entries],
  );

  return (
    <div className="flex flex-col h-full min-h-0 p-6 gap-4 overflow-hidden">
      <PageHeader
        title="Catalog"
        description={
          entries.length === 0
            ? "No tables"
            : `${entries.length} tables · ${pluginCount} plugins${filter ? ` · ${filtered.length} match` : ""}`
        }
      />

      {entries.length > 0 && (
        <Input
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Filter tables, plugins, columns…"
          className="h-8 w-full text-xs flex-none"
        />
      )}

      {entries.length === 0 ? (
        <div className="flex flex-1 items-center justify-center">
          <div className="text-center">
            <Database className="mx-auto size-8 text-shift-muted opacity-40" />
            <p className="mt-3 text-sm text-shift-muted">
              No tables registered
            </p>
          </div>
        </div>
      ) : (
        <div className="flex-1 min-h-0 overflow-auto">
          <Table className="table-fixed">
            <colgroup>
              <col className="w-[140px]" />
              <col />
              <col className="w-[280px]" />
              <col className="w-[240px]" />
              <col className="w-[60px]" />
            </colgroup>
            <TableHeader>
              <TableRow>
                <TableHead>Plugin</TableHead>
                <TableHead>Table</TableHead>
                <TableHead>Keys / PK / Cursor</TableHead>
                <TableHead>Used by</TableHead>
                <TableHead className="text-right">Cols</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {filtered.map((e) => (
                <TableRow key={`${e.plugin}.${e.table}`}>
                  <TableCell>
                    <code className="text-xs text-shift-accent truncate block">
                      {e.plugin}
                    </code>
                  </TableCell>
                  <TableCell>
                    <div className="min-w-0">
                      <code
                        className="text-xs font-medium truncate block"
                        title={e.table}
                      >
                        {e.table}
                      </code>
                      {e.description && (
                        <span
                          className="text-xs text-shift-muted truncate block"
                          title={e.description}
                        >
                          {e.description}
                        </span>
                      )}
                    </div>
                  </TableCell>
                  <TableCell className="whitespace-normal">
                    <div className="flex flex-wrap gap-1 min-w-0">
                      {e.keyColumns.map((k) => (
                        <Badge
                          key={k.name}
                          variant="outline"
                          className={`text-xs max-w-full ${k.required === "required" ? "text-shift-accent" : "text-shift-muted"}`}
                          title={`key column (${k.required})`}
                        >
                          <KeyRound className="size-2.5 mr-0.5 flex-none" />
                          <span className="truncate">{k.name}</span>
                        </Badge>
                      ))}
                      {e.primaryKey.length > 0 && (
                        <Badge
                          variant="outline"
                          className="text-xs text-shift-ok max-w-full"
                          title="primary key"
                        >
                          <Key className="size-2.5 mr-0.5 flex-none" />
                          <span className="truncate">
                            {e.primaryKey.join("+")}
                          </span>
                        </Badge>
                      )}
                      {e.cursor && (
                        <Badge
                          variant="outline"
                          className="text-xs text-shift-warning max-w-full"
                          title={`incremental cursor: ${e.cursor}`}
                        >
                          <span className="truncate">↻ {e.cursor}</span>
                        </Badge>
                      )}
                      {e.keyColumns.length === 0 &&
                        e.primaryKey.length === 0 &&
                        !e.cursor && (
                          <span className="text-xs text-shift-muted">—</span>
                        )}
                    </div>
                  </TableCell>
                  <TableCell className="whitespace-normal">
                    {e.usedByLanes.length === 0 ? (
                      <span className="text-xs text-shift-muted">—</span>
                    ) : (
                      <div className="flex flex-wrap gap-1 min-w-0">
                        {e.usedByLanes.map((u, i) => (
                          <Badge
                            key={`${u.lane}-${i}`}
                            variant="outline"
                            className="text-xs max-w-full"
                            title={JSON.stringify(u.params, null, 2)}
                          >
                            <span className="truncate">{u.lane}</span>
                            {Object.keys(u.params).length > 0 && (
                              <span className="ml-1 text-shift-muted flex-none">
                                ×
                              </span>
                            )}
                          </Badge>
                        ))}
                      </div>
                    )}
                  </TableCell>
                  <TableCell className="text-right text-xs font-mono tabular-nums text-shift-muted">
                    {e.columns.length}
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
