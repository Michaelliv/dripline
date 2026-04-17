import { Link2 } from "lucide-react";
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

type Connection = {
  name: string;
  plugin: string;
  config: Record<string, unknown>;
};

export function ConnectionsPage() {
  const connections =
    useSubscription<Connection[]>("workspace.connections") ?? [];

  return (
    <div className="flex flex-col h-full min-h-0 p-6 gap-4 overflow-hidden">
      <PageHeader
        title="Connections"
        description={
          connections.length === 0
            ? "None configured"
            : `${connections.length} configured · edit .dripline/config.json to change`
        }
      />

      {connections.length === 0 ? (
        <div className="flex flex-1 items-center justify-center">
          <div className="text-center">
            <Link2 className="mx-auto size-8 text-shift-muted opacity-40" />
            <p className="mt-3 text-sm text-shift-muted">
              No connections configured
            </p>
          </div>
        </div>
      ) : (
        <div className="flex-1 min-h-0 overflow-auto">
          <Table className="table-fixed">
            <colgroup>
              <col className="w-[200px]" />
              <col className="w-[140px]" />
              <col />
            </colgroup>
            <TableHeader>
              <TableRow>
                <TableHead>Name</TableHead>
                <TableHead>Plugin</TableHead>
                <TableHead>Config</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {connections.map((c) => (
                <TableRow key={c.name}>
                  <TableCell>
                    <span
                      className="text-xs font-medium truncate block"
                      title={c.name}
                    >
                      {c.name}
                    </span>
                  </TableCell>
                  <TableCell>
                    <code className="text-xs text-shift-accent truncate block">
                      {c.plugin}
                    </code>
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-wrap gap-x-3 gap-y-0.5">
                      {Object.entries(c.config).map(([k, v]) => (
                        <span key={k} className="text-xs truncate max-w-full">
                          <span className="text-shift-muted">{k}:</span>{" "}
                          <code
                            className={
                              v === "***"
                                ? "text-shift-muted"
                                : "text-shift-text"
                            }
                          >
                            {typeof v === "string" ? v : JSON.stringify(v)}
                          </code>
                        </span>
                      ))}
                    </div>
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
