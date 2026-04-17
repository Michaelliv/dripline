import { AlertCircle, Play } from "lucide-react";
import { useMemo, useState } from "react";
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
import { mutate } from "@/lib/api";

type QueryResult = {
  rows: Array<Record<string, unknown>>;
  rowCount: number;
  truncated: boolean;
  durationMs: number;
  attached: string[];
};

type CatalogEntry = { plugin: string; table: string };

export function QueryPage() {
  const catalog = useSubscription<CatalogEntry[]>("workspace.catalog") ?? [];

  // Derive samples from the live catalog so the Query page is generic.
  // First sample is always SHOW TABLES (workspace-agnostic). Second
  // and third pick the first discoverable table for SELECT and
  // COUNT(*) examples — falls back to a comment when the catalog is
  // empty so the user isn't confronted with `FROM <table>` nonsense.
  const samples = useMemo(() => {
    const first = catalog[0]?.table;
    if (!first)
      return [
        "SHOW TABLES",
        "-- No tables registered yet. Load plugins and configure lanes,\n-- then come back to query curated parquet here.",
      ];
    return [
      "SHOW TABLES",
      `SELECT COUNT(*) FROM ${first}`,
      `SELECT * FROM ${first} LIMIT 10`,
    ];
  }, [catalog]);

  const [sql, setSql] = useState("SHOW TABLES");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);

  const runQuery = async () => {
    if (!sql.trim()) return;
    setRunning(true);
    setError(null);
    try {
      const r = (await mutate("workspace.runSql", { sql })) as QueryResult;
      setResult(r);
    } catch (e: any) {
      setError(e?.message ?? "Query failed");
      setResult(null);
    } finally {
      setRunning(false);
    }
  };

  const onKey = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    // Cmd/Ctrl+Enter runs the query.
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault();
      runQuery();
    }
  };

  const columns =
    result && result.rows.length > 0 ? Object.keys(result.rows[0]) : [];

  return (
    <div className="flex flex-col h-full min-h-0 p-6 gap-4 overflow-hidden">
      <PageHeader
        title="Query"
        actions={
          <Button onClick={runQuery} disabled={running} size="sm">
            <Play className="size-3.5 mr-1" />
            {running ? "Running…" : "Run"}
          </Button>
        }
      />

      <div className="flex gap-2 flex-none">
        {samples.map((s, i) => (
          <button
            key={i}
            type="button"
            onClick={() => setSql(s)}
            className="text-xs text-shift-muted hover:text-shift-text"
          >
            sample {i + 1}
          </button>
        ))}
      </div>

      <textarea
        value={sql}
        onChange={(e) => setSql(e.target.value)}
        onKeyDown={onKey}
        spellCheck={false}
        className="font-mono text-sm bg-sidebar-accent/40 border border-sidebar-border rounded p-3 min-h-[140px] max-h-[260px] resize-y focus:outline-none focus:ring-1 focus:ring-shift-accent/30"
      />

      {error && (
        <div className="rounded border border-shift-error/30 bg-shift-error/5 p-3 flex gap-2 items-start">
          <AlertCircle className="size-4 text-shift-error mt-0.5" />
          <pre className="text-xs font-mono text-shift-error whitespace-pre-wrap">
            {error}
          </pre>
        </div>
      )}

      {result && (
        <>
          <div className="flex items-center gap-4 text-xs text-shift-muted flex-none">
            <span>
              {result.rows.length.toLocaleString()} row
              {result.rows.length === 1 ? "" : "s"}
              {result.truncated && (
                <span className="text-shift-warning ml-1">
                  (truncated from {result.rowCount.toLocaleString()})
                </span>
              )}
            </span>
            <span>·</span>
            <span>{result.durationMs}ms</span>
            {result.attached.length > 0 && (
              <>
                <span>·</span>
                <span>attached: {result.attached.join(", ")}</span>
              </>
            )}
          </div>

          {result.rows.length === 0 ? (
            <div className="text-sm text-shift-muted">No rows returned.</div>
          ) : (
            <div className="flex-1 min-h-0 overflow-auto">
              <Table>
                <TableHeader className="sticky top-0 bg-sidebar">
                  <TableRow>
                    {columns.map((c) => (
                      <TableHead key={c} className="text-xs">
                        {c}
                      </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {result.rows.map((r, i) => (
                    <TableRow key={i}>
                      {columns.map((c) => (
                        <TableCell
                          key={c}
                          className="text-xs font-mono tabular-nums"
                        >
                          {formatCell(r[c])}
                        </TableCell>
                      ))}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function formatCell(v: unknown): string {
  if (v == null) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
