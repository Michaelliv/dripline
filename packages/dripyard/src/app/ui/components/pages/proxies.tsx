import { AlertTriangle, Globe, Power, RefreshCw } from "lucide-react";
import { useMemo, useState } from "react";
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
import { mutate } from "@/lib/api";

type ProxyStatus = {
  available: boolean;
  workerCount: number;
  totalRequests: number;
};

type Worker = {
  _id: string;
  name: string;
  url: string;
  createdOn: string | null;
};

type Sample = {
  _id: string;
  timestamp: number;
  workerCount: number;
  totalRequests: number;
  perWorker: Record<string, number>;
};

export function ProxiesPage() {
  const status = useSubscription<ProxyStatus>("proxies.status") ?? {
    available: false,
    workerCount: 0,
    totalRequests: 0,
  };
  const workers = useSubscription<Worker[]>("proxies.list") ?? [];
  const samples = useSubscription<Sample[]>("proxies.samplesRecent", {}) ?? [];

  const [scaleTarget, setScaleTarget] = useState(5);
  const [busy, setBusy] = useState<"scale" | "down" | "refresh" | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Per-sample request rate (req/s) — the stored totalRequests is
  // cumulative, so take first-differences against the prior sample.
  // Flat zero when there's nothing to differentiate against yet.
  const requestRateSeries = useMemo(() => {
    if (samples.length < 2) return [];
    const out: number[] = [];
    for (let i = 1; i < samples.length; i++) {
      const a = samples[i - 1];
      const b = samples[i];
      const dReq = b.totalRequests - a.totalRequests;
      const dSec = (b.timestamp - a.timestamp) / 1000;
      out.push(dSec > 0 ? Math.max(0, dReq / dSec) : 0);
    }
    return out;
  }, [samples]);

  const latestRate = requestRateSeries.at(-1) ?? 0;

  const runMutation = async (
    kind: "scale" | "down" | "refresh",
    args: Record<string, unknown>,
  ) => {
    setBusy(kind);
    setError(null);
    try {
      await mutate(`proxies.${kind}`, args);
    } catch (e: any) {
      setError(e?.message ?? `Failed to ${kind}`);
    } finally {
      setBusy(null);
    }
  };

  return (
    <div className="flex flex-col h-full min-h-0 p-6 gap-4 overflow-hidden">
      <PageHeader
        title="Proxies"
        description={
          !status.available
            ? "Flaregun not configured"
            : `${status.workerCount} worker${status.workerCount === 1 ? "" : "s"} · ${status.totalRequests.toLocaleString()} request${status.totalRequests === 1 ? "" : "s"} · ${latestRate.toFixed(1)} req/s`
        }
        actions={
          status.available ? (
            <>
              {error && (
                <span
                  className="text-xs text-shift-error truncate max-w-[240px]"
                  title={error}
                >
                  {error}
                </span>
              )}
              <input
                type="number"
                min={0}
                value={scaleTarget}
                onChange={(e) => setScaleTarget(Number(e.target.value))}
                className="w-16 h-8 px-2 text-sm bg-[var(--panel)] border border-[var(--border-muted)] rounded focus:outline-none focus:border-shift-accent font-mono tabular-nums"
              />
              <Button
                size="sm"
                onClick={() =>
                  runMutation("scale", { count: scaleTarget })
                }
                disabled={busy !== null}
              >
                {busy === "scale" ? "Scaling…" : "Scale"}
              </Button>
              <Button
                size="sm"
                variant="outline"
                onClick={() => runMutation("refresh", {})}
                disabled={busy !== null}
                title="Resync worker list from Cloudflare"
              >
                <RefreshCw className="size-3.5" />
              </Button>
              <Button
                size="sm"
                variant="outline"
                onClick={() => runMutation("down", {})}
                disabled={busy !== null || status.workerCount === 0}
                title="Tear down all workers"
                className="text-shift-error hover:text-shift-error"
              >
                <Power className="size-3.5" />
              </Button>
            </>
          ) : null
        }
      />

      {!status.available ? (
        <div className="flex flex-1 items-center justify-center">
          <div className="text-center max-w-md">
            <AlertTriangle className="mx-auto size-8 text-shift-warning opacity-60" />
            <p className="mt-3 text-sm text-shift-foam">
              Flaregun is not configured
            </p>
            <p className="mt-2 text-xs text-shift-muted leading-relaxed">
              Set{" "}
              <code className="text-shift-accent">CLOUDFLARE_API_TOKEN</code>{" "}
              and{" "}
              <code className="text-shift-accent">CLOUDFLARE_ACCOUNT_ID</code>{" "}
              in the server environment and restart. Lanes with
              proxyEnabled will fail to run until this is set.
            </p>
          </div>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-3 gap-3">
            <KpiTile label="Workers" value={status.workerCount.toString()} />
            <KpiTile
              label="Total requests"
              value={status.totalRequests.toLocaleString()}
            />
            <KpiTile
              label="Request rate"
              value={`${latestRate.toFixed(1)}/s`}
              sparkline={requestRateSeries}
            />
          </div>

          {workers.length === 0 ? (
            <div className="flex flex-1 items-center justify-center">
              <div className="text-center">
                <Globe className="mx-auto size-8 text-shift-muted opacity-40" />
                <p className="mt-3 text-sm text-shift-muted">
                  No proxy workers deployed
                </p>
                <p className="mt-1 text-xs text-shift-muted">
                  Set a count above and press Scale to deploy workers
                </p>
              </div>
            </div>
          ) : (
            <div className="flex-1 min-h-0 overflow-auto">
              <Table className="table-fixed">
                <colgroup>
                  <col className="w-[260px]" />
                  <col />
                  <col className="w-[140px]" />
                  <col className="w-[140px]" />
                </colgroup>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>URL</TableHead>
                    <TableHead className="text-right">Requests</TableHead>
                    <TableHead>Created</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {workers.map((w) => {
                    const latestSample = samples.at(-1);
                    const count = latestSample?.perWorker?.[w.name] ?? 0;
                    return (
                      <TableRow key={w._id}>
                        <TableCell>
                          <span
                            className="font-medium text-sm truncate block"
                            title={w.name}
                          >
                            {w.name}
                          </span>
                        </TableCell>
                        <TableCell>
                          <code
                            className="text-xs text-shift-muted truncate block"
                            title={w.url}
                          >
                            {w.url}
                          </code>
                        </TableCell>
                        <TableCell className="text-right font-mono tabular-nums text-sm">
                          {count.toLocaleString()}
                        </TableCell>
                        <TableCell className="text-sm text-shift-muted">
                          {formatCreated(w.createdOn)}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function KpiTile({
  label,
  value,
  sparkline,
}: {
  label: string;
  value: string;
  sparkline?: number[];
}) {
  return (
    <div className="bg-[var(--panel)]/40 border border-[var(--border-muted)] rounded p-3">
      <div className="text-xs text-shift-muted uppercase tracking-wide">
        {label}
      </div>
      <div className="mt-1 flex items-end justify-between gap-3">
        <span className="text-2xl font-mono tabular-nums text-shift-foam">
          {value}
        </span>
        {sparkline && sparkline.length > 1 && (
          <Sparkline
            values={sparkline}
            className="text-shift-accent"
            fill="rgb(138 190 183 / 0.15)"
          />
        )}
      </div>
    </div>
  );
}

function formatCreated(iso: string | null): string {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return "—";
    return d.toISOString().slice(0, 10);
  } catch {
    return "—";
  }
}

/**
 * Shared badge used on the Lanes page to flag proxyEnabled lanes.
 * Lives here so the visual language for "this lane routes through
 * flaregun" is defined in one place.
 */
export function ProxiedBadge() {
  return (
    <Badge
      variant="outline"
      className="text-shift-accent border-shift-accent/30"
      title="Routes through flaregun proxy workers"
    >
      <Globe className="size-3 mr-1" />
      proxied
    </Badge>
  );
}
