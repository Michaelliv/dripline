/**
 * HTTP helpers for talking to the dripyard server.
 * The vite dev server proxies /vex/* to localhost:3457.
 */

export async function query<T = any>(
  name: string,
  args: Record<string, any> = {},
): Promise<T> {
  const res = await fetch("/vex/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, args }),
  });
  const data = await res.json();
  if (data.error) throw new Error(data.error);
  return data.data;
}

export async function mutate<T = any>(
  name: string,
  args: Record<string, any> = {},
): Promise<T> {
  const res = await fetch("/vex/mutate", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name, args }),
  });
  const data = await res.json();
  if (data.error) throw new Error(data.error);
  return data.data;
}

export function formatRelativeTime(ts: number, future = false): string {
  const diff = future ? ts - Date.now() : Date.now() - ts;
  if (diff < 0) return future ? "Now" : "Just now";
  if (diff < 5_000) return future ? "<5s" : "Just now";
  if (diff < 60_000)
    return `${Math.floor(diff / 1000)}s${future ? "" : " ago"}`;
  if (diff < 3_600_000)
    return `${Math.floor(diff / 60_000)}m${future ? "" : " ago"}`;
  if (diff < 86_400_000)
    return `${Math.floor(diff / 3_600_000)}h${future ? "" : " ago"}`;
  return new Date(ts).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}
