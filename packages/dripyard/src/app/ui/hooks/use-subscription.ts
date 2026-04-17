import { useEffect, useSyncExternalStore } from "react";

/**
 * Shared subscription pool.
 *
 * One EventSource per unique `name+args` key, ref-counted across
 * all components. When the last consumer unsubscribes, the connection
 * closes immediately. New subscribers get the last known value.
 */

type Listener = () => void;

interface SharedSub {
  source: EventSource;
  listeners: Set<Listener>;
  data: unknown;
  refCount: number;
}

const pool = new Map<string, SharedSub>();

function acquire(key: string, name: string, argsStr: string): SharedSub {
  const existing = pool.get(key);
  if (existing) {
    existing.refCount++;
    return existing;
  }

  const params = new URLSearchParams({ name, args: argsStr });
  const source = new EventSource(`/subscribe?${params}`);

  const sub: SharedSub = {
    source,
    listeners: new Set(),
    data: null,
    refCount: 1,
  };

  source.onmessage = (event) => {
    try {
      sub.data = JSON.parse(event.data);
      for (const listener of sub.listeners) {
        listener();
      }
    } catch {}
  };

  pool.set(key, sub);
  return sub;
}

function release(key: string) {
  const sub = pool.get(key);
  if (!sub) return;
  sub.refCount--;
  if (sub.refCount <= 0) {
    sub.source.close();
    pool.delete(key);
  }
}

export function useSubscription<T>(
  name: string,
  args: Record<string, any> = {},
): T | null {
  const argsStr = JSON.stringify(args);
  const key = `${name}\0${argsStr}`;

  useEffect(() => {
    acquire(key, name, argsStr);
    return () => release(key);
  }, [key, name, argsStr]);

  return useSyncExternalStore(
    (onStoreChange) => {
      const sub = pool.get(key);
      if (!sub) return () => {};
      sub.listeners.add(onStoreChange);
      return () => sub.listeners.delete(onStoreChange);
    },
    () => {
      const sub = pool.get(key);
      return (sub?.data as T) ?? null;
    },
  );
}
