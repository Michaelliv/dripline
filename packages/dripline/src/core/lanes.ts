/**
 * Lane primitives — pure functions for parsing, validating, and naming
 * lanes. No I/O. The runner (commands/run.ts) composes these with the
 * lease store and the engine.
 *
 * A lane is a named slice of work synced on a schedule by exactly one
 * worker at a time. Workers compete for lanes via a single lease per
 * lane that does double duty:
 *
 *   - Phase 1 (work):     TTL = maxRuntime, prevents concurrent runs
 *   - Phase 2 (cooldown): renew with TTL = interval on success, prevents
 *                         re-runs until the next scheduled tick
 *
 * On success: renew → cooldown.
 * On crash:   release → next worker reclaims immediately.
 * On hang:    work TTL expires → next worker reclaims after maxRuntime.
 */

import type { LaneConfig, LaneTable } from "../config/types.js";

/** Default cap on a single run's wall-clock time. */
export const DEFAULT_MAX_RUNTIME_MS = 10 * 60 * 1000; // 10 minutes

// ── Interval parsing ─────────────────────────────────────────────────

const INTERVAL_RE = /^(\d+)(s|m|h|d)$/;
const UNIT_MS: Record<string, number> = {
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

/**
 * Parse a human-readable interval into milliseconds.
 *
 * Accepts `<n><unit>` where unit is `s|m|h|d`. Examples:
 *   "30s" → 30_000, "15m" → 900_000, "1h" → 3_600_000, "1d" → 86_400_000
 *
 * Throws on invalid input — never returns NaN or 0.
 */
export function parseInterval(spec: string): number {
  const m = INTERVAL_RE.exec(spec.trim());
  if (!m) {
    throw new Error(
      `invalid interval "${spec}": expected <n><unit> where unit is s|m|h|d (e.g. "15m")`,
    );
  }
  const n = Number(m[1]);
  if (n <= 0) throw new Error(`invalid interval "${spec}": must be positive`);
  return n * UNIT_MS[m[2]];
}

// ── Validation ───────────────────────────────────────────────────────

export interface ValidatedLane {
  name: string;
  tables: LaneTable[];
  intervalMs: number;
  maxRuntimeMs: number;
}

/**
 * Validate a lane config and resolve durations to milliseconds. Throws
 * with a human-readable error if anything is off, so the runner never
 * has to defensively check shapes at runtime.
 */
export function validateLane(name: string, lane: LaneConfig): ValidatedLane {
  if (!name || typeof name !== "string") {
    throw new Error("lane name must be a non-empty string");
  }
  if (!/^[a-z0-9][a-z0-9_-]*$/i.test(name)) {
    throw new Error(
      `lane name "${name}" must match /^[a-z0-9][a-z0-9_-]*$/i (letters, digits, _, -)`,
    );
  }
  if (!Array.isArray(lane.tables) || lane.tables.length === 0) {
    throw new Error(`lane "${name}": tables must be a non-empty array`);
  }
  for (const [i, t] of lane.tables.entries()) {
    if (!t || typeof t !== "object" || typeof t.name !== "string") {
      throw new Error(
        `lane "${name}": tables[${i}] must be { name: string, params?: object }`,
      );
    }
  }

  const intervalMs = parseInterval(lane.interval);
  // Default: cap at 10 minutes, but never let it equal or exceed the
  // interval itself — use half the interval for short-interval lanes
  // so the strict-less-than invariant below always holds.
  const maxRuntimeMs = lane.maxRuntime
    ? parseInterval(lane.maxRuntime)
    : Math.min(DEFAULT_MAX_RUNTIME_MS, Math.floor(intervalMs / 2));

  if (maxRuntimeMs >= intervalMs) {
    throw new Error(
      `lane "${name}": maxRuntime (${lane.maxRuntime ?? `${maxRuntimeMs}ms`}) must be less than interval (${lane.interval})`,
    );
  }

  return { name, tables: lane.tables, intervalMs, maxRuntimeMs };
}

/**
 * Validate every lane in a config map. Returns the validated lanes in
 * stable iteration order. Throws on the first invalid lane so misconfig
 * fails loudly at startup, not silently mid-run.
 */
export function validateLanes(
  lanes: Record<string, LaneConfig>,
): ValidatedLane[] {
  return Object.entries(lanes).map(([name, lane]) => validateLane(name, lane));
}

// ── Naming conventions ──────────────────────────────────────────────
// Centralized so the runner, the remote store, and any future tooling
// agree on object keys without scattered string interpolation.

/** Lease object key for a lane. Lives at `_leases/lane-<name>.json`. */
export function laneLeaseName(lane: string): string {
  return `lane-${lane}`;
}

/** State path for a lane's cursor metadata. */
export function laneStatePath(lane: string): string {
  return `_state/${lane}/_dripline_sync.parquet`;
}

/** DuckDB schema name for a lane's local tables (must be SQL-safe). */
export function laneSchema(lane: string): string {
  // Lane names already match /^[a-z0-9][a-z0-9_-]*$/i. DuckDB schema
  // names allow letters, digits, and underscore — replace `-` with `_`.
  return `lane_${lane.replace(/-/g, "_")}`;
}
