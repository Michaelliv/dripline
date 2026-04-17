/**
 * `dripline lane add / list / remove` — manage lanes in
 * `.dripline/config.json`.
 *
 * Each lane is a named group of tables synced together on a schedule.
 * Validation happens at `add` time via `validateLane()` so a bad lane
 * can never land in the config file — errors are surfaced immediately
 * instead of halfway through a `dripline run`.
 *
 * Deliberately omitted: `lane edit`. Remove and re-add is simpler and
 * avoids the "edit a running lane" race entirely.
 */

import chalk from "chalk";
import { loadConfig, saveConfig } from "../config/loader.js";
import type { LaneConfig, LaneTable } from "../config/types.js";
import { validateLane } from "../core/lanes.js";
import { Remote, resolveRemote } from "../core/remote.js";
import { bold, dim, info, success, warn } from "../utils/output.js";

/**
 * Thrown for any user-facing config error. `main.ts` catches this and
 * converts it to `process.exit(1)` with a clean message; tests catch
 * it as an ordinary exception. Same pattern as `RunConfigError`.
 */
export class LaneConfigError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LaneConfigError";
  }
}

export interface LaneAddOptions {
  /**
   * Repeated --table flag. Each entry is just the table name. Params
   * are attached via the parallel --params flag in the same order.
   */
  table: string[];
  /**
   * Repeated --params flag, one per --table in matching order. Each
   * value is a comma-separated k=v list: "owner=acme,repo=api".
   * Missing entries or the literal "-" mean "no params for this table".
   */
  params?: string[];
  interval: string;
  maxRuntime?: string;
  /** Overwrite an existing lane with the same name. */
  force?: boolean;
  json?: boolean;
}

/**
 * Parse a single "k=v,k=v" string into an object. Returns undefined
 * for empty / placeholder inputs so we omit the field entirely.
 */
function parseParams(
  spec: string | undefined,
): Record<string, unknown> | undefined {
  if (!spec || spec === "-") return undefined;
  const out: Record<string, unknown> = {};
  for (const pair of spec.split(",")) {
    const eq = pair.indexOf("=");
    if (eq < 0) {
      throw new Error(`invalid --params entry "${pair}": expected key=value`);
    }
    const k = pair.slice(0, eq).trim();
    const v = pair.slice(eq + 1).trim();
    if (!k) throw new Error(`invalid --params entry "${pair}": empty key`);
    out[k] = v;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

export async function laneAdd(
  name: string,
  options: LaneAddOptions,
): Promise<void> {
  if (!options.table || options.table.length === 0) {
    throw new LaneConfigError("at least one --table is required");
  }
  if (!options.interval) {
    throw new LaneConfigError("--interval is required (e.g. --interval 15m)");
  }

  // Pair up tables with their corresponding --params entry by index.
  // A missing or "-" params slot leaves that table param-less.
  const tables: LaneTable[] = [];
  for (let i = 0; i < options.table.length; i++) {
    try {
      const params = parseParams(options.params?.[i]);
      tables.push(
        params
          ? { name: options.table[i], params }
          : { name: options.table[i] },
      );
    } catch (e) {
      throw new LaneConfigError(`lane "${name}": ${(e as Error).message}`);
    }
  }

  const lane: LaneConfig = {
    tables,
    interval: options.interval,
    ...(options.maxRuntime ? { maxRuntime: options.maxRuntime } : {}),
  };

  // Validate BEFORE writing — bad lanes should never touch disk.
  try {
    validateLane(name, lane);
  } catch (e) {
    throw new LaneConfigError(`lane "${name}": ${(e as Error).message}`);
  }

  const config = loadConfig();
  if (config.lanes[name] && !options.force) {
    throw new LaneConfigError(
      `lane "${name}" already exists. Use --force to overwrite.`,
    );
  }
  config.lanes[name] = lane;
  saveConfig(config);

  if (options.json) {
    console.log(JSON.stringify({ success: true, name, lane }));
  } else {
    success(
      `Added lane ${bold(name)} (${tables.length} table${tables.length === 1 ? "" : "s"}, every ${options.interval})`,
    );
  }
}

export async function laneRemove(
  name: string,
  options: { json?: boolean },
): Promise<void> {
  const config = loadConfig();
  if (!config.lanes[name]) {
    if (options.json) {
      console.log(JSON.stringify({ success: false, name }));
      return;
    }
    throw new LaneConfigError(`Lane not found: ${name}`);
  }
  delete config.lanes[name];
  saveConfig(config);

  if (options.json) {
    console.log(JSON.stringify({ success: true, name }));
  } else {
    success(`Removed lane ${bold(name)}`);
    console.log(
      dim(
        "  Note: R2 state (_state/<lane>/, _leases/lane-<lane>.json) is not deleted.",
      ),
    );
  }
}

/**
 * Reset a lane's sync state in the remote bucket: delete
 * `_state/<lane>/_dripline_sync.parquet` so the next `dripline run`
 * sees "first sync ever" for every table in the lane and backfills
 * according to each plugin's `initialCursor`.
 *
 * Also releases the lane's lease (if any) so a run can start
 * immediately without waiting for cooldown. Does NOT touch `raw/` or
 * `curated/` — those stay as historical data.
 */
export async function laneReset(
  name: string,
  options: { yes?: boolean; json?: boolean; hard?: boolean },
): Promise<void> {
  const config = loadConfig();
  if (!config.lanes[name]) {
    throw new LaneConfigError(`Lane not found: ${name}`);
  }
  if (!config.remote) {
    throw new LaneConfigError(
      "no remote configured. Set one with `dripline remote set` first.",
    );
  }

  if (!options.yes && !options.json) {
    const desc = options.hard
      ? `This will delete _state/${name}/ and release the lane lease. raw/ and curated/ are untouched.`
      : `This will release the lane lease for ${name}. Cursor state is preserved.`;
    warn(desc);
    throw new LaneConfigError("Re-run with --yes to confirm.");
  }

  resolveRemote(config.remote);
  const remote = new Remote(config.remote);

  // Wipe cursor state only with --hard.
  let stateKeys: string[] = [];
  if (options.hard) {
    stateKeys = await remote.listObjects(`_state/${name}/`);
    if (stateKeys.length > 0) {
      await remote.deleteObjects(stateKeys);
    }
  }

  // Also drop the lease so the next run can start immediately rather
  // than waiting for cooldown to expire. The lease store is
  // holder-checked on write, not on existence, so a plain delete is
  // safe.
  const leaseKey = `_leases/lane-${name}.json`;
  const leaseKeys = await remote.listObjects(leaseKey);
  if (leaseKeys.length > 0) {
    await remote.deleteObjects(leaseKeys);
  }

  if (options.json) {
    console.log(
      JSON.stringify({
        success: true,
        lane: name,
        stateDeleted: stateKeys.length,
        leaseDeleted: leaseKeys.length,
      }),
    );
    return;
  }

  if (options.hard) {
    success(
      `Reset lane ${bold(name)}: ${stateKeys.length} state file(s), ${leaseKeys.length} lease(s) deleted.`,
    );
    info("Next `dripline run` will backfill per each table's initialCursor.");
  } else {
    success(`Released lease for lane ${bold(name)}. Cursor state preserved.`);
  }
}

export async function laneList(options: { json?: boolean }): Promise<void> {
  const config = loadConfig();
  const names = Object.keys(config.lanes);

  if (options.json) {
    console.log(JSON.stringify({ lanes: config.lanes }));
    return;
  }

  if (names.length === 0) {
    console.log("No lanes configured.");
    console.log(
      dim(
        "  Add one: dripline lane add <name> --table <t> --interval <dur>",
      ),
    );
    return;
  }

  console.log();
  for (const name of names) {
    const lane = config.lanes[name];
    const maxRt = lane.maxRuntime ? ` (max ${lane.maxRuntime})` : "";
    console.log(
      `  ${chalk.cyan(name)}  ${lane.tables.length} table${lane.tables.length === 1 ? "" : "s"}  every ${lane.interval}${dim(maxRt)}`,
    );
    for (const t of lane.tables) {
      const p = t.params
        ? dim(
            ` (${Object.entries(t.params)
              .map(([k, v]) => `${k}=${v}`)
              .join(", ")})`,
          )
        : "";
      console.log(`    ${dim("·")} ${t.name}${p}`);
    }
  }
  console.log();
}
