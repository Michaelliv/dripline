import type { Vex } from "vex-core";
import type { Orchestrator } from "./orchestrator.js";

export class Scheduler {
  private vex: Vex;
  private orchestrator: Orchestrator;
  private managedJobs: Set<string> = new Set();
  private workerId?: string;

  constructor(vex: Vex, orchestrator: Orchestrator) {
    this.vex = vex;
    this.orchestrator = orchestrator;
  }

  /** Set the worker ID for heartbeat and run attribution. */
  setWorkerId(id: string) {
    this.workerId = id;
  }

  /**
   * Read all enabled lanes and register a vex job per lane.
   *
   * Heartbeat is *not* registered here — that's the job of whoever owns
   * the worker row (the dashboard's telemetry.sample job for the
   * embedded worker, or the worker binary's own heartbeat timer for
   * standalone workers). Keeping heartbeat out of the scheduler means
   * dashboard-only mode (no embedded worker) doesn't ship a dead job.
   */
  async start(): Promise<void> {
    await this.refresh();
  }

  /** Reconcile jobs with current lane state. */
  async refresh(): Promise<void> {
    const lanes = await this.vex.query("lanes.list");
    const desired = new Map<string, { schedule: string; laneId: string }>();

    for (const p of lanes) {
      if (!p.enabled) continue;
      const jobName = `sync.${sanitize(p.name)}`;
      desired.set(jobName, { schedule: p.schedule, laneId: p._id });
    }

    // Remove jobs for lanes that no longer exist or are disabled.
    for (const jobName of this.managedJobs) {
      if (!desired.has(jobName)) {
        await this.vex.removeJob(jobName);
        this.managedJobs.delete(jobName);
      }
    }

    // Add or update jobs for enabled lanes. Dripline lanes store
    // their schedule in bare form ("6h", "15m"); vex-core's addJob
    // expects "every <interval>". Normalize so config files written
    // for either system still work.
    for (const [jobName, { schedule, laneId }] of desired) {
      const orchestrator = this.orchestrator;
      const workerId = this.workerId;
      await this.vex.addJob(jobName, {
        schedule: normalizeSchedule(schedule),
        description: `Sync lane: ${laneId}`,
        async handler() {
          await orchestrator.executeLane(laneId, workerId);
        },
      });
      this.managedJobs.add(jobName);
    }
  }

  /** Remove all managed jobs. */
  async stop(): Promise<void> {
    for (const jobName of this.managedJobs) {
      await this.vex.removeJob(jobName);
    }
    this.managedJobs.clear();
  }

  /** List currently managed job names. */
  listJobs(): string[] {
    return [...this.managedJobs];
  }
}

function sanitize(name: string): string {
  return name.replace(/[^a-z0-9_]/gi, "_").toLowerCase();
}

function normalizeSchedule(spec: string): string {
  const trimmed = spec.trim();
  return /^every\s+/i.test(trimmed) ? trimmed : `every ${trimmed}`;
}
