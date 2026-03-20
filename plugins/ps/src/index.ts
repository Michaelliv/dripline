import type { DriplinePluginAPI } from "dripline";
import { syncExec } from "dripline";

export default function ps(dl: DriplinePluginAPI) {
  dl.setName("ps");
  dl.setVersion("0.1.0");

  dl.registerTable("ps_processes", {
    description: "Running processes on the system",
    columns: [
      { name: "pid", type: "number" },
      { name: "ppid", type: "number" },
      { name: "user", type: "string" },
      { name: "cpu", type: "number" },
      { name: "mem", type: "number" },
      { name: "rss_kb", type: "number" },
      { name: "vsz_kb", type: "number" },
      { name: "stat", type: "string" },
      { name: "started", type: "string" },
      { name: "time", type: "string" },
      { name: "command", type: "string" },
    ],
    *list() {
      // Use NUL as delimiter to handle spaces in commands
      const { raw } = syncExec(
        "ps",
        ["-eo", "pid=,ppid=,user=,pcpu=,pmem=,rss=,vsz=,stat=,lstart=,time=,command="],
        { parser: "raw" },
      );

      for (const line of raw.trim().split("\n")) {
        if (!line.trim()) continue;
        // ps output is fixed-width-ish, parse by splitting on whitespace with limits
        const parts = line.trim().split(/\s+/);
        if (parts.length < 11) continue;

        const pid = parseInt(parts[0], 10);
        const ppid = parseInt(parts[1], 10);
        const user = parts[2];
        const cpu = parseFloat(parts[3]);
        const mem = parseFloat(parts[4]);
        const rss = parseInt(parts[5], 10);
        const vsz = parseInt(parts[6], 10);
        const stat = parts[7];
        // lstart is like "Thu Mar 20 10:00:00 2026" — 5 fields
        const started = parts.slice(8, 13).join(" ");
        const time = parts[13];
        const command = parts.slice(14).join(" ");

        if (Number.isNaN(pid)) continue;

        yield { pid, ppid, user, cpu, mem, rss_kb: rss, vsz_kb: vsz, stat, started, time, command };
      }
    },
  });

  dl.registerTable("ps_ports", {
    description: "Processes listening on network ports (requires sudo for full results)",
    columns: [
      { name: "command", type: "string" },
      { name: "pid", type: "number" },
      { name: "user", type: "string" },
      { name: "protocol", type: "string" },
      { name: "local_address", type: "string" },
      { name: "local_port", type: "number" },
    ],
    *list() {
      const { raw } = syncExec(
        "lsof",
        ["-iTCP", "-sTCP:LISTEN", "-nP", "-F", "pcuPn"],
        { parser: "raw", ignoreExitCode: true },
      );

      let current: Record<string, any> = {};

      for (const line of raw.trim().split("\n")) {
        if (!line) continue;
        const tag = line[0];
        const value = line.slice(1);

        switch (tag) {
          case "p":
            if (current.pid !== undefined && current.local_port !== undefined) {
              yield { ...current };
            }
            current = { pid: parseInt(value, 10), protocol: "TCP" };
            break;
          case "c":
            current.command = value;
            break;
          case "u":
            current.user = value;
            break;
          case "P":
            current.protocol = value;
            break;
          case "n": {
            const match = value.match(/^(.*?):(\d+)$/);
            if (match) {
              current.local_address = match[1];
              current.local_port = parseInt(match[2], 10);
              yield { ...current };
              // Reset port info for next entry under same pid
              delete current.local_address;
              delete current.local_port;
            }
            break;
          }
        }
      }
    },
  });
}
