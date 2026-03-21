import type { DriplinePluginAPI } from "dripline";
import { syncExec } from "dripline";

export default function net(dl: DriplinePluginAPI) {
  dl.setName("net");
  dl.setVersion("0.1.0");

  dl.registerTable("net_dns", {
    description: "DNS lookup for a domain",
    columns: [
      { name: "domain", type: "string" },
      { name: "record_type", type: "string" },
      { name: "name", type: "string" },
      { name: "value", type: "string" },
      { name: "ttl", type: "number" },
    ],
    keyColumns: [
      { name: "domain", required: "required", operators: ["="] },
      { name: "record_type", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const domain = ctx.quals.find((q) => q.column === "domain")?.value;
      if (!domain) return;
      const types = (
        ctx.quals.find((q) => q.column === "record_type")?.value ||
        "A,AAAA,MX,NS,TXT,CNAME,SOA"
      ).split(",");

      for (const rtype of types) {
        try {
          const { rows } = syncExec(
            "dig",
            ["+short", "+ttlid", rtype.trim(), domain],
            { parser: "lines" },
          );
          for (const r of rows) {
            const line = (r as any).line?.trim();
            if (!line) continue;
            yield {
              domain,
              record_type: rtype.trim(),
              name: domain,
              value: line,
              ttl: 0,
            };
          }
        } catch {
          // dig might not be available or fail
        }
      }
    },
  });

  dl.registerTable("net_certificate", {
    description: "TLS certificate info for a domain",
    columns: [
      { name: "domain", type: "string" },
      { name: "issuer", type: "string" },
      { name: "subject", type: "string" },
      { name: "not_before", type: "string" },
      { name: "not_after", type: "string" },
      { name: "serial", type: "string" },
      { name: "san", type: "string" },
    ],
    keyColumns: [{ name: "domain", required: "required", operators: ["="] }],
    *list(ctx) {
      const domain = ctx.quals.find((q) => q.column === "domain")?.value;
      if (!domain) return;
      try {
        const { rows } = syncExec(
          "bash",
          [
            "-c",
            `echo | openssl s_client -servername ${domain} -connect ${domain}:443 2>/dev/null | openssl x509 -noout -issuer -subject -dates -serial -ext subjectAltName 2>/dev/null`,
          ],
          { parser: "kv" },
        );

        const kv: Record<string, string> = {};
        for (const r of rows) {
          const row = r as any;
          if (row.key && row.value) kv[row.key.trim()] = row.value.trim();
        }

        yield {
          domain,
          issuer: kv.issuer || "",
          subject: kv.subject || "",
          not_before: kv.notBefore || "",
          not_after: kv.notAfter || "",
          serial: kv.serial || "",
          san: kv["X509v3 Subject Alternative Name"] || "",
        };
      } catch {
        // openssl might fail
      }
    },
  });

  dl.registerTable("net_ping", {
    description: "Ping a host and get latency stats",
    columns: [
      { name: "host", type: "string" },
      { name: "packets_sent", type: "number" },
      { name: "packets_received", type: "number" },
      { name: "packet_loss_pct", type: "number" },
      { name: "min_ms", type: "number" },
      { name: "avg_ms", type: "number" },
      { name: "max_ms", type: "number" },
    ],
    keyColumns: [{ name: "host", required: "required", operators: ["="] }],
    *list(ctx) {
      const host = ctx.quals.find((q) => q.column === "host")?.value;
      if (!host) return;
      try {
        const { rows } = syncExec("ping", ["-c", "4", "-W", "2", host], {
          parser: "raw",
        });
        const output = (rows[0] as any)?.output || "";
        // Parse: 4 packets transmitted, 4 packets received, 0.0% packet loss
        const statsMatch = output.match(
          /(\d+) packets transmitted, (\d+) (?:packets )?received, ([\d.]+)% packet loss/,
        );
        // Parse: round-trip min/avg/max/stddev = 1.234/5.678/9.012/3.456 ms
        const rttMatch = output.match(/= ([\d.]+)\/([\d.]+)\/([\d.]+)/);

        yield {
          host,
          packets_sent: statsMatch ? Number(statsMatch[1]) : 0,
          packets_received: statsMatch ? Number(statsMatch[2]) : 0,
          packet_loss_pct: statsMatch ? Number(statsMatch[3]) : 100,
          min_ms: rttMatch ? Number(rttMatch[1]) : 0,
          avg_ms: rttMatch ? Number(rttMatch[2]) : 0,
          max_ms: rttMatch ? Number(rttMatch[3]) : 0,
        };
      } catch {
        yield {
          host,
          packets_sent: 0,
          packets_received: 0,
          packet_loss_pct: 100,
          min_ms: 0,
          avg_ms: 0,
          max_ms: 0,
        };
      }
    },
  });

  dl.registerTable("net_whois", {
    description: "WHOIS lookup for a domain",
    columns: [
      { name: "domain", type: "string" },
      { name: "registrar", type: "string" },
      { name: "creation_date", type: "string" },
      { name: "expiry_date", type: "string" },
      { name: "name_servers", type: "string" },
      { name: "status", type: "string" },
    ],
    keyColumns: [{ name: "domain", required: "required", operators: ["="] }],
    *list(ctx) {
      const domain = ctx.quals.find((q) => q.column === "domain")?.value;
      if (!domain) return;
      try {
        const { rows } = syncExec("whois", [domain], { parser: "raw" });
        const output = (rows[0] as any)?.output || "";
        const get = (key: string) => {
          const m = output.match(new RegExp(`${key}:\\s*(.+)`, "i"));
          return m ? m[1].trim() : "";
        };
        const ns = (output.match(/Name Server:\s*(.+)/gi) || [])
          .map((l: string) => l.replace(/Name Server:\s*/i, "").trim())
          .join(", ");

        yield {
          domain,
          registrar: get("Registrar") || get("registrar"),
          creation_date: get("Creation Date") || get("created"),
          expiry_date: get("Registry Expiry Date") || get("Expiration Date"),
          name_servers: ns,
          status: get("Domain Status") || get("Status"),
        };
      } catch {
        // whois might not be available
      }
    },
  });
}
