import type { DriplinePluginAPI, QueryContext } from "dripline";
import { commandExists, syncExec } from "dripline";

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

function runTrivyScan(target: string, scanType: "image" | "fs" | "repo"): any {
  try {
    const { rows } = syncExec(
      "trivy",
      [scanType, "--format", "json", "--quiet", target],
      { parser: "json" },
    );
    return rows[0] || null;
  } catch {
    return null;
  }
}

export default function trivy(dl: DriplinePluginAPI) {
  dl.setName("trivy");
  dl.setVersion("0.1.0");

  // trivy_scan_vulnerability: scan a target for vulns
  // key columns: target (required), scan_type (optional: image|fs|repo, default: image)
  dl.registerTable("trivy_scan_vulnerability", {
    description:
      "Scan a container image or filesystem for vulnerabilities using Trivy",
    columns: [
      { name: "target", type: "string" },
      { name: "artifact_type", type: "string" },
      { name: "result_target", type: "string" },
      { name: "result_class", type: "string" },
      { name: "result_type", type: "string" },
      { name: "vulnerability_id", type: "string" },
      { name: "severity", type: "string" },
      { name: "package_name", type: "string" },
      { name: "installed_version", type: "string" },
      { name: "fixed_version", type: "string" },
      { name: "title", type: "string" },
      { name: "description", type: "string" },
      { name: "primary_url", type: "string" },
    ],
    keyColumns: [
      { name: "target", required: "required", operators: ["="] },
      { name: "scan_type", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      if (!commandExists("trivy")) return;
      const target = getQual(ctx, "target");
      if (!target) return;
      const scanType = (getQual(ctx, "scan_type") || "image") as
        | "image"
        | "fs"
        | "repo";

      const report = runTrivyScan(target, scanType);
      if (!report) return;

      const artifactType =
        report.ArtifactType || report.artifact_type || scanType;
      for (const result of report.Results || report.results || []) {
        for (const v of result.Vulnerabilities ||
          result.vulnerabilities ||
          []) {
          yield {
            target,
            artifact_type: artifactType,
            result_target: result.Target || result.target || "",
            result_class: result.Class || result.class || "",
            result_type: result.Type || result.type || "",
            vulnerability_id: v.VulnerabilityID || v.vulnerability_id || "",
            severity: v.Severity || v.severity || "",
            package_name: v.PkgName || v.pkg_name || "",
            installed_version: v.InstalledVersion || v.installed_version || "",
            fixed_version: v.FixedVersion || v.fixed_version || "",
            title: v.Title || v.title || "",
            description: (v.Description || v.description || "").slice(0, 500),
            primary_url: v.PrimaryURL || v.primary_url || "",
          };
        }
      }
    },
  });

  // trivy_scan_secret: scan for secrets
  dl.registerTable("trivy_scan_secret", {
    description: "Scan a filesystem for secrets using Trivy",
    columns: [
      { name: "target", type: "string" },
      { name: "result_target", type: "string" },
      { name: "rule_id", type: "string" },
      { name: "category", type: "string" },
      { name: "severity", type: "string" },
      { name: "title", type: "string" },
      { name: "match", type: "string" },
    ],
    keyColumns: [{ name: "target", required: "required", operators: ["="] }],
    *list(ctx) {
      if (!commandExists("trivy")) return;
      const target = getQual(ctx, "target");
      if (!target) return;
      try {
        const { rows } = syncExec(
          "trivy",
          ["fs", "--scanners", "secret", "--format", "json", "--quiet", target],
          { parser: "json" },
        );
        const report = rows[0] as any;
        if (!report) return;
        for (const result of report.Results || report.results || []) {
          for (const s of result.Secrets || result.secrets || []) {
            yield {
              target,
              result_target: result.Target || result.target || "",
              rule_id: s.RuleID || s.rule_id || "",
              category: s.Category || s.category || "",
              severity: s.Severity || s.severity || "",
              title: s.Title || s.title || "",
              match: s.Match || s.match || "",
            };
          }
        }
      } catch {
        // trivy might fail
      }
    },
  });

  // trivy_scan_package: list packages found in an artifact
  dl.registerTable("trivy_scan_package", {
    description: "List packages found in a container image or filesystem",
    columns: [
      { name: "target", type: "string" },
      { name: "result_target", type: "string" },
      { name: "result_class", type: "string" },
      { name: "result_type", type: "string" },
      { name: "package_name", type: "string" },
      { name: "package_version", type: "string" },
      { name: "package_id", type: "string" },
    ],
    keyColumns: [
      { name: "target", required: "required", operators: ["="] },
      { name: "scan_type", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      if (!commandExists("trivy")) return;
      const target = getQual(ctx, "target");
      if (!target) return;
      const scanType = (getQual(ctx, "scan_type") || "image") as
        | "image"
        | "fs"
        | "repo";
      try {
        const { rows } = syncExec(
          "trivy",
          [scanType, "--list-all-pkgs", "--format", "json", "--quiet", target],
          { parser: "json" },
        );
        const report = rows[0] as any;
        if (!report) return;
        for (const result of report.Results || report.results || []) {
          for (const p of result.Packages || result.packages || []) {
            yield {
              target,
              result_target: result.Target || result.target || "",
              result_class: result.Class || result.class || "",
              result_type: result.Type || result.type || "",
              package_name: p.Name || p.name || "",
              package_version: p.Version || p.version || "",
              package_id: p.ID || p.id || "",
            };
          }
        }
      } catch {
        // trivy might fail
      }
    },
  });
}
