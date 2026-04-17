import type { DriplinePluginAPI } from "dripline";
import { commandExists, syncExec } from "dripline";

export default function docker(dl: DriplinePluginAPI) {
  dl.setName("docker");
  dl.setVersion("0.1.0");

  dl.onInit(() => {
    if (!commandExists("docker")) {
      dl.log.warn(
        "docker not found on PATH — docker tables will be unavailable",
      );
    }
  });

  dl.registerTable("docker_containers", {
    description: "Running and stopped Docker containers",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "image", type: "string" },
      { name: "status", type: "string" },
      { name: "state", type: "string" },
      { name: "ports", type: "string" },
      { name: "created_at", type: "string" },
      { name: "labels", type: "json" },
      { name: "networks", type: "string" },
      { name: "mounts", type: "string" },
      { name: "size", type: "string" },
    ],
    keyColumns: [{ name: "show_all", required: "optional", operators: ["="] }],
    *list(ctx) {
      const showAll = ctx.quals.find((q) => q.column === "show_all")?.value;
      const args = ["ps", "--format", "{{json .}}", "--no-trunc"];
      if (showAll === "true" || showAll === true) args.push("--all");

      const { rows } = syncExec("docker", args, { parser: "jsonlines" });

      for (const r of rows) {
        yield {
          id: r.ID ?? r.id ?? "",
          name: (r.Names ?? r.names ?? "").replace(/^\//, ""),
          image: r.Image ?? r.image ?? "",
          status: r.Status ?? r.status ?? "",
          state: r.State ?? r.state ?? "",
          ports: r.Ports ?? r.ports ?? "",
          created_at: r.CreatedAt ?? r.created_at ?? "",
          labels: JSON.stringify(parseLabels(r.Labels ?? r.labels ?? "")),
          networks: r.Networks ?? r.networks ?? "",
          mounts: r.Mounts ?? r.mounts ?? "",
          size: r.Size ?? r.size ?? "",
        };
      }
    },
  });

  dl.registerTable("docker_images", {
    description: "Docker images on the host",
    columns: [
      { name: "id", type: "string" },
      { name: "repository", type: "string" },
      { name: "tag", type: "string" },
      { name: "created_at", type: "string" },
      { name: "size", type: "string" },
    ],
    *list() {
      const { rows } = syncExec(
        "docker",
        ["images", "--format", "{{json .}}", "--no-trunc"],
        {
          parser: "jsonlines",
        },
      );

      for (const r of rows) {
        yield {
          id: r.ID ?? r.id ?? "",
          repository: r.Repository ?? r.repository ?? "",
          tag: r.Tag ?? r.tag ?? "",
          created_at: r.CreatedAt ?? r.created_at ?? "",
          size: r.Size ?? r.size ?? "",
        };
      }
    },
  });

  dl.registerTable("docker_volumes", {
    description: "Docker volumes",
    columns: [
      { name: "name", type: "string" },
      { name: "driver", type: "string" },
      { name: "mountpoint", type: "string" },
      { name: "labels", type: "json" },
      { name: "scope", type: "string" },
    ],
    *list() {
      const { rows } = syncExec(
        "docker",
        ["volume", "ls", "--format", "{{json .}}"],
        {
          parser: "jsonlines",
        },
      );

      for (const r of rows) {
        yield {
          name: r.Name ?? r.name ?? "",
          driver: r.Driver ?? r.driver ?? "",
          mountpoint: r.Mountpoint ?? r.mountpoint ?? "",
          labels: JSON.stringify(parseLabels(r.Labels ?? r.labels ?? "")),
          scope: r.Scope ?? r.scope ?? "",
        };
      }
    },
  });

  dl.registerTable("docker_networks", {
    description: "Docker networks",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "driver", type: "string" },
      { name: "scope", type: "string" },
      { name: "ipv6", type: "boolean" },
      { name: "internal", type: "boolean" },
      { name: "labels", type: "json" },
    ],
    *list() {
      const { rows } = syncExec(
        "docker",
        ["network", "ls", "--format", "{{json .}}", "--no-trunc"],
        {
          parser: "jsonlines",
        },
      );

      for (const r of rows) {
        yield {
          id: r.ID ?? r.id ?? "",
          name: r.Name ?? r.name ?? "",
          driver: r.Driver ?? r.driver ?? "",
          scope: r.Scope ?? r.scope ?? "",
          ipv6: r.IPv6 === "true" || r.IPv6 === true ? 1 : 0,
          internal: r.Internal === "true" || r.Internal === true ? 1 : 0,
          labels: JSON.stringify(parseLabels(r.Labels ?? r.labels ?? "")),
        };
      }
    },
  });
}

function parseLabels(
  labels: string | Record<string, string>,
): Record<string, string> {
  if (typeof labels === "object") return labels;
  if (!labels) return {};
  const result: Record<string, string> = {};
  for (const pair of labels.split(",")) {
    const eq = pair.indexOf("=");
    if (eq > 0) {
      result[pair.slice(0, eq).trim()] = pair.slice(eq + 1).trim();
    }
  }
  return result;
}
