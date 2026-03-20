import type { DriplinePluginAPI } from "dripline";
import { syncExec } from "dripline";

export default function systemProfiler(dl: DriplinePluginAPI) {
  dl.setName("system-profiler");
  dl.setVersion("0.1.0");

  function getProfilerData(dataType: string): any[] {
    const { rows: [data] } = syncExec("system_profiler", [dataType, "-json"], { parser: "json" });
    return data?.[dataType] ?? [];
  }

  dl.registerTable("sys_software", {
    description: "macOS software overview (version, kernel, uptime)",
    columns: [
      { name: "os_version", type: "string" },
      { name: "kernel_version", type: "string" },
      { name: "boot_volume", type: "string" },
      { name: "boot_mode", type: "string" },
      { name: "computer_name", type: "string" },
      { name: "user_name", type: "string" },
      { name: "uptime", type: "string" },
      { name: "secure_vm", type: "boolean" },
      { name: "system_integrity", type: "boolean" },
    ],
    *list() {
      const items = getProfilerData("SPSoftwareDataType");
      for (const s of items) {
        yield {
          os_version: s.os_version ?? "",
          kernel_version: s.kernel_version ?? "",
          boot_volume: s.boot_volume ?? "",
          boot_mode: s.boot_mode ?? "",
          computer_name: s.local_host_name ?? "",
          user_name: s.user_name ?? "",
          uptime: s.uptime ?? "",
          secure_vm: s.secure_vm === "secure_vm_enabled" ? 1 : 0,
          system_integrity: s.system_integrity === "integrity_enabled" ? 1 : 0,
        };
      }
    },
  });

  dl.registerTable("sys_hardware", {
    description: "Mac hardware info (model, chip, memory)",
    columns: [
      { name: "model_name", type: "string" },
      { name: "model_identifier", type: "string" },
      { name: "chip", type: "string" },
      { name: "total_cores", type: "number" },
      { name: "performance_cores", type: "number" },
      { name: "efficiency_cores", type: "number" },
      { name: "memory", type: "string" },
      { name: "serial_number", type: "string" },
      { name: "hardware_uuid", type: "string" },
    ],
    *list() {
      const items = getProfilerData("SPHardwareDataType");
      for (const h of items) {
        yield {
          model_name: h.machine_name ?? "",
          model_identifier: h.machine_model ?? "",
          chip: h.chip_type ?? "",
          total_cores: parseInt(h.number_processors?.match(/\d+/)?.[0] ?? "0", 10) || null,
          performance_cores: parseInt(h.number_performance_cores?.match(/\d+/)?.[0] ?? "0", 10) || null,
          efficiency_cores: parseInt(h.number_efficiency_cores?.match(/\d+/)?.[0] ?? "0", 10) || null,
          memory: h.physical_memory ?? "",
          serial_number: h.serial_number ?? "",
          hardware_uuid: h.platform_UUID ?? "",
        };
      }
    },
  });

  dl.registerTable("sys_network_interfaces", {
    description: "Network interfaces and their configuration",
    columns: [
      { name: "name", type: "string" },
      { name: "type", type: "string" },
      { name: "hardware", type: "string" },
      { name: "ipv4_address", type: "string" },
      { name: "ipv6_address", type: "string" },
      { name: "mac_address", type: "string" },
      { name: "media_subtype", type: "string" },
      { name: "is_active", type: "boolean" },
    ],
    *list() {
      const items = getProfilerData("SPNetworkDataType");
      for (const n of items) {
        const ipv4 = n.IPv4?.Addresses?.[0] ?? n.ip_address?.[0] ?? "";
        const ipv6 = n.IPv6?.Addresses?.[0] ?? "";
        yield {
          name: n._name ?? "",
          type: n.type ?? "",
          hardware: n.hardware ?? "",
          ipv4_address: ipv4,
          ipv6_address: ipv6,
          mac_address: n.Ethernet?.["MAC Address"] ?? "",
          media_subtype: n["media_subtype"] ?? "",
          is_active: n.interface?.toLowerCase() !== "off" && ipv4 ? 1 : 0,
        };
      }
    },
  });

  dl.registerTable("sys_storage", {
    description: "Storage volumes and disks",
    columns: [
      { name: "name", type: "string" },
      { name: "mount_point", type: "string" },
      { name: "file_system", type: "string" },
      { name: "size", type: "string" },
      { name: "free_space", type: "string" },
      { name: "writable", type: "boolean" },
      { name: "device_name", type: "string" },
      { name: "physical_drive_type", type: "string" },
    ],
    *list() {
      const items = getProfilerData("SPStorageDataType");
      for (const v of items) {
        yield {
          name: v._name ?? "",
          mount_point: v.mount_point ?? "",
          file_system: v.file_system ?? "",
          size: v.size_in_bytes ? formatBytes(v.size_in_bytes) : v.free_space_in_bytes ? "" : "",
          free_space: v.free_space_in_bytes ? formatBytes(v.free_space_in_bytes) : "",
          writable: v.writable === "yes" ? 1 : 0,
          device_name: v.physical_drive?.device_name ?? v.device_name ?? "",
          physical_drive_type: v.physical_drive?.medium_type ?? "",
        };
      }
    },
  });

  dl.registerTable("sys_displays", {
    description: "Connected displays",
    columns: [
      { name: "name", type: "string" },
      { name: "vendor_id", type: "string" },
      { name: "resolution", type: "string" },
      { name: "pixels", type: "string" },
      { name: "retina", type: "boolean" },
      { name: "connection_type", type: "string" },
      { name: "main_display", type: "boolean" },
    ],
    *list() {
      const items = getProfilerData("SPDisplaysDataType");
      for (const gpu of items) {
        const displays = gpu.spdisplays_ndrvs ?? [];
        for (const d of displays) {
          yield {
            name: d._name ?? "",
            vendor_id: d._spdisplays_display_vendor_id ?? "",
            resolution: d._spdisplays_resolution ?? "",
            pixels: d._spdisplays_pixels ?? "",
            retina: d.spdisplays_retina === "spdisplays_yes" ? 1 : 0,
            connection_type: d.spdisplays_connection_type ?? "",
            main_display: d.spdisplays_main === "spdisplays_yes" ? 1 : 0,
          };
        }
      }
    },
  });
}

function formatBytes(bytes: number): string {
  if (bytes >= 1e12) return `${(bytes / 1e12).toFixed(1)} TB`;
  if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(1)} GB`;
  if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(1)} MB`;
  return `${bytes} B`;
}
