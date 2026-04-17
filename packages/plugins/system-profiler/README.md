# dripline-plugin-system-profiler

macOS system_profiler plugin for [dripline](https://github.com/Michaelliv/dripline) — query hardware, software, network, storage, and display info with SQL.

## Install

```bash
dripline plugin install git:github.com/Michaelliv/dripline#packages/plugins/system-profiler
```

macOS only. Uses `system_profiler -json`.

## Tables

| Table | Description |
|-------|-------------|
| `sys_software` | macOS version, kernel, uptime |
| `sys_hardware` | Model, chip, memory, cores |
| `sys_network_interfaces` | Network interfaces and IPs |
| `sys_storage` | Disks and volumes |
| `sys_displays` | Connected displays |

## Examples

### System overview

```sql
SELECT os_version, kernel_version, computer_name, uptime FROM sys_software;
```

```
┌────────────────────┬────────────────┬────────────────┬──────────────┐
│ os_version         │ kernel_version │ computer_name  │ uptime       │
├────────────────────┼────────────────┼────────────────┼──────────────┤
│ macOS 15.5 (24F74) │ Darwin 24.5.0  │ Janes-MacBook  │ up 3:12:45:0 │
└────────────────────┴────────────────┴────────────────┴──────────────┘
```

### Hardware info

```sql
SELECT model_name, chip, memory, total_cores FROM sys_hardware;
```

```
┌─────────────┬──────────────┬────────┬─────────────┐
│ model_name  │ chip         │ memory │ total_cores │
├─────────────┼──────────────┼────────┼─────────────┤
│ MacBook Pro │ Apple M3 Pro │ 36 GB  │ 12          │
└─────────────┴──────────────┴────────┴─────────────┘
```

### Storage volumes

```sql
SELECT name, mount_point, file_system, size, free_space FROM sys_storage;
```

```
┌──────────────┬──────────────┬─────────────┬──────────┬────────────┐
│ name         │ mount_point  │ file_system │ size     │ free_space │
├──────────────┼──────────────┼─────────────┼──────────┼────────────┤
│ Macintosh HD │ /            │ APFS        │ 994.7 GB │ 420.3 GB   │
└──────────────┴──────────────┴─────────────┴──────────┴────────────┘
```

### Connected displays

```sql
SELECT name, resolution, retina, main_display FROM sys_displays;
```

### Active network interfaces

```sql
SELECT name, type, ipv4_address, mac_address
FROM sys_network_interfaces
WHERE is_active = true;
```
