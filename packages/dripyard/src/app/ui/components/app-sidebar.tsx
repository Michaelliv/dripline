import {
  Activity,
  Database,
  Globe,
  HardDrive,
  LayoutDashboard,
  Library,
  Link2,
  Package,
  Server,
  Terminal,
} from "lucide-react";
import { useLocation, useNavigate } from "react-router-dom";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarSeparator,
} from "@/components/ui/sidebar";

type SidebarPage =
  | "overview"
  | "lanes"
  | "runs"
  | "workers"
  | "proxies"
  | "plugins"
  | "connections"
  | "catalog"
  | "warehouse"
  | "query";

const NAV_ITEMS: Array<{
  page: SidebarPage;
  path: string;
  label: string;
  icon: typeof Activity;
}> = [
  { page: "overview", path: "/", label: "Overview", icon: LayoutDashboard },
  { page: "lanes", path: "/lanes", label: "Lanes", icon: Database },
  { page: "runs", path: "/runs", label: "Runs", icon: Activity },
  { page: "workers", path: "/workers", label: "Workers", icon: Server },
  { page: "proxies", path: "/proxies", label: "Proxies", icon: Globe },
  { page: "plugins", path: "/plugins", label: "Plugins", icon: Package },
  { page: "catalog", path: "/catalog", label: "Catalog", icon: Library },
  {
    page: "warehouse",
    path: "/warehouse",
    label: "Warehouse",
    icon: HardDrive,
  },
  { page: "query", path: "/query", label: "Query", icon: Terminal },
  {
    page: "connections",
    path: "/connections",
    label: "Connections",
    icon: Link2,
  },
];

function pageFromPath(pathname: string): SidebarPage {
  const seg = pathname.split("/")[1] || "overview";
  const valid: SidebarPage[] = [
    "overview",
    "lanes",
    "runs",
    "workers",
    "proxies",
    "plugins",
    "connections",
    "catalog",
    "warehouse",
    "query",
  ];
  return valid.includes(seg as SidebarPage) ? (seg as SidebarPage) : "overview";
}

export function AppSidebar() {
  const navigate = useNavigate();
  const location = useLocation();
  const activePage = pageFromPath(location.pathname);

  return (
    <Sidebar>
      <SidebarHeader className="p-2">
        <div className="px-2 py-1">
          <span className="text-sm font-medium text-sidebar-foreground">
            dripyard
          </span>
        </div>
        <SidebarMenu>
          {NAV_ITEMS.map(({ page, path, label, icon: Icon }) => (
            <SidebarMenuItem key={page}>
              <SidebarMenuButton
                isActive={activePage === page}
                onClick={() => navigate(path)}
                tooltip={label}
              >
                <Icon className="size-4" />
                <span>{label}</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
          ))}
        </SidebarMenu>
      </SidebarHeader>

      <SidebarSeparator className="!w-auto mx-3" />

      <SidebarContent />
      <SidebarFooter className="p-2" />
    </Sidebar>
  );
}
