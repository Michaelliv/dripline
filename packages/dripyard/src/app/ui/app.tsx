import { Route, Routes } from "react-router-dom";
import { AppSidebar } from "./components/app-sidebar";
import { CatalogPage } from "./components/pages/catalog";
import { ConnectionsPage } from "./components/pages/connections";
import { OverviewPage } from "./components/pages/overview";
import { LanesPage } from "./components/pages/lanes";
import { PluginsPage } from "./components/pages/plugins";
import { ProxiesPage } from "./components/pages/proxies";
import { QueryPage } from "./components/pages/query";
import { RunDetailPage } from "./components/pages/run-detail";
import { RunsPage } from "./components/pages/runs";
import { WarehousePage } from "./components/pages/warehouse";
import { WorkersPage } from "./components/pages/workers";
import {
  PageHeaderProvider,
  PageHeaderSlot,
} from "./components/ui/page-header";
import {
  SidebarInset,
  SidebarProvider,
  SidebarTrigger,
} from "./components/ui/sidebar";

export function App() {
  return (
    <PageHeaderProvider>
      <SidebarProvider className="!min-h-0 h-dvh">
        <AppSidebar />
        <SidebarInset className="overflow-hidden">
          <div className="grid h-full grid-rows-[44px_minmax(0,1fr)]">
            <header className="flex h-11 items-center gap-3 px-4 border-b border-[var(--border-muted)]">
              <SidebarTrigger />
              <PageHeaderSlot />
            </header>

            <Routes>
              <Route path="/" element={<OverviewPage />} />
              <Route path="/lanes" element={<LanesPage />} />
              <Route path="/runs" element={<RunsPage />} />
              <Route path="/runs/:runId" element={<RunDetailPage />} />
              <Route path="/workers" element={<WorkersPage />} />
              <Route path="/proxies" element={<ProxiesPage />} />
              <Route path="/plugins" element={<PluginsPage />} />
              <Route path="/connections" element={<ConnectionsPage />} />
              <Route path="/catalog" element={<CatalogPage />} />
              <Route path="/warehouse" element={<WarehousePage />} />
              <Route path="/query" element={<QueryPage />} />
            </Routes>
          </div>
        </SidebarInset>
      </SidebarProvider>
    </PageHeaderProvider>
  );
}
