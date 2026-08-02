import { inject, provide, type InjectionKey } from "vue";

import { useDownloadDashboard } from "@/composables/useDownloadDashboard";

// Every dashboard surface (toolbars, panel, navs, settings) reads the one store the
// shell builds, so adding a control never means threading another prop through App.
export type DashboardContext = ReturnType<typeof useDownloadDashboard>;

const DASHBOARD_CONTEXT: InjectionKey<DashboardContext> =
  Symbol("dashboard-context");

/** Builds the dashboard store once and hands it to the subtree. Shell-only. */
export function provideDashboard(): DashboardContext {
  const dashboard = useDownloadDashboard();
  provide(DASHBOARD_CONTEXT, dashboard);
  return dashboard;
}

export function useDashboard(): DashboardContext {
  const context = inject(DASHBOARD_CONTEXT);
  if (!context) throw new Error("useDashboard must be used inside the app shell.");
  return context;
}
