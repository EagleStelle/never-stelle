import { computed, ref, watch } from "vue";
import { useLocalStorage } from "@vueuse/core";
import IconHistory from "~icons/material-symbols/schedule";
import IconTray from "~icons/material-symbols/inbox";

import { useDashboardSettings } from "./useDashboardSettings";
import { useTaskQueue } from "./useTaskQueue";
import { useToastStack } from "./useToastStack";
import { COUNT_ICONS, MENU_ICONS, SITE_LABELS } from "../ui";
import { MENU_KEYS, type MenuKey, type PageKey, type TaskFilter, type ViewMode, type SettingsSection } from "../types";
import { countTasks, isFilterKey, isMenuKey, isPageKey, isViewMode } from "../utils/dashboard";

export function useDownloadDashboard() {
  const toastStack = useToastStack();
  const settingsState = useDashboardSettings({ toast: toastStack.toast });
  const url = ref("");
  const taskQueue = useTaskQueue({
    getSavedSettings: settingsState.getSavedSettings,
    toast: toastStack.toast,
    url,
  });

  const activePage = useLocalStorage<PageKey>("neverstelle.activePage", "downloads");
  const activeMenu = useLocalStorage<MenuKey>("neverstelle.activeMenu", "all");
  const activeFilter = useLocalStorage<TaskFilter>("neverstelle.activeFilter", "all");
  const viewMode = useLocalStorage<ViewMode>("neverstelle.viewMode", "grid");
  const themeMode = useLocalStorage<"light" | "dark">("neverstelle.themeMode", "dark");

  if (!isPageKey(activePage.value)) activePage.value = "downloads";
  if (!isMenuKey(activeMenu.value)) activeMenu.value = "all";
  if (!isFilterKey(activeFilter.value)) activeFilter.value = "all";
  if (!isViewMode(viewMode.value)) viewMode.value = "grid";
  if (themeMode.value !== "light") themeMode.value = "dark";

  const isLightMode = computed(() => themeMode.value === "light");
  const navigationItems = computed(() => MENU_KEYS.map((key) => ({ key, label: SITE_LABELS[key], icon: MENU_ICONS[key] })));
  const pageItems = computed(() => [
    { key: "downloads" as PageKey, label: "Downloads", icon: IconTray },
    { key: "history" as PageKey, label: "History", icon: IconHistory },
  ]);
  const menuTasks = computed(() => {
    const tasks = taskQueue.taskItems.value;
    return activeMenu.value === "all" ? tasks : tasks.filter((task) => (task.site_category || "others") === activeMenu.value);
  });
  const filteredTasks = computed(() => {
    if (activeFilter.value === "active") return menuTasks.value.filter((task) => ["pending", "running"].includes(task.status));
    if (activeFilter.value === "done") return menuTasks.value.filter((task) => ["completed", "failed"].includes(task.status));
    return menuTasks.value;
  });
  const countsForActiveMenu = computed(() => countTasks(menuTasks.value));
  const activeMenuLabel = computed(() => SITE_LABELS[activeMenu.value] || "matching");
  const countCards = computed(() => [
    { label: "Queued", value: countsForActiveMenu.value.queued, icon: COUNT_ICONS.queued },
    { label: "Active", value: countsForActiveMenu.value.running, icon: COUNT_ICONS.running },
    { label: "Done", value: countsForActiveMenu.value.completed, icon: COUNT_ICONS.completed },
    { label: "Failed", value: countsForActiveMenu.value.failed, icon: COUNT_ICONS.failed },
  ]);

  function setActivePage(page: PageKey): void {
    activePage.value = isPageKey(page) ? page : "downloads";
  }

  function setActiveMenu(menu: MenuKey): void {
    activeMenu.value = isMenuKey(menu) ? menu : "all";
  }

  function setActiveFilter(filter: TaskFilter): void {
    activeFilter.value = filter;
  }

  function setViewMode(mode: ViewMode): void {
    viewMode.value = mode;
  }

  function toggleThemeMode(): void {
    themeMode.value = isLightMode.value ? "dark" : "light";
  }

  function openSettings(event?: Event, section?: SettingsSection) {
    settingsState.openSettings(event, section);
    setActivePage("settings");
  }

  watch(
    themeMode,
    (mode) => document.documentElement.classList.toggle("light-mode", mode === "light"),
    { immediate: true },
  );

  return {
    activePage,
    activeFilter,
    activeMenu,
    activeMenuLabel,
    countCards,
    filteredTasks,
    isLightMode,
    navigationItems,
    pageItems,
    setActivePage,
    setActiveFilter,
    setActiveMenu,
    setViewMode,
    toggleThemeMode,
    url,
    viewMode,
    ...settingsState,
    openSettings,
    ...taskQueue,
    ...toastStack,
  };
}
