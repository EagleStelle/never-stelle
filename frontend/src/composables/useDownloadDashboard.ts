import { computed, nextTick, ref, watch } from "vue";
import { useEventListener, useLocalStorage } from "@vueuse/core";
import IconHistory from "~icons/material-symbols/schedule";
import IconTray from "~icons/material-symbols/inbox";
import IconMedia from "~icons/material-symbols/perm-media";
import IconVideo from "~icons/material-symbols/movie";
import IconImage from "~icons/material-symbols/image";

import { useDashboardSettings } from "./useDashboardSettings";
import { useTaskQueue } from "./useTaskQueue";
import { useToastStack } from "./useToastStack";
import {
  COUNT_ICONS,
  FALLBACK_SOURCE_ICON,
  PAGE_ROUTES,
  SOURCE_ICON_COMPONENTS,
} from "../ui";
import type { MediaFilter, MenuKey, PageKey, SettingsSection, SourceProfile, TaskFilter, ViewMode } from "../types";
import {
  countTasks,
  isFilterKey,
  isMediaFilter,
  isMenuKey,
  isPageKey,
  isSettingsSection,
  isViewMode,
  faviconUrlForHost,
  hostFromUrl,
  mergeSourceProfiles,
  sourceLabelFromKey,
} from "../utils/dashboard";
import { mediaKindForTask } from "../utils/task";

const SETTINGS_ROUTE_BY_SECTION: Record<SettingsSection, string> = {
  downloads: "/settings/locations",
  cookies: "/settings/cookies",
  "folder-template": "/settings/folder",
  "filename-template": "/settings/filename",
};

function settingsSectionFromPath(path: string): SettingsSection {
  const last = path.split("/").filter(Boolean).at(-1) || "";
  if (last === "locations") return "downloads";
  if (last === "folder") return "folder-template";
  if (last === "filename") return "filename-template";
  return isSettingsSection(last) ? last : "downloads";
}

function sourceInitials(label: string): string {
  const parts = label.trim().split(/\s+/).filter(Boolean);
  const value = parts.length > 1 ? `${parts[0][0]}${parts[1][0]}` : label.slice(0, 2);
  return value.toUpperCase() || "?";
}

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
  const mediaFilter = useLocalStorage<MediaFilter>("neverstelle.mediaFilter", "all");
  const viewMode = useLocalStorage<ViewMode>("neverstelle.viewMode", "grid");
  const themeMode = useLocalStorage<"light" | "dark">("neverstelle.themeMode", "dark");

  if (!isPageKey(activePage.value)) activePage.value = "downloads";
  if (activePage.value === "settings") activePage.value = "downloads";
  if (!isFilterKey(activeFilter.value)) activeFilter.value = "all";
  if (!isMediaFilter(mediaFilter.value)) mediaFilter.value = "all";
  if (!isViewMode(viewMode.value)) viewMode.value = "grid";
  if (themeMode.value !== "light") themeMode.value = "dark";

  const taskSourceProfiles = computed<SourceProfile[]>(() =>
    taskQueue.taskItems.value
      .map((task) => {
        const key = task.source_key || "others";
        return {
          key,
          label: sourceLabelFromKey(key),
          hosts: [],
          icon: "",
          icon_url: faviconUrlForHost(hostFromUrl(String(task.source_url || ""))),
        };
      })
      .filter((profile) => profile.key),
  );
  const sourceProfiles = computed<SourceProfile[]>(() =>
    mergeSourceProfiles(settingsState.sourceProfiles.value, taskSourceProfiles.value),
  );

  const isLightMode = computed(() => themeMode.value === "light");
  const navigationItems = computed(() => [
    { key: "all", label: "All platform", icon: IconTray },
    ...sourceProfiles.value.map((profile) => ({
      key: profile.key,
      label: profile.label,
      icon: SOURCE_ICON_COMPONENTS[profile.icon || profile.key] || FALLBACK_SOURCE_ICON,
      iconUrl: profile.icon_url || "",
      initials: sourceInitials(profile.label),
    })),
  ]);
  const pageItems = computed(() => [
    { key: "downloads" as PageKey, label: "Downloads", icon: IconTray },
    { key: "history" as PageKey, label: "History", icon: IconHistory },
  ]);
  const menuTasks = computed(() => {
    const tasks = taskQueue.taskItems.value;
    return activeMenu.value === "all" ? tasks : tasks.filter((task) => (task.source_key || "others") === activeMenu.value);
  });
  const mediaTasks = computed(() =>
    mediaFilter.value === "all"
      ? menuTasks.value
      : menuTasks.value.filter((task) => mediaKindForTask(task) === mediaFilter.value),
  );
  const mediaFilterItems = computed(() => [
    { key: "all", label: "All media", icon: IconMedia },
    { key: "video", label: "Video", icon: IconVideo },
    { key: "image", label: "Images", icon: IconImage },
  ]);
  const activeTasks = computed(() => mediaTasks.value.filter((task) => ["pending", "running", "failed"].includes(task.status)));
  const completedTasks = computed(() => mediaTasks.value.filter((task) => ["completed"].includes(task.status)));
  const countsForActiveMenu = computed(() => countTasks(menuTasks.value));
  const activeMenuLabel = computed(() => {
    if (activeMenu.value === "all") return "All";
    return sourceProfiles.value.find((profile) => profile.key === activeMenu.value)?.label || "matching";
  });
  const countCards = computed(() => [
    { label: "Queued", value: countsForActiveMenu.value.queued, icon: COUNT_ICONS.queued },
    { label: "Active", value: countsForActiveMenu.value.running, icon: COUNT_ICONS.running },
    { label: "Done", value: countsForActiveMenu.value.completed, icon: COUNT_ICONS.completed },
    { label: "Failed", value: countsForActiveMenu.value.failed, icon: COUNT_ICONS.failed },
  ]);

  let applyingRoute = false;

  function routeFor(): string {
    if (settingsState.settingsOpen.value) {
      return SETTINGS_ROUTE_BY_SECTION[settingsState.settingsSection.value] || PAGE_ROUTES.settings;
    }
    return PAGE_ROUTES[activePage.value] || PAGE_ROUTES.downloads;
  }

  function applyCurrentRoute(): void {
    const path = window.location.pathname || "/";
    applyingRoute = true;
    if (path.startsWith("/settings")) {
      settingsState.openSettings(undefined, settingsSectionFromPath(path));
    } else {
      settingsState.closeSettings();
      activePage.value = path.startsWith("/history") ? "history" : "downloads";
    }
    void nextTick(() => {
      applyingRoute = false;
      const canonical = routeFor();
      if (window.location.pathname !== canonical) window.history.replaceState({}, "", canonical);
    });
  }

  function syncRoute(): void {
    if (applyingRoute) return;
    const route = routeFor();
    if (window.location.pathname !== route) window.history.pushState({}, "", route);
  }

  function setActivePage(page: PageKey): void {
    activePage.value = isPageKey(page) ? page : "downloads";
  }

  function setActiveMenu(menu: MenuKey): void {
    activeMenu.value = isMenuKey(menu, sourceProfiles.value) ? menu : "all";
  }

  function setActiveFilter(filter: TaskFilter): void {
    activeFilter.value = filter;
  }

  function setMediaFilter(value: MediaFilter): void {
    mediaFilter.value = isMediaFilter(value) ? value : "all";
  }

  function setViewMode(mode: ViewMode): void {
    viewMode.value = mode;
  }

  function setSettingsSection(section: SettingsSection, shouldFocus = false): void {
    settingsState.setSettingsSection(section, shouldFocus);
  }

  function toggleThemeMode(): void {
    themeMode.value = isLightMode.value ? "dark" : "light";
  }

  function openSettings(event?: Event, section?: SettingsSection) {
    settingsState.openSettings(event, section);
  }

  applyCurrentRoute();

  watch(
    [activePage, settingsState.settingsOpen, settingsState.settingsSection],
    () => syncRoute(),
    { flush: "post" },
  );
  watch(
    sourceProfiles,
    (profiles) => {
      if (!isMenuKey(activeMenu.value, profiles)) activeMenu.value = "all";
    },
    { immediate: true },
  );
  watch(
    themeMode,
    (mode) => document.documentElement.classList.toggle("light-mode", mode === "light"),
    { immediate: true },
  );
  useEventListener(window, "popstate", applyCurrentRoute);

  return {
    activePage,
    activeFilter,
    activeMenu,
    activeMenuLabel,
    activeTasks,
    completedTasks,
    countCards,
    isLightMode,
    mediaFilter,
    mediaFilterItems,
    navigationItems,
    pageItems,
    setActivePage,
    setActiveFilter,
    setActiveMenu,
    setMediaFilter,
    setViewMode,
    toggleThemeMode,
    url,
    viewMode,
    ...settingsState,
    sourceProfiles,
    setSettingsSection,
    openSettings,
    ...taskQueue,
    ...toastStack,
  };
}
