import { computed, nextTick, reactive, ref, watch } from "vue";
import { useEventListener, useLocalStorage } from "@vueuse/core";
import IconTray from "~icons/material-symbols/inbox";
import IconMedia from "~icons/material-symbols/perm-media";
import IconVideo from "~icons/material-symbols/movie";
import IconImage from "~icons/material-symbols/image";

import { useDashboardSettings } from "@/composables/useDashboardSettings";
import { useAuth } from "@/composables/useAuth";
import { useTaskQueue } from "@/composables/useTaskQueue";
import { useHistory } from "@/composables/useHistory";
import { useTrackers } from "@/composables/useTrackers";
import { useSonner } from "@/composables/useSonner";
import { COUNT_ICONS, PAGE_ICONS, PAGE_ROUTES } from "@/ui";
import type {
  MediaFilter,
  MenuKey,
  PageKey,
  PostProcessingSelection,
  QualitySelection,
  SettingsSection,
  SourceProfile,
  TaskCounts,
  TaskFilter,
  ViewMode,
} from "@/types";
import {
  createQualitySelection,
  createPostProcessingSelection,
  constrainPostProcessingSelection,
  countTasks,
  emptyTaskCounts,
  isFilterKey,
  isMediaFilter,
  isMenuKey,
  isPageKey,
  isViewMode,
  mergeSourceProfiles,
  postProcessingCapabilitiesForQuality,
  sourceIconUrl,
  sourceLabelFromKey,
} from "@/utils/dashboard";
import { mediaKindForTask } from "@/utils/task";

// Settings overlay rides in a ?settings=<slug> query param, not its own path.
const SETTINGS_SLUG_BY_SECTION: Record<SettingsSection, string> = {
  account: "account",
  defaults: "defaults",
  locations: "locations",
  cookies: "cookies",
  trackers: "trackers",
  format: "format",
  fields: "fields",
  scraper: "scraper",
  slug: "slug",
  templates: "templates",
  naming: "naming",
};

const SETTINGS_SECTION_BY_SLUG = Object.fromEntries(
  Object.entries(SETTINGS_SLUG_BY_SECTION).map(([section, slug]) => [
    slug,
    section,
  ]),
) as Record<string, SettingsSection>;

function settingsSectionFromSlug(slug: string): SettingsSection {
  return SETTINGS_SECTION_BY_SLUG[slug] || "locations";
}

function hasSourceCounts(counts?: Partial<TaskCounts>): boolean {
  return Boolean(
    counts &&
      (Number(counts.queued) ||
        Number(counts.running) ||
        Number(counts.completed) ||
        Number(counts.failed)),
  );
}

export function useDownloadDashboard() {
  const auth = useAuth();
  const sonner = useSonner();
  const settingsState = useDashboardSettings({ toast: sonner.toast });
  const url = ref("");
  // Per-download override: follows the settings default until the user edits it,
  // then sticks for the session and never writes back to settings.
  const downloadSelection = reactive<QualitySelection>(
    createQualitySelection(),
  );
  const downloadQualityTouched = ref(false);
  const downloadPostProcessing = reactive<PostProcessingSelection>(
    createPostProcessingSelection(),
  );
  const downloadPostProcessingTouched = ref(false);
  const qualityOptions = computed(() => settingsState.settings.quality_options);
  function setDownloadQuality(selection: QualitySelection): void {
    downloadQualityTouched.value = true;
    Object.assign(
      downloadSelection,
      createQualitySelection(selection, qualityOptions.value),
    );
  }
  function setDownloadPostProcessing(selection: PostProcessingSelection): void {
    downloadPostProcessingTouched.value = true;
    Object.assign(
      downloadPostProcessing,
      createPostProcessingSelection(selection),
    );
  }
  const getQuality = () =>
    createQualitySelection(downloadSelection, qualityOptions.value);
  const getPostProcessing = () =>
    constrainPostProcessingSelection(
      downloadPostProcessing,
      postProcessingCapabilitiesForQuality(downloadSelection, qualityOptions.value),
    );
  const taskQueue = useTaskQueue({
    getSavedSettings: settingsState.getSavedSettings,
    getQuality,
    getPostProcessing,
    toast: sonner.toast,
    url,
  });
  watch(
    () => settingsState.settings.default_quality,
    (value) => {
      if (!downloadQualityTouched.value)
        Object.assign(
          downloadSelection,
          createQualitySelection(value[value.mode], qualityOptions.value),
        );
    },
    { deep: true, immediate: true },
  );
  watch(
    () => settingsState.settings.default_post_processing,
    (value) => {
      if (!downloadPostProcessingTouched.value)
        Object.assign(
          downloadPostProcessing,
          createPostProcessingSelection(value),
        );
    },
    { deep: true, immediate: true },
  );
  watch(
    qualityOptions,
    (value) => {
      Object.assign(
        downloadSelection,
        createQualitySelection(downloadSelection, value),
      );
    },
    { deep: true },
  );

  const activePage = useLocalStorage<PageKey>(
    "neverstelle.activePage",
    "downloads",
  );
  const activeMenu = useLocalStorage<MenuKey>("neverstelle.activeMenu", "all");
  const activeFilter = useLocalStorage<TaskFilter>(
    "neverstelle.activeFilter",
    "all",
  );
  const mediaFilter = useLocalStorage<MediaFilter>(
    "neverstelle.mediaFilter",
    "all",
  );
  const viewMode = useLocalStorage<ViewMode>("neverstelle.viewMode", "grid");
  const themeMode = useLocalStorage<"light" | "dark">(
    "neverstelle.themeMode",
    "dark",
  );

  if (!isPageKey(activePage.value)) activePage.value = "downloads";
  if (!isFilterKey(activeFilter.value)) activeFilter.value = "all";
  if (!isMediaFilter(mediaFilter.value)) mediaFilter.value = "all";
  if (!isViewMode(viewMode.value)) viewMode.value = "grid";
  if (themeMode.value !== "light") themeMode.value = "dark";

  // History is server-paginated; source + text search run in SQL, media filter client-side below.
  const historySourceKey = computed(() =>
    activeMenu.value === "all" ? "" : activeMenu.value,
  );
  const historyEnabled = computed(
    () => auth.authenticated.value && activePage.value === "history",
  );
  // User input drives historySearch; historySearchQuery drives the query key.
  const historySearch = ref("");
  const historySearchQuery = ref("");
  const submitHistorySearch = () => {
    historySearchQuery.value = historySearch.value;
  };
  const history = useHistory({
    sourceKey: historySourceKey,
    search: historySearchQuery,
    enabled: historyEnabled,
  });
  const trackersPage = computed(
    () => auth.authenticated.value && activePage.value === "trackers",
  );
  const trackerState = useTrackers({
    enabled: trackersPage,
    tasks: taskQueue.taskItems,
    toast: sonner.toast,
    url,
  });
  const trackerHistory = useHistory({
    sourceKey: ref(""),
    search: historySearchQuery,
    trackerId: trackerState.openTrackerId,
    enabled: computed(() => trackersPage.value && Boolean(trackerState.openTrackerId.value)),
  });

  const taskSourceProfiles = computed<SourceProfile[]>(() =>
    taskQueue.taskItems.value
      .flatMap((task): SourceProfile[] => {
        const key = task.source_key || "";
        if (!key) return [];
        return [
          {
            key,
            label: sourceLabelFromKey(key),
            hosts: [],
          },
        ];
      }),
  );
  // Nav chips for history-only platforms; server counts enumerate every source key.
  const menuKeyProfiles = computed<SourceProfile[]>(() =>
    Object.keys(taskQueue.countsByMenu.value)
      .filter((key) => key !== "all")
      .filter((key) => key && hasSourceCounts(taskQueue.countsByMenu.value[key]))
      .map((key) => ({
        key,
        label: sourceLabelFromKey(key),
        hosts: [],
      })),
  );
  const sourceProfiles = computed<SourceProfile[]>(() =>
    mergeSourceProfiles(
      settingsState.sourceProfiles.value,
      taskSourceProfiles.value,
      menuKeyProfiles.value,
    ),
  );

  const isLightMode = computed(() => themeMode.value === "light");
  const navigationItems = computed(() => {
    const profiles = sourceProfiles.value.map((profile) => ({
      key: profile.key,
      label: profile.label,
      iconUrl: sourceIconUrl(profile.key),
    })).sort((a, b) => a.label.localeCompare(b.label));

    return [
      { key: "all", label: "All platforms", icon: IconTray },
      ...profiles,
    ];
  });
  const pageItems = computed(() => [
    { key: "downloads" as PageKey, label: "Downloads", icon: PAGE_ICONS.downloads },
    { key: "trackers" as PageKey, label: "Trackers", icon: PAGE_ICONS.trackers },
    { key: "history" as PageKey, label: "History", icon: PAGE_ICONS.history },
  ]);
  const menuTasks = computed(() => {
    const tasks = taskQueue.taskItems.value;
    return activeMenu.value === "all"
      ? tasks
      : tasks.filter((task) => task.source_key === activeMenu.value);
  });
  const mediaTasks = computed(() =>
    mediaFilter.value === "all"
      ? menuTasks.value
      : menuTasks.value.filter(
          (task) => mediaKindForTask(task) === mediaFilter.value,
        ),
  );
  const mediaFilterItems = computed(() => [
    { key: "all", label: "All media", icon: IconMedia },
    { key: "video", label: "Video", icon: IconVideo },
    { key: "image", label: "Images", icon: IconImage },
  ]);
  const activeTasks = computed(() =>
    mediaTasks.value.filter((task) =>
      ["pending", "running", "failed"].includes(task.status),
    ),
  );
  // History entries arrive already source-filtered from the server; apply the media filter here.
  const completedTasks = computed(() =>
    mediaFilter.value === "all"
      ? history.entries.value
      : history.entries.value.filter(
          (task) => mediaKindForTask(task) === mediaFilter.value,
        ),
  );
  const menuTrackers = computed(() =>
    activeMenu.value === "all"
      ? trackerState.trackers.value
      : trackerState.trackers.value.filter((tracker) => tracker.source_key === activeMenu.value),
  );
  // Queue rows first, then its history page; the media filter narrows both, a search shows history only.
  const trackerTasks = computed(() => {
    const queued = historySearchQuery.value.trim() ? [] : mediaTasks.value.filter(
      (task) =>
        task.tracker_id === trackerState.openTrackerId.value &&
        ["pending", "running", "failed"].includes(task.status),
    );
    const done =
      mediaFilter.value === "all"
        ? trackerHistory.entries.value
        : trackerHistory.entries.value.filter(
            (task) => mediaKindForTask(task) === mediaFilter.value,
          );
    return [...queued, ...done];
  });
  const countsForActiveMenu = computed<TaskCounts>(
    () => {
      if (mediaFilter.value === "all") {
        return taskQueue.countsByMenu.value[activeMenu.value] || emptyTaskCounts();
      }
      const mediaCounts = taskQueue.countsByMediaMenu.value[mediaFilter.value]?.[activeMenu.value];
      if (mediaCounts) return mediaCounts;
      return activePage.value === "history" ? countTasks(completedTasks.value) : countTasks(mediaTasks.value);
    },
  );
  const activeMenuLabel = computed(() => {
    if (activeMenu.value === "all") return "All";
    return (
      sourceProfiles.value.find((profile) => profile.key === activeMenu.value)
        ?.label || "matching"
    );
  });
  const countCards = computed(() => [
    {
      label: "Queued",
      value: countsForActiveMenu.value.queued,
      icon: COUNT_ICONS.queued,
    },
    {
      label: "Active",
      value: countsForActiveMenu.value.running,
      icon: COUNT_ICONS.running,
    },
    {
      label: "Done",
      value: countsForActiveMenu.value.completed,
      icon: COUNT_ICONS.completed,
    },
    {
      label: "Failed",
      value: countsForActiveMenu.value.failed,
      icon: COUNT_ICONS.failed,
    },
  ]);

  let applyingRoute = false;

  // Base page owns the path and an open tracker its id; a new link and the settings pane ride as query params.
  function routeFor(): string {
    let path: string = PAGE_ROUTES[activePage.value];
    const query = new URLSearchParams();
    if (trackerState.newTrackerUrl.value) {
      path += "/new";
      query.set("url", trackerState.newTrackerUrl.value);
    } else if (trackerState.openTrackerId.value) {
      path += `/${trackerState.openTrackerId.value}`;
    }
    const slug = settingsState.settingsOpen.value && SETTINGS_SLUG_BY_SECTION[settingsState.settingsSection.value];
    if (slug) query.set("settings", slug);
    const search = query.toString();
    return search ? `${path}?${search}` : path;
  }

  function applyCurrentRoute(): void {
    const path = window.location.pathname || "/";
    const query = new URLSearchParams(window.location.search);
    const slug = query.get("settings");
    applyingRoute = true;
    const [, root = "", tracker = ""] = path.split("/");
    activePage.value = isPageKey(root) ? root : "downloads";
    const trackerRoute = activePage.value === "trackers" ? tracker : "";
    trackerState.newTrackerUrl.value = trackerRoute === "new" ? query.get("url") || "" : "";
    trackerState.openTrackerId.value = trackerRoute === "new" ? "" : trackerRoute;
    if (slug !== null) {
      settingsState.openSettings(undefined, settingsSectionFromSlug(slug));
    } else {
      settingsState.closeSettings();
    }
    void nextTick(() => {
      applyingRoute = false;
      const canonical = routeFor();
      if (window.location.pathname + window.location.search !== canonical) {
        window.history.replaceState({}, "", canonical);
      }
    });
  }

  function syncRoute(): void {
    if (applyingRoute) return;
    const route = routeFor();
    if (window.location.pathname + window.location.search !== route) {
      window.history.pushState({}, "", route);
    }
  }

  function setActivePage(page: PageKey): void {
    activePage.value = isPageKey(page) ? page : "downloads";
    trackerState.openTrackerId.value = "";
    trackerState.newTrackerUrl.value = "";
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

  function toggleThemeMode(): void {
    themeMode.value = isLightMode.value ? "dark" : "light";
  }

  function openSettings(event?: Event, section?: SettingsSection) {
    settingsState.openSettings(event, section);
  }

  async function saveSettingsDraft(): Promise<void> {
    if (await settingsState.saveSettingsDraft()) await taskQueue.loadRenameCounts();
  }

  // Scan reconciles the history table, then the paginated query reloads page one.
  async function refreshHistoryPage(): Promise<void> {
    await taskQueue.refreshHistory();
    await history.refresh();
  }

  applyCurrentRoute();

  watch(
    [
      activePage,
      trackerState.openTrackerId,
      trackerState.newTrackerUrl,
      settingsState.settingsOpen,
      settingsState.settingsSection,
    ],
    () => syncRoute(),
    { flush: "post" },
  );
  watch(settingsState.settingsOpen, (open) => {
    if (open) void taskQueue.loadRenameCounts();
  });
  watch(
    sourceProfiles,
    (profiles) => {
      if (!isMenuKey(activeMenu.value, profiles)) activeMenu.value = "all";
    },
    { immediate: true },
  );
  watch(
    themeMode,
    (mode) =>
      document.documentElement.classList.toggle("light-mode", mode === "light"),
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
    menuTrackers,
    trackerTasks,
    trackerHistoryLoading: trackerHistory.loading,
    trackerHistoryError: trackerHistory.historyError,
    trackerHistoryHasMore: trackerHistory.hasMore,
    trackerHistoryFetchingMore: trackerHistory.fetchingMore,
    loadMoreTrackerHistory: trackerHistory.loadMore,
    ...trackerState,
    setActiveFilter,
    setActiveMenu,
    setMediaFilter,
    setViewMode,
    toggleThemeMode,
    url,
    downloadSelection,
    downloadPostProcessing,
    qualityOptions,
    setDownloadQuality,
    setDownloadPostProcessing,
    viewMode,
    ...settingsState,
    sourceProfiles,
    openSettings,
    saveSettingsDraft,
    ...taskQueue,
    ...sonner,
    historySearch,
    submitHistorySearch,
    historyLoading: history.loading,
    historyError: history.historyError,
    historyHasMore: history.hasMore,
    historyFetchingMore: history.fetchingMore,
    loadMoreHistory: history.loadMore,
    refreshHistory: refreshHistoryPage,
  };
}
