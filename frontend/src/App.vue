<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, reactive, ref } from "vue";
import { Download, Grid3X3, List, Moon, Settings, Sun, Upload, X } from "@lucide/vue";

import {
  addTask as createTask,
  cleanupNfoFiles,
  clearCompletedTasks,
  clearPendingTasks,
  deleteInstagramCookies,
  fetchTaskFile,
  getTasks,
  getUiConfig,
  hideTask as hideTaskRequest,
  markTaskDelivered,
  removeTask as removeTaskRequest,
  saveSettings,
  uploadInstagramCookies,
} from "./api";
import TaskList from "./components/TaskList.vue";
import ToastStack from "./components/ToastStack.vue";
import {
  MENU_KEYS,
  SITE_KEYS,
  type CookiesStatus,
  type MenuKey,
  type RuntimeSettings,
  type SavedSettings,
  type SettingsSection,
  type SiteKey,
  type SiteLocations,
  type TaskCounts,
  type TaskFilter,
  type TaskItem,
  type ToastMessage,
  type ToastType,
  type UiConfigResponse,
  type ViewMode,
} from "./types";

const SITE_LABELS: Record<MenuKey, string> = {
  all: "All",
  youtube: "YouTube",
  facebook: "Facebook",
  instagram: "Instagram",
  tiktok: "TikTok",
  others: "Others",
};

const FILTER_LABELS: Record<TaskFilter, string> = {
  all: "All",
  active: "Active",
  done: "Done",
};

const SETTINGS_SECTIONS: Array<{ key: SettingsSection; label: string }> = [
  { key: "downloads", label: "Downloads" },
  { key: "instagram", label: "Instagram" },
  { key: "advanced", label: "Advanced" },
];

const POLL_RUNNING_MS = 2000;
const POLL_PENDING_MS = 5000;

function createSiteLocations(source: Partial<SiteLocations> = {}): SiteLocations {
  return {
    youtube: source.youtube || "",
    facebook: source.facebook || "",
    instagram: source.instagram || "",
    tiktok: source.tiktok || "",
    others: source.others || "",
  };
}

function createTemplateSettings(source: Partial<SavedSettings["template_settings"]> = {}) {
  return {
    folder_template: source.folder_template || "",
    filename_template: source.filename_template || "",
  };
}

function createCookiesStatus(source: Partial<CookiesStatus> = {}): CookiesStatus {
  return {
    configured: Boolean(source.configured),
    source: source.source || "none",
    filename: source.filename || "",
    uploaded_at: source.uploaded_at || "",
  };
}

function getTabId(): string {
  const key = "neverstelle.tabId";
  let value = sessionStorage.getItem(key);
  if (!value) {
    value =
      window.crypto && typeof window.crypto.randomUUID === "function"
        ? window.crypto.randomUUID()
        : `tab-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    sessionStorage.setItem(key, value);
  }
  return value;
}

function readJsonRecord(key: string): Record<string, boolean> {
  try {
    return JSON.parse(sessionStorage.getItem(key) || "{}") || {};
  } catch {
    return {};
  }
}

function isMenuKey(value: string | null): value is MenuKey {
  return MENU_KEYS.includes(value as MenuKey);
}

function isFilterKey(value: string | null): value is TaskFilter {
  return value === "all" || value === "active" || value === "done";
}

function isViewMode(value: string | null): value is ViewMode {
  return value === "grid" || value === "table";
}

const defaults = reactive<SavedSettings>({
  site_locations: createSiteLocations(),
  save_mode: "nas",
  template_settings: createTemplateSettings(),
});

const settings = reactive<RuntimeSettings>({
  site_locations: createSiteLocations(),
  save_mode: "nas",
  template_settings: createTemplateSettings(),
  download_locations: [],
  instagram_ytdlp_cookies: createCookiesStatus(),
});

const settingsDraft = reactive<SavedSettings>({
  site_locations: createSiteLocations(),
  save_mode: "nas",
  template_settings: createTemplateSettings(),
});

const tabId = getTabId();
const url = ref("");
const visibleTasks = ref<TaskItem[]>([]);
const taskCache = new Map<string, Partial<TaskItem>>();
const deliveredDeviceDownloads = reactive<Record<string, boolean>>(readJsonRecord("neverstelle.deviceDelivered"));
const deviceDownloadsInFlight = new Set<string>();
const storedActiveMenu = localStorage.getItem("neverstelle.activeMenu");
const storedActiveFilter = localStorage.getItem("neverstelle.activeFilter");
const storedViewMode = localStorage.getItem("neverstelle.viewMode");
const activeMenu = ref<MenuKey>(isMenuKey(storedActiveMenu) ? storedActiveMenu : "all");
const activeFilter = ref<TaskFilter>(isFilterKey(storedActiveFilter) ? storedActiveFilter : "all");
const viewMode = ref<ViewMode>(isViewMode(storedViewMode) ? storedViewMode : "grid");
const settingsOpen = ref(false);
const settingsSection = ref<SettingsSection>("downloads");
const toasts = ref<ToastMessage[]>([]);
const srStatus = ref("");
const isLightMode = ref(false);
const settingsModalRef = ref<HTMLElement | null>(null);
const cookiesInput = ref<HTMLInputElement | null>(null);
const cleanupBusy = ref(false);
const lastFocusedTrigger = ref<HTMLElement | null>(null);

let toastId = 0;
let pollHandle: number | null = null;
let pollIntervalMs = 0;

const savedSettings = computed<SavedSettings>(() => getSavedSettings());
const countsForActiveMenu = computed(() => countTasks(menuTasks.value));
const activeMenuLabel = computed(() => SITE_LABELS[activeMenu.value] || "matching");
const cookiesStatusText = computed(() => {
  const info = settings.instagram_ytdlp_cookies;
  if (!info.configured) return "No yt-dlp cookies saved.";
  const parts = [info.filename ? `Connected with ${info.filename}` : "Connected to yt-dlp cookies"];
  const when = formatTimestamp(info.uploaded_at);
  if (when) parts.push(when);
  return parts.join(" - ");
});

const menuTasks = computed(() => {
  const merged = mergeTaskData(visibleTasks.value);
  if (activeMenu.value === "all") return merged;
  return merged.filter((task) => (task.site_category || "others") === activeMenu.value);
});

const filteredTasks = computed(() => {
  if (activeFilter.value === "active") {
    return menuTasks.value.filter((task) => ["pending", "running"].includes(task.status));
  }
  if (activeFilter.value === "done") {
    return menuTasks.value.filter((task) => ["completed", "failed"].includes(task.status));
  }
  return menuTasks.value;
});

function siteLabel(site: SiteKey | MenuKey): string {
  return SITE_LABELS[site] || "Others";
}

function getSavedSettings(): SavedSettings {
  return {
    site_locations: {
      youtube: settings.site_locations.youtube || defaults.site_locations.youtube || "",
      facebook: settings.site_locations.facebook || defaults.site_locations.facebook || "",
      instagram: settings.site_locations.instagram || defaults.site_locations.instagram || "",
      tiktok: settings.site_locations.tiktok || defaults.site_locations.tiktok || "",
      others: settings.site_locations.others || defaults.site_locations.others || "",
    },
    save_mode: settings.save_mode === "device" ? "device" : defaults.save_mode || "nas",
    template_settings: {
      folder_template: settings.template_settings.folder_template || defaults.template_settings.folder_template || "",
      filename_template: settings.template_settings.filename_template || defaults.template_settings.filename_template || "",
    },
  };
}

function applyServerSettings(data: UiConfigResponse): void {
  if (data.site_default_locations) {
    Object.assign(defaults.site_locations, createSiteLocations({ ...defaults.site_locations, ...data.site_default_locations }));
    Object.assign(settings.site_locations, createSiteLocations({ ...settings.site_locations, ...data.site_default_locations }));
  }

  settings.save_mode = data.save_mode === "device" ? "device" : "nas";
  defaults.save_mode = settings.save_mode;

  if (data.template_settings) {
    Object.assign(defaults.template_settings, createTemplateSettings({ ...defaults.template_settings, ...data.template_settings }));
    Object.assign(settings.template_settings, createTemplateSettings({ ...settings.template_settings, ...data.template_settings }));
  }

  settings.download_locations = Array.isArray(data.download_locations) ? data.download_locations : [];
  settings.instagram_ytdlp_cookies = createCookiesStatus(data.instagram_ytdlp_cookies || {});
}

async function persistSettings(payload: SavedSettings, successMessage = ""): Promise<void> {
  const data = await saveSettings(payload);
  applyServerSettings(data);
  if (successMessage) toast(successMessage);
}

async function fetchUIConfig(): Promise<void> {
  try {
    applyServerSettings(await getUiConfig());
  } catch (error) {
    console.error(error);
  }
}

function copySettingsToDraft(): void {
  const current = getSavedSettings();
  Object.assign(settingsDraft.site_locations, current.site_locations);
  settingsDraft.save_mode = current.save_mode;
  Object.assign(settingsDraft.template_settings, current.template_settings);
}

async function updateSaveMode(mode: "nas" | "device"): Promise<void> {
  const payload = getSavedSettings();
  if (payload.save_mode === mode) return;
  payload.save_mode = mode;
  try {
    await persistSettings(payload, "Save mode updated.");
  } catch (error) {
    toast(errorMessage(error, "Could not save save mode."), "error");
  }
}

function setActiveMenu(menu: MenuKey): void {
  activeMenu.value = isMenuKey(menu) ? menu : "all";
  localStorage.setItem("neverstelle.activeMenu", activeMenu.value);
}

function setActiveFilter(filter: TaskFilter): void {
  activeFilter.value = filter;
  localStorage.setItem("neverstelle.activeFilter", filter);
}

function setViewMode(mode: ViewMode): void {
  viewMode.value = mode;
  localStorage.setItem("neverstelle.viewMode", mode);
}

function setSettingsSection(section: SettingsSection, shouldFocus = false): void {
  settingsSection.value = section;
  if (!shouldFocus) return;
  const focusTargets: Record<SettingsSection, string> = {
    downloads: "youtubeLocationInput",
    instagram: "instagramYtdlpCookiesInput",
    advanced: "folderTemplateInput",
  };
  void nextTick(() => {
    settingsModalRef.value?.querySelector<HTMLElement>(`#${focusTargets[settingsSection.value]}`)?.focus();
  });
}

function openSettings(event?: Event, section: SettingsSection = "downloads"): void {
  lastFocusedTrigger.value = event?.currentTarget instanceof HTMLElement ? event.currentTarget : document.activeElement instanceof HTMLElement ? document.activeElement : null;
  copySettingsToDraft();
  settingsOpen.value = true;
  document.body.classList.add("overflow-hidden");
  setSettingsSection(section, true);
}

function closeSettings(): void {
  settingsOpen.value = false;
  document.body.classList.remove("overflow-hidden");
  lastFocusedTrigger.value?.focus();
}

async function saveSettingsDraft(): Promise<void> {
  const payload: SavedSettings = {
    site_locations: createSiteLocations(settingsDraft.site_locations),
    save_mode: settingsDraft.save_mode === "device" ? "device" : "nas",
    template_settings: createTemplateSettings(settingsDraft.template_settings),
  };
  try {
    await persistSettings(payload);
    toast("Settings saved.");
    closeSettings();
  } catch (error) {
    toast(errorMessage(error, "Could not save settings."), "error");
  }
}

async function connectInstagramCookies(): Promise<void> {
  const file = cookiesInput.value?.files?.[0];
  if (!file) {
    toast("Choose a cookies file first.", "error");
    return;
  }
  try {
    applyServerSettings(await uploadInstagramCookies(file));
    if (cookiesInput.value) cookiesInput.value.value = "";
    toast("yt-dlp cookies connected.");
  } catch (error) {
    toast(errorMessage(error, "Could not connect yt-dlp cookies."), "error");
  }
}

async function removeInstagramCookies(): Promise<void> {
  if (!settings.instagram_ytdlp_cookies.configured) return;
  try {
    applyServerSettings(await deleteInstagramCookies());
    if (cookiesInput.value) cookiesInput.value.value = "";
    toast("yt-dlp cookies disconnected.");
  } catch (error) {
    toast(errorMessage(error, "Could not remove yt-dlp cookies."), "error");
  }
}

async function cleanNfo(): Promise<void> {
  cleanupBusy.value = true;
  try {
    const data = await cleanupNfoFiles();
    if (data.errors && data.errors.length) {
      toast(`Deleted ${data.deleted} .nfo file${data.deleted === 1 ? "" : "s"}, ${data.errors.length} could not be removed.`, "error");
    } else {
      toast(data.deleted === 0 ? "No .nfo files found." : `Deleted ${data.deleted} .nfo file${data.deleted === 1 ? "" : "s"}.`);
    }
  } catch (error) {
    toast(errorMessage(error, "Could not delete .nfo files."), "error");
  } finally {
    cleanupBusy.value = false;
  }
}

function mergeTaskData(tasks: TaskItem[]): TaskItem[] {
  return tasks.map((task) => {
    const cached = taskCache.get(task.vid) || {};
    const merged = {
      ...task,
      resolved_folder: task.resolved_folder || cached.resolved_folder || "",
      resolved_filename: task.resolved_filename || cached.resolved_filename || "",
      resolved_full_path: task.resolved_full_path || cached.resolved_full_path || "",
      site_category: task.site_category || cached.site_category || "others",
      site_label: task.site_label || SITE_LABELS[(task.site_category as MenuKey) || "others"] || "Others",
    };
    taskCache.set(task.vid, {
      resolved_folder: merged.resolved_folder,
      resolved_filename: merged.resolved_filename,
      resolved_full_path: merged.resolved_full_path,
      site_category: merged.site_category,
    });
    return merged;
  });
}

function countTasks(tasks: TaskItem[]): TaskCounts {
  return {
    queued: tasks.filter((task) => task.status === "pending").length,
    running: tasks.filter((task) => task.status === "running").length,
    completed: tasks.filter((task) => task.status === "completed").length,
    failed: tasks.filter((task) => task.status === "failed").length,
  };
}

function syncPoll(counts: TaskCounts): void {
  const targetMs = counts.running > 0 ? POLL_RUNNING_MS : counts.queued > 0 ? POLL_PENDING_MS : 0;
  if (targetMs !== pollIntervalMs) {
    if (pollHandle) {
      window.clearInterval(pollHandle);
      pollHandle = null;
    }
    pollIntervalMs = targetMs;
  }
  if (targetMs > 0 && !pollHandle && !document.hidden) {
    pollHandle = window.setInterval(() => void loadTasks(true), targetMs);
  }
}

async function loadTasks(silent = false): Promise<void> {
  try {
    const data = await getTasks();
    visibleTasks.value = data.tasks || [];
    processCompletedDeviceDownloads(visibleTasks.value);
    syncPoll(countTasks(visibleTasks.value));
  } catch (error) {
    if (!silent) toast(errorMessage(error, "Could not load tasks."), "error");
  }
}

async function addDownloadTask(): Promise<void> {
  const sourceUrl = url.value.trim();
  if (!sourceUrl) {
    toast("Paste a supported URL first.", "error");
    return;
  }

  const currentSettings = getSavedSettings();
  try {
    const data = await createTask({
      url: sourceUrl,
      site_locations: currentSettings.site_locations,
      save_mode: currentSettings.save_mode,
      client_tab_id: tabId,
    });
    url.value = "";
    const firstTask = Array.isArray(data.created) ? data.created[0] : null;
    if (firstTask && data.reused && currentSettings.save_mode === "device" && firstTask.status === "completed") {
      await downloadTaskFile(firstTask.vid);
      toast("That file was already downloaded.");
    } else if (data.reused) {
      toast(firstTask && firstTask.status === "completed" ? "That file was already downloaded." : "That download is already in your list.");
    } else {
      toast(currentSettings.save_mode === "device" ? "Device download queued." : "Download added.");
    }
    await loadTasks(true);
  } catch (error) {
    toast(errorMessage(error, "Failed to add task."), "error");
  }
}

function deliveredKey(taskId: string): string {
  return `${taskId}:${tabId}`;
}

function persistDeliveredDeviceDownloads(): void {
  sessionStorage.setItem("neverstelle.deviceDelivered", JSON.stringify(deliveredDeviceDownloads));
}

async function triggerTaskFileDownload(taskId: string): Promise<void> {
  const response = await fetchTaskFile(taskId);
  const blob = await response.blob();
  const objectUrl = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = objectUrl;
  link.rel = "noopener";
  link.download = filenameFromContentDisposition(response.headers.get("content-disposition") || "");
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
}

function filenameFromContentDisposition(header: string): string {
  const utf8Match = header.match(/filename\*=UTF-8''([^;]+)/i);
  const asciiMatch = header.match(/filename="?([^";]+)"?/i);
  return utf8Match ? decodeURIComponent(utf8Match[1]) : asciiMatch ? asciiMatch[1] : "download";
}

async function acknowledgeDeliveredTask(taskId: string): Promise<void> {
  try {
    await markTaskDelivered(taskId, tabId);
  } catch {
    // Acknowledgement is best-effort; the browser download has already started.
  }
}

async function downloadTaskFile(taskId: string, options: { skipReload?: boolean } = {}): Promise<void> {
  await triggerTaskFileDownload(taskId);
  await acknowledgeDeliveredTask(taskId);
  deliveredDeviceDownloads[deliveredKey(taskId)] = true;
  persistDeliveredDeviceDownloads();
  if (!options.skipReload) {
    await loadTasks(true);
  }
}

function processCompletedDeviceDownloads(tasks: TaskItem[]): void {
  tasks.forEach((task) => {
    if (task.save_mode !== "device" || task.status !== "completed" || !task.resolved_full_path) return;
    const allowedTabs = Array.isArray(task.device_request_tabs) ? task.device_request_tabs : [];
    if (!allowedTabs.includes(tabId)) return;
    const key = deliveredKey(task.vid);
    if (deliveredDeviceDownloads[key] || deviceDownloadsInFlight.has(key)) return;
    deviceDownloadsInFlight.add(key);
    downloadTaskFile(task.vid, { skipReload: true })
      .catch((error) => toast(errorMessage(error, "Could not download that file."), "error"))
      .finally(() => {
        deviceDownloadsInFlight.delete(key);
        persistDeliveredDeviceDownloads();
      });
  });
}

async function removeTask(taskId: string): Promise<void> {
  try {
    await removeTaskRequest(taskId);
    toast("Task removed from the list.");
    await loadTasks(true);
  } catch (error) {
    toast(errorMessage(error, "Could not remove task."), "error");
  }
}

async function hideTask(taskId: string): Promise<void> {
  try {
    await hideTaskRequest(taskId);
    toast("Task cleared.");
    await loadTasks(true);
  } catch (error) {
    toast(errorMessage(error, "Could not hide task."), "error");
  }
}

async function clearPending(): Promise<void> {
  try {
    const data = await clearPendingTasks();
    toast(data.cleared === 0 ? "No queued tasks to clear." : `Cleared ${data.cleared} queued task${data.cleared === 1 ? "" : "s"}.`);
    await loadTasks(true);
  } catch (error) {
    toast(errorMessage(error, "Could not clear queue."), "error");
  }
}

async function clearCompleted(): Promise<void> {
  try {
    const data = await clearCompletedTasks();
    if ((data.cleared || 0) === 0 && (data.skipped || 0) === 0) {
      toast("No done tasks to clear.");
    } else if ((data.skipped || 0) > 0) {
      toast(
        `Cleared ${data.cleared || 0} done task${(data.cleared || 0) === 1 ? "" : "s"}. ${data.skipped} device download${
          data.skipped === 1 ? " is" : "s are"
        } still waiting to finish delivery.`,
      );
    } else {
      toast(`Cleared ${data.cleared} done task${data.cleared === 1 ? "" : "s"}.`);
    }
    await loadTasks(true);
  } catch (error) {
    toast(errorMessage(error, "Could not clear done."), "error");
  }
}

function getThemeMode(): "light" | "dark" {
  return localStorage.getItem("neverstelle.themeMode") === "light" ? "light" : "dark";
}

function applyTheme(mode: "light" | "dark"): void {
  const nextMode = mode === "light" ? "light" : "dark";
  isLightMode.value = nextMode === "light";
  document.body.classList.toggle("light-mode", isLightMode.value);
  localStorage.setItem("neverstelle.themeMode", nextMode);
}

function toggleThemeMode(): void {
  applyTheme(isLightMode.value ? "dark" : "light");
}

function formatTimestamp(value: string): string {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString();
}

function toast(message: string, type: ToastType = "success"): void {
  const id = ++toastId;
  toasts.value.push({ id, message, type });
  srStatus.value = message;
  window.setTimeout(() => {
    toasts.value = toasts.value.filter((item) => item.id !== id);
  }, 3200);
}

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error && error.message ? error.message : fallback;
}

function controlButtonClass(active: boolean): string {
  return active
    ? "bg-[var(--ns-accent)] text-[color:var(--ns-strong-text)]"
    : "text-[var(--ns-muted)] hover:text-[var(--ns-text)]";
}

function saveModeButtonClass(mode: "nas" | "device"): string {
  return savedSettings.value.save_mode === mode ? "bg-[var(--ns-accent)] text-[color:var(--ns-strong-text)]" : "text-[var(--ns-muted)]";
}

function handleVisibilityChange(): void {
  if (document.hidden) {
    if (pollHandle) {
      window.clearInterval(pollHandle);
      pollHandle = null;
    }
    return;
  }
  void loadTasks(true);
}

function handleKeyDown(event: KeyboardEvent): void {
  if (!settingsOpen.value) return;
  if (event.key === "Escape") {
    closeSettings();
    return;
  }
  if (event.key !== "Tab") return;
  const focusable = Array.from(
    settingsModalRef.value?.querySelectorAll<HTMLElement>("button, select, input, textarea, [href], [tabindex]:not([tabindex='-1'])") || [],
  ).filter((item) => !item.hasAttribute("disabled"));
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  if (!first || !last) return;
  if (event.shiftKey && document.activeElement === first) {
    event.preventDefault();
    last.focus();
  } else if (!event.shiftKey && document.activeElement === last) {
    event.preventDefault();
    first.focus();
  }
}

onMounted(() => {
  if (!localStorage.getItem("neverstelle.activeMenu")) {
    localStorage.setItem("neverstelle.activeMenu", "all");
  }
  applyTheme(getThemeMode());
  document.addEventListener("visibilitychange", handleVisibilityChange);
  document.addEventListener("keydown", handleKeyDown);
  void fetchUIConfig();
  void loadTasks();
});

onBeforeUnmount(() => {
  document.removeEventListener("visibilitychange", handleVisibilityChange);
  document.removeEventListener("keydown", handleKeyDown);
  document.body.classList.remove("overflow-hidden");
  if (pollHandle) window.clearInterval(pollHandle);
});
</script>

<template>
  <div class="min-h-screen bg-[var(--ns-bg)] text-[var(--ns-text)] font-sans">
    <a
      class="sr-only focus:not-sr-only focus:absolute focus:left-4 focus:top-4 focus:z-50 focus:rounded-xl focus:bg-white focus:px-4 focus:py-2 focus:font-bold focus:text-slate-900"
      href="#mainContent"
    >
      Skip to content
    </a>

    <main id="mainContent" class="mx-auto max-w-[1180px] px-4 pt-2 pb-4 md:pt-3 md:pb-6" tabindex="-1">
      <section class="mb-4 flex flex-wrap items-start justify-between gap-5">
        <div class="flex min-w-0 flex-1 flex-col gap-4">
          <div class="flex items-center justify-between gap-3">
            <a href="/" class="inline-flex min-w-0 items-center gap-[0.55rem] sm:gap-[0.7rem]" aria-label="Never Stelle Home">
              <img src="/assets/logo.png" alt="" class="h-[2.8rem] w-auto flex-none sm:h-[3.05rem]" />
              <span
                class="translate-y-[2px] whitespace-nowrap font-['League_Spartan',ui-sans-serif,system-ui,sans-serif] text-[clamp(1.55rem,2.65vw,2.3rem)] font-extrabold uppercase tracking-[0.03em] text-[var(--ns-cta-bg)] [text-shadow:0_1px_0_rgba(33,5,53,0.06)]"
              >
                Never Stelle
              </span>
            </a>
            <div class="flex items-center gap-2">
              <button
                type="button"
                class="flex h-12 w-12 shrink-0 items-center justify-center rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel)] text-[var(--ns-text)] transition hover:-translate-y-px hover:border-[var(--ns-border-strong)]"
                :aria-label="isLightMode ? 'Switch to dark mode' : 'Switch to light mode'"
                :aria-pressed="isLightMode"
                :title="isLightMode ? 'Switch to dark mode' : 'Switch to light mode'"
                @click="toggleThemeMode"
              >
                <Sun v-if="isLightMode" class="h-5 w-5" />
                <Moon v-else class="h-5 w-5" />
              </button>
              <button
                type="button"
                class="flex h-12 w-12 shrink-0 items-center justify-center rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel)] text-[var(--ns-text)] transition hover:-translate-y-px hover:border-[var(--ns-border-strong)]"
                aria-label="Open settings"
                @click="openSettings"
              >
                <Settings class="h-5 w-5" />
              </button>
            </div>
          </div>
        </div>
      </section>

      <section class="mt-4 rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel)] p-4 shadow-soft">
        <form class="space-y-3" @submit.prevent="addDownloadTask">
          <div class="grid grid-cols-1 gap-2.5 lg:grid-cols-[minmax(0,1fr)_auto_auto] lg:items-center">
            <div
              class="overflow-hidden rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] focus-within:border-[color-mix(in_srgb,var(--ns-accent)_70%,transparent)] focus-within:ring-2 focus-within:ring-[color-mix(in_srgb,var(--ns-accent)_20%,transparent)]"
            >
              <div class="flex h-[46px] items-center gap-2 pr-2">
                <input
                  v-model="url"
                  name="url"
                  type="url"
                  autocomplete="off"
                  placeholder="Paste a supported URL"
                  required
                  class="h-full w-full bg-transparent px-4 text-base text-[var(--ns-text)] outline-none placeholder:text-[color:var(--ns-input-placeholder)]"
                />
              </div>
            </div>

            <div class="flex h-[46px] w-full rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] p-1 shadow-soft lg:w-auto lg:min-w-[290px]" aria-label="Save mode">
              <button
                type="button"
                class="flex-1 rounded-xl px-4 text-sm font-semibold transition"
                :class="saveModeButtonClass('nas')"
                :aria-pressed="savedSettings.save_mode === 'nas'"
                @click="updateSaveMode('nas')"
              >
                Save to NAS
              </button>
              <button
                type="button"
                class="flex-1 rounded-xl px-4 text-sm font-semibold transition"
                :class="saveModeButtonClass('device')"
                :aria-pressed="savedSettings.save_mode === 'device'"
                @click="updateSaveMode('device')"
              >
                Save to device
              </button>
            </div>

            <button
              type="submit"
              class="flex h-[46px] w-full shrink-0 items-center justify-center rounded-2xl border border-transparent bg-[var(--ns-cta-bg)] font-bold text-[var(--ns-cta-text)] shadow-[inset_0_0_0_1px_rgba(255,255,255,0.04)] transition hover:-translate-y-px hover:bg-[var(--ns-cta-bg-hover)] lg:w-[46px]"
              aria-label="Download"
            >
              <Download class="h-5 w-5" />
            </button>
          </div>
        </form>
      </section>

      <section class="my-5 flex flex-col gap-3 xl:flex-row xl:items-center">
        <div class="min-w-0 flex-1 overflow-hidden rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel)] shadow-soft">
          <div class="grid grid-cols-2 md:grid-cols-4">
            <div class="flex items-center justify-between gap-3 border-b border-r border-[var(--ns-border)] px-4 py-3 md:border-b-0">
              <span class="text-sm text-[var(--ns-muted)]">Queued</span>
              <strong class="text-2xl font-bold leading-none">{{ countsForActiveMenu.queued }}</strong>
            </div>
            <div class="flex items-center justify-between gap-3 border-b border-[var(--ns-border)] px-4 py-3 md:border-b-0 md:border-r">
              <span class="text-sm text-[var(--ns-muted)]">Active</span>
              <strong class="text-2xl font-bold leading-none">{{ countsForActiveMenu.running }}</strong>
            </div>
            <div class="flex items-center justify-between gap-3 border-r border-[var(--ns-border)] px-4 py-3 md:border-r">
              <span class="text-sm text-[var(--ns-muted)]">Done</span>
              <strong class="text-2xl font-bold leading-none">{{ countsForActiveMenu.completed }}</strong>
            </div>
            <div class="flex items-center justify-between gap-3 px-4 py-3">
              <span class="text-sm text-[var(--ns-muted)]">Failed</span>
              <strong class="text-2xl font-bold leading-none">{{ countsForActiveMenu.failed }}</strong>
            </div>
          </div>
        </div>

        <div class="flex w-full gap-2 xl:w-auto xl:shrink-0">
          <button
            type="button"
            class="inline-flex h-10 flex-1 items-center justify-center rounded-2xl border-[1.5px] border-[var(--ns-danger-border)] bg-[var(--ns-danger-bg)] px-4 text-sm font-semibold text-[var(--ns-danger-text)] transition hover:-translate-y-px hover:border-[var(--ns-danger-hover-border)] hover:bg-[var(--ns-danger-hover-bg)] hover:text-[var(--ns-danger-hover-text)] xl:min-w-[124px] xl:flex-none"
            @click="clearCompleted"
          >
            Clear done
          </button>
          <button
            type="button"
            class="inline-flex h-10 flex-1 items-center justify-center rounded-2xl border-[1.5px] border-[var(--ns-danger-border)] bg-[var(--ns-danger-bg)] px-4 text-sm font-semibold text-[var(--ns-danger-text)] transition hover:-translate-y-px hover:border-[var(--ns-danger-hover-border)] hover:bg-[var(--ns-danger-hover-bg)] hover:text-[var(--ns-danger-hover-text)] xl:min-w-[128px] xl:flex-none"
            @click="clearPending"
          >
            Clear queue
          </button>
        </div>
      </section>

      <section aria-labelledby="downloadsHeading">
        <div class="mb-3 flex flex-col gap-3">
          <h2 id="downloadsHeading" class="text-xs font-semibold uppercase tracking-[0.12em] text-[var(--ns-muted)]">Downloads</h2>

          <div class="rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel)] p-2 shadow-soft">
            <div class="flex flex-col gap-2 lg:flex-row lg:items-center lg:justify-between">
              <div class="max-w-full overflow-x-auto">
                <nav class="inline-flex w-max flex-nowrap items-center gap-0.5 whitespace-nowrap rounded-xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] p-1 shadow-soft" aria-label="Task sources">
                  <button
                    v-for="menu in MENU_KEYS"
                    :key="menu"
                    class="inline-flex h-8 items-center rounded-lg px-4 text-sm font-semibold transition"
                    :class="controlButtonClass(activeMenu === menu)"
                    type="button"
                    :aria-pressed="activeMenu === menu"
                    @click="setActiveMenu(menu)"
                  >
                    {{ SITE_LABELS[menu] }}
                  </button>
                </nav>
              </div>

              <div class="flex flex-wrap items-center gap-2">
                <div class="flex items-center gap-0.5 rounded-xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] p-1 shadow-soft">
                  <button
                    class="flex h-8 w-8 items-center justify-center rounded-lg transition"
                    :class="controlButtonClass(viewMode === 'grid')"
                    type="button"
                    aria-label="Grid view"
                    :aria-pressed="viewMode === 'grid'"
                    @click="setViewMode('grid')"
                  >
                    <Grid3X3 class="h-4 w-4" />
                  </button>
                  <button
                    class="flex h-8 w-8 items-center justify-center rounded-lg transition"
                    :class="controlButtonClass(viewMode === 'table')"
                    type="button"
                    aria-label="Table view"
                    :aria-pressed="viewMode === 'table'"
                    @click="setViewMode('table')"
                  >
                    <List class="h-4 w-4" />
                  </button>
                </div>

                <div class="flex items-center gap-0.5 rounded-xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] p-1 shadow-soft">
                  <button
                    v-for="(label, filter) in FILTER_LABELS"
                    :key="filter"
                    class="h-8 rounded-lg px-4 text-sm font-semibold transition"
                    :class="controlButtonClass(activeFilter === filter)"
                    type="button"
                    :aria-pressed="activeFilter === filter"
                    @click="setActiveFilter(filter)"
                  >
                    {{ label }}
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>

        <TaskList
          :tasks="filteredTasks"
          :view-mode="viewMode"
          :active-menu="activeMenu"
          :active-menu-label="activeMenuLabel"
          @download="(taskId) => downloadTaskFile(taskId).catch((error) => toast(errorMessage(error, 'Could not download that file.'), 'error'))"
          @hide="hideTask"
          @remove="removeTask"
        />
      </section>
    </main>

    <div
      v-show="settingsOpen"
      ref="settingsModalRef"
      class="settings-modal fixed inset-0 z-50 flex overflow-hidden overscroll-contain bg-[color:var(--ns-overlay)] p-0 text-[var(--ns-text)] sm:items-center sm:justify-center sm:p-4"
      role="dialog"
      aria-modal="true"
      aria-labelledby="settingsTitle"
      @click.self="closeSettings"
    >
      <div class="flex h-[100dvh] w-full flex-col overflow-hidden rounded-t-[1.75rem] border border-[var(--ns-border)] bg-[var(--ns-panel)] text-[var(--ns-text)] shadow-soft sm:h-[min(88dvh,760px)] sm:max-h-[min(88dvh,760px)] sm:max-w-[940px] sm:rounded-2xl">
        <div class="flex items-center justify-between gap-4 border-b border-[var(--ns-border)] px-4 py-3 sm:px-6 sm:py-4">
          <div class="min-w-0 flex min-h-[44px] items-center">
            <h2 id="settingsTitle" class="text-2xl font-bold leading-none text-[var(--ns-text)]">Settings</h2>
          </div>
          <button
            type="button"
            class="flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] text-[var(--ns-muted)] transition hover:-translate-y-px hover:border-[var(--ns-border-strong)] hover:text-[var(--ns-text)]"
            aria-label="Close settings"
            @click="closeSettings"
          >
            <X class="h-5 w-5" />
          </button>
        </div>

        <datalist id="downloadLocationSuggestions">
          <option v-for="location in settings.download_locations" :key="location" :value="location"></option>
        </datalist>

        <div class="min-h-0 flex-1 overflow-hidden px-4 pb-4 pt-3 sm:px-6 sm:pb-5 sm:pt-4">
          <div class="grid h-full min-h-0 grid-rows-[auto_minmax(0,1fr)] gap-4 md:grid-cols-[220px_minmax(0,1fr)] md:grid-rows-1">
            <nav class="settings-scroll -mx-1 flex min-h-0 shrink-0 gap-2 overflow-x-auto overscroll-contain touch-pan-x px-1 pb-1 text-[var(--ns-text)] md:mx-0 md:flex-col md:overflow-x-hidden md:overflow-y-auto md:touch-pan-y md:px-0 md:pb-0" aria-label="Settings sections">
              <button
                v-for="section in SETTINGS_SECTIONS"
                :key="section.key"
                type="button"
                class="settings-nav-btn flex min-h-[48px] min-w-[148px] items-center rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] px-4 text-left text-sm font-semibold transition hover:border-[var(--ns-border-strong)] aria-pressed:bg-[color-mix(in_srgb,var(--ns-accent)_26%,var(--ns-panel2))] aria-pressed:border-[var(--ns-border-strong)] aria-pressed:text-[var(--ns-text)] aria-pressed:[box-shadow:inset_0_0_0_1px_color-mix(in_srgb,var(--ns-accent)_22%,transparent)] md:min-w-0"
                :class="settingsSection === section.key ? 'text-[var(--ns-text)]' : 'text-[var(--ns-muted)] hover:text-[var(--ns-text)]'"
                :aria-pressed="settingsSection === section.key"
                @click="setSettingsSection(section.key, true)"
              >
                {{ section.label }}
              </button>
            </nav>

            <div class="min-h-0 overflow-hidden md:min-w-0">
              <div class="settings-scroll h-full overflow-y-auto overscroll-contain touch-pan-y pr-1">
                <div class="flex min-h-full flex-col">
                  <section
                    v-show="settingsSection === 'downloads'"
                    class="flex-1 rounded-3xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] p-4 text-[var(--ns-text)] sm:p-5"
                  >
                    <div class="grid gap-3 sm:grid-cols-2">
                      <label
                        v-for="site in SITE_KEYS"
                        :key="site"
                        class="block rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel)] p-3 text-sm font-semibold"
                      >
                        <span class="mb-2 block text-[var(--ns-text)]">{{ siteLabel(site) }}</span>
                        <input
                          :id="`${site}LocationInput`"
                          v-model="settingsDraft.site_locations[site]"
                          list="downloadLocationSuggestions"
                          placeholder="Enter a save path"
                          class="min-h-[48px] w-full rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] px-4 py-3 text-[var(--ns-text)] outline-none transition placeholder:text-[var(--ns-muted)] focus:border-[color-mix(in_srgb,var(--ns-accent)_70%,transparent)] focus:ring-2 focus:ring-[color-mix(in_srgb,var(--ns-accent)_20%,transparent)]"
                        />
                      </label>
                    </div>
                  </section>

                  <section
                    v-show="settingsSection === 'instagram'"
                    class="flex-1 rounded-3xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] p-4 text-[var(--ns-text)] sm:p-5"
                  >
                    <div class="flex h-full flex-col rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel)] p-3">
                      <div class="mb-3 text-sm font-semibold text-[var(--ns-text)]">yt-dlp Cookies</div>

                      <label class="flex flex-col text-sm font-semibold">
                        <span class="mb-2 block text-[var(--ns-text)]">Cookies</span>
                        <input
                          id="instagramYtdlpCookiesInput"
                          ref="cookiesInput"
                          type="file"
                          accept=".txt,.cookies,text/plain"
                          class="block min-h-[48px] w-full rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] px-4 py-3 text-sm text-[var(--ns-text)] outline-none transition file:mr-3 file:rounded-xl file:border-0 file:bg-[var(--ns-accent)] file:px-3 file:py-2 file:text-sm file:font-semibold file:text-[color:var(--ns-strong-text)] focus:border-[color-mix(in_srgb,var(--ns-accent)_70%,transparent)] focus:ring-2 focus:ring-[color-mix(in_srgb,var(--ns-accent)_20%,transparent)]"
                        />
                      </label>

                      <div class="mt-3 rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] px-4 py-3 text-sm text-[var(--ns-muted)]">
                        {{ cookiesStatusText }}
                      </div>

                      <div class="mt-auto flex flex-wrap justify-end gap-3 pt-3">
                        <button
                          type="button"
                          class="h-11 rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] px-4 text-sm font-semibold text-[var(--ns-text)] transition hover:-translate-y-px hover:border-[var(--ns-border-strong)] hover:bg-[var(--ns-panel)] disabled:cursor-not-allowed disabled:opacity-50"
                          :disabled="!settings.instagram_ytdlp_cookies.configured"
                          @click="removeInstagramCookies"
                        >
                          Disconnect
                        </button>
                        <button
                          type="button"
                          class="inline-flex h-11 items-center justify-center gap-2 rounded-2xl bg-[var(--ns-accent)] px-4 text-sm font-semibold text-[color:var(--ns-strong-text)] transition hover:-translate-y-px hover:brightness-110"
                          @click="connectInstagramCookies"
                        >
                          <Upload class="h-4 w-4" />
                          Connect
                        </button>
                      </div>
                    </div>
                  </section>

                  <section
                    v-show="settingsSection === 'advanced'"
                    class="flex-1 rounded-3xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] p-4 text-[var(--ns-text)] sm:p-5"
                  >
                    <div class="grid gap-3">
                      <label class="block rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel)] p-3 text-sm font-semibold">
                        <span class="mb-2 block text-[var(--ns-text)]">Folder Template</span>
                        <input
                          id="folderTemplateInput"
                          v-model="settingsDraft.template_settings.folder_template"
                          class="min-h-[48px] w-full rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] px-4 py-3 font-mono text-sm text-[var(--ns-text)] outline-none transition placeholder:text-[var(--ns-muted)] focus:border-[color-mix(in_srgb,var(--ns-accent)_70%,transparent)] focus:ring-2 focus:ring-[color-mix(in_srgb,var(--ns-accent)_20%,transparent)]"
                        />
                      </label>
                      <label class="block rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel)] p-3 text-sm font-semibold">
                        <span class="mb-2 block text-[var(--ns-text)]">Filename Template</span>
                        <textarea
                          v-model="settingsDraft.template_settings.filename_template"
                          rows="2"
                          class="min-h-[84px] w-full rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] px-4 py-3 font-mono text-sm text-[var(--ns-text)] outline-none transition placeholder:text-[var(--ns-muted)] focus:border-[color-mix(in_srgb,var(--ns-accent)_70%,transparent)] focus:ring-2 focus:ring-[color-mix(in_srgb,var(--ns-accent)_20%,transparent)]"
                        ></textarea>
                      </label>
                    </div>
                  </section>
                </div>
              </div>
            </div>
          </div>
        </div>

        <footer class="border-t border-[var(--ns-border)] bg-[var(--ns-panel)] px-4 py-4 text-[var(--ns-text)] sm:px-6">
          <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <button
              type="button"
              class="h-11 w-full rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] px-5 text-sm font-semibold text-[var(--ns-muted)] transition hover:-translate-y-px hover:border-[var(--ns-border-strong)] hover:text-[var(--ns-text)] disabled:cursor-wait disabled:opacity-70 sm:w-auto"
              :disabled="cleanupBusy"
              title="Find and delete all .nfo sidecar files from the downloads folder"
              @click="cleanNfo"
            >
              {{ cleanupBusy ? "Deleting..." : "Delete .nfo files" }}
            </button>
            <div class="grid grid-cols-2 gap-3 sm:flex sm:flex-wrap sm:justify-end">
              <button
                class="h-11 rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel2)] px-5 text-sm font-semibold text-[var(--ns-muted)] transition hover:-translate-y-px hover:border-[var(--ns-border-strong)] hover:text-[var(--ns-text)]"
                type="button"
                @click="closeSettings"
              >
                Cancel
              </button>
              <button
                class="h-11 rounded-2xl bg-[var(--ns-accent)] px-5 text-sm font-semibold text-[color:var(--ns-strong-text)] transition hover:-translate-y-px hover:brightness-110"
                type="button"
                @click="saveSettingsDraft"
              >
                Save
              </button>
            </div>
          </div>
        </footer>
      </div>
    </div>

    <ToastStack :toasts="toasts" />
    <div class="sr-only" aria-live="polite" aria-atomic="true">{{ srStatus }}</div>
  </div>
</template>
