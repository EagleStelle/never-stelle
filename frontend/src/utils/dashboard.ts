import { MENU_KEYS, PAGE_KEYS, type CookiesStatus, type MenuKey, type PageKey, type SavedSettings, type SiteLocations, type TaskCounts, type TaskFilter, type TaskItem, type ViewMode } from "../types";

export function createSiteLocations(source: Partial<SiteLocations> = {}): SiteLocations {
  return {
    youtube: source.youtube || "",
    tiktok: source.tiktok || "",
    instagram: source.instagram || "",
    twitter: source.twitter || "",
    facebook: source.facebook || "",
    reddit: source.reddit || "",
    twitch: source.twitch || "",
    pinterest: source.pinterest || "",
    bluesky: source.bluesky || "",
    linkedin: source.linkedin || "",
    others: source.others || "",
  };
}

export function createTemplateSettings(source: Partial<SavedSettings["template_settings"]> = {}) {
  return {
    folder_template: source.folder_template || "",
    filename_template: source.filename_template || "",
  };
}

export function createCookiesStatus(source: Partial<CookiesStatus> = {}): CookiesStatus {
  return {
    configured: Boolean(source.configured),
    source: source.source || "none",
    filename: source.filename || "",
    uploaded_at: source.uploaded_at || "",
  };
}

export function getTabId(): string {
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

export function readJsonRecord(key: string): Record<string, boolean> {
  try {
    return JSON.parse(sessionStorage.getItem(key) || "{}") || {};
  } catch {
    return {};
  }
}

export function isMenuKey(value: string | null): value is MenuKey {
  return MENU_KEYS.includes(value as MenuKey);
}

export function isPageKey(value: string | null): value is PageKey {
  return PAGE_KEYS.includes(value as PageKey);
}

export function isFilterKey(value: string | null): value is TaskFilter {
  return value === "all" || value === "active" || value === "done";
}

export function isViewMode(value: string | null): value is ViewMode {
  return value === "grid" || value === "table";
}

export function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error && error.message ? error.message : fallback;
}

export function formatTimestamp(value: string): string {
  if (!value) return "";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "" : date.toLocaleString();
}

export function countTasks(tasks: TaskItem[]): TaskCounts {
  return {
    queued: tasks.filter((task) => task.status === "pending").length,
    running: tasks.filter((task) => task.status === "running").length,
    completed: tasks.filter((task) => task.status === "completed").length,
    failed: tasks.filter((task) => task.status === "failed").length,
  };
}

export function filenameFromContentDisposition(header: string): string {
  const utf8Match = header.match(/filename\*=UTF-8''([^;]+)/i);
  const asciiMatch = header.match(/filename="?([^";]+)"?/i);
  return utf8Match ? decodeURIComponent(utf8Match[1]) : asciiMatch ? asciiMatch[1] : "download";
}
