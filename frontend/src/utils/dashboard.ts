import { DEFAULT_SOURCE_PROFILES } from "../ui";
import {
  FALLBACK_SOURCE_KEY,
  PAGE_KEYS,
  type CookiesStatus,
  type MediaFilter,
  type MenuKey,
  type PageKey,
  type SavedSettings,
  type SettingsSection,
  type SourceLocations,
  type SourceProfile,
  type SourceTemplates,
  type TaskCounts,
  type TaskFilter,
  type TaskItem,
  type ViewMode,
} from "../types";

export function normalizeSourceKey(value: unknown): string {
  const key = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return key || FALLBACK_SOURCE_KEY;
}

export function sourceLabelFromKey(key: string): string {
  const normalized = normalizeSourceKey(key);
  if (normalized === FALLBACK_SOURCE_KEY) return "Others";
  return normalized
    .split("-")
    .map((part) => {
      const chunks = part.match(/[a-z]+|\d+/gi) || [part];
      return chunks.map((chunk) => (/^\d+$/.test(chunk) ? chunk : chunk[0].toUpperCase() + chunk.slice(1))).join("");
    })
    .join(" ");
}

const COMMON_HOST_PREFIXES = new Set(["amp", "m", "mobile", "www"]);

export function displayHost(host: string): string {
  let parts = String(host || "")
    .trim()
    .toLowerCase()
    .replace(/^\*\./, "")
    .split(".")
    .filter(Boolean);
  while (parts.length > 2 && COMMON_HOST_PREFIXES.has(parts[0])) parts = parts.slice(1);
  return parts.join(".");
}

export function hostFromUrl(sourceUrl: string): string {
  let url = String(sourceUrl || "").trim();
  if (!url) return "";
  if (!url.includes("://")) url = `https://${url}`;
  try {
    return new URL(url).hostname.toLowerCase();
  } catch {
    return "";
  }
}

export function faviconUrlForHost(host: string): string {
  const display = displayHost(host);
  return display ? `https://www.google.com/s2/favicons?domain=${display}&sz=64` : "";
}

export function createSourceProfile(source: Partial<SourceProfile> = {}): SourceProfile {
  const key = normalizeSourceKey(source.key || source.label || "");
  return {
    key,
    label: String(source.label || sourceLabelFromKey(key)),
    hosts: Array.isArray(source.hosts) ? source.hosts.map(String).filter(Boolean) : [],
    icon: source.icon || "",
    icon_url: source.icon_url || "",
  };
}

export function mergeSourceProfiles(...sources: Array<Array<Partial<SourceProfile>> | undefined>): SourceProfile[] {
  const merged = new Map<string, SourceProfile>();
  for (const list of sources) {
    for (const item of list || []) {
      const profile = createSourceProfile(item);
      const existing = merged.get(profile.key);
      if (!existing) {
        merged.set(profile.key, profile);
        continue;
      }
      const hosts = [...existing.hosts];
      for (const host of profile.hosts) {
        if (!hosts.includes(host)) hosts.push(host);
      }
      merged.set(profile.key, {
        ...existing,
        ...Object.fromEntries(Object.entries(profile).filter(([, value]) => value !== "")),
        hosts,
      });
    }
  }
  if (merged.has(FALLBACK_SOURCE_KEY)) {
    const fallback = merged.get(FALLBACK_SOURCE_KEY)!;
    merged.delete(FALLBACK_SOURCE_KEY);
    merged.set(FALLBACK_SOURCE_KEY, fallback);
  }
  return [...merged.values()];
}

export function createSourceLocations(
  source: Partial<SourceLocations> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
): SourceLocations {
  const out: SourceLocations = {};
  for (const profile of profiles) out[profile.key] = source[profile.key] || "";
  for (const [key, value] of Object.entries(source)) out[normalizeSourceKey(key)] = String(value || "");
  return out;
}

export function createTemplateSettings(source: Partial<SavedSettings["template_settings"]> = {}) {
  return {
    folder_template: source.folder_template || "",
    filename_template: source.filename_template || "",
  };
}

export function createSourceTemplates(
  source: Record<string, Partial<SavedSettings["template_settings"]>> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
  fallback = createTemplateSettings(),
): SourceTemplates {
  const out: SourceTemplates = {};
  for (const profile of profiles) {
    out[profile.key] = createTemplateSettings(source[profile.key] || fallback);
  }
  for (const [key, value] of Object.entries(source)) {
    out[normalizeSourceKey(key)] = createTemplateSettings(value);
  }
  return out;
}

export function createCookiesStatus(source: Partial<CookiesStatus> = {}): CookiesStatus {
  return {
    configured: Boolean(source.configured),
    source: source.source || "none",
    filename: source.filename || "",
    uploaded_at: source.uploaded_at || "",
  };
}

export function isMenuKey(value: string | null, sourceProfiles: SourceProfile[] = []): value is MenuKey {
  if (!value) return false;
  return value === "all" || sourceProfiles.some((profile) => profile.key === value);
}

export function isPageKey(value: string | null): value is PageKey {
  return PAGE_KEYS.includes(value as PageKey);
}

export function isSettingsSection(value: string | null): value is SettingsSection {
  return value === "downloads" || value === "cookies" || value === "folder-template" || value === "filename-template";
}

export function isFilterKey(value: string | null): value is TaskFilter {
  return value === "all" || value === "active" || value === "done";
}

export function isMediaFilter(value: string | null): value is MediaFilter {
  return value === "all" || value === "image" || value === "video";
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
