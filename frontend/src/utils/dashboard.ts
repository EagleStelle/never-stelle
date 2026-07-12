import { DEFAULT_SOURCE_PROFILES } from "../ui";
import {
  FALLBACK_SOURCE_KEY,
  PAGE_KEYS,
  type CookiesStatus,
  type MediaFilter,
  type MediaMode,
  type MenuKey,
  type PageKey,
  type QualityOptions,
  type QualityPreset,
  type QualitySelection,
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

export const LOSSLESS_AUDIO_FORMATS = new Set(["flac", "wav", "alac"]);

export function isLosslessAudioFormat(format: string): boolean {
  return LOSSLESS_AUDIO_FORMATS.has(String(format || "").toLowerCase());
}

const DEFAULT_QUALITY_OPTIONS: QualityOptions = {
  video: [
    { key: "best", label: "Best" },
    { key: "1080p", label: "1080p" },
    { key: "720p", label: "720p" },
    { key: "480p", label: "480p" },
  ],
  video_containers: [
    { key: "mp4", label: "MP4", codecs: ["av1", "h264", "h265"] },
    { key: "mkv", label: "MKV", codecs: ["av1", "vp9", "h264", "h265"] },
    { key: "webm", label: "WebM", codecs: ["av1", "vp9"] },
  ],
  video_codecs: [
    { key: "auto", label: "Auto" },
    { key: "av1", label: "AV1" },
    { key: "vp9", label: "VP9" },
    { key: "h264", label: "H.264" },
    { key: "h265", label: "H.265" },
  ],
  audio_formats: [
    { key: "mp3", label: "MP3" },
    { key: "m4a", label: "M4A" },
    { key: "opus", label: "Opus" },
    { key: "aac", label: "AAC" },
    { key: "flac", label: "FLAC" },
    { key: "wav", label: "WAV" },
  ],
  audio_bitrates: [
    { key: "best", label: "Best" },
    { key: "320", label: "320 kbps" },
    { key: "192", label: "192 kbps" },
    { key: "128", label: "128 kbps" },
  ],
};

function createQualityPreset(source: Partial<QualityPreset>): QualityPreset {
  const key = String(source.key || "").trim().toLowerCase();
  return {
    key,
    label: String(source.label || key || "Unknown"),
    codecs: Array.isArray(source.codecs)
      ? source.codecs.map((codec) => String(codec || "").trim().toLowerCase()).filter(Boolean)
      : undefined,
  };
}

function createQualityPresetList(
  source: Array<Partial<QualityPreset>> | undefined,
  fallback: QualityPreset[],
): QualityPreset[] {
  const sourceItems = Array.isArray(source) ? source : [];
  const items = sourceItems
    .map(createQualityPreset)
    .filter((item) => item.key);
  return items.length ? items : fallback.map(createQualityPreset);
}

export function createQualityOptions(source: Partial<QualityOptions> = {}): QualityOptions {
  return {
    video: createQualityPresetList(source.video, DEFAULT_QUALITY_OPTIONS.video),
    video_containers: createQualityPresetList(source.video_containers, DEFAULT_QUALITY_OPTIONS.video_containers),
    video_codecs: createQualityPresetList(source.video_codecs, DEFAULT_QUALITY_OPTIONS.video_codecs),
    audio_formats: createQualityPresetList(source.audio_formats, DEFAULT_QUALITY_OPTIONS.audio_formats),
    audio_bitrates: createQualityPresetList(source.audio_bitrates, DEFAULT_QUALITY_OPTIONS.audio_bitrates),
  };
}

export function containerCodecs(container: string, containers: QualityPreset[]): Set<string> {
  const key = String(container || "").trim().toLowerCase();
  return new Set(containers.find((item) => item.key === key)?.codecs || []);
}

export function isCodecAllowed(codec: string, container: string, containers: QualityPreset[]): boolean {
  const key = String(codec || "").trim().toLowerCase();
  return key === "auto" || containerCodecs(container, containers).has(key);
}

export function resolveCodec(codec: string, container: string, containers: QualityPreset[]): string {
  const key = String(codec || "").trim().toLowerCase();
  return isCodecAllowed(key, container, containers) ? key : "auto";
}

function optionKey(value: unknown, items: QualityPreset[], fallback: string): string {
  const key = String(value || "").trim().toLowerCase();
  return items.some((item) => item.key === key) ? key : fallback;
}

export function createQualitySelection(
  source: Partial<QualitySelection> = {},
  qualityOptions: Partial<QualityOptions> = {},
): QualitySelection {
  const options = createQualityOptions(qualityOptions);
  const mode: MediaMode = source.mode === "audio" ? "audio" : "video";
  const videoContainer = optionKey(source.video_container, options.video_containers, "mp4");
  return {
    mode,
    video_quality: optionKey(source.video_quality, options.video, "best"),
    video_container: videoContainer,
    video_codec: resolveCodec(optionKey(source.video_codec, options.video_codecs, "auto"), videoContainer, options.video_containers),
    audio_format: optionKey(source.audio_format, options.audio_formats, "mp3"),
    audio_bitrate: optionKey(source.audio_bitrate, options.audio_bitrates, "best"),
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
  return (
    value === "downloads" ||
    value === "cookies" ||
    value === "quality" ||
    value === "folder-template" ||
    value === "filename-template"
  );
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
