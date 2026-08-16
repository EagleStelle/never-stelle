import type { Component } from "vue";
import IconHighQuality from "~icons/material-symbols/high-quality";
import Icon4k from "~icons/material-symbols/4k";
import Icon2k from "~icons/material-symbols/2k";
import IconHd from "~icons/material-symbols/hd";
import IconSd from "~icons/material-symbols/sd";
import IconGraphicEq from "~icons/material-symbols/graphic-eq";
import IconAudioFile from "~icons/material-symbols/audio-file";
import IconVideoFile from "~icons/material-symbols/video-file";
import IconMemory from "~icons/material-symbols/memory";
import IconAutoAwesome from "~icons/material-symbols/auto-awesome";

import { DEFAULT_SOURCE_PROFILES } from "@/ui";
import {
  PAGE_KEYS,
  type CookiePolicy,
  type CookiePolicyDefaults,
  type CookiePolicyField,
  type CookiesStatus,
  type SourceCookiePolicies,
  type MediaFilter,
  type MediaMode,
  type MenuKey,
  type PageKey,
  type PostProcessingSelection,
  type PostProcessingSaveAs,
  type QualityOptions,
  type QualityPreset,
  type QualitySelection,
  type FieldRoles,
  type PlatformScrapeRules,
  type SavedSettings,
  type ScrapeRule,
  type SettingsSection,
  type SourceFields,
  type SourceLocationOptions,
  type SourceLocations,
  type SourceProfile,
  type SlugToken,
  type SourceScrapeRules,
  type SourceSlugTokens,
  type SourceTemplates,
  type NamingFlagValue,
  type SourceTitleCleaning,
  type SourceTokenRoles,
  type TaskCounts,
  type TaskFilter,
  type TaskItem,
  type TokenRole,
  type ViewMode,
} from "@/types";

const REMOVED_SOURCE_KEYS = new Set(["others"]);

export function normalizeSourceKey(value: unknown): string {
  const key = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return REMOVED_SOURCE_KEYS.has(key) ? "" : key;
}

export function sourceLabelFromKey(key: string): string {
  const normalized = normalizeSourceKey(key);
  if (!normalized) return "Unresolved";
  return normalized
    .split("-")
    .map((part) => {
      const chunks = part.match(/[a-z]+|\d+/gi) || [part];
      return chunks
        .map((chunk) =>
          /^\d+$/.test(chunk) ? chunk : chunk[0].toUpperCase() + chunk.slice(1),
        )
        .join("");
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
  while (parts.length > 2 && COMMON_HOST_PREFIXES.has(parts[0]))
    parts = parts.slice(1);
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

// Share buttons copy the link wrapped in a title and CJK boilerplate; keep only the link
// so native URL validation still passes. Non-URL input passes through to that validation.
const PASTED_URL_RE = /https?:\/\/[^\s<>"'`　-〿＀-￯]+/;

export function extractUrl(sourceUrl: unknown): string {
  const text = String(sourceUrl || "").trim();
  if (!text) return "";
  const match = PASTED_URL_RE.exec(text);
  return match ? match[0].replace(/[.,;:!?)\]}»]+$/, "") : text;
}

export function faviconUrlForHost(host: string): string {
  const display = displayHost(host);
  return display
    ? `https://www.google.com/s2/favicons?domain=${display}&sz=64`
    : "";
}

export function createSourceProfile(
  source: Partial<SourceProfile> = {},
): SourceProfile {
  const key = normalizeSourceKey(source.key || source.label || "");
  const raw = source as Partial<SourceProfile> & {
    externalBackend?: string;
    settingsManaged?: boolean;
  };
  const profile: SourceProfile = {
    key,
    label: String(source.label || sourceLabelFromKey(key)),
    hosts: Array.isArray(source.hosts)
      ? source.hosts.map(String).filter(Boolean)
      : [],
    icon: source.icon || "",
    icon_url: source.icon_url || "",
  };
  if (source.external !== undefined)
    profile.external = Boolean(source.external);
  if (
    source.settings_managed !== undefined ||
    raw.settingsManaged !== undefined
  ) {
    profile.settings_managed = Boolean(
      source.settings_managed ?? raw.settingsManaged,
    );
  }
  const externalBackend = source.external_backend || raw.externalBackend || "";
  if (externalBackend) profile.external_backend = String(externalBackend);
  return profile;
}

export function isSourceSettingsManaged(
  source: Partial<SourceProfile>,
): boolean {
  if (source.settings_managed !== undefined)
    return source.settings_managed !== false;
  return !source.external;
}

export function settingsManagedSourceProfiles(
  profiles: SourceProfile[],
): SourceProfile[] {
  return profiles.filter(isSourceSettingsManaged);
}

export function mergeSourceProfiles(
  ...sources: Array<Array<Partial<SourceProfile>> | undefined>
): SourceProfile[] {
  const merged = new Map<string, SourceProfile>();
  for (const list of sources) {
    for (const item of list || []) {
      const profile = createSourceProfile(item);
      if (!profile.key) continue;
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
        ...Object.fromEntries(
          Object.entries(profile).filter(([, value]) => value !== ""),
        ),
        hosts,
      });
    }
  }
  return [...merged.values()];
}

// Subpaths are keyed by source, then by that source's learned URL format.
function formatLocations(raw: unknown): Record<string, string> {
  if (!raw || typeof raw !== "object") return {};
  const out: Record<string, string> = {};
  for (const [format, value] of Object.entries(raw as Record<string, unknown>)) {
    out[format] = normalizeSubpath(value);
  }
  return out;
}

// Mirror of the server's rule: posix separators, no anchor, no traversal.
export function normalizeSubpath(raw: unknown): string {
  const value = String(raw || "").trim().replace(/\\/g, "/");
  if (!value || value.startsWith("/") || /^[a-zA-Z]:/.test(value)) return "";
  const parts = value.split("/").filter((part) => part && part !== ".");
  return parts.includes("..") ? "" : parts.join("/");
}

export function createSourceLocations(
  source: Record<string, unknown> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
): SourceLocations {
  const out: SourceLocations = {};
  for (const profile of profiles) out[profile.key] = formatLocations(source[profile.key]);
  for (const [key, value] of Object.entries(source)) {
    const normalizedKey = normalizeSourceKey(key);
    if (normalizedKey && !out[normalizedKey]) out[normalizedKey] = formatLocations(value);
  }
  return out;
}

export function createSourceLocationOptions(
  source: Record<string, unknown> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
): SourceLocationOptions {
  const out: SourceLocationOptions = {};
  const options = (raw: unknown): string[] => {
    const values = Array.isArray(raw) ? raw.map(normalizeSubpath) : [];
    return [...new Set(["", ...values])];
  };
  for (const profile of profiles) out[profile.key] = options(source[profile.key]);
  for (const [key, value] of Object.entries(source)) {
    const normalizedKey = normalizeSourceKey(key);
    if (normalizedKey && !out[normalizedKey]) out[normalizedKey] = options(value);
  }
  return out;
}

export function createTemplateSettings(
  source: Partial<SavedSettings["template_settings"]> = {},
) {
  return {
    folder_template: source.folder_template || "{{username}}",
    filename_template: source.filename_template || "{{username}} - {{title}} [{{id}}]",
  };
}

export const LOSSLESS_AUDIO_FORMATS = new Set(["flac", "wav", "alac"]);

export function isLosslessAudioFormat(format: string): boolean {
  return LOSSLESS_AUDIO_FORMATS.has(String(format || "").toLowerCase());
}

const QUALITY_PRESET_ICONS: Record<string, Component> = {
  best: IconHighQuality,
  "2160p60": Icon4k,
  "1440p60": Icon2k,
  "1080p60": IconHd,
  "1080p": IconHd,
  "720p": IconHd,
  "480p": IconSd,
  "320": IconGraphicEq,
  "192": IconGraphicEq,
  "128": IconGraphicEq,
  auto: IconAutoAwesome,
  mp3: IconAudioFile,
  m4a: IconAudioFile,
  opus: IconAudioFile,
  aac: IconAudioFile,
  flac: IconAudioFile,
  wav: IconAudioFile,
  mp4: IconVideoFile,
  mkv: IconVideoFile,
  webm: IconVideoFile,
  av1: IconMemory,
  vp9: IconMemory,
  h264: IconMemory,
  h265: IconMemory,
};

const DEFAULT_QUALITY_OPTIONS: QualityOptions = {
  video: [
    { key: "best", label: "Best", icon: IconHighQuality },
    { key: "2160p60", label: "2160p60", icon: Icon4k },
    { key: "1440p60", label: "1440p60", icon: Icon2k },
    { key: "1080p60", label: "1080p60", icon: IconHd },
    { key: "1080p", label: "1080p", icon: IconHd },
    { key: "720p", label: "720p", icon: IconHd },
    { key: "480p", label: "480p", icon: IconSd },
  ],
  video_containers: [
    {
      key: "auto",
      label: "Auto",
      icon: IconAutoAwesome,
      codecs: ["av1", "vp9", "h264", "h265"],
      audio_codecs: ["aac", "opus", "mp3", "flac"],
    },
    {
      key: "mp4",
      label: "MP4",
      icon: IconVideoFile,
      codecs: ["av1", "h264", "h265"],
      audio_codecs: ["aac", "mp3"],
    },
    {
      key: "mkv",
      label: "MKV",
      icon: IconVideoFile,
      codecs: ["av1", "vp9", "h264", "h265"],
      audio_codecs: ["aac", "opus", "mp3", "flac"],
    },
    {
      key: "webm",
      label: "WebM",
      icon: IconVideoFile,
      codecs: ["av1", "vp9"],
      audio_codecs: ["opus"],
    },
  ],
  video_codecs: [
    { key: "auto", label: "Auto", icon: IconAutoAwesome },
    { key: "av1", label: "AV1", icon: IconMemory },
    { key: "vp9", label: "VP9", icon: IconMemory },
    { key: "h264", label: "H.264", icon: IconMemory },
    { key: "h265", label: "H.265", icon: IconMemory },
  ],
  video_audio_codecs: [
    { key: "auto", label: "Auto", icon: IconAutoAwesome },
    { key: "aac", label: "AAC", icon: IconAudioFile },
    { key: "opus", label: "Opus", icon: IconAudioFile },
    { key: "mp3", label: "MP3", icon: IconAudioFile },
    { key: "flac", label: "FLAC", icon: IconAudioFile },
  ],
  audio_formats: [
    { key: "auto", label: "Auto", icon: IconAutoAwesome },
    { key: "mp3", label: "MP3", icon: IconAudioFile },
    { key: "m4a", label: "M4A", icon: IconAudioFile },
    { key: "opus", label: "Opus", icon: IconAudioFile },
    { key: "aac", label: "AAC", icon: IconAudioFile },
    { key: "flac", label: "FLAC", icon: IconAudioFile },
    { key: "wav", label: "WAV", icon: IconAudioFile },
  ],
  audio_bitrates: [
    { key: "best", label: "Best", icon: IconHighQuality },
    { key: "320", label: "320 kbps", icon: IconGraphicEq },
    { key: "192", label: "192 kbps", icon: IconGraphicEq },
    { key: "128", label: "128 kbps", icon: IconGraphicEq },
  ],
};

function createQualityPreset(source: Partial<QualityPreset>): QualityPreset {
  const key = String(source.key || "")
    .trim()
    .toLowerCase();
  const preset: QualityPreset = {
    key,
    label: String(source.label || key || "Unknown"),
    icon: source.icon || QUALITY_PRESET_ICONS[key],
  };
  if (Array.isArray(source.codecs)) {
    preset.codecs = source.codecs
      .map((codec) => String(codec || "").trim().toLowerCase())
      .filter(Boolean);
  }
  if (Array.isArray(source.audio_codecs)) {
    preset.audio_codecs = source.audio_codecs
      .map((codec) => String(codec || "").trim().toLowerCase())
      .filter(Boolean);
  }
  if (Array.isArray(source.embed_capabilities)) {
    const valid = new Set<PostProcessingCapability>([
      "metadata",
      "subtitles",
      "automatic_subtitles",
      "chapters",
      "thumbnail",
    ]);
    preset.embed_capabilities = source.embed_capabilities.filter((capability) =>
      valid.has(capability),
    );
  }
  return preset;
}

function createQualityPresetList(
  source: Array<Partial<QualityPreset>> | undefined,
  fallback: QualityPreset[],
): QualityPreset[] {
  const sourceItems = Array.isArray(source) ? source : [];
  const items = sourceItems.map(createQualityPreset).filter((item) => item.key);
  return items.length ? items : fallback.map(createQualityPreset);
}

export function createQualityOptions(
  source: Partial<QualityOptions> = {},
): QualityOptions {
  return {
    video: createQualityPresetList(source.video, DEFAULT_QUALITY_OPTIONS.video),
    video_containers: createQualityPresetList(
      source.video_containers,
      DEFAULT_QUALITY_OPTIONS.video_containers,
    ),
    video_codecs: createQualityPresetList(
      source.video_codecs,
      DEFAULT_QUALITY_OPTIONS.video_codecs,
    ),
    video_audio_codecs: createQualityPresetList(
      source.video_audio_codecs,
      DEFAULT_QUALITY_OPTIONS.video_audio_codecs,
    ),
    audio_formats: createQualityPresetList(
      source.audio_formats,
      DEFAULT_QUALITY_OPTIONS.audio_formats,
    ),
    audio_bitrates: createQualityPresetList(
      source.audio_bitrates,
      DEFAULT_QUALITY_OPTIONS.audio_bitrates,
    ),
  };
}

function optionKey(
  value: unknown,
  items: QualityPreset[],
  fallback: string,
): string {
  const key = String(value || "")
    .trim()
    .toLowerCase();
  return items.some((item) => item.key === key) ? key : fallback;
}

// Auto imposes no codec, so it fits every container; an explicit codec must be one the
// container can play back. Missing compatibility data (older backend) allows everything.
export function isCodecCompatibleWithContainer(
  codec: string,
  container: string,
  containers: QualityPreset[],
): boolean {
  const codecKey = String(codec || "").trim().toLowerCase();
  if (!codecKey || codecKey === "auto") return true;
  const preset = containers.find((item) => item.key === container);
  if (!preset || !Array.isArray(preset.codecs)) return true;
  return preset.codecs.includes(codecKey);
}

// The codec picker for a container: Auto plus the codecs that container can play back.
export function videoCodecOptionsForContainer(
  options: QualityOptions,
  container: string,
): QualityPreset[] {
  return options.video_codecs.filter((codec) =>
    isCodecCompatibleWithContainer(codec.key, container, options.video_containers),
  );
}

export function isAudioCodecCompatibleWithVideoContainer(
  codec: string,
  container: string,
  containers: QualityPreset[],
): boolean {
  const codecKey = String(codec || "").trim().toLowerCase();
  if (!codecKey || codecKey === "auto") return true;
  const preset = containers.find((item) => item.key === container);
  if (!preset || !Array.isArray(preset.audio_codecs)) return true;
  return preset.audio_codecs.includes(codecKey);
}

export function videoAudioCodecOptionsForContainer(
  options: QualityOptions,
  container: string,
): QualityPreset[] {
  return options.video_audio_codecs.filter((codec) =>
    isAudioCodecCompatibleWithVideoContainer(
      codec.key,
      container,
      options.video_containers,
    ),
  );
}

export function createQualitySelection(
  source: Partial<QualitySelection> = {},
  qualityOptions: Partial<QualityOptions> = {},
): QualitySelection {
  const options = createQualityOptions(qualityOptions);
  const mode: MediaMode = source.mode === "audio" ? "audio" : "video";
  const videoContainer = optionKey(source.video_container, options.video_containers, "auto");
  const videoCodec = optionKey(source.video_codec, options.video_codecs, "auto");
  const videoAudioCodec = optionKey(
    source.video_audio_codec,
    options.video_audio_codecs,
    "auto",
  );
  const audioFormat = optionKey(source.audio_format, options.audio_formats, "auto");
  return {
    mode,
    video_quality: optionKey(source.video_quality, options.video, "best"),
    video_container: videoContainer,
    // Drop a codec the container can't play back (e.g. VP9 in MP4) so it never forces an
    // unplayable stream; Auto lets the backend pick a container-compatible codec.
    video_codec: isCodecCompatibleWithContainer(videoCodec, videoContainer, options.video_containers)
      ? videoCodec
      : "auto",
    video_audio_codec: isAudioCodecCompatibleWithVideoContainer(
      videoAudioCodec,
      videoContainer,
      options.video_containers,
    )
      ? videoAudioCodec
      : "auto",
    audio_format: audioFormat,
    audio_bitrate: optionKey(
      source.audio_bitrate,
      options.audio_bitrates,
      "best",
    ),
  };
}

export function createPostProcessingSelection(
  source: Partial<PostProcessingSelection> = {},
): PostProcessingSelection {
  return {
    metadata: Boolean(source.metadata),
    subtitles: Boolean(source.subtitles),
    automatic_subtitles: Boolean(source.automatic_subtitles),
    chapters: Boolean(source.chapters),
    thumbnail: Boolean(source.thumbnail),
    save_as: source.save_as === "embed" ? "embed" : "sidecar",
  };
}

export type PostProcessingCapability = Exclude<keyof PostProcessingSelection, "save_as">;
export type PostProcessingCapabilities = Record<PostProcessingCapability, boolean>;
export const POST_PROCESSING_FIELDS: Array<{
  key: PostProcessingCapability;
  label: string;
}> = [
  { key: "metadata", label: "Metadata" },
  { key: "subtitles", label: "Subtitles" },
  { key: "automatic_subtitles", label: "Subtitles (auto-generated)" },
  { key: "chapters", label: "Chapters" },
  { key: "thumbnail", label: "Thumbnail" },
];

const ALL_POST_PROCESSING_CAPABILITIES: PostProcessingCapabilities = {
  metadata: true,
  subtitles: true,
  automatic_subtitles: true,
  chapters: true,
  thumbnail: true,
};

function embedCapabilitiesForPreset(
  presets: QualityPreset[],
  key: string,
): PostProcessingCapabilities {
  const preset = presets.find((candidate) => candidate.key === key);
  // Older/cached settings payloads have no capability metadata. Keep controls
  // available until the authoritative backend options arrive.
  if (!preset || preset.embed_capabilities === undefined) {
    return { ...ALL_POST_PROCESSING_CAPABILITIES };
  }
  const supported = new Set(preset.embed_capabilities);
  return {
    metadata: supported.has("metadata"),
    subtitles: supported.has("subtitles"),
    automatic_subtitles: supported.has("automatic_subtitles"),
    chapters: supported.has("chapters"),
    thumbnail: supported.has("thumbnail"),
  };
}

export function postProcessingCapabilitiesForQuality(
  quality: QualitySelection,
  options: QualityOptions,
  saveAs: PostProcessingSaveAs,
): PostProcessingCapabilities {
  if (saveAs === "sidecar") return { ...ALL_POST_PROCESSING_CAPABILITIES };
  return quality.mode === "audio"
    ? embedCapabilitiesForPreset(options.audio_formats, quality.audio_format)
    : embedCapabilitiesForPreset(options.video_containers, quality.video_container);
}

export function postProcessingCapabilitiesForDefaults(
  quality: QualitySelection,
  options: QualityOptions,
  saveAs: PostProcessingSaveAs,
): PostProcessingCapabilities {
  if (saveAs === "sidecar") return { ...ALL_POST_PROCESSING_CAPABILITIES };
  const video = embedCapabilitiesForPreset(options.video_containers, quality.video_container);
  const audio = embedCapabilitiesForPreset(options.audio_formats, quality.audio_format);
  return {
    // Defaults are shared by the independent video and audio modes. A feature
    // remains selectable when either configured output can carry it.
    metadata: video.metadata || audio.metadata,
    subtitles: video.subtitles || audio.subtitles,
    automatic_subtitles: video.automatic_subtitles || audio.automatic_subtitles,
    chapters: video.chapters || audio.chapters,
    thumbnail: video.thumbnail || audio.thumbnail,
  };
}

export function constrainPostProcessingSelection(
  selection: PostProcessingSelection,
  capabilities: PostProcessingCapabilities,
): PostProcessingSelection {
  return createPostProcessingSelection({
    ...selection,
    metadata: capabilities.metadata && selection.metadata,
    subtitles: capabilities.subtitles && selection.subtitles,
    automatic_subtitles:
      capabilities.automatic_subtitles && selection.automatic_subtitles,
    chapters: capabilities.chapters && selection.chapters,
    thumbnail: capabilities.thumbnail && selection.thumbnail,
  });
}

export function createSourceTemplates(
  source: Record<string, any> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
  fallback = createTemplateSettings(),
): SourceTemplates {
  const out: SourceTemplates = {};
  for (const profile of profiles) {
    out[profile.key] = {};
    const rawVal = source[profile.key];
    if (rawVal && typeof rawVal === "object") {
      for (const [fmt, settings_dict] of Object.entries(rawVal)) {
        if (settings_dict && typeof settings_dict === "object") {
          out[profile.key][fmt] = createTemplateSettings(settings_dict);
        }
      }
    }
  }
  for (const [key, rawVal] of Object.entries(source)) {
    const normalizedKey = normalizeSourceKey(key);
    if (normalizedKey && !out[normalizedKey]) {
      out[normalizedKey] = {};
      if (rawVal && typeof rawVal === "object") {
        for (const [fmt, settings_dict] of Object.entries(rawVal)) {
          if (settings_dict && typeof settings_dict === "object") {
            out[normalizedKey][fmt] = createTemplateSettings(settings_dict);
          }
        }
      }
    }
  }
  return out;
}

export function createScrapeRule(source: Partial<ScrapeRule> = {}): ScrapeRule {
  return {
    token: String(source.token || "").trim(),
    match_label: String(source.match_label || ""),
    selector: String(source.selector || ""),
    attr: String(source.attr || "").trim() || "text",
    multi: Boolean(source.multi),
    xpath: String(source.xpath || ""),
    format: String(source.format || ""),
  };
}

export function createPlatformScrapeRules(
  source: Partial<PlatformScrapeRules> = {},
): PlatformScrapeRules {
  return {
    rules: Array.isArray(source.rules)
      ? source.rules.map(createScrapeRule)
      : [],
  };
}

export function createSourceScrapeRules(
  source: Record<string, Partial<PlatformScrapeRules>> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
): SourceScrapeRules {
  const out: SourceScrapeRules = {};
  for (const profile of profiles)
    out[profile.key] = createPlatformScrapeRules(source[profile.key] || {});
  for (const [key, value] of Object.entries(source)) {
    const normalizedKey = normalizeSourceKey(key);
    if (normalizedKey) out[normalizedKey] = createPlatformScrapeRules(value);
  }
  return out;
}

const SLUG_PART_RE = /^(path:\d+|query:\S+)$/;

export function normalizeSlugPart(value: unknown): string {
  const part = String(value || "").trim();
  return SLUG_PART_RE.test(part) ? part : "";
}

export function createSlugToken(source: Partial<SlugToken> = {}): SlugToken {
  return {
    part: normalizeSlugPart(source.part),
    token: normalizeTokenName(source.token),
  };
}

export function createSourceSlugTokens(
  source: Record<string, SlugToken[]> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
): SourceSlugTokens {
  const normalizeList = (list: SlugToken[] | undefined): SlugToken[] => {
    const out: SlugToken[] = [];
    const seenParts = new Set<string>();
    const seenTokens = new Set<string>();
    for (const item of Array.isArray(list) ? list : []) {
      const entry = createSlugToken(item);
      if (!entry.part || seenParts.has(entry.part)) continue;
      if (entry.token) {
        if (seenTokens.has(entry.token)) continue;
        seenTokens.add(entry.token);
      }
      seenParts.add(entry.part);
      out.push(entry);
    }
    return out;
  };
  const out: SourceSlugTokens = {};
  for (const profile of profiles) out[profile.key] = normalizeList(source[profile.key]);
  for (const [key, value] of Object.entries(source)) {
    const normalizedKey = normalizeSourceKey(key);
    if (normalizedKey) out[normalizedKey] = normalizeList(value);
  }
  return out;
}

export const TOKEN_ROLES = new Set<TokenRole>([
  "creator",
  "title",
  "ignore",
]);

export function normalizeTokenName(value: unknown): string {
  const token = String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();
  return /^[a-zA-Z_]/.test(token) ? token : "";
}

// A filename token as it appears in templates: {{token}}. Built in script so the literal
// braces never reach Vue's template tokenizer. Defined once, reused by every pane that
// renders a token chip (Scraper, Slug).
export function tokenLabel(token: string): string {
  return `{{${token}}}`;
}

export function displayUrlTemplate(value: unknown): string {
  return String(value || "").replace(
    /(^|[^{])\{([a-zA-Z_][a-zA-Z0-9_]*)\}(?!\})/g,
    (_match, prefix: string, token: string) => `${prefix}${tokenLabel(token)}`,
  );
}

export function createSourceTokenRoles(
  source: Record<string, Record<string, string>> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
): SourceTokenRoles {
  const normalizedSource: Record<string, Record<string, string>> = {};
  for (const [rawKey, roles] of Object.entries(source || {})) {
    const normalizedKey = normalizeSourceKey(rawKey);
    if (normalizedKey) normalizedSource[normalizedKey] = roles || {};
  }
  const keys = new Set([
    ...profiles.map((profile) => profile.key),
    ...Object.keys(normalizedSource),
  ]);
  const out: SourceTokenRoles = {};
  for (const key of keys) {
    const roles: Record<string, TokenRole> = {};
    for (const [token, role] of Object.entries(normalizedSource[key] || {})) {
      const normalizedToken = normalizeTokenName(token);
      const rawRole = String(role || "").trim().toLowerCase();
      const normalizedRole = (
        rawRole === "username" || rawRole === "nickname" ? "creator" : rawRole
      ) as TokenRole;
      if (!normalizedToken || !TOKEN_ROLES.has(normalizedRole)) continue;
      roles[normalizedToken] = normalizedRole;
    }
    out[key] = roles;
  }
  return out;
}

const FIELD_RE = /[^A-Za-z0-9_[\]]+/g;
const SCRAPER_FIELD_RE = /^scraper\[([A-Za-z_][A-Za-z0-9_]*)\]$/;

export function scraperField(token: unknown): string {
  const normalized = normalizeTokenName(token);
  return normalized ? `scraper[${normalized}]` : "";
}

export function scraperTokenFromField(value: unknown): string {
  const match = SCRAPER_FIELD_RE.exec(String(value || "").trim());
  return match?.[1] || "";
}

export function isScraperField(value: unknown): boolean {
  return Boolean(scraperTokenFromField(value));
}

// Keep identifier chars plus gallery-dl [sub] nesting; strip everything else.
export function normalizeField(value: unknown): string {
  const raw = String(value || "").trim();
  if (raw.toLowerCase().startsWith("scraper[") && raw.endsWith("]")) {
    return scraperField(raw.slice(8, -1));
  }
  return raw.replace(FIELD_RE, "");
}

export function createFieldRoles(
  source: Partial<FieldRoles> = {},
): FieldRoles {
  const list = (values: unknown): string[] => {
    const out: string[] = [];
    for (const value of Array.isArray(values) ? values : []) {
      const field = normalizeField(value);
      if (field && !out.includes(field)) out.push(field);
    }
    return out;
  };
  return { username: list(source.username), nickname: list(source.nickname), title: list(source.title) };
}

export function createSourceFields(
  source: Record<string, Partial<FieldRoles>> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
): SourceFields {
  const out: SourceFields = {};
  for (const profile of profiles)
    out[profile.key] = createFieldRoles(source[profile.key] || {});
  for (const [key, value] of Object.entries(source)) {
    const normalizedKey = normalizeSourceKey(key);
    if (normalizedKey) out[normalizedKey] = createFieldRoles(value);
  }
  return out;
}

// Numeric caps. max_chars is positive-only; stem_max_chars may be 0 to turn the cap off.
const NAMING_NUMBER_KEYS = new Set(["max_chars", "stem_max_chars"]);

// Keep only user-set flags; untouched sources stay empty so the server applies rule defaults.
// Choices arrive as strings and must survive verbatim; coercing them to Boolean would
// collapse every option to `true`.
export function createNamingFlags(
  source: Record<string, NamingFlagValue> = {},
): Record<string, NamingFlagValue> {
  const out: Record<string, NamingFlagValue> = {};
  for (const [key, value] of Object.entries(source || {})) {
    if (NAMING_NUMBER_KEYS.has(key)) {
      const parsed = Math.floor(Number(value));
      if (
        Number.isFinite(parsed) &&
        (key === "stem_max_chars" ? parsed >= 0 : parsed > 0)
      ) {
        out[key] = parsed;
      }
    } else if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed) out[key] = trimmed;
    } else {
      out[key] = Boolean(value);
    }
  }
  return out;
}

export function createSourceTitleCleaning(
  source: Record<string, Record<string, NamingFlagValue>> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
): SourceTitleCleaning {
  const out: SourceTitleCleaning = {};
  for (const profile of profiles)
    out[profile.key] = createNamingFlags(source[profile.key] || {});
  for (const [key, value] of Object.entries(source)) {
    const normalizedKey = normalizeSourceKey(key);
    if (normalizedKey) out[normalizedKey] = createNamingFlags(value);
  }
  return out;
}

const COOKIE_POLICY_FIELDS: CookiePolicyField[] = [
  "limit",
  "window",
  "delay",
  "cooldown",
  "wait",
];

export const BUILTIN_COOKIE_POLICY_DEFAULTS: CookiePolicyDefaults = {
  limit: 20,
  window: 300,
  delay: 5,
  cooldown: 900,
  wait: 300,
};

export function createCookiePolicy(source: CookiePolicy = {}): CookiePolicy {
  // Blank fields stay absent so the source keeps inheriting the default.
  const out: CookiePolicy = {};
  for (const field of COOKIE_POLICY_FIELDS) {
    const value = Number(source?.[field]);
    if (source?.[field] !== undefined && source?.[field] !== null && Number.isFinite(value)) {
      out[field] = value;
    }
  }
  return out;
}

export function createSourceCookiePolicies(
  source: Record<string, CookiePolicy> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
): SourceCookiePolicies {
  const out: SourceCookiePolicies = {};
  for (const profile of profiles) out[profile.key] = createCookiePolicy(source[profile.key] || {});
  for (const [key, value] of Object.entries(source)) {
    const normalizedKey = normalizeSourceKey(key);
    if (normalizedKey) out[normalizedKey] = createCookiePolicy(value);
  }
  return out;
}

export function createCookiePolicyDefaults(
  source: Partial<CookiePolicyDefaults> = {},
): CookiePolicyDefaults {
  const out = { ...BUILTIN_COOKIE_POLICY_DEFAULTS };
  for (const field of COOKIE_POLICY_FIELDS) {
    const value = Number(source?.[field]);
    if (Number.isFinite(value)) out[field] = value;
  }
  return out;
}

export function createCookiesStatus(
  source: Partial<CookiesStatus> = {},
): CookiesStatus {
  const cookies = (source.cookies || []).map((cookie) => ({
    id: String(cookie?.id || ""),
    filename: String(cookie?.filename || "cookies.txt"),
    uploaded_at: String(cookie?.uploaded_at || ""),
  }));
  return {
    configured: cookies.length > 0,
    count: cookies.length,
    cookies,
  };
}

export function isMenuKey(
  value: string | null,
  sourceProfiles: SourceProfile[] = [],
): value is MenuKey {
  if (!value) return false;
  return (
    value === "all" || sourceProfiles.some((profile) => profile.key === value)
  );
}

export function isPageKey(value: string | null): value is PageKey {
  return PAGE_KEYS.includes(value as PageKey);
}

export function isSettingsSection(
  value: string | null,
): value is SettingsSection {
  return (
    value === "account" ||
    value === "locations" ||
    value === "cookies" ||
    value === "defaults" ||
    value === "format" ||
    value === "templates" ||
    value === "naming" ||
    value === "fields" ||
    value === "scraper" ||
    value === "slug"
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

export function emptyTaskCounts(): TaskCounts {
  return { queued: 0, running: 0, completed: 0, failed: 0 };
}

export function countTasks(tasks: TaskItem[]): TaskCounts {
  return {
    queued: tasks.filter((task) => task.status === "pending").length,
    running: tasks.filter((task) => task.status === "running").length,
    completed: tasks.filter((task) => task.status === "completed").length,
    failed: tasks.filter((task) => task.status === "failed").length,
  };
}
