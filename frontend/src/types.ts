import type { Component } from "vue";

export const PAGE_KEYS = ["downloads", "history", "settings"] as const;

export type SourceKey = string;
export type MenuKey = "all" | SourceKey;
export type PageKey = (typeof PAGE_KEYS)[number];
export type TaskStatus =
  "pending" | "running" | "completed" | "failed" | string;
export type TaskFilter = "all" | "active" | "done";
export type MediaFilter = "all" | "image" | "video";
export type ViewMode = "grid" | "table";
export type MediaMode = "video" | "audio";
export type SettingsSection =
  | "account"
  | "defaults"
  | "locations"
  | "cookies"
  | "format"
  | "fields"
  | "scraper"
  | "slug"
  | "templates"
  | "naming";
export type ToastType = "success" | "error";

// Download folder per source, keyed by the source's learned URL format.
// Per source, per learned URL format: a subpath under `<media>/<source_key>`, "" being that root.
export type SourceLocations = Record<string, Record<string, string>>;
// Selectable subpaths under each source's root.
export type SourceLocationOptions = Record<string, string[]>;

export interface TemplateSettings {
  folder_template: string;
  filename_template: string;
}

export type SourceTemplatesPayload = Record<
  string,
  Record<string, Partial<TemplateSettings>>
>;

export interface SourceProfile {
  key: string;
  label: string;
  hosts: string[];
  icon?: string;
  icon_url?: string;
  external?: boolean;
  external_backend?: string;
  settings_managed?: boolean;
}

export type SourceTemplates = Record<string, Record<string, TemplateSettings>>;

export interface ScrapeRule {
  token: string;
  match_label: string;
  selector: string;
  attr: string;
  multi: boolean;
  xpath: string;
  // Learned template (display form) this rule is scoped to; it fires only when the
  // download URL matches this format. Empty format rules are shown under the first format.
  format: string;
}

export interface PlatformScrapeRules {
  rules: ScrapeRule[];
}

export type SourceScrapeRules = Record<string, PlatformScrapeRules>;
export type TokenRole =
  "creator" | "username" | "nickname" | "title" | "ignore";
export type SourceTokenRoles = Record<string, Record<string, TokenRole>>;

// Per source, the URL-part → named token mappings the user configured from the
// learned format. `part` is a position: "path:<n>" or "query:<key>".
export interface SlugToken {
  part: string;
  token: string;
}
export type SourceSlugTokens = Record<string, SlugToken[]>;

// Read-only learned URL shape surfaced for the Slug pane. Segments with reserved:true
// are auto tokens (id/creator) shown but not user-nameable.
export type LearnedSegmentKind =
  "id" | "creator" | "username" | "nickname" | "var" | "literal" | "query";
export interface LearnedSegment {
  part: string;
  label: string;
  kind: LearnedSegmentKind;
  reserved: boolean;
}
export interface LearnedFormat {
  templates: string[];
  segments: LearnedSegment[];
}
export type LearnedFormats = Record<string, LearnedFormat>;

// Per source, the ordered field-preference lists for username/nickname/title.
export interface FieldRoles {
  username: string[];
  nickname: string[];
  title: string[];
}
export type SourceFields = Record<string, FieldRoles>;
// Per source, every Naming setting: boolean cleaning rules, numeric caps, and the
// string-valued choices. Missing keys fall back to the rule or choice default.
export type NamingFlagValue = boolean | number | string;
export type SourceTitleCleaning = Record<
  string,
  Record<string, NamingFlagValue>
>;

export interface TitleCleaningRule {
  key: string;
  label: string;
  default: boolean;
}

export interface NamingChoiceOption {
  value: string;
  label: string;
}

export interface NamingChoice {
  key: string;
  label: string;
  default: string;
  options: NamingChoiceOption[];
}

export interface ProbeField {
  field: string;
  value: string;
}

export interface ProbeFieldsResponse {
  source_key: string;
  fields: ProbeField[];
  field_roles?: Partial<FieldRoles>;
  url_field_roles?: Partial<FieldRoles>;
  saved?: boolean;
}

// Outcome of adding a platform from a link: which source it resolved to, whether the
// platform was newly created, and whether a URL format could be learned from the link.
export interface LearnFormatResult {
  source_key: string;
  created: boolean;
  learned: boolean;
  media_id: string;
  format_template?: string;
}

export interface ScrapeTestResult {
  token: string;
  value: string;
  matched: boolean;
}

export interface ScrapeTestResponse {
  fetched: boolean;
  results: ScrapeTestResult[];
  detail?: string;
}

export interface QualityPreset {
  key: string;
  label: string;
  icon?: Component;
  // Container presets carry codecs they can play back; codec pickers filter on them.
  codecs?: string[];
  audio_codecs?: string[];
  embed_capabilities?: Array<
    "metadata" | "thumbnail" | "subtitles" | "automatic_subtitles"
  >;
}

export interface QualitySelection {
  mode: MediaMode;
  video_quality: string;
  video_container: string;
  video_codec: string;
  video_audio_codec: string;
  audio_format: string;
  audio_bitrate: string;
}

export interface QualityOptions {
  video: QualityPreset[];
  video_containers: QualityPreset[];
  video_codecs: QualityPreset[];
  video_audio_codecs: QualityPreset[];
  audio_formats: QualityPreset[];
  audio_bitrates: QualityPreset[];
}

export type PostProcessingSaveAs = "sidecar" | "embed";

export interface PostProcessingSelection {
  metadata: boolean;
  thumbnail: boolean;
  subtitles: boolean;
  automatic_subtitles: boolean;
  save_as: PostProcessingSaveAs;
}

export interface CookieFile {
  id: string;
  filename: string;
  uploaded_at: string;
}

export interface CookiesStatus {
  configured: boolean;
  count: number;
  cookies: CookieFile[];
}

export type CookiesMap = Record<string, CookiesStatus>;

export type CookiePolicyField = "limit" | "window" | "delay" | "cooldown" | "wait";

// Only the fields a source overrides; the rest fall back to the global cookie default,
// then to cookie_policy_defaults.
export type CookiePolicy = Partial<Record<CookiePolicyField, number>>;

export type SourceCookiePolicies = Record<string, CookiePolicy>;

export type CookiePolicyDefaults = Record<CookiePolicyField, number>;

// Global naming defaults: the same flag shape as one source's overrides, applied
// wherever a source has not overridden the flag itself.
export type NamingDefaults = Record<string, NamingFlagValue>;

export interface AuthSettings {
  username: string;
  password_configured: boolean;
}

export interface AccountSettingsDraft {
  username: string;
  current_password: string;
  new_password: string;
  confirm_password: string;
}

export interface SavedSettings {
  source_profiles: SourceProfile[];
  source_locations: SourceLocations;
  template_settings: TemplateSettings;
  source_templates: SourceTemplates;
  default_quality: QualitySelection;
  default_post_processing: PostProcessingSelection;
  source_scrape_rules: SourceScrapeRules;
  source_token_roles: SourceTokenRoles;
  source_slug_tokens: SourceSlugTokens;
  source_fields: SourceFields;
  source_title_cleaning: SourceTitleCleaning;
  source_cookie_policies: SourceCookiePolicies;
  // Global defaults a source inherits until it overrides them. Empty entries mean
  // "keep the built-in", so the built-ins stay the single source of truth.
  default_cookie_policy: CookiePolicy;
  default_fields: FieldRoles;
  default_naming: NamingDefaults;
}

export interface SettingsDraft extends SavedSettings {
  account: AccountSettingsDraft;
}

export interface TemplateToken {
  key: string;
  description?: string;
}

export interface RuntimeSettings extends SavedSettings {
  auth: AuthSettings;
  media_root: string;
  source_location_options: SourceLocationOptions;
  ytdlp_cookies: CookiesMap;
  cookie_policy_defaults: CookiePolicyDefaults;
  quality_options: QualityOptions;
  template_tokens: TemplateToken[];
  field_defaults: FieldRoles;
  source_field_defaults: SourceFields;
  title_cleaning_rules: TitleCleaningRule[];
  naming_choices: NamingChoice[];
  learned_formats: LearnedFormats;
}

export interface UiConfigResponse {
  auth?: Partial<AuthSettings>;
  media_root?: string;
  source_profiles?: Array<Partial<SourceProfile>>;
  source_locations?: SourceLocations;
  source_location_options?: SourceLocationOptions;
  template_settings?: Partial<TemplateSettings>;
  source_templates?: SourceTemplatesPayload;
  source_scrape_rules?: Record<string, Partial<PlatformScrapeRules>>;
  source_token_roles?: Record<string, Record<string, string>>;
  source_slug_tokens?: Record<string, SlugToken[]>;
  learned_formats?: LearnedFormats;
  learn_result?: LearnFormatResult;
  source_fields?: Record<string, Partial<FieldRoles>>;
  source_field_defaults?: Record<string, Partial<FieldRoles>>;
  source_title_cleaning?: Record<string, Record<string, NamingFlagValue>>;
  field_defaults?: Partial<FieldRoles>;
  title_cleaning_rules?: TitleCleaningRule[];
  naming_choices?: NamingChoice[];
  ytdlp_cookies?: Record<string, Partial<CookiesStatus>>;
  source_cookie_policies?: Record<string, CookiePolicy>;
  cookie_policy_defaults?: Partial<CookiePolicyDefaults>;
  default_cookie_policy?: CookiePolicy;
  default_fields?: Partial<FieldRoles>;
  default_naming?: NamingDefaults;
  default_quality?: Partial<QualitySelection>;
  default_post_processing?: Partial<PostProcessingSelection>;
  quality_options?: Partial<QualityOptions>;
  template_tokens?: TemplateToken[];
  default_filename_template?: string;
  default_folder_template?: string;
  settings_loaded_at?: number;
}

export interface TaskItem {
  vid: string;
  status: TaskStatus;
  status_label: string;
  progress: number;
  progress_pct: number;
  source_url: string;
  creator: string;
  file_size: number;
  resolved_folder: string;
  resolved_filename: string;
  resolved_full_path: string;
  preview_warning: string;
  can_remove: boolean;
  can_cancel: boolean;
  can_retry: boolean;
  task_type: string;
  source_key: string;
  source_pending?: boolean;
  source_candidates?: string[];
  error: string;
  can_download: boolean;
  can_resolve?: boolean;
  resolve_failed?: boolean;
  quality?: QualitySelection;
  post_processing?: PostProcessingSelection;
  external?: boolean;
  external_backend?: string;
  created_at?: string;
  updated_at?: string;
}

export interface TaskCounts {
  queued: number;
  running: number;
  completed: number;
  failed: number;
}

export interface TasksResponse {
  tasks: TaskItem[];
  scanning?: number;
  resolving?: number;
  resolve_passes?: Record<string, ResolvePassReport>;
  counts?: TaskCounts;
  counts_by_menu?: Partial<Record<string, TaskCounts>>;
  counts_by_media_menu?: Partial<Record<MediaFilter, Partial<Record<string, TaskCounts>>>>;
}

export interface HistoryResponse {
  entries: TaskItem[];
  next_cursor?: string;
}

export interface AddTaskResponse {
  created: TaskItem[];
  reused: boolean;
}

export type ProbeKind = "video" | "playlist" | "radio";

export interface PlaylistEntry {
  index: number;
  url: string;
  title: string;
  creator: string;
  duration: number | null;
  id: string;
}

export interface ProbeResponse {
  kind: ProbeKind;
  url: string;
  title: string;
  entries: PlaylistEntry[];
}

export interface ClearTasksResponse {
  cleared: number;
  failed?: string[];
}

export interface ScanMediaResponse {
  checked: number;
  missing: number;
  added: number;
  unchanged: number;
  renamed: number;
  rename_failed: number;
  needs_resolve: number;
}

export type ResolveScope = "flagged" | "all";

export interface ResolveScopeResponse {
  flagged: number;
  total: number;
}

export interface ResolveResponse {
  queued: number;
  pass_id: number;
}

export interface ResolvePassReport {
  queued: number;
  resolved: number;
  skipped: number;
  failed: number;
}

export interface AuthSessionResponse {
  authenticated: boolean;
  username: string;
}

export interface LoginPayload {
  username: string;
  password: string;
}

export interface CredentialsPayload {
  username: string;
  current_password: string;
  new_password?: string;
}
