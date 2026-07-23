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
  | "downloads"
  | "cookies"
  | "quality"
  | "format"
  | "creator"
  | "scraper"
  | "slug"
  | "templates";
export type ToastType = "success" | "error";

export type SourceLocations = Record<string, string>;

export interface TemplateSettings {
  folder_template: string;
  filename_template: string;
}

export type SourceTemplatesPayload = Record<
  string,
  Partial<TemplateSettings> | Record<string, Partial<TemplateSettings>>
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
  // download URL matches this format. "" is a legacy rule, migrated to the first format.
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

// Per source, the ordered field-preference lists for the username/nickname tokens.
export interface CreatorFieldRoles {
  username: string[];
  nickname: string[];
}
export type SourceCreatorFields = Record<string, CreatorFieldRoles>;
// Per source, title-cleaning flags plus a numeric max_chars; missing keys use the rule default.
export type SourceTitleCleaning = Record<
  string,
  Record<string, boolean | number>
>;

export interface TitleCleaningRule {
  key: string;
  label: string;
  default: boolean;
}

export interface ProbeField {
  field: string;
  value: string;
}

export interface ProbeFieldsResponse {
  source_key: string;
  fields: ProbeField[];
  creator_fields?: Partial<CreatorFieldRoles>;
  url_creator_fields?: Partial<CreatorFieldRoles>;
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
  // Container presets carry the codecs they can play back; the codec picker filters on it.
  codecs?: string[];
}

export interface QualitySelection {
  mode: MediaMode;
  video_quality: string;
  video_container: string;
  video_codec: string;
  audio_format: string;
  audio_bitrate: string;
}

export interface QualityOptions {
  video: QualityPreset[];
  video_containers: QualityPreset[];
  video_codecs: QualityPreset[];
  audio_formats: QualityPreset[];
  audio_bitrates: QualityPreset[];
}

export interface CookiesStatus {
  configured: boolean;
  source: "uploaded" | "none" | string;
  filename: string;
  uploaded_at: string;
}

export type CookiesMap = Record<string, CookiesStatus>;

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
  site_locations: SourceLocations;
  template_settings: TemplateSettings;
  source_templates: SourceTemplates;
  default_quality: QualitySelection;
  source_scrape_rules: SourceScrapeRules;
  source_token_roles: SourceTokenRoles;
  source_slug_tokens: SourceSlugTokens;
  source_creator_fields: SourceCreatorFields;
  source_title_cleaning: SourceTitleCleaning;
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
  download_locations: string[];
  ytdlp_cookies: CookiesMap;
  quality_options: QualityOptions;
  template_tokens: TemplateToken[];
  creator_field_defaults: CreatorFieldRoles;
  source_creator_field_defaults: SourceCreatorFields;
  title_cleaning_rules: TitleCleaningRule[];
  learned_formats: LearnedFormats;
}

export interface UiConfigResponse {
  auth?: Partial<AuthSettings>;
  download_locations?: string[];
  source_profiles?: Array<Partial<SourceProfile>>;
  source_default_locations?: SourceLocations;
  site_default_locations?: SourceLocations;
  template_settings?: Partial<TemplateSettings>;
  source_templates?: SourceTemplatesPayload;
  source_scrape_rules?: Record<string, Partial<PlatformScrapeRules>>;
  source_token_roles?: Record<string, Record<string, string>>;
  source_slug_tokens?: Record<string, SlugToken[]>;
  learned_formats?: LearnedFormats;
  learn_result?: LearnFormatResult;
  source_creator_fields?: Record<string, Partial<CreatorFieldRoles>>;
  source_creator_field_defaults?: Record<string, Partial<CreatorFieldRoles>>;
  source_title_cleaning?: Record<string, Record<string, boolean | number>>;
  creator_field_defaults?: Partial<CreatorFieldRoles>;
  title_cleaning_rules?: TitleCleaningRule[];
  ytdlp_cookies?: Record<string, Partial<CookiesStatus>>;
  default_quality?: Partial<QualitySelection>;
  quality_options?: Partial<QualityOptions>;
  template_tokens?: TemplateToken[];
  default_filename_template?: string;
  default_folder_template?: string;
  default_general_location?: string;
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
  quality?: QualitySelection;
  external?: boolean;
  external_backend?: string;
  created_at?: string;
  completed_at?: string;
}

export interface TaskCounts {
  queued: number;
  running: number;
  completed: number;
  failed: number;
}

export interface TasksResponse {
  tasks: TaskItem[];
  counts?: TaskCounts;
  counts_by_menu?: Partial<Record<string, TaskCounts>>;
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
