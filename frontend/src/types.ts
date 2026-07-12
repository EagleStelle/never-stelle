export const FALLBACK_SOURCE_KEY = "others";
export const PAGE_KEYS = ["downloads", "history", "settings"] as const;

export type SourceKey = string;
export type MenuKey = "all" | SourceKey;
export type PageKey = (typeof PAGE_KEYS)[number];
export type TaskStatus = "pending" | "running" | "completed" | "failed" | string;
export type TaskFilter = "all" | "active" | "done";
export type MediaFilter = "all" | "image" | "video";
export type ViewMode = "grid" | "table";
export type MediaMode = "video" | "audio";
export type SettingsSection = "downloads" | "cookies" | "quality" | "folder-template" | "filename-template";
export type ToastType = "success" | "error";

export type SourceLocations = Record<string, string>;

export interface TemplateSettings {
  folder_template: string;
  filename_template: string;
}

export interface SourceProfile {
  key: string;
  label: string;
  hosts: string[];
  icon?: string;
  icon_url?: string;
}

export type SourceTemplates = Record<string, TemplateSettings>;

export interface QualityPreset {
  key: string;
  label: string;
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

export interface SavedSettings {
  source_profiles: SourceProfile[];
  site_locations: SourceLocations;
  template_settings: TemplateSettings;
  source_templates: SourceTemplates;
  default_quality: QualitySelection;
}

export interface RuntimeSettings extends SavedSettings {
  download_locations: string[];
  ytdlp_cookies: CookiesMap;
  quality_options: QualityOptions;
}

export interface UiConfigResponse {
  download_locations?: string[];
  source_profiles?: Array<Partial<SourceProfile>>;
  source_default_locations?: SourceLocations;
  site_default_locations?: SourceLocations;
  template_settings?: Partial<TemplateSettings>;
  source_templates?: Record<string, Partial<TemplateSettings>>;
  ytdlp_cookies?: Record<string, Partial<CookiesStatus>>;
  default_quality?: Partial<QualitySelection>;
  quality_options?: Partial<QualityOptions>;
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

export interface ToastMessage {
  id: number;
  message: string;
  type: ToastType;
}
