export const SITE_KEYS = ["youtube", "tiktok", "instagram", "twitter", "facebook", "reddit", "twitch", "pinterest", "bluesky", "linkedin", "others"] as const;
export const MENU_KEYS = ["all", ...SITE_KEYS] as const;
export const PAGE_KEYS = ["downloads", "history", "settings"] as const;

export type SiteKey = (typeof SITE_KEYS)[number];
export type MenuKey = (typeof MENU_KEYS)[number];
export type PageKey = (typeof PAGE_KEYS)[number];
export type SaveMode = "nas" | "device";
export type TaskStatus = "pending" | "running" | "completed" | "failed" | string;
export type TaskFilter = "all" | "active" | "done";
export type ViewMode = "grid" | "table";
export type SettingsSection = "downloads" | "instagram" | "advanced";
export type ToastType = "success" | "error";

export type SiteLocations = Record<SiteKey, string>;

export interface TemplateSettings {
  folder_template: string;
  filename_template: string;
}

export interface CookiesStatus {
  configured: boolean;
  source: "uploaded" | "none" | string;
  filename: string;
  uploaded_at: string;
}

export type CookiesMap = Record<string, CookiesStatus>;

export interface SavedSettings {
  site_locations: SiteLocations;
  save_mode: SaveMode;
  template_settings: TemplateSettings;
}

export interface RuntimeSettings extends SavedSettings {
  download_locations: string[];
  ytdlp_cookies: CookiesMap;
}

export interface UiConfigResponse {
  download_locations?: string[];
  site_default_locations?: Partial<SiteLocations>;
  save_mode?: SaveMode | string;
  template_settings?: Partial<TemplateSettings>;
  ytdlp_cookies?: Record<string, Partial<CookiesStatus>>;
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
  resolved_folder: string;
  resolved_filename: string;
  resolved_full_path: string;
  preview_warning: string;
  can_remove: boolean;
  can_hide: boolean;
  hidden: boolean;
  task_type: string;
  site_category: SiteKey | string;
  site_label: string;
  error: string;
  save_mode: SaveMode | string;
  can_download: boolean;
  device_request_tabs: string[];
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
  counts_by_menu?: Partial<Record<MenuKey, TaskCounts>>;
}

export interface AddTaskResponse {
  created: TaskItem[];
  reused: boolean;
}

export interface ClearTasksResponse {
  cleared: number;
  skipped?: number;
  failed?: string[];
}

export interface CleanupNfoResponse {
  deleted: number;
  errors: Array<{ file: string; error: string }>;
}

export interface ToastMessage {
  id: number;
  message: string;
  type: ToastType;
}
