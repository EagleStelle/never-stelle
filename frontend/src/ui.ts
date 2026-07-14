import type { Component } from "vue";
import IconWarning from "~icons/material-symbols/warning";
import IconCheck from "~icons/material-symbols/check-circle";
import IconClock from "~icons/material-symbols/schedule";
import IconFolder from "~icons/material-symbols/folder";
import IconTray from "~icons/material-symbols/inbox";
import IconSpinner from "~icons/material-symbols/sync";

import type { SettingsSection, SourceProfile, TaskFilter } from "./types";

export const DEFAULT_SOURCE_PROFILES: SourceProfile[] = [];

export const SITE_LABELS: Record<string, string> = { all: "All" };

export const SOURCE_ICON_COMPONENTS: Record<string, Component> = {
  others: IconFolder,
  "material-symbols:folder": IconFolder,
};

export const FALLBACK_SOURCE_ICON = IconFolder;

export const FILTER_LABELS: Record<TaskFilter, string> = {
  all: "All",
  active: "Active",
  done: "Done",
};

export const SETTINGS_SECTIONS: Array<{ key: SettingsSection; label: string; route: string }> = [
  { key: "account", label: "Account", route: "/settings/account" },
  { key: "downloads", label: "Locations", route: "/settings/locations" },
  { key: "cookies", label: "Cookies", route: "/settings/cookies" },
  { key: "quality", label: "Quality", route: "/settings/quality" },
  { key: "folder-template", label: "Folder", route: "/settings/folder" },
  { key: "filename-template", label: "Filename", route: "/settings/filename" },
  { key: "scraper", label: "Scraper", route: "/settings/scraper" },
];

export const COUNT_ICONS: Record<"queued" | "running" | "completed" | "failed", Component> = {
  queued: IconClock,
  running: IconSpinner,
  completed: IconCheck,
  failed: IconWarning,
};

export const TASKS_QUERY_KEY = ["tasks"] as const;
export const HISTORY_QUERY_KEY = ["history"] as const;
export const UI_CONFIG_QUERY_KEY = ["ui-config"] as const;
export const POLL_RUNNING_MS = 2000;
export const POLL_PENDING_MS = 5000;
export const HISTORY_PAGE_SIZE = 50;
export const PAGE_ROUTES = {
  downloads: "/downloads",
  history: "/history",
  settings: "/settings/locations",
} as const;
