import type { Component } from "vue";
import IconAccount from "~icons/material-symbols/admin-panel-settings";
import IconCheck from "~icons/material-symbols/check-circle";
import IconClock from "~icons/material-symbols/schedule";
import IconCookie from "~icons/material-symbols/cookie";
import IconDefaults from "~icons/material-symbols/tune";
import IconDownloads from "~icons/material-symbols/download";
import IconFields from "~icons/material-symbols/badge";
import IconFolder from "~icons/material-symbols/folder";
import IconFormat from "~icons/material-symbols/pattern";
import IconNaming from "~icons/material-symbols/text-format";
import IconRadar from "~icons/material-symbols/radar";
import IconRuleFolder from "~icons/material-symbols/rule-folder";
import IconScraper from "~icons/material-symbols/travel-explore";
import IconSlug from "~icons/material-symbols/link";
import IconSpinner from "~icons/material-symbols/sync";
import IconWarning from "~icons/material-symbols/warning";

import type { PageKey, SettingsSection, SourceProfile } from "@/types";

export const DEFAULT_SOURCE_PROFILES: SourceProfile[] = [];

// Reuse toasts name each state the way Swaratelle names it, so a delegated Iwara
// download and a local one report the same outcome in the same words.
export const REUSED_TASK_MESSAGES: Record<string, string> = {
  completed: "Already downloaded.",
  pending: "Already queued.",
  running: "Already downloading.",
};

export const REUSED_TASK_FALLBACK = "Already in your list.";

// Swaratelle answers a bad link with HTTP 200 and a failed record, so the toast is
// the only feedback: the row never reaches the queue.
export const QUEUE_FAILED_MESSAGE = "Could not queue.";

export const COUNT_ICONS: Record<
  "queued" | "running" | "completed" | "failed",
  Component
> = {
  queued: IconClock,
  running: IconSpinner,
  completed: IconCheck,
  failed: IconWarning,
};

// One glyph per page and per settings section, so every place that names one shows the same icon.
export const PAGE_ICONS: Record<PageKey, Component> = {
  downloads: IconDownloads,
  trackers: IconRadar,
  history: IconClock,
};

export const SETTINGS_SECTION_ICONS: Record<SettingsSection, Component> = {
  account: IconAccount,
  defaults: IconDefaults,
  locations: IconFolder,
  cookies: IconCookie,
  trackers: IconRadar,
  format: IconFormat,
  slug: IconSlug,
  scraper: IconScraper,
  fields: IconFields,
  templates: IconRuleFolder,
  naming: IconNaming,
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
  trackers: "/trackers",
} as const;
export const TRACKERS_QUERY_KEY = ["trackers"] as const;
export const TRACKER_INTERVALS: { key: string; label: string }[] = [
  { key: String(3600), label: "Every hour" },
  { key: String(3 * 3600), label: "Every 3 hours" },
  { key: String(6 * 3600), label: "Every 6 hours" },
  { key: String(12 * 3600), label: "Every 12 hours" },
  { key: String(24 * 3600), label: "Every day" },
  { key: String(7 * 24 * 3600), label: "Every week" },
];
