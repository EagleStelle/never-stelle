import type { Component } from "vue";
import IconCheck from "~icons/material-symbols/check-circle";
import IconClock from "~icons/material-symbols/schedule";
import IconClose from "~icons/material-symbols/close";
import IconCookie from "~icons/material-symbols/cookie";
import IconDefaults from "~icons/material-symbols/tune";
import IconDelete from "~icons/material-symbols/delete";
import IconDismiss from "~icons/material-symbols/visibility-off";
import IconDownloads from "~icons/material-symbols/download";
import IconFields from "~icons/material-symbols/badge";
import IconFolder from "~icons/material-symbols/folder";
import IconFormat from "~icons/material-symbols/pattern";
import IconMore from "~icons/material-symbols/more-horiz";
import IconNaming from "~icons/material-symbols/text-format";
import IconPause from "~icons/material-symbols/pause";
import IconQueue from "~icons/material-symbols/playlist-add";
import IconProfile from "~icons/material-symbols/account-circle";
import IconRadar from "~icons/material-symbols/radar";
import IconResolve from "~icons/material-symbols/cloud-sync";
import IconResume from "~icons/material-symbols/play-arrow";
import IconRetry from "~icons/material-symbols/replay";
import IconRuleFolder from "~icons/material-symbols/rule-folder";
import IconScraper from "~icons/material-symbols/travel-explore";
import IconSecurity from "~icons/material-symbols/shield-lock";
import IconSlug from "~icons/material-symbols/link";
import IconSpinner from "~icons/material-symbols/sync";
import IconStop from "~icons/material-symbols/stop";
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
  account: IconProfile,
};

// One glyph per action, shared by row buttons and selection footers.
export const ACTION_ICONS = {
  check: IconSpinner,
  delete: IconDelete,
  dismiss: IconDismiss,
  download: IconDownloads,
  more: IconMore,
  pause: IconPause,
  queue: IconQueue,
  refresh: IconSpinner,
  remove: IconClose,
  resolve: IconResolve,
  resume: IconResume,
  retry: IconRetry,
  stop: IconStop,
} satisfies Record<string, Component>;

// A sync glyph turns with its arrows: a nudge on button hover, a spin while busy.
export const syncIconClass = (busy: boolean) => (busy ? "animate-sync" : "group-hover:-rotate-45");

// Grid view: as many tile columns as fit, one on a phone.
export const TILE_GRID = "grid grid-cols-[repeat(auto-fill,minmax(min(100%,18rem),1fr))] gap-3";

// A list item a touch hold selects, so a hold starts no text selection or link menu.
export const SELECTABLE_ITEM = "pointer-coarse:select-none pointer-coarse:[-webkit-touch-callout:none]";
// While its list is selecting, only the item's direct children (cells, card sections) take presses.
export const SELECTING_ITEM = "cursor-pointer *:**:pointer-events-none";

export const SETTINGS_SECTION_ICONS: Record<SettingsSection, Component> = {
  security: IconSecurity,
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
  account: "/account",
} as const;
export const TRACKERS_QUERY_KEY = ["trackers"] as const;
// Under the trackers key, so whatever refreshes trackers refreshes their item lists too.
export const TRACKER_ENTRIES_QUERY_KEY = [...TRACKERS_QUERY_KEY, "entries"] as const;
export const TRACKER_INTERVALS: { key: string; label: string }[] = [
  { key: String(3600), label: "Every hour" },
  { key: String(3 * 3600), label: "Every 3 hours" },
  { key: String(6 * 3600), label: "Every 6 hours" },
  { key: String(12 * 3600), label: "Every 12 hours" },
  { key: String(24 * 3600), label: "Every day" },
  { key: String(7 * 24 * 3600), label: "Every week" },
];
