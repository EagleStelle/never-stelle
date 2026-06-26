import type { Component } from "vue";
import IconWarning from "~icons/material-symbols/warning";
import IconCheck from "~icons/material-symbols/check-circle";
import IconClock from "~icons/material-symbols/schedule";
import IconFolder from "~icons/material-symbols/folder";
import IconTray from "~icons/material-symbols/inbox";
import IconSpinner from "~icons/material-symbols/sync";
import IconYoutube from "~icons/simple-icons/youtube";
import IconFacebook from "~icons/simple-icons/facebook";
import IconInstagram from "~icons/simple-icons/instagram";
import IconTiktok from "~icons/simple-icons/tiktok";
import IconX from "~icons/simple-icons/x";
import IconReddit from "~icons/simple-icons/reddit";
import IconTwitch from "~icons/simple-icons/twitch";
import IconPinterest from "~icons/simple-icons/pinterest";
import IconBluesky from "~icons/simple-icons/bluesky";
import IconLinkedin from "~icons/simple-icons/linkedin";

import type { MenuKey, SettingsSection, TaskFilter } from "./types";

export const SITE_LABELS: Record<MenuKey, string> = {
  all: "All",
  youtube: "YouTube",
  tiktok: "TikTok",
  instagram: "Instagram",
  twitter: "X (Twitter)",
  facebook: "Facebook",
  reddit: "Reddit",
  twitch: "Twitch",
  pinterest: "Pinterest",
  bluesky: "Bluesky",
  linkedin: "LinkedIn",
  others: "Others",
};

export const FILTER_LABELS: Record<TaskFilter, string> = {
  all: "All",
  active: "Active",
  done: "Done",
};

export const SETTINGS_SECTIONS: Array<{ key: SettingsSection; label: string }> = [
  { key: "downloads", label: "Downloads" },
  { key: "instagram", label: "Instagram" },
  { key: "advanced", label: "Advanced" },
];

export const MENU_ICONS: Record<MenuKey, Component> = {
  all: IconTray,
  youtube: IconYoutube,
  tiktok: IconTiktok,
  instagram: IconInstagram,
  twitter: IconX,
  facebook: IconFacebook,
  reddit: IconReddit,
  twitch: IconTwitch,
  pinterest: IconPinterest,
  bluesky: IconBluesky,
  linkedin: IconLinkedin,
  others: IconFolder,
};

export const COUNT_ICONS: Record<"queued" | "running" | "completed" | "failed", Component> = {
  queued: IconClock,
  running: IconSpinner,
  completed: IconCheck,
  failed: IconWarning,
};

export const TASKS_QUERY_KEY = ["tasks"] as const;
export const UI_CONFIG_QUERY_KEY = ["ui-config"] as const;
export const POLL_RUNNING_MS = 2000;
export const POLL_PENDING_MS = 5000;
