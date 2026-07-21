import type { Component } from "vue";
import IconAccount from "~icons/material-symbols/admin-panel-settings";
import IconCookie from "~icons/material-symbols/cookie";
import IconDescription from "~icons/material-symbols/description";
import IconFolder from "~icons/material-symbols/folder";
import IconQuality from "~icons/material-symbols/high-quality";
import IconCreator from "~icons/material-symbols/badge";
import IconRuleFolder from "~icons/material-symbols/rule-folder";
import IconScraper from "~icons/material-symbols/travel-explore";
import IconSlug from "~icons/material-symbols/link";

import type { SettingsSection } from "../../types";
import Account from "./sections/Settings/Account.vue";
import Cookies from "./sections/Settings/Cookies.vue";
import Locations from "./sections/Settings/Locations.vue";
import Quality from "./sections/Settings/Quality.vue";
import Filename from "./sections/Templates/Filename.vue";
import Folder from "./sections/Templates/Folder.vue";
import Creator from "./sections/Metadata/Creator.vue";
import Scraper from "./sections/Metadata/Scraper.vue";
import Slug from "./sections/Metadata/Slug.vue";

export interface SettingsSectionDef {
  key: SettingsSection;
  label: string;
  group: string;
  icon: Component;
  component: Component;
  // Element to focus when the pane opens; sources-driven panes take the first source key.
  // Omit for panes whose first control isn't a plain input (nothing focusable to target).
  focusId?: (firstSourceKey: string) => string;
  // Panes that render one row per source show an empty-state until a source exists.
  requiresSources: boolean;
}

// Single source of truth for settings panes: register a section here (plus its type
// key) and the sidebar, tabs, and content area pick it up automatically.
export const SETTINGS_SECTION_DEFS: SettingsSectionDef[] = [
  {
    key: "account",
    label: "Account",
    group: "Settings",
    icon: IconAccount,
    component: Account,
    focusId: () => "accountUsernameInput",
    requiresSources: false,
  },
  {
    key: "downloads",
    label: "Locations",
    group: "Settings",
    icon: IconFolder,
    component: Locations,
    focusId: (source) => `${source}LocationInput`,
    requiresSources: true,
  },
  {
    key: "cookies",
    label: "Cookies",
    group: "Settings",
    icon: IconCookie,
    component: Cookies,
    focusId: (source) => `${source}CookiesInput`,
    requiresSources: false,
  },
  {
    key: "quality",
    label: "Quality",
    group: "Settings",
    icon: IconQuality,
    component: Quality,
    requiresSources: false,
  },
  {
    key: "creator",
    label: "Creator",
    group: "Metadata",
    icon: IconCreator,
    component: Creator,
    focusId: (source) => `${source}CreatorProbeInput`,
    requiresSources: true,
  },
  {
    key: "slug",
    label: "Slug",
    group: "Metadata",
    icon: IconSlug,
    component: Slug,
    requiresSources: true,
  },
  {
    key: "scraper",
    label: "Scraper",
    group: "Metadata",
    icon: IconScraper,
    component: Scraper,
    focusId: (source) => `${source}ScraperProbeInput`,
    requiresSources: true,
  },
  {
    key: "folder-template",
    label: "Folder",
    group: "Templates",
    icon: IconRuleFolder,
    component: Folder,
    focusId: (source) => `${source}FolderTemplateInput`,
    requiresSources: true,
  },
  {
    key: "filename-template",
    label: "Filename",
    group: "Templates",
    icon: IconDescription,
    component: Filename,
    focusId: (source) => `${source}FilenameTemplateInput`,
    requiresSources: true,
  },
];

// Ordered group labels, first-seen order preserved.
export const SETTINGS_SECTION_GROUPS: string[] = SETTINGS_SECTION_DEFS.reduce<
  string[]
>(
  (groups, def) =>
    groups.includes(def.group) ? groups : [...groups, def.group],
  [],
);
