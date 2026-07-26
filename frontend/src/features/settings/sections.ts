import type { Component } from "vue";
import IconAccount from "~icons/material-symbols/admin-panel-settings";
import IconCookie from "~icons/material-symbols/cookie";
import IconDescription from "~icons/material-symbols/description";
import IconFolder from "~icons/material-symbols/folder";
import IconQuality from "~icons/material-symbols/high-quality";
import IconCreator from "~icons/material-symbols/badge";
import IconFormat from "~icons/material-symbols/pattern";
import IconRuleFolder from "~icons/material-symbols/rule-folder";
import IconScraper from "~icons/material-symbols/travel-explore";
import IconSlug from "~icons/material-symbols/link";

import type { SettingsSection } from "../../types";
import Account from "./sections/Settings/Account.vue";
import Cookies from "./sections/Settings/Cookies.vue";
import Locations from "./sections/Settings/Locations.vue";
import Quality from "./sections/Settings/Quality.vue";
import Templates from "./sections/Metadata/Templates.vue";
import Fields from "./sections/Metadata/Fields.vue";
import Format from "./sections/Metadata/Format.vue";
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
    key: "format",
    label: "Format",
    group: "Metadata",
    icon: IconFormat,
    component: Format,
    focusId: () => "formatLearnInput",
    requiresSources: false,
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
    key: "creator",
    label: "Fields",
    group: "Metadata",
    icon: IconCreator,
    component: Fields,
    focusId: (source) => `${source}CreatorProbeInput`,
    requiresSources: true,
  },
  {
    key: "templates",
    label: "Templates",
    group: "Metadata",
    icon: IconRuleFolder,
    component: Templates,
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
