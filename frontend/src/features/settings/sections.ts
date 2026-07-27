import type { Component } from "vue";
import IconAccount from "~icons/material-symbols/admin-panel-settings";
import IconCookie from "~icons/material-symbols/cookie";
import IconFolder from "~icons/material-symbols/folder";
import IconDefaults from "~icons/material-symbols/tune";
import IconFields from "~icons/material-symbols/badge";
import IconFormat from "~icons/material-symbols/pattern";
import IconNaming from "~icons/material-symbols/text-format";
import IconRuleFolder from "~icons/material-symbols/rule-folder";
import IconScraper from "~icons/material-symbols/travel-explore";
import IconSlug from "~icons/material-symbols/link";

import type { SettingsSection } from "@/types";
import Account from "@/features/settings/sections/Settings/Account.vue";
import Cookies from "@/features/settings/sections/Settings/Cookies.vue";
import Locations from "@/features/settings/sections/Settings/Locations.vue";
import Defaults from "@/features/settings/sections/Settings/Defaults.vue";
import Templates from "@/features/settings/sections/Metadata/Templates.vue";
import Fields from "@/features/settings/sections/Metadata/Fields.vue";
import Format from "@/features/settings/sections/Metadata/Format.vue";
import Naming from "@/features/settings/sections/Metadata/Naming.vue";
import Scraper from "@/features/settings/sections/Metadata/Scraper.vue";
import Slug from "@/features/settings/sections/Metadata/Slug.vue";

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
  // Global fallbacks for every pane that also takes per-source overrides, so they read
  // before the panes that override them.
  {
    key: "defaults",
    label: "Defaults",
    group: "Settings",
    icon: IconDefaults,
    component: Defaults,
    requiresSources: false,
  },
  {
    key: "locations",
    label: "Locations",
    group: "Settings",
    icon: IconFolder,
    component: Locations,
    requiresSources: true,
  },
  {
    key: "cookies",
    label: "Cookies",
    group: "Settings",
    icon: IconCookie,
    component: Cookies,
    requiresSources: true,
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
    key: "fields",
    label: "Fields",
    group: "Metadata",
    icon: IconFields,
    component: Fields,
    focusId: (source) => `${source}FieldsProbeInput`,
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
  // Last stage of the pipeline: Templates decides the shape, Naming the text written to disk.
  {
    key: "naming",
    label: "Naming",
    group: "Metadata",
    icon: IconNaming,
    component: Naming,
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
