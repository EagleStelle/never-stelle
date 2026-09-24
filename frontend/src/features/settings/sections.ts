import type { Component } from "vue";

import type { SettingsSection } from "@/types";
import { SETTINGS_SECTION_ICONS } from "@/ui";
import Account from "@/features/settings/sections/Settings/Account.vue";
import Cookies from "@/features/settings/sections/Settings/Cookies.vue";
import Locations from "@/features/settings/sections/Settings/Locations.vue";
import Defaults from "@/features/settings/sections/Settings/Defaults.vue";
import Trackers from "@/features/settings/sections/Settings/Trackers.vue";
import Scrolling from "@/features/settings/sections/Settings/Scrolling.vue";
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
    icon: SETTINGS_SECTION_ICONS.account,
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
    icon: SETTINGS_SECTION_ICONS.defaults,
    component: Defaults,
    requiresSources: false,
  },
  {
    key: "locations",
    label: "Locations",
    group: "Settings",
    icon: SETTINGS_SECTION_ICONS.locations,
    component: Locations,
    requiresSources: true,
  },
  {
    key: "cookies",
    label: "Cookies",
    group: "Settings",
    icon: SETTINGS_SECTION_ICONS.cookies,
    component: Cookies,
    requiresSources: true,
  },
  {
    key: "trackers",
    label: "Trackers",
    group: "Settings",
    icon: SETTINGS_SECTION_ICONS.trackers,
    component: Trackers,
    focusId: () => "trackerPageSizeInput",
    requiresSources: false,
  },
  {
    key: "scrolling",
    label: "Scrolling",
    group: "Settings",
    icon: SETTINGS_SECTION_ICONS.scrolling,
    component: Scrolling,
    focusId: (source) => `${source}ScrollingProbeInput`,
    requiresSources: true,
  },
  {
    key: "format",
    label: "Format",
    group: "Metadata",
    icon: SETTINGS_SECTION_ICONS.format,
    component: Format,
    focusId: () => "formatLearnInput",
    requiresSources: false,
  },
  {
    key: "slug",
    label: "Slug",
    group: "Metadata",
    icon: SETTINGS_SECTION_ICONS.slug,
    component: Slug,
    requiresSources: true,
  },
  {
    key: "scraper",
    label: "Scraper",
    group: "Metadata",
    icon: SETTINGS_SECTION_ICONS.scraper,
    component: Scraper,
    focusId: (source) => `${source}ScraperProbeInput`,
    requiresSources: true,
  },
  {
    key: "fields",
    label: "Fields",
    group: "Metadata",
    icon: SETTINGS_SECTION_ICONS.fields,
    component: Fields,
    focusId: (source) => `${source}FieldsProbeInput`,
    requiresSources: true,
  },
  {
    key: "templates",
    label: "Templates",
    group: "Metadata",
    icon: SETTINGS_SECTION_ICONS.templates,
    component: Templates,
    requiresSources: true,
  },
  // Last stage of the pipeline: Templates decides the shape, Naming the text written to disk.
  {
    key: "naming",
    label: "Naming",
    group: "Metadata",
    icon: SETTINGS_SECTION_ICONS.naming,
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
