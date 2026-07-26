import { inject, provide, type ComputedRef, type InjectionKey } from "vue";

import type {
  CookiesMap,
  LearnedFormats,
  ProbeFieldsResponse,
  RuntimeSettings,
  SettingsDraft,
  SettingsSection,
  SourceProfile,
} from "@/types";

// Shared state every settings section reads, provided once by the shell so panes
// stay prop-free: adding a section never means threading more props through View.
export interface SettingsContext {
  open: ComputedRef<boolean>;
  settings: RuntimeSettings;
  settingsDraft: SettingsDraft;
  learnedFormatsDraft: LearnedFormats;
  cookieStatuses: ComputedRef<CookiesMap>;
  editableSourceProfiles: ComputedRef<SourceProfile[]>;
  connectCookies: (platform: string, file?: File) => void;
  connectCookiesSource: (source: string, file?: File) => void;
  removeCookies: (platform: string) => void;
  // Stage a link's format in the accordion; Save performs the backend learn.
  learnFormat: (url: string) => Promise<string>;
  // Probe creator metadata. Successful learned fields are saved server-side.
  probeCreatorFields: (url: string, sourceKey?: string) => Promise<ProbeFieldsResponse>;
  // Stage a source's learned URL templates in a new order (reorder or delete).
  reorderFormatTemplates: (sourceKey: string, templates: string[]) => Promise<void>;
  markSettingsDraftDirty: (section?: SettingsSection) => void;
  saveSettingsDraft: () => Promise<void>;
  copySettingsToDraft: () => void;
  close: () => void;
}

const SETTINGS_CONTEXT: InjectionKey<SettingsContext> = Symbol("settings-context");

export function provideSettingsContext(context: SettingsContext): void {
  provide(SETTINGS_CONTEXT, context);
}

export function useSettingsContext(): SettingsContext {
  const context = inject(SETTINGS_CONTEXT);
  if (!context) throw new Error("useSettingsContext must be used inside SettingsView.");
  return context;
}
