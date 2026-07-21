import { inject, provide, type ComputedRef, type InjectionKey } from "vue";

import type { CookiesMap, RuntimeSettings, SavedSettings, SourceProfile } from "../../types";

// Shared state every settings section reads, provided once by the shell so panes
// stay prop-free: adding a section never means threading more props through View.
export interface SettingsContext {
  open: ComputedRef<boolean>;
  settings: RuntimeSettings;
  settingsDraft: SavedSettings;
  cookieStatuses: CookiesMap;
  editableSourceProfiles: ComputedRef<SourceProfile[]>;
  connectCookies: (platform: string, file?: File) => void;
  connectCookiesSource: (source: string, file?: File) => void;
  removeCookies: (platform: string) => void;
  // Add a platform from a link and learn its format; resolves to the source key it hit.
  learnFormat: (url: string) => Promise<string>;
  // Persist a source's learned URL templates in a new order (reorder or delete). Live.
  reorderFormatTemplates: (sourceKey: string, templates: string[]) => Promise<void>;
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
