import { computed, nextTick, onBeforeUnmount, reactive, ref, watch } from "vue";
import { watchDebounced } from "@vueuse/core";
import { useMutation, useQuery, useQueryClient } from "@tanstack/vue-query";

import { deletePlatformCookies, getUiConfig, saveSettings, uploadPlatformCookies } from "../api";
import { useAuth } from "./useAuth";
import { DEFAULT_SOURCE_PROFILES, UI_CONFIG_QUERY_KEY } from "../ui";
import type {
  CookiesMap,
  CookiesStatus,
  RuntimeSettings,
  SavedSettings,
  SettingsSection,
  SourceLocations,
  SourceProfile,
  SourceTemplates,
  ToastType,
  UiConfigResponse,
} from "../types";
import {
  createCookiesStatus,
  createQualityOptions,
  createQualitySelection,
  createSourceLocations,
  createSourceProfile,
  createSourceTemplates,
  createTemplateSettings,
  errorMessage,
  mergeSourceProfiles,
  normalizeSourceKey,
} from "../utils/dashboard";

function replaceRecord<T>(target: Record<string, T>, source: Record<string, T>): void {
  for (const key of Object.keys(target)) delete target[key];
  Object.assign(target, source);
}

function replaceProfiles(target: SourceProfile[], source: SourceProfile[]): void {
  target.splice(0, target.length, ...source.map((profile) => createSourceProfile(profile)));
}

function createCookiesMap(
  source: Record<string, Partial<CookiesStatus>> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
): CookiesMap {
  const keys = new Set([...profiles.map((profile) => profile.key), ...Object.keys(source).map(normalizeSourceKey)]);
  return Object.fromEntries([...keys].map((key) => [key, createCookiesStatus(source[key] || {})])) as CookiesMap;
}

interface UseDashboardSettingsOptions {
  toast: (message: string, type?: ToastType) => void;
}

export function useDashboardSettings({ toast }: UseDashboardSettingsOptions) {
  const auth = useAuth();
  const queryClient = useQueryClient();
  const defaults = reactive<SavedSettings>({
    source_profiles: mergeSourceProfiles(DEFAULT_SOURCE_PROFILES),
    site_locations: createSourceLocations(),
    template_settings: createTemplateSettings(),
    source_templates: createSourceTemplates(),
    default_quality: createQualitySelection(),
  });
  const settings = reactive<RuntimeSettings>({
    auth: { username: "", password_configured: false },
    source_profiles: mergeSourceProfiles(DEFAULT_SOURCE_PROFILES),
    site_locations: createSourceLocations(),
    template_settings: createTemplateSettings(),
    source_templates: createSourceTemplates(),
    default_quality: createQualitySelection(),
    download_locations: [],
    ytdlp_cookies: createCookiesMap(),
    quality_options: createQualityOptions(),
  });
  const settingsDraft = reactive<SavedSettings>({
    source_profiles: mergeSourceProfiles(DEFAULT_SOURCE_PROFILES),
    site_locations: createSourceLocations(),
    template_settings: createTemplateSettings(),
    source_templates: createSourceTemplates(),
    default_quality: createQualitySelection(),
  });

  const settingsOpen = ref(false);
  const settingsSection = ref<SettingsSection>("downloads");
  const lastFocusedTrigger = ref<HTMLElement | null>(null);
  let lastSavedSnapshot = "";

  const uiConfigQuery = useQuery<UiConfigResponse>({
    queryKey: UI_CONFIG_QUERY_KEY,
    queryFn: getUiConfig,
    enabled: auth.authenticated,
    staleTime: 60_000,
  });
  const saveSettingsMutation = useMutation({ mutationFn: saveSettings });
  const uploadCookiesMutation = useMutation({
    mutationFn: ({ platform, file, source }: { platform: string; file: File; source?: string }) =>
      uploadPlatformCookies(platform, file, source),
  });
  const deleteCookiesMutation = useMutation({ mutationFn: (platform: string) => deletePlatformCookies(platform) });

  const sourceProfiles = computed<SourceProfile[]>(() => mergeSourceProfiles(settings.source_profiles));
  const savedSettings = computed<SavedSettings>(() => getSavedSettings());
  const cookieStatuses = computed<CookiesMap>(() => settings.ytdlp_cookies);

  function normalizeSavedPayload(source: SavedSettings): SavedSettings {
    const profiles = mergeSourceProfiles(DEFAULT_SOURCE_PROFILES, source.source_profiles);
    const templateSettings = createTemplateSettings(source.template_settings);
    return {
      source_profiles: profiles,
      site_locations: createSourceLocations(source.site_locations, profiles),
      template_settings: templateSettings,
      source_templates: createSourceTemplates(source.source_templates, profiles, templateSettings),
      default_quality: createQualitySelection(source.default_quality, settings.quality_options),
    };
  }

  function getSavedSettings(): SavedSettings {
    const profiles = mergeSourceProfiles(DEFAULT_SOURCE_PROFILES, defaults.source_profiles, settings.source_profiles);
    const templateSettings = createTemplateSettings({
      ...defaults.template_settings,
      ...settings.template_settings,
    });
    return {
      source_profiles: profiles,
      site_locations: createSourceLocations(
        { ...defaults.site_locations, ...settings.site_locations } as SourceLocations,
        profiles,
      ),
      template_settings: templateSettings,
      source_templates: createSourceTemplates(
        { ...defaults.source_templates, ...settings.source_templates } as SourceTemplates,
        profiles,
        templateSettings,
      ),
      default_quality: createQualitySelection({
        ...defaults.default_quality,
        ...settings.default_quality,
      }, settings.quality_options),
    };
  }

  function applyServerSettings(data: UiConfigResponse): void {
    settings.auth = {
      username: String(data.auth?.username || ""),
      password_configured: Boolean(data.auth?.password_configured),
    };

    const profiles = mergeSourceProfiles(DEFAULT_SOURCE_PROFILES, data.source_profiles);
    replaceProfiles(defaults.source_profiles, profiles);
    replaceProfiles(settings.source_profiles, profiles);

    const locations = data.source_default_locations || data.site_default_locations || {};
    replaceRecord(defaults.site_locations, createSourceLocations(locations, profiles));
    replaceRecord(settings.site_locations, createSourceLocations(locations, profiles));

    const templateSettings = createTemplateSettings(data.template_settings || {});
    Object.assign(defaults.template_settings, templateSettings);
    Object.assign(settings.template_settings, templateSettings);

    const templates = createSourceTemplates(data.source_templates || {}, profiles, templateSettings);
    replaceRecord(defaults.source_templates, templates);
    replaceRecord(settings.source_templates, templates);

    const qualityOptions = createQualityOptions(data.quality_options || {});
    settings.quality_options = qualityOptions;

    const defaultQuality = createQualitySelection(data.default_quality || {}, qualityOptions);
    Object.assign(defaults.default_quality, defaultQuality);
    Object.assign(settings.default_quality, defaultQuality);

    settings.download_locations = Array.isArray(data.download_locations) ? data.download_locations : [];
    replaceRecord(settings.ytdlp_cookies, createCookiesMap(data.ytdlp_cookies || {}, profiles));
  }

  function cacheUiConfig(data: UiConfigResponse): void {
    applyServerSettings(data);
    queryClient.setQueryData(UI_CONFIG_QUERY_KEY, data);
  }

  async function persistSettings(payload: SavedSettings, successMessage = ""): Promise<void> {
    cacheUiConfig(await saveSettingsMutation.mutateAsync(payload));
    if (successMessage) toast(successMessage);
  }

  function copySettingsToDraft(): void {
    const current = getSavedSettings();
    const normalized = normalizeSavedPayload(current);
    replaceProfiles(settingsDraft.source_profiles, normalized.source_profiles);
    replaceRecord(settingsDraft.site_locations, normalized.site_locations);
    Object.assign(settingsDraft.template_settings, normalized.template_settings);
    replaceRecord(settingsDraft.source_templates, normalized.source_templates);
    Object.assign(settingsDraft.default_quality, normalized.default_quality);
    lastSavedSnapshot = JSON.stringify(normalizeSavedPayload(settingsDraft));
  }

  function setSettingsSection(section: SettingsSection, shouldFocus = false): void {
    settingsSection.value = section;
    if (!shouldFocus) return;
    const firstSource = sourceProfiles.value[0]?.key || "settings";
    const focusTargets: Record<SettingsSection, string> = {
      account: "accountUsernameInput",
      downloads: `${firstSource}LocationInput`,
      cookies: `${firstSource}CookiesInput`,
      quality: "defaultQualityMode",
      "folder-template": `${firstSource}FolderTemplateInput`,
      "filename-template": `${firstSource}FilenameTemplateInput`,
    };
    void nextTick(() => document.getElementById(focusTargets[settingsSection.value])?.focus());
  }

  function openSettings(event?: Event, section: SettingsSection = "downloads"): void {
    lastFocusedTrigger.value = event?.currentTarget instanceof HTMLElement
      ? event.currentTarget
      : document.activeElement instanceof HTMLElement
        ? document.activeElement
        : null;
    copySettingsToDraft();
    settingsOpen.value = true;
    setSettingsSection(section, true);
  }

  function closeSettings(): void {
    settingsOpen.value = false;
  }

  async function autoSaveSettings(): Promise<void> {
    const payload = normalizeSavedPayload(settingsDraft);
    const snapshot = JSON.stringify(payload);
    if (snapshot === lastSavedSnapshot) return;
    lastSavedSnapshot = snapshot;
    try {
      await persistSettings(payload);
    } catch (error) {
      toast(errorMessage(error, "Could not save settings."), "error");
    }
  }

  async function connectCookies(platform: string, file?: File): Promise<void> {
    if (!file) {
      toast("Choose a cookies file first.", "error");
      return;
    }
    try {
      cacheUiConfig(await uploadCookiesMutation.mutateAsync({ platform: normalizeSourceKey(platform), file }));
      toast("Cookies connected.");
    } catch (error) {
      toast(errorMessage(error, "Could not connect cookies."), "error");
    }
  }

  async function connectCookiesForSource(source: string, file?: File): Promise<void> {
    if (!source.trim()) {
      toast("Paste a link or domain first.", "error");
      return;
    }
    if (!file) {
      toast("Choose a cookies file first.", "error");
      return;
    }
    try {
      cacheUiConfig(await uploadCookiesMutation.mutateAsync({ platform: "others", file, source: source.trim() }));
      toast("Cookies connected.");
    } catch (error) {
      toast(errorMessage(error, "Could not connect cookies."), "error");
    }
  }

  async function removeCookies(platform: string): Promise<void> {
    const key = normalizeSourceKey(platform);
    if (!settings.ytdlp_cookies[key]?.configured) return;
    try {
      cacheUiConfig(await deleteCookiesMutation.mutateAsync(key));
      toast("Cookies removed.");
    } catch (error) {
      toast(errorMessage(error, "Could not remove cookies."), "error");
    }
  }

  let draftSeeded = false;
  watch(
    () => uiConfigQuery.data.value,
    (data) => {
      if (!data) return;
      applyServerSettings(data);
      if (!draftSeeded) {
        copySettingsToDraft();
        draftSeeded = true;
      }
    },
    { immediate: true },
  );
  watch(settingsOpen, (open) => {
    document.body.classList.toggle("dialog-open", open);
    if (!open) lastFocusedTrigger.value?.focus();
  });

  watchDebounced(
    settingsDraft,
    () => {
      if (settingsOpen.value) void autoSaveSettings();
    },
    { deep: true, debounce: 500, maxWait: 2000 },
  );

  onBeforeUnmount(() => document.body.classList.remove("dialog-open"));

  return {
    closeSettings,
    connectCookies,
    connectCookiesForSource,
    cookieStatuses,
    getSavedSettings,
    openSettings,
    removeCookies,
    savedSettings,
    setSettingsSection,
    settings,
    settingsDraft,
    settingsOpen,
    settingsSection,
    sourceProfiles,
  };
}
