import { computed, nextTick, onBeforeUnmount, reactive, ref, watch } from "vue";
import { watchDebounced } from "@vueuse/core";
import { useMutation, useQuery, useQueryClient } from "@tanstack/vue-query";

import {
  deletePlatformCookies,
  getUiConfig,
  saveSettings,
  uploadPlatformCookies,
} from "../api";
import { useAuth } from "./useAuth";
import { DEFAULT_SOURCE_PROFILES, UI_CONFIG_QUERY_KEY } from "../ui";
import type {
  CookiesMap,
  CookiesStatus,
  LearnedFormats,
  RuntimeSettings,
  SavedSettings,
  SettingsSection,
  SourceCreatorFields,
  SourceLocations,
  SourceProfile,
  SourceSlugTokens,
  SourceTemplates,
  SourceTitleCleaning,
  SourceTokenRoles,
  ToastType,
  UiConfigResponse,
} from "../types";
import {
  createCookiesStatus,
  createQualityOptions,
  createCreatorFieldRoles,
  createQualitySelection,
  createSourceCreatorFields,
  createSourceLocations,
  createSourceProfile,
  createSourceScrapeRules,
  createSourceSlugTokens,
  createSourceTemplates,
  createSourceTitleCleaning,
  createSourceTokenRoles,
  createTemplateSettings,
  errorMessage,
  mergeSourceProfiles,
  normalizeSourceKey,
  settingsManagedSourceProfiles,
} from "../utils/dashboard";

function replaceRecord<T>(
  target: Record<string, T>,
  source: Record<string, T>,
): void {
  for (const key of Object.keys(target)) delete target[key];
  Object.assign(target, source);
}

function replaceProfiles(
  target: SourceProfile[],
  source: SourceProfile[],
): void {
  target.splice(
    0,
    target.length,
    ...source.map((profile) => createSourceProfile(profile)),
  );
}

function recordForProfiles<T>(
  source: Record<string, T> = {},
  profiles: SourceProfile[],
): Record<string, T> {
  const keys = new Set(profiles.map((profile) => profile.key));
  return Object.fromEntries(
    Object.entries(source).filter(([key]) => keys.has(normalizeSourceKey(key))),
  ) as Record<string, T>;
}

function createCookiesMap(
  source: Record<string, Partial<CookiesStatus>> = {},
  profiles: SourceProfile[] = DEFAULT_SOURCE_PROFILES,
): CookiesMap {
  const keys = new Set([
    ...profiles.map((profile) => profile.key),
    ...Object.keys(source).map(normalizeSourceKey),
  ]);
  return Object.fromEntries(
    [...keys].map((key) => [key, createCookiesStatus(source[key] || {})]),
  ) as CookiesMap;
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
    source_scrape_rules: createSourceScrapeRules(),
    source_token_roles: createSourceTokenRoles(),
    source_slug_tokens: createSourceSlugTokens(),
    source_creator_fields: createSourceCreatorFields(),
    source_title_cleaning: createSourceTitleCleaning(),
  });
  const settings = reactive<RuntimeSettings>({
    auth: { username: "", password_configured: false },
    source_profiles: mergeSourceProfiles(DEFAULT_SOURCE_PROFILES),
    site_locations: createSourceLocations(),
    template_settings: createTemplateSettings(),
    source_templates: createSourceTemplates(),
    default_quality: createQualitySelection(),
    source_scrape_rules: createSourceScrapeRules(),
    source_token_roles: createSourceTokenRoles(),
    source_slug_tokens: createSourceSlugTokens(),
    source_creator_fields: createSourceCreatorFields(),
    source_title_cleaning: createSourceTitleCleaning(),
    download_locations: [],
    ytdlp_cookies: createCookiesMap(),
    quality_options: createQualityOptions(),
    template_tokens: [],
    creator_field_defaults: { username: [], nickname: [] },
    source_creator_field_defaults: createSourceCreatorFields(),
    title_cleaning_rules: [],
    learned_formats: {},
  });
  const settingsDraft = reactive<SavedSettings>({
    source_profiles: mergeSourceProfiles(DEFAULT_SOURCE_PROFILES),
    site_locations: createSourceLocations(),
    template_settings: createTemplateSettings(),
    source_templates: createSourceTemplates(),
    default_quality: createQualitySelection(),
    source_scrape_rules: createSourceScrapeRules(),
    source_token_roles: createSourceTokenRoles(),
    source_slug_tokens: createSourceSlugTokens(),
    source_creator_fields: createSourceCreatorFields(),
    source_title_cleaning: createSourceTitleCleaning(),
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
    mutationFn: ({
      platform,
      file,
      source,
    }: {
      platform: string;
      file: File;
      source?: string;
    }) => uploadPlatformCookies(platform, file, source),
  });
  const deleteCookiesMutation = useMutation({
    mutationFn: (platform: string) => deletePlatformCookies(platform),
  });

  const sourceProfiles = computed<SourceProfile[]>(() =>
    mergeSourceProfiles(settings.source_profiles),
  );
  const savedSettings = computed<SavedSettings>(() => getSavedSettings());
  const cookieStatuses = computed<CookiesMap>(() => settings.ytdlp_cookies);

  function normalizeSavedPayload(source: SavedSettings): SavedSettings {
    const profiles = settingsManagedSourceProfiles(
      mergeSourceProfiles(DEFAULT_SOURCE_PROFILES, source.source_profiles),
    );
    const templateSettings = createTemplateSettings(source.template_settings);
    return {
      source_profiles: profiles,
      site_locations: createSourceLocations(
        recordForProfiles(source.site_locations, profiles),
        profiles,
      ),
      template_settings: templateSettings,
      source_templates: createSourceTemplates(
        recordForProfiles(source.source_templates, profiles),
        profiles,
        templateSettings,
      ),
      default_quality: createQualitySelection(
        source.default_quality,
        settings.quality_options,
      ),
      source_scrape_rules: createSourceScrapeRules(
        recordForProfiles(source.source_scrape_rules, profiles),
        profiles,
      ),
      source_token_roles: createSourceTokenRoles(
        source.source_token_roles as SourceTokenRoles,
        profiles,
      ),
      source_slug_tokens: createSourceSlugTokens(
        recordForProfiles(source.source_slug_tokens as SourceSlugTokens, profiles),
        profiles,
      ),
      source_creator_fields: createSourceCreatorFields(
        recordForProfiles(
          source.source_creator_fields as SourceCreatorFields,
          profiles,
        ),
        profiles,
      ),
      source_title_cleaning: createSourceTitleCleaning(
        recordForProfiles(
          source.source_title_cleaning as SourceTitleCleaning,
          profiles,
        ),
        profiles,
      ),
    };
  }

  function getSavedSettings(): SavedSettings {
    const profiles = settingsManagedSourceProfiles(
      mergeSourceProfiles(
        DEFAULT_SOURCE_PROFILES,
        defaults.source_profiles,
        settings.source_profiles,
      ),
    );
    const templateSettings = createTemplateSettings({
      ...defaults.template_settings,
      ...settings.template_settings,
    });
    return {
      source_profiles: profiles,
      site_locations: createSourceLocations(
        recordForProfiles(
          {
            ...defaults.site_locations,
            ...settings.site_locations,
          } as SourceLocations,
          profiles,
        ),
        profiles,
      ),
      template_settings: templateSettings,
      source_templates: createSourceTemplates(
        recordForProfiles(
          {
            ...defaults.source_templates,
            ...settings.source_templates,
          } as SourceTemplates,
          profiles,
        ),
        profiles,
        templateSettings,
      ),
      default_quality: createQualitySelection(
        {
          ...defaults.default_quality,
          ...settings.default_quality,
        },
        settings.quality_options,
      ),
      source_scrape_rules: createSourceScrapeRules(
        recordForProfiles(
          { ...defaults.source_scrape_rules, ...settings.source_scrape_rules },
          profiles,
        ),
        profiles,
      ),
      source_token_roles: createSourceTokenRoles(
        {
          ...defaults.source_token_roles,
          ...settings.source_token_roles,
        } as SourceTokenRoles,
        profiles,
      ),
      source_slug_tokens: createSourceSlugTokens(
        recordForProfiles(
          {
            ...defaults.source_slug_tokens,
            ...settings.source_slug_tokens,
          } as SourceSlugTokens,
          profiles,
        ),
        profiles,
      ),
      source_creator_fields: createSourceCreatorFields(
        recordForProfiles(
          {
            ...defaults.source_creator_fields,
            ...settings.source_creator_fields,
          },
          profiles,
        ),
        profiles,
      ),
      source_title_cleaning: createSourceTitleCleaning(
        recordForProfiles(
          {
            ...defaults.source_title_cleaning,
            ...settings.source_title_cleaning,
          },
          profiles,
        ),
        profiles,
      ),
    };
  }

  function applyServerSettings(data: UiConfigResponse): void {
    settings.auth = {
      username: String(data.auth?.username || ""),
      password_configured: Boolean(data.auth?.password_configured),
    };

    const profiles = mergeSourceProfiles(
      DEFAULT_SOURCE_PROFILES,
      data.source_profiles,
    );
    const managedProfiles = settingsManagedSourceProfiles(profiles);
    replaceProfiles(defaults.source_profiles, profiles);
    replaceProfiles(settings.source_profiles, profiles);

    const locations =
      data.source_default_locations || data.site_default_locations || {};
    replaceRecord(
      defaults.site_locations,
      createSourceLocations(
        recordForProfiles(locations, managedProfiles),
        managedProfiles,
      ),
    );
    replaceRecord(
      settings.site_locations,
      createSourceLocations(
        recordForProfiles(locations, managedProfiles),
        managedProfiles,
      ),
    );

    const templateSettings = createTemplateSettings(
      data.template_settings || {},
    );
    Object.assign(defaults.template_settings, templateSettings);
    Object.assign(settings.template_settings, templateSettings);

    const templates = createSourceTemplates(
      recordForProfiles(data.source_templates || {}, managedProfiles),
      managedProfiles,
      templateSettings,
    );
    replaceRecord(defaults.source_templates, templates);
    replaceRecord(settings.source_templates, templates);

    const scrapeRules = createSourceScrapeRules(
      recordForProfiles(data.source_scrape_rules || {}, managedProfiles),
      managedProfiles,
    );
    replaceRecord(defaults.source_scrape_rules, scrapeRules);
    replaceRecord(settings.source_scrape_rules, scrapeRules);

    const tokenRoles = createSourceTokenRoles(
      data.source_token_roles || {},
      managedProfiles,
    );
    replaceRecord(defaults.source_token_roles, tokenRoles);
    replaceRecord(settings.source_token_roles, tokenRoles);

    const slugTokens = createSourceSlugTokens(
      recordForProfiles(data.source_slug_tokens || {}, managedProfiles),
      managedProfiles,
    );
    replaceRecord(defaults.source_slug_tokens, slugTokens);
    replaceRecord(settings.source_slug_tokens, slugTokens);

    settings.learned_formats = (data.learned_formats || {}) as LearnedFormats;

    const creatorFields = createSourceCreatorFields(
      recordForProfiles(data.source_creator_fields || {}, managedProfiles),
      managedProfiles,
    );
    replaceRecord(defaults.source_creator_fields, creatorFields);
    replaceRecord(settings.source_creator_fields, creatorFields);

    const titleCleaning = createSourceTitleCleaning(
      recordForProfiles(data.source_title_cleaning || {}, managedProfiles),
      managedProfiles,
    );
    replaceRecord(defaults.source_title_cleaning, titleCleaning);
    replaceRecord(settings.source_title_cleaning, titleCleaning);

    settings.creator_field_defaults = createCreatorFieldRoles(
      data.creator_field_defaults || {},
    );
    replaceRecord(
      settings.source_creator_field_defaults,
      createSourceCreatorFields(
        recordForProfiles(data.source_creator_field_defaults || {}, managedProfiles),
        managedProfiles,
      ),
    );
    settings.title_cleaning_rules = Array.isArray(data.title_cleaning_rules)
      ? data.title_cleaning_rules
      : [];

    const qualityOptions = createQualityOptions(data.quality_options || {});
    settings.quality_options = qualityOptions;

    const defaultQuality = createQualitySelection(
      data.default_quality || {},
      qualityOptions,
    );
    Object.assign(defaults.default_quality, defaultQuality);
    Object.assign(settings.default_quality, defaultQuality);

    settings.template_tokens = Array.isArray(data.template_tokens)
      ? data.template_tokens
      : [];
    settings.download_locations = Array.isArray(data.download_locations)
      ? data.download_locations
      : [];
    replaceRecord(
      settings.ytdlp_cookies,
      createCookiesMap(
        recordForProfiles(data.ytdlp_cookies || {}, managedProfiles),
        managedProfiles,
      ),
    );
  }

  function cacheUiConfig(data: UiConfigResponse): void {
    applyServerSettings(data);
    queryClient.setQueryData(UI_CONFIG_QUERY_KEY, data);
  }

  async function persistSettings(
    payload: SavedSettings,
    successMessage = "",
  ): Promise<void> {
    cacheUiConfig(await saveSettingsMutation.mutateAsync(payload));
    if (successMessage) toast(successMessage);
  }

  function copySettingsToDraft(): void {
    const current = getSavedSettings();
    const normalized = normalizeSavedPayload(current);
    replaceProfiles(settingsDraft.source_profiles, normalized.source_profiles);
    replaceRecord(settingsDraft.site_locations, normalized.site_locations);
    Object.assign(
      settingsDraft.template_settings,
      normalized.template_settings,
    );
    replaceRecord(settingsDraft.source_templates, normalized.source_templates);
    Object.assign(settingsDraft.default_quality, normalized.default_quality);
    replaceRecord(
      settingsDraft.source_scrape_rules,
      normalized.source_scrape_rules,
    );
    replaceRecord(
      settingsDraft.source_token_roles,
      normalized.source_token_roles,
    );
    replaceRecord(
      settingsDraft.source_creator_fields,
      normalized.source_creator_fields,
    );
    replaceRecord(
      settingsDraft.source_title_cleaning,
      normalized.source_title_cleaning,
    );
    lastSavedSnapshot = JSON.stringify(normalizeSavedPayload(settingsDraft));
  }

  function setSettingsSection(
    section: SettingsSection,
    shouldFocus = false,
  ): void {
    settingsSection.value = section;
    if (!shouldFocus) return;
    const firstSource =
      settingsManagedSourceProfiles(sourceProfiles.value).find((profile) => profile.key)?.key || "settings";
    const focusTargets: Record<SettingsSection, string> = {
      account: "accountUsernameInput",
      downloads: `${firstSource}LocationInput`,
      cookies: `${firstSource}CookiesInput`,
      quality: "defaultQualityMode",
      creator: `${firstSource}CreatorProbeInput`,
      scraper: `${firstSource}ScraperProbeInput`,
      slug: `${firstSource}SlugSection`,
      "folder-template": `${firstSource}FolderTemplateInput`,
      "filename-template": `${firstSource}FilenameTemplateInput`,
    };
    void nextTick(() =>
      document.getElementById(focusTargets[settingsSection.value])?.focus(),
    );
  }

  function openSettings(
    event?: Event,
    section: SettingsSection = "downloads",
  ): void {
    lastFocusedTrigger.value =
      event?.currentTarget instanceof HTMLElement
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

  async function saveSettingsDraft(): Promise<void> {
    const payload = normalizeSavedPayload(settingsDraft);
    const snapshot = JSON.stringify(payload);
    if (snapshot === lastSavedSnapshot) return;
    lastSavedSnapshot = snapshot;
    try {
      await persistSettings(payload, "Settings saved.");
    } catch (error) {
      toast(errorMessage(error, "Could not save settings."), "error");
    }
  }

  function clearSettingsDraft(): void {
    copySettingsToDraft();
    toast("Changes cleared.");
  }

  async function connectCookies(platform: string, file?: File): Promise<void> {
    if (!file) {
      toast("Choose a cookies file first.", "error");
      return;
    }
    try {
      cacheUiConfig(
        await uploadCookiesMutation.mutateAsync({
          platform: normalizeSourceKey(platform),
          file,
        }),
      );
      toast("Cookies connected.");
    } catch (error) {
      toast(errorMessage(error, "Could not connect cookies."), "error");
    }
  }

  async function connectCookiesForSource(
    source: string,
    file?: File,
  ): Promise<void> {
    if (!source.trim()) {
      toast("Paste a link or domain first.", "error");
      return;
    }
    if (!file) {
      toast("Choose a cookies file first.", "error");
      return;
    }
    try {
      cacheUiConfig(
        await uploadCookiesMutation.mutateAsync({
          platform: "source",
          file,
          source: source.trim(),
        }),
      );
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

  const hasUnsavedChanges = computed(() => {
    return (
      JSON.stringify(normalizeSavedPayload(settingsDraft)) !== lastSavedSnapshot
    );
  });

  return {
    closeSettings,
    connectCookies,
    connectCookiesForSource,
    cookieStatuses,
    getSavedSettings,
    hasUnsavedChanges,
    openSettings,
    removeCookies,
    savedSettings,
    setSettingsSection,
    settings,
    settingsDraft,
    settingsOpen,
    settingsSection,
    sourceProfiles,
    saveSettingsDraft,
    clearSettingsDraft,
  };
}
