import { computed, nextTick, onBeforeUnmount, reactive, ref, watch } from "vue";
import { useMutation, useQuery, useQueryClient } from "@tanstack/vue-query";

import { deletePlatformCookies, getUiConfig, saveSettings, uploadPlatformCookies } from "../api";
import { UI_CONFIG_QUERY_KEY } from "../ui";
import { SITE_KEYS, type CookiesMap, type CookiesStatus, type RuntimeSettings, type SavedSettings, type SettingsSection, type ToastType, type UiConfigResponse } from "../types";
import { createCookiesStatus, createSiteLocations, createTemplateSettings, errorMessage } from "../utils/dashboard";

function createCookiesMap(source: Record<string, Partial<CookiesStatus>> = {}): CookiesMap {
  return Object.fromEntries(SITE_KEYS.map((key) => [key, createCookiesStatus(source[key] || {})])) as CookiesMap;
}

interface UseDashboardSettingsOptions {
  toast: (message: string, type?: ToastType) => void;
}

export function useDashboardSettings({ toast }: UseDashboardSettingsOptions) {
  const queryClient = useQueryClient();
  const defaults = reactive<SavedSettings>({
    site_locations: createSiteLocations(),
    template_settings: createTemplateSettings(),
  });
  const settings = reactive<RuntimeSettings>({
    site_locations: createSiteLocations(),
    template_settings: createTemplateSettings(),
    download_locations: [],
    ytdlp_cookies: createCookiesMap(),
  });
  const settingsDraft = reactive<SavedSettings>({
    site_locations: createSiteLocations(),
    template_settings: createTemplateSettings(),
  });

  const settingsOpen = ref(false);
  const settingsSection = ref<SettingsSection>("downloads");
  const lastFocusedTrigger = ref<HTMLElement | null>(null);

  const uiConfigQuery = useQuery<UiConfigResponse>({
    queryKey: UI_CONFIG_QUERY_KEY,
    queryFn: getUiConfig,
    staleTime: 60_000,
  });
  const saveSettingsMutation = useMutation({ mutationFn: saveSettings });
  const uploadCookiesMutation = useMutation({ mutationFn: ({ platform, file }: { platform: string; file: File }) => uploadPlatformCookies(platform, file) });
  const deleteCookiesMutation = useMutation({ mutationFn: (platform: string) => deletePlatformCookies(platform) });

  const savedSettings = computed<SavedSettings>(() => getSavedSettings());
  const cookieStatuses = computed<CookiesMap>(() => settings.ytdlp_cookies);

  function getSavedSettings(): SavedSettings {
    return {
      site_locations: {
        youtube: settings.site_locations.youtube || defaults.site_locations.youtube || "",
        tiktok: settings.site_locations.tiktok || defaults.site_locations.tiktok || "",
        instagram: settings.site_locations.instagram || defaults.site_locations.instagram || "",
        twitter: settings.site_locations.twitter || defaults.site_locations.twitter || "",
        facebook: settings.site_locations.facebook || defaults.site_locations.facebook || "",
        reddit: settings.site_locations.reddit || defaults.site_locations.reddit || "",
        twitch: settings.site_locations.twitch || defaults.site_locations.twitch || "",
        pinterest: settings.site_locations.pinterest || defaults.site_locations.pinterest || "",
        bluesky: settings.site_locations.bluesky || defaults.site_locations.bluesky || "",
        linkedin: settings.site_locations.linkedin || defaults.site_locations.linkedin || "",
        others: settings.site_locations.others || defaults.site_locations.others || "",
      },
      template_settings: {
        folder_template: settings.template_settings.folder_template || defaults.template_settings.folder_template || "",
        filename_template: settings.template_settings.filename_template || defaults.template_settings.filename_template || "",
      },
    };
  }

  function applyServerSettings(data: UiConfigResponse): void {
    if (data.site_default_locations) {
      Object.assign(defaults.site_locations, createSiteLocations({ ...defaults.site_locations, ...data.site_default_locations }));
      Object.assign(settings.site_locations, createSiteLocations({ ...settings.site_locations, ...data.site_default_locations }));
    }
    if (data.template_settings) {
      Object.assign(defaults.template_settings, createTemplateSettings({ ...defaults.template_settings, ...data.template_settings }));
      Object.assign(settings.template_settings, createTemplateSettings({ ...settings.template_settings, ...data.template_settings }));
    }
    settings.download_locations = Array.isArray(data.download_locations) ? data.download_locations : [];
    Object.assign(settings.ytdlp_cookies, createCookiesMap(data.ytdlp_cookies || {}));
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
    Object.assign(settingsDraft.site_locations, current.site_locations);
    Object.assign(settingsDraft.template_settings, current.template_settings);
  }

  function setSettingsSection(section: SettingsSection, shouldFocus = false): void {
    settingsSection.value = section;
    if (!shouldFocus) return;
    const focusTargets: Record<SettingsSection, string> = {
      downloads: "youtubeLocationInput",
      instagram: "instagramYtdlpCookiesInput",
      advanced: "folderTemplateInput",
    };
    void nextTick(() => document.getElementById(focusTargets[settingsSection.value])?.focus());
  }

  function openSettings(event?: Event, section: SettingsSection = "downloads"): void {
    lastFocusedTrigger.value = event?.currentTarget instanceof HTMLElement ? event.currentTarget : document.activeElement instanceof HTMLElement ? document.activeElement : null;
    copySettingsToDraft();
    settingsOpen.value = true;
    setSettingsSection(section, true);
  }

  function closeSettings(): void {
    settingsOpen.value = false;
  }

  async function saveSettingsDraft(): Promise<void> {
    const payload: SavedSettings = {
      site_locations: createSiteLocations(settingsDraft.site_locations),
      template_settings: createTemplateSettings(settingsDraft.template_settings),
    };
    try {
      await persistSettings(payload);
      toast("Settings saved.");
      closeSettings();
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
      cacheUiConfig(await uploadCookiesMutation.mutateAsync({ platform, file }));
      toast("Cookies connected.");
    } catch (error) {
      toast(errorMessage(error, "Could not connect cookies."), "error");
    }
  }

  async function removeCookies(platform: string): Promise<void> {
    if (!settings.ytdlp_cookies[platform]?.configured) return;
    try {
      cacheUiConfig(await deleteCookiesMutation.mutateAsync(platform));
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
      // Seed the draft once config arrives so the Settings page shows the
      // saved/default paths even when it is reached via reload (not a click,
      // which is the only other place the draft is copied).
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

  onBeforeUnmount(() => document.body.classList.remove("dialog-open"));

  return {
    connectCookies,
    cookieStatuses,
    getSavedSettings,
    openSettings,
    removeCookies,
    saveSettingsDraft,
    savedSettings,
    setSettingsSection,
    settings,
    settingsDraft,
    settingsOpen,
    settingsSection,
  };
}
