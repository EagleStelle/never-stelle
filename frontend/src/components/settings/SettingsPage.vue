<script setup lang="ts">
import { computed, nextTick, ref } from "vue";
import IconRadio from "~icons/material-symbols/radio-button-checked";
import IconUpload from "~icons/material-symbols/upload";
import {
  TabsContent,
  TabsList,
  TabsRoot,
  TabsTrigger,
} from "reka-ui";

import { SETTINGS_SECTIONS, SITE_LABELS } from "../../ui";
import { SITE_KEYS, type RuntimeSettings, type SavedSettings, type SettingsSection, type SiteKey } from "../../types";

const props = defineProps<{
  cleanupBusy: boolean;
  cookiesStatusText: string;
  section: SettingsSection;
  settings: RuntimeSettings;
  settingsDraft: SavedSettings;
}>();

const emit = defineEmits<{
  cleanNfo: [];
  connectInstagramCookies: [file?: File];
  removeInstagramCookies: [];
  save: [];
  "update:section": [section: SettingsSection];
}>();

const cookiesInput = ref<HTMLInputElement | null>(null);

const sectionModel = computed({
  get: () => props.section,
  set: (value) => emit("update:section", value),
});

function siteLabel(site: SiteKey): string {
  return SITE_LABELS[site] || "Others";
}

function selectSection(section: SettingsSection): void {
  sectionModel.value = section;
  const focusTargets: Record<SettingsSection, string> = {
    downloads: "youtubeLocationInput",
    instagram: "instagramYtdlpCookiesInput",
    advanced: "folderTemplateInput",
  };
  void nextTick(() => document.getElementById(focusTargets[section])?.focus());
}

function connectCookies(): void {
  emit("connectInstagramCookies", cookiesInput.value?.files?.[0]);
}
</script>

<template>
  <section class="flex flex-col flex-1 min-h-0 border border-primary-muted rounded-lg bg-secondary overflow-hidden" aria-labelledby="settingsTitle">
    <header class="flex items-center justify-between gap-2 p-2 border-b border-primary-muted bg-secondary-muted">
      <h1 id="settingsTitle" class="m-0 leading-none">Settings</h1>
    </header>

    <datalist id="downloadLocationSuggestions">
      <option v-for="location in settings.download_locations" :key="location" :value="location"></option>
    </datalist>

    <TabsRoot v-model="sectionModel" class="grid min-h-0 flex-1 gap-2 p-2 md:grid-cols-12" orientation="vertical">
      <TabsList class="flex gap-1.5 overflow-x-auto pb-1 md:col-span-3 md:flex-col md:overflow-x-hidden md:overflow-y-auto md:pb-0" aria-label="Settings sections">
        <TabsTrigger
          v-for="item in SETTINGS_SECTIONS"
          :key="item.key"
          class="settings-tab-trigger inline-flex items-center justify-center gap-1.5 min-h-9 px-3 flex-none border border-primary-muted rounded-lg bg-secondary-muted text-text-muted leading-none transition-all duration-200 ease-out active:scale-95 hover:border-primary hover:text-text md:w-full md:justify-start"
          :value="item.key"
          @click="selectSection(item.key)"
        >
          {{ item.label }}
        </TabsTrigger>
      </TabsList>

      <div class="min-h-0 overflow-auto md:col-span-9">
        <TabsContent value="downloads" class="min-h-full">
          <div class="grid gap-2 sm:grid-cols-2">
            <label v-for="site in SITE_KEYS" :key="site" class="grid min-w-0 gap-1.5 border border-primary-muted rounded-lg bg-secondary-muted text-text p-2 focus-within:border-primary">
              <span>{{ siteLabel(site) }}</span>
              <input
                :id="`${site}LocationInput`"
                v-model="settingsDraft.site_locations[site]"
                list="downloadLocationSuggestions"
                placeholder="Enter a save path"
              />
            </label>
          </div>
        </TabsContent>

        <TabsContent value="instagram" class="min-h-full">
          <div class="grid gap-2">
            <label class="grid min-w-0 gap-1.5 border border-primary-muted rounded-lg bg-secondary-muted text-text p-2 focus-within:border-primary">
              <span>Cookies</span>
              <input id="instagramYtdlpCookiesInput" ref="cookiesInput" type="file" accept=".txt,.cookies,text/plain" />
            </label>

            <div class="border border-primary-muted rounded-lg bg-secondary-muted text-text-muted p-2">{{ cookiesStatusText }}</div>

            <div class="flex items-center gap-1.5">
              <button
                type="button"
                class="inline-flex items-center justify-center gap-1.5 min-h-9 px-3 border border-primary-muted rounded-lg bg-primary text-background leading-none transition-all duration-200 ease-out active:scale-95 hover:border-primary hover:bg-text hover:text-background md:flex-none"
                :disabled="!settings.instagram_ytdlp_cookies.configured"
                @click="emit('removeInstagramCookies')"
              >
                Disconnect
              </button>
              <button type="button" class="inline-flex items-center justify-center gap-1.5 min-h-9 px-3 border border-text rounded-lg bg-primary text-background leading-none transition-all duration-200 ease-out active:scale-95 hover:border-primary hover:bg-text hover:text-background" @click="connectCookies">
                <IconUpload aria-hidden="true" />
                <span>Connect</span>
              </button>
            </div>
          </div>
        </TabsContent>

        <TabsContent value="advanced" class="min-h-full">
          <div class="grid gap-2">
            <label class="grid min-w-0 gap-1.5 border border-primary-muted rounded-lg bg-secondary-muted text-text p-2 focus-within:border-primary">
              <span>Folder Template</span>
              <input id="folderTemplateInput" v-model="settingsDraft.template_settings.folder_template" class="font-mono" />
            </label>
            <label class="grid min-w-0 gap-1.5 border border-primary-muted rounded-lg bg-secondary-muted text-text p-2 focus-within:border-primary">
              <span>Filename Template</span>
              <textarea v-model="settingsDraft.template_settings.filename_template" rows="3" class="font-mono"></textarea>
            </label>
          </div>
        </TabsContent>
      </div>
    </TabsRoot>

    <footer class="flex items-center justify-between gap-2 p-2 border-t border-primary-muted bg-secondary-muted">
      <button
        type="button"
                class="inline-flex items-center justify-center gap-1.5 min-h-9 px-3 border border-primary-muted rounded-lg bg-primary text-background leading-none transition-all duration-200 ease-out active:scale-95 hover:border-primary hover:bg-text hover:text-background md:flex-none"
        :disabled="cleanupBusy"
        title="Find and delete all .nfo sidecar files from the downloads folder"
        @click="emit('cleanNfo')"
      >
        <IconRadio aria-hidden="true" />
        <span>{{ cleanupBusy ? "Deleting..." : "Delete .nfo files" }}</span>
      </button>
      <div class="flex items-center gap-1.5">
        <button type="button" class="inline-flex items-center justify-center gap-1.5 min-h-9 px-3 border border-text rounded-lg bg-primary text-background leading-none transition-all duration-200 ease-out active:scale-95 hover:border-primary hover:bg-text hover:text-background" @click="emit('save')">Save Settings</button>
      </div>
    </footer>
  </section>
</template>

<style scoped>
.settings-tab-trigger[data-state="active"] {
  border-color: var(--primary);
  background: var(--primary);
  color: var(--background);
}
</style>
