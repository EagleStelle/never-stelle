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
  <section class="flex flex-col flex-1 min-h-0 border border-accent-subtle rounded-lg bg-panel overflow-hidden" aria-labelledby="settingsTitle">
    <header class="flex items-center justify-between gap-[0.75rem] p-[0.75rem] border-b border-accent-subtle bg-panel-subtle">
      <h1 id="settingsTitle" class="m-0 text-[1.45rem] font-[900] leading-none">Settings</h1>
    </header>

    <datalist id="downloadLocationSuggestions">
      <option v-for="location in settings.download_locations" :key="location" :value="location"></option>
    </datalist>

    <TabsRoot v-model="sectionModel" class="grid grid-rows-[auto_minmax(0,1fr)] min-h-0 flex-1 gap-[0.65rem] p-[0.75rem] md:grid-cols-[12rem_minmax(0,1fr)] md:grid-rows-[minmax(0,1fr)]" orientation="vertical">
      <TabsList class="flex gap-[0.4rem] overflow-x-auto pb-[0.15rem] md:flex-col md:overflow-x-hidden md:overflow-y-auto md:pb-0" aria-label="Settings sections">
        <TabsTrigger
          v-for="item in SETTINGS_SECTIONS"
          :key="item.key"
          class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.45rem] px-[0.8rem] flex-none border border-accent-subtle rounded-lg bg-panel-subtle text-text-muted font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:border-accent hover:text-text data-[state=active]:border-accent data-[state=active]:bg-accent data-[state=active]:text-bg md:w-full md:justify-start"
          :value="item.key"
          @click="selectSection(item.key)"
        >
          {{ item.label }}
        </TabsTrigger>
      </TabsList>

      <div class="min-h-0 overflow-auto">
        <TabsContent value="downloads" class="min-h-full">
          <div class="grid gap-[0.6rem] sm:grid-cols-[repeat(2,minmax(0,1fr))]">
            <label v-for="site in SITE_KEYS" :key="site" class="grid min-w-0 gap-[0.42rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text p-[0.65rem] text-[0.88rem] font-[800] focus-within:border-accent">
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
          <div class="grid gap-[0.6rem]">
            <label class="grid min-w-0 gap-[0.42rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text p-[0.65rem] text-[0.88rem] font-[800] focus-within:border-accent">
              <span>Cookies</span>
              <input id="instagramYtdlpCookiesInput" ref="cookiesInput" type="file" accept=".txt,.cookies,text/plain" />
            </label>

            <div class="border border-accent-subtle rounded-lg bg-panel-subtle text-text-muted p-[0.75rem] text-[0.86rem]">{{ cookiesStatusText }}</div>

            <div class="flex items-center gap-[0.45rem]">
              <button
                type="button"
                class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.55rem] px-[0.85rem] text-[0.9rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text-muted font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:border-accent hover:text-text md:flex-none"
                :disabled="!settings.instagram_ytdlp_cookies.configured"
                @click="emit('removeInstagramCookies')"
              >
                Disconnect
              </button>
              <button type="button" class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.55rem] px-[0.85rem] text-[0.9rem] border border-text rounded-lg bg-accent text-bg font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:border-accent hover:bg-text hover:text-bg" @click="connectCookies">
                <IconUpload aria-hidden="true" />
                <span>Connect</span>
              </button>
            </div>
          </div>
        </TabsContent>

        <TabsContent value="advanced" class="min-h-full">
          <div class="grid gap-[0.6rem]">
            <label class="grid min-w-0 gap-[0.42rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text p-[0.65rem] text-[0.88rem] font-[800] focus-within:border-accent">
              <span>Folder Template</span>
              <input id="folderTemplateInput" v-model="settingsDraft.template_settings.folder_template" class="font-[var(--font-mono)] text-[0.84rem]" />
            </label>
            <label class="grid min-w-0 gap-[0.42rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text p-[0.65rem] text-[0.88rem] font-[800] focus-within:border-accent">
              <span>Filename Template</span>
              <textarea v-model="settingsDraft.template_settings.filename_template" rows="3" class="font-[var(--font-mono)] text-[0.84rem]"></textarea>
            </label>
          </div>
        </TabsContent>
      </div>
    </TabsRoot>

    <footer class="flex items-center justify-between gap-[0.75rem] p-[0.75rem] border-t border-accent-subtle bg-panel-subtle">
      <button
        type="button"
        class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.55rem] px-[0.85rem] text-[0.9rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text-muted font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:border-accent hover:text-text md:flex-none"
        :disabled="cleanupBusy"
        title="Find and delete all .nfo sidecar files from the downloads folder"
        @click="emit('cleanNfo')"
      >
        <IconRadio aria-hidden="true" />
        <span>{{ cleanupBusy ? "Deleting..." : "Delete .nfo files" }}</span>
      </button>
      <div class="flex items-center gap-[0.45rem]">
        <button type="button" class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.55rem] px-[0.85rem] text-[0.9rem] border border-text rounded-lg bg-accent text-bg font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:border-accent hover:bg-text hover:text-bg" @click="emit('save')">Save Settings</button>
      </div>
    </footer>
  </section>
</template>
