<script setup lang="ts">
import { computed, nextTick, reactive, watch } from "vue";
import IconTrash from "~icons/material-symbols/delete";
import IconUpload from "~icons/material-symbols/upload";
import { TabsContent, TabsList, TabsRoot, TabsTrigger } from "reka-ui";

import { SETTINGS_SECTIONS } from "../../ui";
import type {
  CookiesMap,
  RuntimeSettings,
  SavedSettings,
  SettingsSection,
  SourceProfile,
} from "../../types";
import {
  createTemplateSettings,
  mergeSourceProfiles,
} from "../../utils/dashboard";

const props = defineProps<{
  cookieStatuses: CookiesMap;
  section: SettingsSection;
  settings: RuntimeSettings;
  settingsDraft: SavedSettings;
  sourceProfiles: SourceProfile[];
}>();

const emit = defineEmits<{
  connectCookies: [platform: string, file?: File];
  removeCookies: [platform: string];
  save: [];
  "update:section": [section: SettingsSection];
}>();

const cookieFiles = reactive<Record<string, File | null>>({});

const sectionModel = computed({
  get: () => props.section,
  set: (value) => emit("update:section", value),
});

const sourceProfiles = computed<SourceProfile[]>(() => {
  return mergeSourceProfiles(
    props.sourceProfiles,
    props.settingsDraft.source_profiles,
  );
});

watch(
  sourceProfiles,
  (profiles) => {
    for (const profile of profiles) {
      if (
        !props.settingsDraft.source_profiles.some(
          (item) => item.key === profile.key,
        )
      ) {
        props.settingsDraft.source_profiles.push(profile);
      }
      if (!props.settingsDraft.source_templates[profile.key]) {
        props.settingsDraft.source_templates[profile.key] =
          createTemplateSettings(props.settingsDraft.template_settings);
      }
      if (!(profile.key in props.settingsDraft.site_locations))
        props.settingsDraft.site_locations[profile.key] = "";
    }
  },
  { immediate: true },
);

function selectSection(section: SettingsSection): void {
  sectionModel.value = section;
  const focusTargets: Record<SettingsSection, string> = {
    downloads: `${sourceProfiles.value[0]?.key || "settings"}LocationInput`,
    cookies: `${sourceProfiles.value[0]?.key || "settings"}CookiesInput`,
    "folder-template": `${sourceProfiles.value[0]?.key || "settings"}FolderTemplateInput`,
    "filename-template": `${sourceProfiles.value[0]?.key || "settings"}FilenameTemplateInput`,
  };
  void nextTick(() => document.getElementById(focusTargets[section])?.focus());
}

function onCookieFile(site: string, event: Event): void {
  cookieFiles[site] = (event.target as HTMLInputElement).files?.[0] || null;
}

function openPicker(site: string): void {
  document.getElementById(`${site}CookiesInput`)?.click();
}

function connect(site: string): void {
  emit("connectCookies", site, cookieFiles[site] || undefined);
}
</script>

<template>
  <section class="flex flex-col flex-1 min-h-0 p-4" aria-label="Settings">
    <datalist id="downloadLocationSuggestions">
      <option
        v-for="location in settings.download_locations"
        :key="location"
        :value="location"
      ></option>
    </datalist>

    <TabsRoot
      v-model="sectionModel"
      class="grid min-h-0 flex-1 gap-5 md:grid-cols-12"
      orientation="vertical"
    >
      <TabsList
        class="flex gap-1.5 overflow-x-auto pb-1 md:col-span-3 md:flex-col md:overflow-x-hidden md:overflow-y-auto md:pb-0"
        aria-label="Settings sections"
      >
        <TabsTrigger
          v-for="item in SETTINGS_SECTIONS"
          :key="item.key"
          class="settings-tab-trigger inline-flex items-center justify-center gap-1.5 min-h-9 px-3.5 flex-none rounded-xl bg-transparent text-text-muted leading-none transition-all duration-300 ease-glass active:scale-[0.97] hover:bg-white/10 hover:text-text md:w-full md:justify-start"
          :value="item.key"
          @click="selectSection(item.key)"
        >
          {{ item.label }}
        </TabsTrigger>
      </TabsList>

      <div class="min-h-0 overflow-auto md:col-span-9 md:pl-1">
        <TabsContent value="downloads" class="min-h-full focus:outline-none">
          <p
            v-if="sourceProfiles.length === 0"
            class="rounded-xl border border-(--glass-border) bg-white/[0.03] p-4 text-text-muted"
          >
            No sources yet.
          </p>
          <div class="grid gap-3 sm:grid-cols-2">
            <label
              v-for="site in sourceProfiles"
              :key="site.key"
              class="grid min-w-0 gap-2 text-text"
            >
              <span
                class="text-xs font-medium uppercase tracking-wider text-text-muted"
                >{{ site.label }}</span
              >
              <input
                :id="`${site.key}LocationInput`"
                v-model="settingsDraft.site_locations[site.key]"
                list="downloadLocationSuggestions"
                placeholder="Enter a save path"
              />
            </label>
          </div>
        </TabsContent>

        <TabsContent value="cookies" class="min-h-full focus:outline-none">
          <p
            v-if="sourceProfiles.length === 0"
            class="rounded-xl border border-(--glass-border) bg-white/[0.03] p-4 text-text-muted"
          >
            No sources yet.
          </p>
          <div class="grid gap-3 sm:grid-cols-2">
            <div
              v-for="site in sourceProfiles"
              :key="site.key"
              class="grid min-w-0 gap-2 text-text"
            >
              <span
                class="text-xs font-medium uppercase tracking-wider text-text-muted"
                >{{ site.label }}</span
              >
              <div class="flex items-center gap-2">
                <input
                  :id="`${site.key}CookiesInput`"
                  type="file"
                  accept=".txt,.cookies,text/plain"
                  class="hidden"
                  @change="onCookieFile(site.key, $event)"
                />
                <template v-if="!cookieStatuses[site.key]?.configured">
                  <button
                    type="button"
                    class="flex flex-1 items-center min-w-0 h-10 rounded-lg glass-soft glass-hoverable px-3 text-sm text-left transition-all duration-300 ease-glass"
                    @click="openPicker(site.key)"
                  >
                    <span
                      class="truncate"
                      :class="
                        cookieFiles[site.key] ? 'text-text' : 'text-text-muted'
                      "
                      >{{
                        cookieFiles[site.key]?.name || "Choose a cookies file"
                      }}</span
                    >
                  </button>
                  <button
                    type="button"
                    class="inline-flex items-center justify-center w-10 h-10 shrink-0 rounded-xl bg-primary text-text-on-primary transition-all duration-300 ease-glass active:scale-[0.96]"
                    :title="`Save ${site.label} cookies`"
                    :aria-label="`Save ${site.label} cookies`"
                    @click="connect(site.key)"
                  >
                    <IconUpload class="w-5 h-5" aria-hidden="true" />
                  </button>
                </template>
                <template v-else>
                  <div
                    class="flex flex-1 items-center min-w-0 h-10 rounded-lg glass-soft px-3 text-sm"
                  >
                    <span class="truncate text-text">{{
                      cookieStatuses[site.key].filename || "cookies.txt"
                    }}</span>
                  </div>
                  <button
                    type="button"
                    class="inline-flex items-center justify-center w-10 h-10 shrink-0 rounded-xl glass-soft glass-hoverable text-text-muted transition-all duration-300 ease-glass active:scale-[0.96] hover:text-text"
                    :title="`Delete ${site.label} cookies`"
                    :aria-label="`Delete ${site.label} cookies`"
                    @click="emit('removeCookies', site.key)"
                  >
                    <IconTrash class="w-5 h-5" aria-hidden="true" />
                  </button>
                </template>
              </div>
            </div>
          </div>
        </TabsContent>

        <TabsContent
          value="folder-template"
          class="min-h-full focus:outline-none"
        >
          <p
            v-if="sourceProfiles.length === 0"
            class="rounded-xl border border-(--glass-border) bg-white/[0.03] p-4 text-text-muted"
          >
            No sources yet.
          </p>
          <div class="grid gap-3 sm:grid-cols-2">
            <label
              v-for="site in sourceProfiles"
              :key="site.key"
              class="grid min-w-0 gap-2 text-text"
            >
              <span
                class="text-xs font-medium uppercase tracking-wider text-text-muted"
                >{{ site.label }}</span
              >
              <input
                :id="`${site.key}FolderTemplateInput`"
                v-model="
                  settingsDraft.source_templates[site.key].folder_template
                "
                class="font-mono"
              />
            </label>
          </div>
        </TabsContent>

        <TabsContent
          value="filename-template"
          class="min-h-full focus:outline-none"
        >
          <p
            v-if="sourceProfiles.length === 0"
            class="rounded-xl border border-(--glass-border) bg-white/[0.03] p-4 text-text-muted"
          >
            No sources yet.
          </p>
          <div class="grid gap-3 sm:grid-cols-2">
            <label
              v-for="site in sourceProfiles"
              :key="site.key"
              class="grid min-w-0 gap-2 text-text"
            >
              <span
                class="text-xs font-medium uppercase tracking-wider text-text-muted"
                >{{ site.label }}</span
              >
              <textarea
                :id="`${site.key}FilenameTemplateInput`"
                v-model="
                  settingsDraft.source_templates[site.key].filename_template
                "
                rows="3"
                class="font-mono"
              ></textarea>
            </label>
          </div>
        </TabsContent>
      </div>
    </TabsRoot>

    <footer
      class="shrink-0 mt-6 flex items-center justify-end gap-2 pt-4 border-t border-(--glass-border)"
    >
      <button
        type="button"
        class="inline-flex items-center justify-center gap-1.5 min-h-9 px-5 rounded-xl bg-primary text-text-on-primary leading-none transition-all duration-300 ease-glass active:scale-[0.97]"
        @click="emit('save')"
      >
        Save Settings
      </button>
    </footer>
  </section>
</template>

<style scoped>
.settings-tab-trigger[data-state="active"] {
  background: var(--primary);
  color: var(--text-on-primary);
  box-shadow: 0 8px 24px -8px rgba(237, 158, 89, 0.6);
}
</style>
