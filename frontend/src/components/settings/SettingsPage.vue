<script setup lang="ts">
import { computed, nextTick, reactive, watch } from "vue";
import IconTrash from "~icons/material-symbols/delete";
import IconUpload from "~icons/material-symbols/upload";
import { TabsContent, TabsList, TabsRoot, TabsTrigger } from "reka-ui";

import Button from "../ui/Button.vue";
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
  connectCookiesSource: [source: string, file?: File];
  removeCookies: [platform: string];
  save: [];
  "update:section": [section: SettingsSection];
}>();

const cookieFiles = reactive<Record<string, File | null>>({});
const newCookie = reactive<{ source: string; file: File | null }>({
  source: "",
  file: null,
});

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

function onNewCookieFile(event: Event): void {
  newCookie.file = (event.target as HTMLInputElement).files?.[0] || null;
}

function openNewPicker(): void {
  document.getElementById("newSourceCookiesInput")?.click();
}

function connectNew(): void {
  emit("connectCookiesSource", newCookie.source, newCookie.file || undefined);
  newCookie.source = "";
  newCookie.file = null;
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
          class="settings-tab-trigger glass-selected inline-flex items-center justify-center gap-1.5 min-h-9 px-3.5 flex-none rounded-lg glass-soft glass-hoverable text-white [.light-mode_&]:text-black leading-none transition-all duration-300 ease-glass active:scale-[0.97] hover:text-white [.light-mode_&]:hover:text-black md:w-full md:justify-start"
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
            class="rounded-lg glass p-4 text-white [.light-mode_&]:text-black"
          >
            No sources yet.
          </p>
          <div class="grid gap-3 sm:grid-cols-2">
            <label
              v-for="site in sourceProfiles"
              :key="site.key"
              class="grid min-w-0 gap-2 text-white [.light-mode_&]:text-black"
            >
              <span
                class="text-xs font-medium uppercase tracking-wider text-white [.light-mode_&]:text-black"
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
          <div class="mb-4 grid gap-2 text-white [.light-mode_&]:text-black">
            <span
              class="text-xs font-medium uppercase tracking-wider text-white [.light-mode_&]:text-black"
              >Add cookies for a link</span
            >
            <div class="flex items-center gap-2">
              <input
                v-model="newCookie.source"
                type="text"
                inputmode="url"
                placeholder="Paste a link or domain (e.g. instagram.com)"
                class="flex flex-1 items-center min-w-0 h-10 rounded-lg glass-soft px-3 text-sm text-white [.light-mode_&]:text-black placeholder:text-white [.light-mode_&]:placeholder:text-black focus:outline-none"
              />
              <input
                id="newSourceCookiesInput"
                type="file"
                accept=".txt,.cookies,text/plain"
                class="hidden"
                @change="onNewCookieFile"
              />
              <Button
                variant="soft"
                size="lg"
                type="button"
                class="min-w-0 justify-start"
                @click="openNewPicker"
              >
                <span class="truncate">{{
                  newCookie.file?.name || "Choose file"
                }}</span>
              </Button>
              <Button
                variant="primary"
                size="lg"
                type="button"
                class="shrink-0"
                title="Save cookies for link"
                aria-label="Save cookies for link"
                @click="connectNew"
              >
                <template #icon>
                  <IconUpload class="w-5 h-5" aria-hidden="true" />
                </template>
              </Button>
            </div>
          </div>
          <p
            v-if="sourceProfiles.length === 0"
            class="rounded-lg glass p-4 text-white [.light-mode_&]:text-black"
          >
            No sources yet.
          </p>
          <div class="grid gap-3 sm:grid-cols-2">
            <div
              v-for="site in sourceProfiles"
              :key="site.key"
              class="grid min-w-0 gap-2 text-white [.light-mode_&]:text-black"
            >
              <span
                class="text-xs font-medium uppercase tracking-wider text-white [.light-mode_&]:text-black"
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
                  <Button
                    variant="soft"
                    size="lg"
                    type="button"
                    class="flex-1 min-w-0 justify-start"
                    @click="openPicker(site.key)"
                  >
                    <span class="truncate">{{
                      cookieFiles[site.key]?.name || "Choose a cookies file"
                    }}</span>
                  </Button>
                  <Button
                    variant="primary"
                    size="lg"
                    type="button"
                    class="shrink-0"
                    :title="`Save ${site.label} cookies`"
                    :aria-label="`Save ${site.label} cookies`"
                    @click="connect(site.key)"
                  >
                    <template #icon>
                      <IconUpload class="w-5 h-5" aria-hidden="true" />
                    </template>
                  </Button>
                </template>
                <template v-else>
                  <div
                    class="flex flex-1 items-center min-w-0 h-10 rounded-lg glass-soft px-3 text-sm"
                  >
                    <span class="truncate text-white [.light-mode_&]:text-black">{{
                      cookieStatuses[site.key].filename || "cookies.txt"
                    }}</span>
                  </div>
                  <Button
                    variant="soft"
                    size="lg"
                    type="button"
                    class="shrink-0"
                    :title="`Delete ${site.label} cookies`"
                    :aria-label="`Delete ${site.label} cookies`"
                    @click="emit('removeCookies', site.key)"
                  >
                    <template #icon>
                      <IconTrash class="w-5 h-5" aria-hidden="true" />
                    </template>
                  </Button>
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
            class="rounded-lg glass p-4 text-white [.light-mode_&]:text-black"
          >
            No sources yet.
          </p>
          <div class="grid gap-3 sm:grid-cols-2">
            <label
              v-for="site in sourceProfiles"
              :key="site.key"
              class="grid min-w-0 gap-2 text-white [.light-mode_&]:text-black"
            >
              <span
                class="text-xs font-medium uppercase tracking-wider text-white [.light-mode_&]:text-black"
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
            class="rounded-lg glass p-4 text-white [.light-mode_&]:text-black"
          >
            No sources yet.
          </p>
          <div class="grid gap-3 sm:grid-cols-2">
            <label
              v-for="site in sourceProfiles"
              :key="site.key"
              class="grid min-w-0 gap-2 text-white [.light-mode_&]:text-black"
            >
              <span
                class="text-xs font-medium uppercase tracking-wider text-white [.light-mode_&]:text-black"
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
      class="shrink-0 mt-6 flex items-center justify-end gap-2 rounded-lg glass p-3"
    >
      <Button variant="primary" size="md" type="button" @click="emit('save')">
        Save Settings
      </Button>
    </footer>
  </section>
</template>

