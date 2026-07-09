<script setup lang="ts">
import { computed, nextTick, reactive, ref, watch } from "vue";
import type { Component } from "vue";
import IconCookie from "~icons/material-symbols/cookie";
import IconDescription from "~icons/material-symbols/description";
import IconFolder from "~icons/material-symbols/folder";
import IconRuleFolder from "~icons/material-symbols/rule-folder";
import IconSearch from "~icons/material-symbols/search";
import IconTrash from "~icons/material-symbols/delete";
import IconUpload from "~icons/material-symbols/upload";
import {
  TabsContent,
  TabsList,
  TabsRoot,
  TabsTrigger,
} from "reka-ui";

import { Button } from "../../components/ui/button";
import { Dialog } from "../../components/ui/dialog";
import { Input } from "../../components/ui/input";
import {
  Sidebar,
  SidebarHeader,
  SidebarContent,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuItem,
  SidebarMenuButton,
} from "../../components/ui/sidebar";
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
  open: boolean;
  section: SettingsSection;
  settings: RuntimeSettings;
  settingsDraft: SavedSettings;
  sourceProfiles: SourceProfile[];
}>();

const emit = defineEmits<{
  connectCookies: [platform: string, file?: File];
  connectCookiesSource: [source: string, file?: File];
  removeCookies: [platform: string];
  "update:open": [open: boolean];
  "update:section": [section: SettingsSection];
}>();

const SECTION_ICONS: Record<SettingsSection, Component> = {
  downloads: IconFolder,
  cookies: IconCookie,
  "folder-template": IconRuleFolder,
  "filename-template": IconDescription,
};

const SECTION_GROUPS: Array<{ label: string; keys: SettingsSection[] }> = [
  { label: "Settings", keys: ["downloads", "cookies"] },
  { label: "Templates", keys: ["folder-template", "filename-template"] },
];

const cookieFiles = reactive<Record<string, File | null>>({});
const newCookie = reactive<{ source: string; file: File | null }>({
  source: "",
  file: null,
});
const sectionSearch = ref("");

const openModel = computed({
  get: () => props.open,
  set: (value) => emit("update:open", value),
});

const sectionModel = computed({
  get: () => props.section,
  set: (value) => emit("update:section", value),
});

const sectionByKey = computed(() =>
  Object.fromEntries(SETTINGS_SECTIONS.map((item) => [item.key, item])),
);

const sectionGroups = computed(() => {
  const term = sectionSearch.value.trim().toLowerCase();
  const build = (filter: boolean) =>
    SECTION_GROUPS.map((group) => ({
      label: group.label,
      items: group.keys
        .map((key) => sectionByKey.value[key])
        .filter(
          (item) =>
            item && (!filter || item.label.toLowerCase().includes(term)),
        ),
    })).filter((group) => group.items.length);
  if (!term) return build(false);
  const filtered = build(true);
  return filtered.length ? filtered : build(false);
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
        props.settingsDraft.site_locations[profile.key] =
          props.settings.site_locations[profile.key] || "";
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
  const file = (event.target as HTMLInputElement).files?.[0] || null;
  cookieFiles[site] = file;
  if (file) emit("connectCookies", site, file);
}

function openPicker(site: string): void {
  document.getElementById(`${site}CookiesInput`)?.click();
}

function onNewCookieFile(event: Event): void {
  const file = (event.target as HTMLInputElement).files?.[0] || null;
  newCookie.file = file;
  if (file) connectNew();
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
  <Dialog
    v-model:open="openModel"
    title="Settings"
    hide-title
    description="Configure download locations, cookies, and naming templates."
    overlay-class="settings-overlay fixed inset-0 z-60 bg-black/50 backdrop-blur-sm"
    content-class="settings-content fixed left-1/2 top-1/2 z-70 flex w-[min(980px,96vw)] h-[min(700px,92vh)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none sm:flex-row"
  >
        <datalist id="downloadLocationSuggestions">
          <option
            v-for="location in settings.download_locations"
            :key="location"
            :value="location"
          ></option>
        </datalist>

        <TabsRoot
          v-model="sectionModel"
          class="flex min-h-0 flex-1 flex-col sm:flex-row"
          orientation="vertical"
        >
          <!-- Sidebar -->
          <Sidebar class="p-3 sm:h-full sm:w-60 sm:p-4 gap-3">
            <SidebarHeader>
              <Input
                size="lg"
                v-model="sectionSearch"
                type="text"
                placeholder="Search"
                aria-label="Search settings"
                class="hidden sm:flex"
              >
                <template #icon>
                  <IconSearch class="h-5 w-5" aria-hidden="true" />
                </template>
              </Input>
            </SidebarHeader>

            <SidebarContent>
              <TabsList
                class="flex gap-1 overflow-x-auto pb-1 sm:flex-col sm:overflow-x-hidden sm:overflow-y-auto sm:pb-0"
                aria-label="Settings sections"
              >
                <SidebarGroup v-for="group in sectionGroups" :key="group.label">
                  <SidebarGroupLabel
                    class="hidden sm:block sm:not-first:mt-3"
                  >
                    {{ group.label }}
                  </SidebarGroupLabel>
                  <SidebarMenu class="flex-row sm:flex-col gap-1">
                    <SidebarMenuItem
                      v-for="item in group.items"
                      :key="item.key"
                      class="flex-none sm:w-full"
                    >
                      <TabsTrigger
                        as-child
                        :value="item.key"
                        @click="selectSection(item.key)"
                      >
                        <SidebarMenuButton as="div" class="cursor-pointer">
                          <component
                            :is="SECTION_ICONS[item.key]"
                            class="h-5 w-5 shrink-0 opacity-80 group-data-[state=active]:opacity-100"
                            aria-hidden="true"
                          />
                          <span class="whitespace-nowrap font-medium">{{
                            item.label
                          }}</span>
                        </SidebarMenuButton>
                      </TabsTrigger>
                    </SidebarMenuItem>
                  </SidebarMenu>
                </SidebarGroup>
              </TabsList>
            </SidebarContent>
          </Sidebar>

          <!-- Content -->
          <div class="relative min-h-0 min-w-0 flex-1">
            <div class="h-full overflow-y-auto px-5 py-6 sm:px-8">
              <TabsContent
                v-for="item in SETTINGS_SECTIONS"
                :key="item.key"
                :value="item.key"
                class="pr-10 focus:outline-none"
              >
                <p
                  v-if="sourceProfiles.length === 0 && item.key !== 'cookies'"
                  class="rounded-lg glass p-4 text-white in-[.light-mode]:text-black"
                >
                  No sources yet.
                </p>

                <!-- Locations -->
                <template v-if="item.key === 'downloads'">
                  <div class="flex flex-col">
                    <div
                      v-for="site in sourceProfiles"
                      :key="site.key"
                      class="settings-row"
                    >
                      <span class="settings-row-label">{{ site.label }}</span>
                      <div class="settings-row-control">
                        <Input
                          :id="`${site.key}LocationInput`"
                          v-model="settingsDraft.site_locations[site.key]"
                          list="downloadLocationSuggestions"
                          placeholder="Enter a save path"
                        />
                      </div>
                    </div>
                  </div>
                </template>

                <!-- Cookies -->
                <template v-else-if="item.key === 'cookies'">
                  <div class="settings-row">
                    <div class="flex w-full items-center gap-2">
                      <Input
                        size="lg"
                        v-model="newCookie.source"
                        type="text"
                        inputmode="url"
                        placeholder="Paste a link or domain"
                        class="flex-1"
                      />
                      <input
                        id="newSourceCookiesInput"
                        type="file"
                        accept=".txt,.cookies,text/plain"
                        class="hidden"
                        @change="onNewCookieFile"
                      />
                      <Button
                        variant="primary"
                        size="lg"
                        type="button"
                        class="shrink-0"
                        title="Upload cookies for link"
                        aria-label="Upload cookies for link"
                        @click="openNewPicker"
                      >
                        <template #icon>
                          <IconUpload class="w-5 h-5" aria-hidden="true" />
                        </template>
                      </Button>
                    </div>
                  </div>

                  <p
                    v-if="sourceProfiles.length === 0"
                    class="mt-3 rounded-lg glass p-4 text-white in-[.light-mode]:text-black"
                  >
                    No sources yet.
                  </p>

                  <div
                    v-for="site in sourceProfiles"
                    :key="site.key"
                    class="settings-row"
                  >
                    <span class="settings-row-label">{{ site.label }}</span>
                    <div class="settings-row-control">
                      <div class="flex items-center gap-2">
                        <input
                          :id="`${site.key}CookiesInput`"
                          type="file"
                          accept=".txt,.cookies,text/plain"
                          class="hidden"
                          @change="onCookieFile(site.key, $event)"
                        />
                        <template v-if="!cookieStatuses[site.key]?.configured">
                          <div
                            class="flex flex-1 items-center min-w-0 h-10 rounded-lg glass-soft px-3 text-sm"
                          >
                            <span
                              class="truncate text-white/60 in-[.light-mode]:text-black/60"
                              >{{
                                cookieFiles[site.key]?.name || "No cookies file"
                              }}</span
                            >
                          </div>
                          <Button
                            variant="primary"
                            size="lg"
                            type="button"
                            class="shrink-0"
                            :title="`Upload ${site.label} cookies`"
                            :aria-label="`Upload ${site.label} cookies`"
                            @click="openPicker(site.key)"
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
                            <span
                              class="truncate text-white in-[.light-mode]:text-black"
                              >{{
                                cookieStatuses[site.key].filename ||
                                "cookies.txt"
                              }}</span
                            >
                          </div>
                          <Button
                            variant="soft"
                            size="lg"
                            type="button"
                            class="shrink-0 bg-[#ef4444]! text-white! hover:bg-[#dc2626]!"
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
                </template>

                <!-- Folder template -->
                <template v-else-if="item.key === 'folder-template'">
                  <div
                    v-for="site in sourceProfiles"
                    :key="site.key"
                    class="settings-row"
                  >
                    <span class="settings-row-label">{{ site.label }}</span>
                    <div class="settings-row-control">
                      <Input
                        :id="`${site.key}FolderTemplateInput`"
                        v-model="
                          settingsDraft.source_templates[site.key]
                            .folder_template
                        "
                        input-class="font-mono"
                      />
                    </div>
                  </div>
                </template>

                <!-- Filename template -->
                <template v-else>
                  <div
                    v-for="site in sourceProfiles"
                    :key="site.key"
                    class="settings-row"
                  >
                    <span class="settings-row-label">{{ site.label }}</span>
                    <div class="settings-row-control">
                      <Input
                        :id="`${site.key}FilenameTemplateInput`"
                        v-model="
                          settingsDraft.source_templates[site.key]
                            .filename_template
                        "
                        input-class="font-mono"
                      />
                    </div>
                  </div>
                </template>
              </TabsContent>
            </div>
          </div>
        </TabsRoot>
  </Dialog>
</template>

<style scoped>
.settings-row {
  display: flex;
  flex-direction: column;
  gap: 0.15rem;
  padding: 0.375rem 0;
  border-bottom: 1px solid var(--glass-border);
}

.settings-row:last-child {
  border-bottom: 0;
}

.settings-row-label {
  font-size: 0.875rem;
  font-weight: 500;
  color: inherit;
}

.settings-row-control {
  width: 100%;
}

@media (min-width: 640px) {
  .settings-row {
    flex-direction: row;
    align-items: center;
    gap: 0.75rem;
  }

  .settings-row-label {
    flex: 0 0 120px;
  }

  .settings-row-control {
    flex: 1 1 auto;
    width: 100%;
  }
}
</style>
