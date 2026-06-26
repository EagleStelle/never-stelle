<script setup lang="ts">
import { computed, nextTick, reactive } from"vue";
import IconUpload from"~icons/material-symbols/upload";
import IconTrash from"~icons/material-symbols/delete";
import {
 TabsContent,
 TabsList,
 TabsRoot,
 TabsTrigger,
} from"reka-ui";

import { SETTINGS_SECTIONS, SITE_LABELS } from"../../ui";
import { SITE_KEYS, type CookiesMap, type RuntimeSettings, type SavedSettings, type SettingsSection, type SiteKey } from"../../types";

const props = defineProps<{
 cookieStatuses: CookiesMap;
 section: SettingsSection;
 settings: RuntimeSettings;
 settingsDraft: SavedSettings;
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

function siteLabel(site: SiteKey): string {
 return SITE_LABELS[site] ||"Others";
}

function selectSection(section: SettingsSection): void {
 sectionModel.value = section;
 const focusTargets: Record<SettingsSection, string> = {
 downloads:"youtubeLocationInput",
 instagram:"youtubeCookiesInput",
 advanced:"folderTemplateInput",
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
 <option v-for="location in settings.download_locations" :key="location" :value="location"></option>
 </datalist>

 <TabsRoot v-model="sectionModel" class="grid min-h-0 flex-1 gap-5 md:grid-cols-12" orientation="vertical">
 <TabsList class="flex gap-1.5 overflow-x-auto pb-1 md:col-span-3 md:flex-col md:overflow-x-hidden md:overflow-y-auto md:pb-0" aria-label="Settings sections">
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
 <div class="grid gap-3 sm:grid-cols-2">
 <label v-for="site in SITE_KEYS" :key="site" class="grid min-w-0 gap-2 text-text">
 <span class="text-xs font-medium uppercase tracking-wider text-text-muted">{{ siteLabel(site) }}</span>
 <input
 :id="`${site}LocationInput`"
 v-model="settingsDraft.site_locations[site]"
 list="downloadLocationSuggestions"
 placeholder="Enter a save path"
 />
 </label>
 </div>
 </TabsContent>

 <TabsContent value="instagram" class="min-h-full focus:outline-none">
 <div class="grid gap-3 sm:grid-cols-2">
 <div v-for="site in SITE_KEYS" :key="site" class="grid min-w-0 gap-2 text-text">
 <span class="text-xs font-medium uppercase tracking-wider text-text-muted">{{ siteLabel(site) }}</span>
 <div class="flex items-center gap-2">
 <input
 :id="`${site}CookiesInput`"
 type="file"
 accept=".txt,.cookies,text/plain"
 class="hidden"
 @change="onCookieFile(site, $event)"
 />
 <template v-if="!cookieStatuses[site]?.configured">
 <button
 type="button"
 class="flex flex-1 items-center min-w-0 h-10 rounded-lg glass-soft glass-hoverable px-3 text-sm text-left transition-all duration-300 ease-glass"
 @click="openPicker(site)"
 >
 <span class="truncate" :class="cookieFiles[site] ?'text-text':'text-text-muted'">{{ cookieFiles[site]?.name ||'Choose a cookies file'}}</span>
 </button>
 <button
 type="button"
 class="inline-flex items-center justify-center w-10 h-10 shrink-0 rounded-xl bg-primary text-text-on-primary transition-all duration-300 ease-glass active:scale-[0.96] shadow-[0_8px_22px_-10px_rgba(237,158,89,0.65)] hover:shadow-[0_12px_28px_-8px_rgba(237,158,89,0.85)] hover:brightness-105"
 :title="`Save ${siteLabel(site)} cookies`"
 :aria-label="`Save ${siteLabel(site)} cookies`"
 @click="connect(site)"
 >
 <IconUpload class="w-5 h-5" aria-hidden="true" />
 </button>
 </template>
 <template v-else>
 <div class="flex flex-1 items-center min-w-0 h-10 rounded-lg glass-soft px-3 text-sm">
 <span class="truncate text-text">{{ cookieStatuses[site].filename ||'cookies.txt'}}</span>
 </div>
 <button
 type="button"
 class="inline-flex items-center justify-center w-10 h-10 shrink-0 rounded-xl glass-soft glass-hoverable text-text-muted transition-all duration-300 ease-glass active:scale-[0.96] hover:text-text"
 :title="`Delete ${siteLabel(site)} cookies`"
 :aria-label="`Delete ${siteLabel(site)} cookies`"
 @click="emit('removeCookies', site)"
 >
 <IconTrash class="w-5 h-5" aria-hidden="true" />
 </button>
 </template>
 </div>
 </div>
 </div>
 </TabsContent>

 <TabsContent value="advanced" class="min-h-full focus:outline-none">
 <div class="grid max-w-xl gap-4">
 <label class="grid min-w-0 gap-2 text-text">
 <span class="text-xs font-medium uppercase tracking-wider text-text-muted">Folder Template</span>
 <input id="folderTemplateInput" v-model="settingsDraft.template_settings.folder_template" class="font-mono" />
 </label>
 <label class="grid min-w-0 gap-2 text-text">
 <span class="text-xs font-medium uppercase tracking-wider text-text-muted">Filename Template</span>
 <textarea v-model="settingsDraft.template_settings.filename_template" rows="3" class="font-mono"></textarea>
 </label>
 </div>
 </TabsContent>
 </div>
 </TabsRoot>

 <footer class="shrink-0 mt-6 flex items-center justify-end gap-2 pt-4 border-t border-(--glass-border)">
 <button type="button" class="inline-flex items-center justify-center gap-1.5 min-h-9 px-5 rounded-xl bg-primary text-text-on-primary leading-none transition-all duration-300 ease-glass active:scale-[0.97] shadow-[0_8px_22px_-10px_rgba(237,158,89,0.65)] hover:shadow-[0_12px_28px_-8px_rgba(237,158,89,0.85)] hover:brightness-105" @click="emit('save')">Save Settings</button>
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
