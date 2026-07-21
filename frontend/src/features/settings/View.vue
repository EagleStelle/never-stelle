<script setup lang="ts">
import { computed, nextTick } from "vue";
import IconClose from "~icons/material-symbols/close";
import { TabsContent, TabsRoot } from "reka-ui";

import { Card } from "../../components/ui/card";
import { DialogShell as Dialog } from "../../components/ui/dialog";
import type {
  CookiesMap,
  RuntimeSettings,
  SavedSettings,
  SettingsSection,
  SourceProfile,
} from "../../types";
import { useSettingsDraft } from "./composables/useSettingsDraft";
import { provideSettingsContext } from "./context";
import { SETTINGS_SECTION_DEFS } from "./sections";
import SettingsSidebar from "./Sidebar.vue";

const props = defineProps<{
  cookieStatuses: CookiesMap;
  open: boolean;
  section: SettingsSection;
  settings: RuntimeSettings;
  settingsDraft: SavedSettings;
  sourceProfiles: SourceProfile[];
  learnFormat: (url: string) => Promise<string>;
  reorderFormatTemplates: (sourceKey: string, templates: string[]) => Promise<void>;
}>();

const emit = defineEmits<{
  connectCookies: [platform: string, file?: File];
  connectCookiesSource: [source: string, file?: File];
  removeCookies: [platform: string];
  "update:open": [open: boolean];
  "update:section": [section: SettingsSection];
}>();

// Edits auto-save, so every close path (X / Esc / click-outside) just closes.
const openModel = computed({
  get: () => props.open,
  set: (value) => emit("update:open", value),
});

const sectionModel = computed({
  get: () => props.section,
  set: (value) => emit("update:section", value),
});

const { editableSourceProfiles } = useSettingsDraft(props);

provideSettingsContext({
  open: computed(() => props.open),
  settings: props.settings,
  settingsDraft: props.settingsDraft,
  cookieStatuses: props.cookieStatuses,
  editableSourceProfiles,
  connectCookies: (platform, file) => emit("connectCookies", platform, file),
  connectCookiesSource: (source, file) => emit("connectCookiesSource", source, file),
  removeCookies: (platform) => emit("removeCookies", platform),
  learnFormat: (url) => props.learnFormat(url),
  reorderFormatTemplates: (sourceKey, templates) =>
    props.reorderFormatTemplates(sourceKey, templates),
  close: () => emit("update:open", false),
});

// Switch pane, then focus its primary control on the next tick.
function selectSection(section: SettingsSection): void {
  sectionModel.value = section;
  const firstSource = editableSourceProfiles.value[0]?.key || "settings";
  const id = SETTINGS_SECTION_DEFS.find((def) => def.key === section)?.focusId?.(firstSource);
  if (id) void nextTick(() => document.getElementById(id)?.focus());
}
</script>

<template>
  <Dialog
    v-model:open="openModel"
    title="Settings"
    hide-title
    :show-close="false"
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
      <SettingsSidebar
        @close="openModel = false"
        @select="selectSection"
      />

      <!-- Content -->
      <div class="relative min-h-0 min-w-0 flex-1 flex flex-col">
        <!-- Desktop Header & Close Button -->
        <div
          class="hidden sm:flex h-14 shrink-0 items-center justify-between px-4 gap-2 border-0 border-b border-(--glass-border) mb-4 sm:mb-0 sm:border-0"
        >
          <div></div>
          <button
            type="button"
            @click="openModel = false"
            class="inline-flex h-9 w-9 items-center justify-center rounded-lg bg-transparent text-white/70 in-[.light-mode]:text-black/70 transition-all duration-300 ease-glass hover:text-white in-[.light-mode]:hover:text-black active:scale-[0.96]"
            aria-label="Close"
          >
            <IconClose class="h-6 w-6" aria-hidden="true" />
          </button>
        </div>

        <!-- Scrollable Settings -->
        <div class="flex-1 overflow-y-auto px-5 pb-6 pt-4 sm:pt-0 sm:px-8">
          <TabsContent
            v-for="def in SETTINGS_SECTION_DEFS"
            :key="def.key"
            :value="def.key"
            class="focus:outline-none"
          >
            <Card
              v-if="def.requiresSources && editableSourceProfiles.length === 0"
              class="px-6"
            >
              <p class="text-[0.8125rem] text-white/55 in-[.light-mode]:text-black/55">
                No sources yet.
              </p>
            </Card>
            <component :is="def.component" />
          </TabsContent>
        </div>
      </div>
    </TabsRoot>
  </Dialog>
</template>
