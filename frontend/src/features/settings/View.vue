<script setup lang="ts">
import { computed, nextTick, ref } from "vue";
import IconClose from "~icons/material-symbols/close";
import IconSave from "~icons/material-symbols/save";
import IconUndo from "~icons/material-symbols/undo";
import { TabsContent, TabsRoot } from "reka-ui";

import { Button } from "../../components/ui/button";
import { Dialog } from "../../components/ui/dialog";
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
  hasUnsavedChanges: boolean;
}>();

const emit = defineEmits<{
  connectCookies: [platform: string, file?: File];
  connectCookiesSource: [source: string, file?: File];
  removeCookies: [platform: string];
  "update:open": [open: boolean];
  "update:section": [section: SettingsSection];
  save: [];
  clear: [];
}>();

const confirmingClose = ref(false);

// Gate every close path (X / Esc / click-outside) on unsaved edits.
const openModel = computed({
  get: () => props.open,
  set: (value) => {
    if (!value && props.hasUnsavedChanges) {
      confirmingClose.value = true;
      return;
    }
    emit("update:open", value);
  },
});

function discardAndClose(): void {
  confirmingClose.value = false;
  emit("update:open", false);
}

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
        :has-unsaved-changes="hasUnsavedChanges"
        @save="emit('save')"
        @clear="emit('clear')"
        @close="openModel = false"
        @select="selectSection"
      />

      <!-- Content -->
      <div class="relative min-h-0 min-w-0 flex-1 flex flex-col">
        <!-- Desktop Header & Close Button -->
        <div
          class="hidden sm:flex h-14 shrink-0 items-center justify-between px-4 gap-2 border-0 border-b border-(--glass-border) mb-4 sm:mb-0 sm:border-0"
        >
          <div class="flex items-center gap-2">
            <template v-if="hasUnsavedChanges">
              <Button variant="soft" @click="emit('clear')">
                <template #icon>
                  <IconUndo class="w-5 h-5" aria-hidden="true" />
                </template>
                Clear
              </Button>
              <Button variant="primary" @click="emit('save')">
                <template #icon>
                  <IconSave class="w-5 h-5" aria-hidden="true" />
                </template>
                Save
              </Button>
            </template>
          </div>
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
            <p
              v-if="def.requiresSources && editableSourceProfiles.length === 0"
              class="rounded-lg glass p-4 text-white in-[.light-mode]:text-black"
            >
              No sources yet.
            </p>
            <component :is="def.component" />
          </TabsContent>
        </div>
      </div>
    </TabsRoot>
  </Dialog>

  <Dialog
    v-model:open="confirmingClose"
    title="Discard changes?"
    description="You have unsaved changes."
    hide-title
    :show-close="false"
    overlay-class="fixed inset-0 z-80 bg-black/60 backdrop-blur-sm"
    content-class="fixed left-1/2 top-1/2 z-90 flex w-[min(400px,92vw)] -translate-x-1/2 -translate-y-1/2 flex-col gap-4 rounded-2xl border border-(--glass-border) bg-primary p-6 focus:outline-none"
  >
    <div class="flex flex-col gap-1">
      <h2 class="text-lg font-semibold">Discard changes?</h2>
      <p class="text-sm text-white/60 in-[.light-mode]:text-black/60">
        You have unsaved changes. Close without saving?
      </p>
    </div>
    <div class="flex justify-end gap-2">
      <Button variant="soft" @click="confirmingClose = false">Cancel</Button>
      <Button variant="primary" @click="discardAndClose">Discard</Button>
    </div>
  </Dialog>
</template>
