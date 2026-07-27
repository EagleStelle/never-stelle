<script setup lang="ts">
import { computed, nextTick, ref } from "vue";
import IconClose from "~icons/material-symbols/close";
import { TabsContent, TabsRoot } from "reka-ui";

import { DialogShell as Dialog } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import type {
  CookiesMap,
  LearnedFormats,
  RuntimeSettings,
  SettingsDraft,
  SettingsSection,
  SourceProfile,
  ProbeFieldsResponse,
} from "@/types";
import { useSettingsDraft } from "@/features/settings/composables/useSettingsDraft";
import { provideSettingsContext } from "@/features/settings/context";
import { Card } from "@/components/ui/card";
import { SETTINGS_SECTION_DEFS } from "@/features/settings/sections";
import SettingsSidebar from "@/features/settings/Sidebar.vue";

const props = defineProps<{
  cookieStatuses: CookiesMap;
  open: boolean;
  section: SettingsSection;
  settings: RuntimeSettings;
  settingsDraft: SettingsDraft;
  learnedFormatsDraft: LearnedFormats;
  sourceProfiles: SourceProfile[];
  learnFormat: (url: string) => Promise<string>;
  probeFields: (
    url: string,
    sourceKey?: string,
  ) => Promise<ProbeFieldsResponse>;
  reorderFormatTemplates: (
    sourceKey: string,
    templates: string[],
  ) => Promise<void>;
  reorderCookies: (sourceKey: string, cookieIds: string[]) => Promise<void>;
  hasUnsavedChanges: boolean;
  markSettingsDraftDirty: (section?: SettingsSection) => void;
  saveSettingsDraft: () => Promise<void>;
  copySettingsToDraft: () => void;
}>();

const emit = defineEmits<{
  connectCookies: [platform: string, file?: File];
  removeCookies: [platform: string, cookieId: string];
  "update:open": [open: boolean];
  "update:section": [section: SettingsSection];
}>();

const confirmCloseOpen = ref(false);
const saving = ref(false);

const openModel = computed({
  get: () => props.open,
  set: (value) => {
    if (!value) {
      if (props.hasUnsavedChanges) {
        confirmCloseOpen.value = true;
      } else {
        emit("update:open", false);
      }
    } else {
      emit("update:open", true);
    }
  },
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
  learnedFormatsDraft: props.learnedFormatsDraft,
  cookieStatuses: computed(() => props.cookieStatuses),
  editableSourceProfiles,
  connectCookies: (platform, file) => emit("connectCookies", platform, file),
  removeCookies: (platform, cookieId) => emit("removeCookies", platform, cookieId),
  learnFormat: (url) => props.learnFormat(url),
  probeFields: (url, sourceKey) =>
    props.probeFields(url, sourceKey),
  reorderFormatTemplates: (sourceKey, templates) =>
    props.reorderFormatTemplates(sourceKey, templates),
  reorderCookies: (sourceKey, cookieIds) => props.reorderCookies(sourceKey, cookieIds),
  markSettingsDraftDirty: (section) =>
    props.markSettingsDraftDirty(section || props.section),
  saveSettingsDraft: () => props.saveSettingsDraft(),
  copySettingsToDraft: () => props.copySettingsToDraft(),
  close: () => {
    if (props.hasUnsavedChanges) {
      confirmCloseOpen.value = true;
    } else {
      emit("update:open", false);
    }
  },
});

// Switch pane, then focus its primary control on the next tick.
function selectSection(section: SettingsSection): void {
  sectionModel.value = section;
  const firstSource = editableSourceProfiles.value[0]?.key || "settings";
  const id = SETTINGS_SECTION_DEFS.find(
    (def) => def.key === section,
  )?.focusId?.(firstSource);
  if (id) void nextTick(() => document.getElementById(id)?.focus());
}

function cancelClose() {
  confirmCloseOpen.value = false;
}

function discardAndClose() {
  confirmCloseOpen.value = false;
  props.copySettingsToDraft();
  emit("update:open", false);
}

async function saveAndClose() {
  if (saving.value) return;
  saving.value = true;
  try {
    await props.saveSettingsDraft();
    confirmCloseOpen.value = false;
    emit("update:open", false);
  } catch (err) {
    // Keep dialog open if save fails
  } finally {
    saving.value = false;
  }
}

async function saveChanges() {
  if (saving.value) return;
  saving.value = true;
  try {
    await props.saveSettingsDraft();
  } catch (err) {
  } finally {
    saving.value = false;
  }
}

function discardChanges() {
  props.copySettingsToDraft();
}

function markActivePaneDirty(event: Event): void {
  if (!event.isTrusted) return;
  const target = event.target;
  if (target instanceof Element && target.closest("[data-settings-system]")) {
    return;
  }
  props.markSettingsDraftDirty(props.section);
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
    content-class="settings-content fixed inset-0 z-70 flex w-full h-full max-w-none translate-x-0 translate-y-0 flex-col overflow-hidden rounded-none border-0 bg-primary focus:outline-none sm:left-1/2 sm:top-1/2 sm:right-auto sm:bottom-auto sm:w-[min(980px,96vw)] sm:h-[min(700px,92vh)] sm:-translate-x-1/2 sm:-translate-y-1/2 sm:rounded-2xl sm:border sm:border-(--glass-border) sm:flex-row"
  >
    <TabsRoot
      v-model="sectionModel"
      class="flex min-h-0 flex-1 flex-col sm:flex-row"
      orientation="vertical"
    >
      <SettingsSidebar @close="openModel = false" @select="selectSection" />

      <!-- Content -->
      <!-- `sm:gap-4` separates the desktop header from the panes. The header is
           `display:none` on mobile, so it contributes no gap there. -->
      <div class="relative min-h-0 min-w-0 flex-1 flex flex-col sm:gap-4">
        <!-- Desktop Header & Close Button -->
        <div
          class="hidden sm:flex h-14 shrink-0 items-center justify-between px-4 gap-2 border-0 border-b border-(--glass-border) sm:border-0"
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

        <!-- Scrollable Settings. The vertical inset is not spacing: `overflow-y-auto`
             clips at its own edge and a focus ring draws outside its element, so a first
             or last row sitting flush would lose its ring. Rows are spaced by group gaps. -->
        <div
          class="flex-1 overflow-y-auto px-5 pb-6 pt-4 sm:pt-1 sm:px-8"
          @input.capture="markActivePaneDirty"
          @change.capture="markActivePaneDirty"
        >
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
              <p class="text-[0.8125rem] text-muted-foreground">
                No sources yet.
              </p>
            </Card>
            <component :is="def.component" />
          </TabsContent>
        </div>

        <!-- Save / Discard Footer -->
        <div
          class="shrink-0 flex justify-end px-5 py-4 border-0 border-t border-(--glass-border) bg-primary/45 backdrop-blur-md sm:px-6"
        >
          <div class="flex items-center justify-end gap-3">
            <Button
              variant="ghost"
              :disabled="saving || !hasUnsavedChanges"
              @click="discardChanges"
            >
              Discard
            </Button>
            <Button
              variant="primary"
              :disabled="saving || !hasUnsavedChanges"
              @click="saveChanges"
            >
              {{ saving ? "Saving..." : "Save" }}
            </Button>
          </div>
        </div>
      </div>
    </TabsRoot>

    <!-- Confirmation Dialog Overlay -->
    <div
      v-if="confirmCloseOpen"
      class="absolute inset-0 z-80 flex items-center justify-center bg-black/60 backdrop-blur-md transition-all duration-300 rounded-none sm:rounded-2xl"
    >
      <div
        class="glass-chrome flex w-[min(400px,90vw)] flex-col gap-4 rounded-xl border border-(--glass-border) bg-primary p-6 shadow-2xl"
      >
        <h3
          class="text-base font-semibold text-white in-[.light-mode]:text-black"
        >
          Unsaved Changes
        </h3>
        <p class="text-sm text-white/70 in-[.light-mode]:text-black/70">
          You have unsaved changes. Do you want to save them before closing?
        </p>
        <div
          class="flex flex-col gap-2 sm:flex-row sm:justify-end sm:gap-3 mt-2"
        >
          <Button
            variant="ghost"
            class="sm:order-1"
            :disabled="saving"
            @click="cancelClose"
          >
            Cancel
          </Button>
          <Button
            variant="ghost"
            class="sm:order-2"
            :disabled="saving"
            @click="discardAndClose"
          >
            Discard
          </Button>
          <Button
            variant="primary"
            class="sm:order-3"
            :disabled="saving"
            @click="saveAndClose"
          >
            {{ saving ? "Saving..." : "Save" }}
          </Button>
        </div>
      </div>
    </div>
  </Dialog>
</template>
