<script setup lang="ts">
import { computed, nextTick, reactive, ref, watch } from "vue";
import type { Component } from "vue";
import IconAccount from "~icons/material-symbols/admin-panel-settings";
import IconClose from "~icons/material-symbols/close";
import IconCookie from "~icons/material-symbols/cookie";
import IconDescription from "~icons/material-symbols/description";
import IconFolder from "~icons/material-symbols/folder";
import IconLogout from "~icons/material-symbols/logout";
import IconRuleFolder from "~icons/material-symbols/rule-folder";
import IconSave from "~icons/material-symbols/save";
import IconSearch from "~icons/material-symbols/search";
import IconTrash from "~icons/material-symbols/delete";
import IconQuality from "~icons/material-symbols/high-quality";
import IconUpload from "~icons/material-symbols/upload";
import IconUndo from "~icons/material-symbols/undo";
import { TabsContent, TabsList, TabsRoot, TabsTrigger } from "reka-ui";
import { toast } from "vue-sonner";

import { Button } from "../../components/ui/button";
import { Combobox } from "../../components/ui/combobox";
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
import { useAuth } from "../../composables/useAuth";
import type {
  CookiesMap,
  QualitySelection,
  RuntimeSettings,
  SavedSettings,
  SettingsSection,
  SourceProfile,
} from "../../types";
import {
  createTemplateSettings,
  errorMessage,
  isCodecAllowed,
  isLosslessAudioFormat,
  mergeSourceProfiles,
  resolveCodec,
  settingsManagedSourceProfiles,
} from "../../utils/dashboard";

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

const SECTION_ICONS: Record<SettingsSection, Component> = {
  account: IconAccount,
  downloads: IconFolder,
  cookies: IconCookie,
  quality: IconQuality,
  "folder-template": IconRuleFolder,
  "filename-template": IconDescription,
};

const SECTION_GROUPS: Array<{ label: string; keys: SettingsSection[] }> = [
  { label: "Settings", keys: ["account", "downloads", "cookies", "quality"] },
  { label: "Templates", keys: ["folder-template", "filename-template"] },
];

const auth = useAuth();
const cookieFiles = reactive<Record<string, File | null>>({});
const newCookie = reactive<{ source: string; file: File | null }>({
  source: "",
  file: null,
});
const credentials = reactive({
  username: "",
  current_password: "",
  new_password: "",
  confirm_password: "",
});
const credentialSaving = ref(false);
const sectionSearch = ref("");

const isAccountChanged = computed(() => {
  const originalUsername = auth.username.value || props.settings.auth.username || "";
  return credentials.username !== originalUsername || credentials.new_password.length > 0;
});

// Grey out codecs the chosen container can't hold.
const codecItems = computed(() =>
  props.settings.quality_options.video_codecs.map((codec) => ({
    ...codec,
    disabled: !isCodecAllowed(
      codec.key,
      props.settingsDraft.default_quality.video_container,
      props.settings.quality_options.video_containers,
    ),
  })),
);

// Mutate the draft in place; a container switch may invalidate the codec -> Auto.
function setDefaultQuality(patch: Partial<QualitySelection>): void {
  const quality = props.settingsDraft.default_quality;
  Object.assign(quality, patch);
  quality.video_codec = resolveCodec(
    quality.video_codec,
    quality.video_container,
    props.settings.quality_options.video_containers,
  );
}

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
const editableSourceProfiles = computed<SourceProfile[]>(() =>
  settingsManagedSourceProfiles(sourceProfiles.value),
);

watch(
  editableSourceProfiles,
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
  const firstEditableSource = editableSourceProfiles.value[0]?.key || "settings";
  const focusTargets: Record<SettingsSection, string> = {
    account: "accountUsernameInput",
    downloads: `${firstEditableSource}LocationInput`,
    cookies: `${firstEditableSource}CookiesInput`,
    quality: "defaultQualityMode",
    "folder-template": `${firstEditableSource}FolderTemplateInput`,
    "filename-template": `${firstEditableSource}FilenameTemplateInput`,
  };
  void nextTick(() => document.getElementById(focusTargets[section])?.focus());
}

function resetCredentials(): void {
  credentials.username =
    auth.username.value || props.settings.auth.username || credentials.username;
  credentials.current_password = "";
  credentials.new_password = "";
  credentials.confirm_password = "";
}

async function saveCredentials(): Promise<void> {
  if (!credentials.current_password) {
    toast.error("Enter your current password.");
    return;
  }
  if (credentials.new_password !== credentials.confirm_password) {
    toast.error("New passwords do not match.");
    return;
  }
  credentialSaving.value = true;
  try {
    await auth.updateCredentials({
      username: credentials.username,
      current_password: credentials.current_password,
      new_password: credentials.new_password || "",
    });
    credentials.username = auth.username.value || credentials.username;
    credentials.current_password = "";
    credentials.new_password = "";
    credentials.confirm_password = "";
    toast.success("Account saved.");
  } catch (error) {
    toast.error(errorMessage(error, "Could not save account."));
  } finally {
    credentialSaving.value = false;
  }
}

async function signOut(): Promise<void> {
  await auth.logout();
  openModel.value = false;
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

watch(
  () => props.open,
  (open) => {
    if (open) resetCredentials();
  },
  { immediate: true },
);

watch(
  () => [props.settings.auth.username, auth.username.value],
  () => {
    if (!credentials.username) resetCredentials();
  },
);
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
      <!-- Sidebar -->
      <Sidebar
        class="relative p-3 sm:h-full sm:w-60 sm:p-4 gap-3 flex flex-col"
      >
        <!-- Mobile Header -->
        <div class="flex sm:hidden items-center justify-between px-1">
          <div class="flex items-center gap-2">
            <span class="text-lg font-bold ml-1">Settings</span>
            <template v-if="hasUnsavedChanges">
              <Button variant="soft" size="sm" @click="emit('clear')" title="Clear changes" aria-label="Clear changes">
                <template #icon><IconUndo class="w-4 h-4" aria-hidden="true" /></template>
              </Button>
              <Button variant="primary" size="sm" @click="emit('save')" title="Save changes" aria-label="Save changes">
                <template #icon><IconSave class="w-4 h-4" aria-hidden="true" /></template>
              </Button>
            </template>
          </div>
          <button
            type="button"
            @click="openModel = false"
            class="inline-flex h-9 w-9 items-center justify-center rounded-lg bg-transparent text-white/70 in-[.light-mode]:text-black/70 hover:text-white in-[.light-mode]:hover:text-black active:scale-[0.96] transition-all"
            aria-label="Close"
          >
            <IconClose class="h-6 w-6" aria-hidden="true" />
          </button>
        </div>

        <SidebarHeader class="hidden sm:flex">
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
            <SidebarGroup v-for="group in sectionGroups" :key="group.label" class="flex-row sm:flex-col w-auto sm:w-full gap-1">
              <SidebarGroupLabel class="hidden sm:block sm:not-first:mt-3">
                {{ group.label }}
              </SidebarGroupLabel>
              <SidebarMenu class="flex-row sm:flex-col gap-1 w-auto sm:w-full">
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
                    <SidebarMenuButton as="div" class="cursor-pointer w-auto sm:w-full justify-center sm:justify-start px-3.5 sm:px-3.5 h-8 sm:h-10">
                      <component
                        :is="SECTION_ICONS[item.key]"
                        class="hidden sm:block h-5 w-5 shrink-0 opacity-80 group-data-[state=active]:opacity-100"
                        aria-hidden="true"
                      />
                      <span class="whitespace-nowrap font-medium text-sm">{{
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
      <div class="relative min-h-0 min-w-0 flex-1 flex flex-col">
        <!-- Desktop Header & Close Button -->
        <div class="hidden sm:flex h-14 shrink-0 items-center justify-between px-4 gap-2 border-0 border-b border-(--glass-border) mb-4 sm:mb-0 sm:border-0">
          <div class="flex items-center gap-2">
            <template v-if="hasUnsavedChanges">
              <Button variant="soft" @click="emit('clear')">
                <template #icon><IconUndo class="w-5 h-5" aria-hidden="true" /></template>
                Clear
              </Button>
              <Button variant="primary" @click="emit('save')">
                <template #icon><IconSave class="w-5 h-5" aria-hidden="true" /></template>
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
            v-for="item in SETTINGS_SECTIONS"
            :key="item.key"
            :value="item.key"
            class="pr-10 focus:outline-none"
          >
            <p
              v-if="
                editableSourceProfiles.length === 0 &&
                item.key !== 'account' &&
                item.key !== 'cookies' &&
                item.key !== 'quality'
              "
              class="rounded-lg glass p-4 text-white in-[.light-mode]:text-black"
            >
              No sources yet.
            </p>

            <!-- Account -->
            <template v-if="item.key === 'account'">
              <form class="flex flex-col" @submit.prevent="saveCredentials">
                <div class="settings-row">
                  <span class="settings-row-label">Username</span>
                  <div class="settings-row-control">
                    <Input
                      id="accountUsernameInput"
                      v-model="credentials.username"
                      size="lg"
                      type="text"
                      autocomplete="username"
                      placeholder="Username"
                    />
                  </div>
                </div>
                <div class="settings-row">
                  <span class="settings-row-label">Current Password</span>
                  <div class="settings-row-control">
                    <Input
                      v-model="credentials.current_password"
                      size="lg"
                      type="password"
                      autocomplete="current-password"
                      placeholder="Current password"
                    />
                  </div>
                </div>
                <div class="settings-row">
                  <span class="settings-row-label">New Password</span>
                  <div class="settings-row-control">
                    <Input
                      v-model="credentials.new_password"
                      size="lg"
                      type="password"
                      autocomplete="new-password"
                      placeholder="New password"
                    />
                  </div>
                </div>
                <div class="settings-row">
                  <span class="settings-row-label">Confirm Password</span>
                  <div class="settings-row-control">
                    <Input
                      v-model="credentials.confirm_password"
                      size="lg"
                      type="password"
                      autocomplete="new-password"
                      placeholder="Confirm password"
                    />
                  </div>
                </div>

                <div class="mt-4 flex flex-wrap gap-2">
                  <Button
                    v-if="isAccountChanged"
                    variant="primary"
                    size="lg"
                    type="submit"
                    :disabled="credentialSaving || auth.loading.value"
                  >
                    <template #icon>
                      <IconSave class="h-5 w-5" aria-hidden="true" />
                    </template>
                    {{ credentialSaving ? "Saving..." : "Save" }}
                  </Button>
                </div>
              </form>
            </template>

            <!-- Locations -->
            <template v-else-if="item.key === 'downloads'">
              <div class="flex flex-col">
                <div
                  v-for="site in editableSourceProfiles"
                  :key="site.key"
                  class="settings-row"
                >
                  <span class="settings-row-label">{{ site.label }}</span>
                  <div class="settings-row-control">
                    <Input
                      size="lg"
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
                v-if="editableSourceProfiles.length === 0"
                class="mt-3 rounded-lg glass p-4 text-white in-[.light-mode]:text-black"
              >
                No sources yet.
              </p>

              <div
                v-for="site in editableSourceProfiles"
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
                            cookieStatuses[site.key].filename || "cookies.txt"
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

            <!-- Quality -->
            <template v-else-if="item.key === 'quality'">
              <h3
                class="mb-2 text-xs font-bold text-white/50 in-[.light-mode]:text-black/50 uppercase tracking-wider"
              >
                Video
              </h3>

              <div class="settings-row">
                <span class="settings-row-label">Quality</span>
                <div class="settings-row-control">
                  <Combobox
                    :model-value="settingsDraft.default_quality.video_quality"
                    :items="settings.quality_options.video"
                    @update:model-value="
                      (val) =>
                        (settingsDraft.default_quality.video_quality = val)
                    "
                    size="lg"
                    layout="fill"
                    placeholder="Choose a quality"
                    empty-text="No presets."
                  />
                </div>
              </div>
              <div class="settings-row">
                <span class="settings-row-label">Container</span>
                <div class="settings-row-control">
                  <Combobox
                    :model-value="settingsDraft.default_quality.video_container"
                    :items="settings.quality_options.video_containers"
                    @update:model-value="
                      (val) => setDefaultQuality({ video_container: val })
                    "
                    size="lg"
                    layout="fill"
                    placeholder="Choose a container"
                    empty-text="No containers."
                  />
                </div>
              </div>
              <div class="settings-row">
                <span class="settings-row-label">Codec</span>
                <div class="settings-row-control">
                  <Combobox
                    :model-value="settingsDraft.default_quality.video_codec"
                    :items="codecItems"
                    @update:model-value="
                      (val) => setDefaultQuality({ video_codec: val })
                    "
                    size="lg"
                    layout="fill"
                    placeholder="Choose a codec"
                    empty-text="No codecs."
                  />
                </div>
              </div>

              <h3
                class="mb-2 mt-6 text-xs font-bold text-white/50 in-[.light-mode]:text-black/50 uppercase tracking-wider"
              >
                Audio
              </h3>

              <div class="settings-row">
                <span class="settings-row-label">Format</span>
                <div class="settings-row-control">
                  <Combobox
                    :model-value="settingsDraft.default_quality.audio_format"
                    :items="settings.quality_options.audio_formats"
                    @update:model-value="
                      (val) =>
                        (settingsDraft.default_quality.audio_format = val)
                    "
                    size="lg"
                    layout="fill"
                    placeholder="Choose a format"
                    empty-text="No formats."
                  />
                </div>
              </div>
              <div
                v-if="
                  !isLosslessAudioFormat(
                    settingsDraft.default_quality.audio_format,
                  )
                "
                class="settings-row"
              >
                <span class="settings-row-label">Bitrate</span>
                <div class="settings-row-control">
                  <Combobox
                    :model-value="settingsDraft.default_quality.audio_bitrate"
                    :items="settings.quality_options.audio_bitrates"
                    @update:model-value="
                      (val) =>
                        (settingsDraft.default_quality.audio_bitrate = val)
                    "
                    size="lg"
                    layout="fill"
                    placeholder="Choose a bitrate"
                    empty-text="No bitrates."
                  />
                </div>
              </div>
            </template>

            <!-- Folder template -->
            <template v-else-if="item.key === 'folder-template'">
              <div
                v-for="site in editableSourceProfiles"
                :key="site.key"
                class="settings-row"
              >
                <span class="settings-row-label">{{ site.label }}</span>
                <div class="settings-row-control">
                  <Input
                    size="lg"
                    :id="`${site.key}FolderTemplateInput`"
                    v-model="
                      settingsDraft.source_templates[site.key].folder_template
                    "
                    input-class="font-mono"
                  />
                </div>
              </div>
            </template>

            <!-- Filename template -->
            <template v-else>
              <div
                v-for="site in editableSourceProfiles"
                :key="site.key"
                class="settings-row"
              >
                <span class="settings-row-label">{{ site.label }}</span>
                <div class="settings-row-control">
                  <Input
                    size="lg"
                    :id="`${site.key}FilenameTemplateInput`"
                    v-model="
                      settingsDraft.source_templates[site.key].filename_template
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
  gap: 0.5rem;
  padding: 0.5rem 0;
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
