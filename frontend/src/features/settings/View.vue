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
import IconScraper from "~icons/material-symbols/travel-explore";
import IconAdd from "~icons/material-symbols/add";
import { TabsContent, TabsList, TabsRoot, TabsTrigger } from "reka-ui";
import { toast } from "vue-sonner";

import { Button } from "../../components/ui/button";
import { Checkbox } from "../../components/ui/checkbox";
import { Combobox } from "../../components/ui/combobox";
import { Dialog } from "../../components/ui/dialog";
import { Input } from "../../components/ui/input";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "../../components/ui/segmented-control";
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
  PlatformScrapeRules,
  QualitySelection,
  RuntimeSettings,
  SavedSettings,
  ScrapeTestResult,
  SettingsSection,
  SourceProfile,
} from "../../types";
import { FALLBACK_SOURCE_KEY } from "../../types";
import { testScrapeRules } from "../../api";
import {
  createPlatformScrapeRules,
  createScrapeRule,
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
  scraper: IconScraper,
};

const SECTION_GROUPS: Array<{ label: string; keys: SettingsSection[] }> = [
  { label: "Settings", keys: ["account", "downloads", "cookies", "quality"] },
  {
    label: "Templates",
    keys: ["folder-template", "filename-template", "scraper"],
  },
];

const SCRAPE_ATTR_ITEMS = [
  { key: "text", label: "Text" },
  { key: "href", label: "Link (href)" },
  { key: "src", label: "src" },
  { key: "alt", label: "alt" },
  { key: "title", label: "title" },
];

// Built in script so the literal braces never reach the template tokenizer.
const ARTIST_TOKEN_EXAMPLE = "{{artist}}";
function tokenLabel(token: string): string {
  return `{{${token}}}`;
}

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
  const originalUsername =
    auth.username.value || props.settings.auth.username || "";
  return (
    credentials.username !== originalUsername ||
    credentials.new_password.length > 0
  );
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
  [...settingsManagedSourceProfiles(sourceProfiles.value)].sort((a, b) => {
    if (a.key === FALLBACK_SOURCE_KEY) return 1;
    if (b.key === FALLBACK_SOURCE_KEY) return -1;
    return a.label.localeCompare(b.label);
  }),
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
      if (!props.settingsDraft.source_scrape_rules[profile.key]) {
        props.settingsDraft.source_scrape_rules[profile.key] =
          createPlatformScrapeRules();
      }
      if (!scrapeTests[profile.key]) {
        scrapeTests[profile.key] = {
          url: "",
          loading: false,
          results: [],
          message: "",
        };
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
  const firstEditableSource =
    editableSourceProfiles.value[0]?.key || "settings";
  const focusTargets: Record<SettingsSection, string> = {
    account: "accountUsernameInput",
    downloads: `${firstEditableSource}LocationInput`,
    cookies: `${firstEditableSource}CookiesInput`,
    quality: "defaultQualityMode",
    "folder-template": `${firstEditableSource}FolderTemplateInput`,
    "filename-template": `${firstEditableSource}FilenameTemplateInput`,
    scraper: `${firstEditableSource}ScraperEnable`,
  };
  void nextTick(() => document.getElementById(focusTargets[section])?.focus());
}

interface ScrapeTestState {
  url: string;
  loading: boolean;
  results: ScrapeTestResult[];
  message: string;
}
const scrapeTests = reactive<Record<string, ScrapeTestState>>({});

function platformRules(key: string): PlatformScrapeRules {
  return props.settingsDraft.source_scrape_rules[key];
}

function addScrapeRule(key: string): void {
  platformRules(key).rules.push(createScrapeRule());
}

function removeScrapeRule(key: string, index: number): void {
  platformRules(key).rules.splice(index, 1);
}

async function runScrapeTest(key: string): Promise<void> {
  const state = scrapeTests[key];
  const url = state.url.trim();
  if (!url) {
    toast.error("Paste a sample URL to test.");
    return;
  }
  state.loading = true;
  state.message = "";
  try {
    const response = await testScrapeRules(url, key, platformRules(key).rules);
    state.results = response.results;
    state.message = response.fetched
      ? response.results.length
        ? ""
        : "Add a rule to test."
      : response.detail || "Could not fetch that page.";
  } catch (error) {
    state.results = [];
    state.message = errorMessage(error, "Could not test scrape rules.");
  } finally {
    state.loading = false;
  }
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
              <Button
                variant="soft"
 
                @click="emit('clear')"
                title="Clear changes"
                aria-label="Clear changes"
              >
                <template #icon
                  ><IconUndo class="w-4 h-4" aria-hidden="true"
                /></template>
              </Button>
              <Button
                variant="primary"
 
                @click="emit('save')"
                title="Save changes"
                aria-label="Save changes"
              >
                <template #icon
                  ><IconSave class="w-4 h-4" aria-hidden="true"
                /></template>
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
            <SidebarGroup
              v-for="group in sectionGroups"
              :key="group.label"
              class="flex-row sm:flex-col w-auto sm:w-full gap-1"
            >
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
                    <SidebarMenuButton
                      as="div"
                      class="cursor-pointer w-auto sm:w-full justify-center sm:justify-start px-3.5 sm:px-3.5 h-8 sm:h-10"
                    >
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
        <div
          class="hidden sm:flex h-14 shrink-0 items-center justify-between px-4 gap-2 border-0 border-b border-(--glass-border) mb-4 sm:mb-0 sm:border-0"
        >
          <div class="flex items-center gap-2">
            <template v-if="hasUnsavedChanges">
              <Button variant="soft" @click="emit('clear')">
                <template #icon
                  ><IconUndo class="w-5 h-5" aria-hidden="true"
                /></template>
                Clear
              </Button>
              <Button variant="primary" @click="emit('save')">
                <template #icon
                  ><IconSave class="w-5 h-5" aria-hidden="true"
                /></template>
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
            class="focus:outline-none"
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
            <template v-else-if="item.key === 'filename-template'">
              <div
                v-for="site in editableSourceProfiles"
                :key="site.key"
                class="settings-row"
              >
                <span class="settings-row-label">{{ site.label }}</span>
                <div class="settings-row-control">
                  <Input
 
                    :id="`${site.key}FilenameTemplateInput`"
                    v-model="
                      settingsDraft.source_templates[site.key].filename_template
                    "
                    input-class="font-mono"
                  />
                </div>
              </div>
            </template>

            <!-- Scraper -->
            <template v-else>
              <div
                v-for="site in editableSourceProfiles"
                :key="site.key"
                class="scraper-card"
              >
                <div class="flex items-center justify-between gap-3 mb-2">
                  <span class="settings-row-label">{{ site.label }}</span>
                  <div class="flex items-center gap-4">
                    <Button
                      v-if="settingsDraft.source_scrape_rules[site.key].enabled"
                      variant="soft"
                      type="button"
                      @click="addScrapeRule(site.key)"
                    >
                      <template #icon>
                        <IconAdd class="w-4 h-4" aria-hidden="true" />
                      </template>
                      Add rule
                    </Button>
                    <SegmentedControl
                      :model-value="settingsDraft.source_scrape_rules[site.key].enabled ? 'enabled' : 'disabled'"
                      @update:model-value="(val: string) => settingsDraft.source_scrape_rules[site.key].enabled = val === 'enabled'"
                    >
                      <SegmentedControlItem value="enabled">Enabled</SegmentedControlItem>
                      <SegmentedControlItem value="disabled">Disabled</SegmentedControlItem>
                    </SegmentedControl>
                  </div>
                </div>

                <template
                  v-if="settingsDraft.source_scrape_rules[site.key].enabled"
                >
                  <p
                    v-if="
                      !settingsDraft.source_scrape_rules[site.key].rules.length
                    "
                    class="text-xs text-white/50 in-[.light-mode]:text-black/50"
                  >
                    No rules yet.
                  </p>

                  <div
                    v-for="(rule, index) in settingsDraft.source_scrape_rules[
                      site.key
                    ].rules"
                    :key="index"
                    class="scraper-rule p-3 mb-3 rounded-xl bg-black/10 in-[.light-mode]:bg-white/40 border border-(--glass-border)"
                  >
                    <div class="scraper-rule-grid">
                      <Input
                        v-model="rule.token"
                        placeholder="token"
                        aria-label="Token name"
                        input-class="font-mono"
                      />
                      <Input
                        v-model="rule.match_label"
                        placeholder="label (optional)"
                        aria-label="Label to anchor on"
                      />
                      <Input
                        v-model="rule.selector"
                        placeholder="CSS selector"
                        aria-label="CSS selector"
                        input-class="font-mono"
                      />
                      <Combobox
                        :model-value="rule.attr"
                        :items="SCRAPE_ATTR_ITEMS"
                        @update:model-value="(val) => (rule.attr = val)"
                        layout="fill"
                        placeholder="Attribute"
                        empty-text="—"
                      />
                    </div>
                    <div class="scraper-rule-actions mt-2">
                      <label class="flex items-center gap-2 cursor-pointer select-none text-sm shrink-0">
                        <Checkbox
                          :checked="rule.multi"
                          @update:checked="(v: boolean) => rule.multi = v"
                        />
                        <span>Multiple</span>
                      </label>
                      <Input
                        v-model="rule.xpath"
                        placeholder="XPath (optional, overrides selector)"
                        aria-label="XPath"
                        input-class="font-mono"
                        class="flex-1"
                      />
                      <Button
                        variant="soft"
                        type="button"
                        title="Remove rule"
                        aria-label="Remove rule"
                        @click="removeScrapeRule(site.key, index)"
                      >
                        <template #icon>
                          <IconTrash class="w-4 h-4" aria-hidden="true" />
                        </template>
                      </Button>
                    </div>
                  </div>

                  <div class="scraper-test">
                    <Input
                      v-model="scrapeTests[site.key].url"
                      type="text"
                      inputmode="url"
                      placeholder="Sample URL to test"
                      class="flex-1"
                    />
                    <Button
                      variant="primary"
                      type="button"
                      class="shrink-0"
                      :disabled="scrapeTests[site.key].loading"
                      @click="runScrapeTest(site.key)"
                    >
                      <template #icon>
                        <IconSearch class="w-4 h-4" aria-hidden="true" />
                      </template>
                      {{
                        scrapeTests[site.key].loading ? "Testing..." : "Test"
                      }}
                    </Button>
                  </div>

                  <p
                    v-if="scrapeTests[site.key].message"
                    class="text-xs text-white/60 in-[.light-mode]:text-black/60"
                  >
                    {{ scrapeTests[site.key].message }}
                  </p>
                  <ul
                    v-if="scrapeTests[site.key].results.length"
                    class="scraper-results"
                  >
                    <li
                      v-for="result in scrapeTests[site.key].results"
                      :key="result.token"
                    >
                      <span class="font-mono shrink-0">{{
                        tokenLabel(result.token)
                      }}</span>
                      <span v-if="result.matched" class="scraper-value">{{
                        result.value
                      }}</span>
                      <span v-else class="scraper-nomatch">no match</span>
                    </li>
                  </ul>
                </template>
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

.scraper-card {
  display: flex;
  flex-direction: column;
  gap: 0.65rem;
  padding: 0.85rem;
  margin-bottom: 0.75rem;
  border: 1px solid var(--glass-border);
  border-radius: 0.75rem;
}

.scraper-toggle,
.scraper-checkbox {
  display: inline-flex;
  align-items: center;
  gap: 0.4rem;
  font-size: 0.8125rem;
  cursor: pointer;
  white-space: nowrap;
}

.scraper-toggle input,
.scraper-checkbox input {
  width: 1rem;
  height: 1rem;
  cursor: pointer;
  accent-color: var(--accent, #6366f1);
}

.scraper-rule {
  display: flex;
  flex-direction: column;
}

.scraper-rule-grid {
  display: grid;
  grid-template-columns: 1fr;
  gap: 0.4rem;
}

.scraper-rule-actions {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.scraper-test {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-top: 0.35rem;
}

.scraper-results {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  font-size: 0.8125rem;
}

.scraper-results li {
  display: flex;
  align-items: baseline;
  gap: 0.5rem;
  min-width: 0;
}

.scraper-value {
  min-width: 0;
  overflow-wrap: anywhere;
}

.scraper-nomatch {
  opacity: 0.5;
  font-style: italic;
}

@media (min-width: 640px) {
  .scraper-rule-grid {
    grid-template-columns:
      minmax(0, 0.9fr) minmax(0, 1fr) minmax(0, 1.2fr)
      minmax(0, 0.9fr);
  }
}
</style>
