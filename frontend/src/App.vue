<script setup lang="ts">
import { computed, useTemplateRef } from "vue";
import { useElementSize } from "@vueuse/core";

import DownloadInput from "./features/downloads/Input.vue";
import DownloadPanel from "./features/downloads/Panel.vue";
import DownloadFilters from "./features/downloads/Filters.vue";
import PlaylistDialog from "./features/downloads/PlaylistDialog.vue";
import NavSide from "./components/layout/NavSide.vue";
import BarStatus from "./components/layout/BarStatus.vue";
import NavBottom from "./components/layout/NavBottom.vue";
import SettingsView from "./features/settings/View.vue";
import { Toaster } from "./components/ui/sonner";
import { Button } from "./components/ui/button";
import { Combobox } from "./components/ui/combobox";
import { Input } from "./components/ui/input";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "./components/ui/segmented-control";
import IconGrid from "~icons/material-symbols/grid-view";
import IconList from "~icons/material-symbols/list";
import IconRefresh from "~icons/material-symbols/sync";
import IconSearch from "~icons/material-symbols/search";

import { useDownloadDashboard } from "./composables/useDownloadDashboard";

const {
  activeMenu,
  activePage,
  addDownloadTask,
  cancelTask,
  clearPending,
  confirmPlaylistSelection,
  connectCookies,
  connectCookiesForSource,
  cookieStatuses,
  countCards,
  downloadTask,
  historyRefreshing,
  historyLoading,
  historyError,
  historyHasMore,
  loadMoreHistory,
  activeTasks,
  completedTasks,
  isLightMode,
  mediaFilter,
  mediaFilterItems,
  navigationItems,
  openSettings,
  pageItems,
  playlistEntries,
  playlistOpen,
  playlistTitle,
  removeCookies,
  removeTask,
  retryTask,
  refreshHistory,
  savedSettings,
  setActiveMenu,
  setActivePage,
  setMediaFilter,
  setTaskSource,
  setViewMode,
  settings,
  settingsDraft,
  settingsOpen,
  settingsSection,
  sourceProfiles,
  downloadSelection,
  qualityOptions,
  setDownloadQuality,
  tasksErrorMessage,
  tasksLoading,
  toggleThemeMode,
  url,
  viewMode,
} = useDownloadDashboard();

// Live status-bar height so toasts dock above it instead of covering it.
const statusBar = useTemplateRef<InstanceType<typeof BarStatus>>("statusBar");
const statusBarEl = computed(
  () => (statusBar.value?.$el as HTMLElement | null) ?? null,
);
const { height: statusBarHeight } = useElementSize(
  statusBarEl,
  { width: 0, height: 0 },
  { box: "border-box" },
);
</script>

<template>
  <div
    class="w-full max-w-full h-dvh overflow-hidden bg-primary text-white in-[.light-mode]:text-black lg:flex"
  >
    <a
      class="sr-only focus:not-sr-only focus:fixed focus:z-50 focus:top-2 focus:left-2 rounded-lg glass-chrome text-white in-[.light-mode]:text-black px-3 py-2 transition-transform duration-300 ease-glass"
      href="#mainContent"
      >Skip to content</a
    >

    <NavSide
      class="shrink-0"
      :active-page="activePage"
      :is-light-mode="isLightMode"
      :page-items="pageItems"
      :settings-open="settingsOpen"
      @open-settings="openSettings"
      @select-page="setActivePage"
      @toggle-theme="toggleThemeMode"
    />

    <div class="flex-1 min-w-0 flex flex-col h-dvh relative">
      <div class="flex-1 relative min-h-0 overflow-hidden">
        <main
          id="mainContent"
          class="absolute inset-0 overflow-y-auto overflow-x-hidden flex flex-col"
          tabindex="-1"
        >
          <!-- Desktop Header: Sticky Top -->
          <div class="hidden lg:block sticky top-0 z-20 shrink-0 pt-4 pb-2 px-4 glass border-0 border-b border-(--glass-border)">
            <template v-if="activePage === 'downloads'">
              <DownloadInput
                v-model:url="url"
                :saved-settings="savedSettings"
                :quality="downloadSelection"
                :quality-options="qualityOptions"
                @update:quality="setDownloadQuality"
                @add-download="addDownloadTask"
              >
                <template #filters>
                  <DownloadFilters
                    :active-menu="activeMenu"
                    :navigation-items="navigationItems"
                    :media-filter="mediaFilter"
                    :media-filter-items="mediaFilterItems"
                    :view-mode="viewMode"
                    @update:active-menu="setActiveMenu"
                    @update:media-filter="setMediaFilter"
                    @update:view-mode="setViewMode"
                  />
                </template>
              </DownloadInput>
            </template>

            <template v-else-if="activePage === 'history'">
              <div class="flex flex-col-reverse lg:flex-col gap-2 w-full">
                <section aria-label="Search history">
                  <div class="flex items-center gap-2">
                    <Input
                      size="lg"
                      class="flex-1 min-w-0"
                      type="text"
                      placeholder="Search history..."
                    >
                      <template #icon>
                        <IconSearch class="w-5 h-5" aria-hidden="true" />
                      </template>
                    </Input>
                    <Button
                      variant="primary"
                      size="lg"
                      type="button"
                      class="shrink-0"
                      aria-label="Refresh history"
                      title="Refresh history"
                      :disabled="historyRefreshing"
                      @click="refreshHistory"
                    >
                      <template #icon>
                        <IconRefresh
                          aria-hidden="true"
                          class="w-6 h-6 transition-transform duration-300 ease-glass group-hover:rotate-45"
                          :class="{ 'animate-spin': historyRefreshing }"
                        />
                      </template>
                    </Button>
                  </div>
                </section>
                
                <div class="flex overflow-x-auto no-scrollbar items-center justify-between lg:justify-end gap-2 w-full pb-1">
                  <div class="shrink-0 lg:ml-auto">
                    <DownloadFilters
                      :active-menu="activeMenu"
                      :navigation-items="navigationItems"
                      :media-filter="mediaFilter"
                      :media-filter-items="mediaFilterItems"
                      :view-mode="viewMode"
                      @update:active-menu="setActiveMenu"
                      @update:media-filter="setMediaFilter"
                      @update:view-mode="setViewMode"
                    />
                  </div>
                </div>
              </div>
            </template>
          </div>

          <!-- Scrollable Content -->
          <div class="flex-1 flex flex-col p-4">
            <DownloadPanel
              v-if="['downloads', 'history'].includes(activePage)"
              :error-message="activePage === 'history' ? historyError : tasksErrorMessage"
              :list-key="`${activePage}|${activeMenu}|${mediaFilter}`"
              :loading="activePage === 'history' ? historyLoading : tasksLoading"
              :page-kind="activePage as 'downloads' | 'history'"
              :source-profiles="sourceProfiles"
              :tasks="activePage === 'downloads' ? activeTasks : completedTasks"
              :view-mode="viewMode"
              :has-more="activePage === 'history' ? historyHasMore : undefined"
              @cancel="cancelTask"
              @clear-pending="clearPending"
              @download="downloadTask"
              @remove="removeTask"
              @retry="retryTask"
              @set-source="setTaskSource"
              @set-view-mode="setViewMode"
              @load-more="loadMoreHistory"
            />
          </div>

          <!-- Mobile Header & Status Bar: Sticky Bottom -->
          <div class="sticky bottom-0 z-20 flex flex-col shrink-0">
            <div class="lg:hidden pt-2 pb-4 px-4 glass border-0 border-t border-(--glass-border)">
              <template v-if="activePage === 'downloads'">
                <DownloadInput
                  v-model:url="url"
                  :saved-settings="savedSettings"
                  :quality="downloadSelection"
                  :quality-options="qualityOptions"
                  @update:quality="setDownloadQuality"
                  @add-download="addDownloadTask"
                >
                  <template #filters>
                    <DownloadFilters
                      :active-menu="activeMenu"
                      :navigation-items="navigationItems"
                      :media-filter="mediaFilter"
                      :media-filter-items="mediaFilterItems"
                      :view-mode="viewMode"
                      @update:active-menu="setActiveMenu"
                      @update:media-filter="setMediaFilter"
                      @update:view-mode="setViewMode"
                    />
                  </template>
                </DownloadInput>
              </template>

              <template v-else-if="activePage === 'history'">
                <div class="flex flex-col-reverse lg:flex-col gap-2 w-full">
                  <section aria-label="Search history">
                    <div class="flex items-center gap-2">
                      <Input
                        size="lg"
                        class="flex-1 min-w-0"
                        type="text"
                        placeholder="Search history..."
                      >
                        <template #icon>
                          <IconSearch class="w-5 h-5" aria-hidden="true" />
                        </template>
                      </Input>
                      <Button
                        variant="primary"
                        size="lg"
                        type="button"
                        class="shrink-0"
                        aria-label="Refresh history"
                        title="Refresh history"
                        :disabled="historyRefreshing"
                        @click="refreshHistory"
                      >
                        <template #icon>
                          <IconRefresh
                            aria-hidden="true"
                            class="w-6 h-6 transition-transform duration-300 ease-glass group-hover:rotate-45"
                            :class="{ 'animate-spin': historyRefreshing }"
                          />
                        </template>
                      </Button>
                    </div>
                  </section>
                  
                  <div class="flex overflow-x-auto no-scrollbar items-center justify-between lg:justify-end gap-2 w-full pb-1">
                    <div class="shrink-0 lg:ml-auto">
                      <DownloadFilters
                        :active-menu="activeMenu"
                        :navigation-items="navigationItems"
                        :media-filter="mediaFilter"
                        :media-filter-items="mediaFilterItems"
                        :view-mode="viewMode"
                        @update:active-menu="setActiveMenu"
                        @update:media-filter="setMediaFilter"
                        @update:view-mode="setViewMode"
                      />
                    </div>
                  </div>
                </div>
              </template>
            </div>

            <BarStatus
              ref="statusBar"
              :count-cards="countCards"
              @clear-queue="clearPending"
            />
          </div>
        </main>
      </div>
    </div>

    <NavBottom
      :active-page="activePage"
      :page-items="pageItems"
      :is-light-mode="isLightMode"
      :settings-open="settingsOpen"
      @select-page="setActivePage"
      @toggle-theme="toggleThemeMode"
      @open-settings="openSettings"
    />

    <SettingsView
      v-model:open="settingsOpen"
      v-model:section="settingsSection"
      :cookie-statuses="cookieStatuses"
      :settings="settings"
      :settings-draft="settingsDraft"
      :source-profiles="sourceProfiles"
      @connect-cookies="connectCookies"
      @connect-cookies-source="connectCookiesForSource"
      @remove-cookies="removeCookies"
    />

    <PlaylistDialog
      v-model:open="playlistOpen"
      :title="playlistTitle"
      :entries="playlistEntries"
      @confirm="confirmPlaylistSelection"
    />

    <Toaster
      :theme="isLightMode ? 'light' : 'dark'"
      :status-bar-clearance="statusBarHeight"
    />
  </div>
</template>
