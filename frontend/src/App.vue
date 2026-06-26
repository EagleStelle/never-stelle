<script setup lang="ts">
import DownloadCommand from "./components/downloads/DownloadCommand.vue";
import DownloadsPanel from "./components/downloads/DownloadsPanel.vue";
import NavSide from "./components/layout/NavSide.vue";
import BarStatus from "./components/layout/BarStatus.vue";
import BarTop from "./components/layout/BarTop.vue";
import NavBottom from "./components/layout/NavBottom.vue";
import SettingsPage from "./components/settings/SettingsPage.vue";
import ToastStack from "./components/ToastStack.vue";
import SegmentedControl from "./components/ui/SegmentedControl.vue";
import SegmentedControlItem from "./components/ui/SegmentedControlItem.vue";
import IconGrid from "~icons/material-symbols/grid-view";
import IconList from "~icons/material-symbols/list";
import IconSearch from "~icons/material-symbols/search";
import IconTrash from "~icons/material-symbols/delete";

import { useDownloadDashboard } from "./composables/useDownloadDashboard";

const {
  activeMenu,
  activeMenuLabel,
  activePage,
  addDownloadTask,
  cleanNfo,
  cleanupBusy,
  clearCompleted,
  clearPending,
  connectInstagramCookies,
  cookiesStatusText,
  countCards,
  downloadTask,
  activeTasks,
  completedTasks,
  hideTask,
  isLightMode,
  navigationItems,
  openSettings,
  pageItems,
  removeInstagramCookies,
  removeTask,
  saveSettingsDraft,
  savedSettings,
  setActiveMenu,
  setActivePage,
  setViewMode,
  settings,
  settingsDraft,
  settingsSection,
  srStatus,
  tasksErrorMessage,
  tasksLoading,
  toasts,
  toggleThemeMode,
  updateSaveMode,
  url,
  viewMode,
} = useDownloadDashboard();
</script>

<template>
  <div class="w-full max-w-[100vw] h-[100dvh] overflow-hidden bg-bg text-text lg:flex">
    <a class="fixed z-80 top-[0.75rem] left-[0.75rem] -translate-y-[160%] focus:translate-y-0 border border-accent rounded-lg bg-panel text-text px-[0.8rem] py-[0.6rem] font-[800] transition-transform duration-[180ms] ease-out" href="#mainContent">Skip to content</a>

    <NavSide
      class="shrink-0"
      :active-page="activePage"
      :is-light-mode="isLightMode"
      :page-items="pageItems"
      @open-settings="openSettings"
      @select-page="setActivePage"
      @toggle-theme="toggleThemeMode"
    />

    <div class="flex-1 min-w-0 flex flex-col h-[100dvh] relative">
      <main id="mainContent" class="flex-1 overflow-y-auto overflow-x-hidden p-[0.65rem] lg:p-[0.75rem] flex flex-col" tabindex="-1">
      <template v-if="activePage === 'downloads'">
        <DownloadCommand
          class="mb-[0.65rem]"
          v-model:url="url"
          :saved-settings="savedSettings"
          @add-download="addDownloadTask"
        />
      </template>

      <template v-else-if="activePage === 'history'">
        <section aria-label="Search history" class="mb-[0.65rem]">
          <div class="flex items-center min-w-0 h-10 overflow-hidden border border-accent-subtle rounded-lg bg-panel-subtle focus-within:border-accent pl-3">
            <IconSearch class="text-text-muted shrink-0 w-5 h-5 mr-2" aria-hidden="true" />
            <input
              class="flex-1 min-w-0 bg-transparent outline-none"
              type="text"
              placeholder="Search history..."
            />
          </div>
        </section>
      </template>

      <div v-if="['downloads', 'history'].includes(activePage)" class="flex flex-wrap items-center justify-between gap-[0.65rem] mb-[0.65rem]">
        <BarTop
          :active-menu="activeMenu"
          :navigation-items="navigationItems"
          @select-menu="setActiveMenu"
        />
        
        <div class="flex items-center gap-[0.45rem]">
          <SegmentedControl
            :model-value="viewMode"
            @update:model-value="(val) => { if (val) setViewMode(val as 'grid' | 'table') }"
            aria-label="View mode"
          >
            <SegmentedControlItem value="grid" aria-label="Grid view" class="w-10 p-0">
              <IconGrid class="w-[1.35rem] h-[1.35rem]" aria-hidden="true" />
            </SegmentedControlItem>
            <SegmentedControlItem value="table" aria-label="Table view" class="w-10 p-0">
              <IconList class="w-[1.35rem] h-[1.35rem]" aria-hidden="true" />
            </SegmentedControlItem>
          </SegmentedControl>

          <button v-if="activePage === 'downloads'" type="button" class="inline-flex items-center justify-center gap-[0.42rem] h-10 px-4 border border-accent-subtle rounded-lg bg-panel-subtle text-text-muted font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:border-accent hover:text-text md:flex-none border-red-600 bg-red-600 text-white hover:bg-red-700 hover:border-red-700 hover:text-white" @click="clearPending">
            <IconTrash aria-hidden="true" />
            <span>Clear Queue</span>
          </button>
        </div>
      </div>

      <template v-if="activePage === 'downloads'">
        <DownloadsPanel
          :active-menu="activeMenu"
          :active-menu-label="activeMenuLabel"
          :error-message="tasksErrorMessage"
          :loading="tasksLoading"
          :tasks="activeTasks"
          :view-mode="viewMode"
          @clear-pending="clearPending"
          @download="downloadTask"
          @hide="hideTask"
          @remove="removeTask"
          @set-view-mode="setViewMode"
        />
      </template>

      <template v-else-if="activePage === 'history'">
        <DownloadsPanel
          :active-menu="activeMenu"
          :active-menu-label="activeMenuLabel"
          :error-message="tasksErrorMessage"
          :loading="tasksLoading"
          :tasks="completedTasks"
          :view-mode="viewMode"
          @clear-pending="clearPending"
          @download="downloadTask"
          @hide="hideTask"
          @remove="removeTask"
          @set-view-mode="setViewMode"
        />
      </template>

      <SettingsPage
        v-else-if="activePage === 'settings'"
        v-model:section="settingsSection"
        :cleanup-busy="cleanupBusy"
        :cookies-status-text="cookiesStatusText"
        :settings="settings"
        :settings-draft="settingsDraft"
        @clean-nfo="cleanNfo"
        @connect-instagram-cookies="connectInstagramCookies"
        @remove-instagram-cookies="removeInstagramCookies"
        @save="saveSettingsDraft"
      />
      </main>
      <BarStatus :count-cards="countCards" />
    </div>

    <NavBottom 
      :active-page="activePage" 
      :page-items="pageItems"
      :is-light-mode="isLightMode" 
      @select-page="setActivePage" 
      @toggle-theme="toggleThemeMode"
      @open-settings="openSettings"
    />

    <ToastStack :toasts="toasts" />
    <div class="sr-only" aria-live="polite" aria-atomic="true">{{ srStatus }}</div>
  </div>
</template>
