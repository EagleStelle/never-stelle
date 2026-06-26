<script setup lang="ts">
import DownloadCommand from "./components/downloads/DownloadCommand.vue";
import DownloadsPanel from "./components/downloads/DownloadsPanel.vue";
import StatsGrid from "./components/downloads/StatsGrid.vue";
import AppSidebar from "./components/layout/AppSidebar.vue";
import AppTopbar from "./components/layout/AppTopbar.vue";
import BottomNav from "./components/layout/BottomNav.vue";
import SettingsPage from "./components/settings/SettingsPage.vue";
import ToastStack from "./components/ToastStack.vue";
import { useDownloadDashboard } from "./composables/useDownloadDashboard";

const {
  activeFilter,
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
  filteredTasks,
  hideTask,
  isLightMode,
  navigationItems,
  openSettings,
  pageItems,
  removeInstagramCookies,
  removeTask,
  saveSettingsDraft,
  savedSettings,
  setActiveFilter,
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
  <div class="w-full max-w-[100vw] min-h-[100dvh] overflow-x-hidden bg-bg text-text lg:flex">
    <a class="fixed z-80 top-[0.75rem] left-[0.75rem] -translate-y-[160%] focus:translate-y-0 border border-accent rounded-lg bg-panel text-text px-[0.8rem] py-[0.6rem] font-[800] transition-transform duration-[180ms] ease-out" href="#mainContent">Skip to content</a>

    <AppSidebar
      class="shrink-0"
      :active-page="activePage"
      :is-light-mode="isLightMode"
      :page-items="pageItems"
      @open-settings="openSettings"
      @select-page="setActivePage"
      @toggle-theme="toggleThemeMode"
    />

    <main id="mainContent" class="flex-1 min-w-0 max-w-[100vw] overflow-x-hidden pt-[0.65rem] px-[0.5rem] pb-[6.2rem] lg:p-[0.75rem]" tabindex="-1">
      <AppTopbar
        v-if="activePage === 'downloads'"
        :active-menu="activeMenu"
        :navigation-items="navigationItems"
        @select-menu="setActiveMenu"
      />

      <template v-if="activePage === 'downloads'">
        <DownloadCommand
          v-model:url="url"
          :saved-settings="savedSettings"
          @add-download="addDownloadTask"
          @update-save-mode="updateSaveMode"
        />

        <StatsGrid :count-cards="countCards" />

        <DownloadsPanel
          :active-filter="activeFilter"
          :active-menu="activeMenu"
          :active-menu-label="activeMenuLabel"
          :error-message="tasksErrorMessage"
          :loading="tasksLoading"
          :tasks="filteredTasks"
          :view-mode="viewMode"
          @clear-completed="clearCompleted"
          @clear-pending="clearPending"
          @download="downloadTask"
          @hide="hideTask"
          @remove="removeTask"
          @set-filter="setActiveFilter"
          @set-view-mode="setViewMode"
        />
      </template>

      <div v-else-if="activePage === 'history'" class="flex flex-col items-center justify-center py-12 text-text-muted">
        <p>History view is under construction.</p>
      </div>

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

    <BottomNav 
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
