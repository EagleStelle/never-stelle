<script setup lang="ts">
import DownloadCommand from "./components/downloads/DownloadCommand.vue";
import DownloadsPanel from "./components/downloads/DownloadsPanel.vue";
import NavSide from "./components/layout/NavSide.vue";
import BarStatus from "./components/layout/BarStatus.vue";
import NavBottom from "./components/layout/NavBottom.vue";
import SettingsPage from "./components/settings/SettingsPage.vue";
import ToastStack from "./components/ToastStack.vue";
import Button from "./components/ui/Button.vue";
import Combobox from "./components/ui/Combobox.vue";
import InputBar from "./components/ui/InputBar.vue";
import SegmentedControl from "./components/ui/SegmentedControl.vue";
import SegmentedControlItem from "./components/ui/SegmentedControlItem.vue";
import IconGrid from "~icons/material-symbols/grid-view";
import IconList from "~icons/material-symbols/list";
import IconRefresh from "~icons/material-symbols/sync";
import IconSearch from "~icons/material-symbols/search";

import { useDownloadDashboard } from "./composables/useDownloadDashboard";

const {
  activeMenu,
  activeMenuLabel,
  activePage,
  addDownloadTask,
  clearPending,
  connectCookies,
  connectCookiesForSource,
  cookieStatuses,
  countCards,
  downloadTask,
  historyRefreshing,
  activeTasks,
  completedTasks,
  isLightMode,
  navigationItems,
  openSettings,
  pageItems,
  removeCookies,
  removeTask,
  refreshHistory,
  saveSettingsDraft,
  savedSettings,
  setActiveMenu,
  setActivePage,
  setTaskSource,
  setViewMode,
  settings,
  settingsDraft,
  settingsSection,
  sourceProfiles,
  srStatus,
  tasksErrorMessage,
  tasksLoading,
  toasts,
  toggleThemeMode,
  url,
  viewMode,
} = useDownloadDashboard();
</script>

<template>
  <div
    class="w-full max-w-full h-dvh overflow-hidden bg-primary text-white [.light-mode_&]:text-black lg:flex"
  >
    <a
      class="sr-only focus:not-sr-only focus:fixed focus:z-50 focus:top-2 focus:left-2 rounded-lg glass-chrome text-white [.light-mode_&]:text-black px-3 py-2 transition-transform duration-300 ease-glass"
      href="#mainContent"
      >Skip to content</a
    >

    <NavSide
      class="shrink-0"
      :active-page="activePage"
      :is-light-mode="isLightMode"
      :page-items="pageItems"
      @open-settings="openSettings"
      @select-page="setActivePage"
      @toggle-theme="toggleThemeMode"
    />

    <div class="flex-1 min-w-0 flex flex-col h-dvh relative">
      <main
        id="mainContent"
        class="flex-1 overflow-y-auto overflow-x-hidden p-4 flex flex-col"
        tabindex="-1"
      >
        <template v-if="activePage === 'downloads'">
          <DownloadCommand
            class="mb-2"
            v-model:url="url"
            :saved-settings="savedSettings"
            @add-download="addDownloadTask"
          />
        </template>

        <template v-else-if="activePage === 'history'">
          <section aria-label="Search history" class="mb-2">
            <div class="flex items-center gap-2">
              <InputBar
                class="flex-1 min-w-0"
                type="text"
                placeholder="Search history..."
              >
                <template #icon>
                  <IconSearch class="w-5 h-5" aria-hidden="true" />
                </template>
              </InputBar>
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
        </template>

        <div
          v-if="['downloads', 'history'].includes(activePage)"
          class="flex items-center justify-end gap-2 mb-2 pb-1"
        >
          <div class="flex items-center gap-2">
            <Combobox
              :model-value="activeMenu"
              :items="navigationItems"
              @update:model-value="(val) => setActiveMenu(val as any)"
              class="w-40"
              placeholder="Search platform..."
              empty-text="No platforms found."
            />

            <SegmentedControl
              :model-value="viewMode"
              @update:model-value="
                (val) => {
                  if (val) setViewMode(val as 'grid' | 'table');
                }
              "
              aria-label="View mode"
            >
              <SegmentedControlItem value="grid" aria-label="Grid view">
                <IconGrid class="w-4 h-4" aria-hidden="true" />
                <span>Grid</span>
              </SegmentedControlItem>
              <SegmentedControlItem value="table" aria-label="Table view">
                <IconList class="w-4 h-4" aria-hidden="true" />
                <span>Table</span>
              </SegmentedControlItem>
            </SegmentedControl>
          </div>
        </div>

        <template v-if="activePage === 'downloads'">
          <DownloadsPanel
            :active-menu="activeMenu"
            :active-menu-label="activeMenuLabel"
            :error-message="tasksErrorMessage"
            :loading="tasksLoading"
            page-kind="downloads"
            :source-profiles="sourceProfiles"
            :tasks="activeTasks"
            :view-mode="viewMode"
            @clear-pending="clearPending"
            @download="downloadTask"
            @remove="removeTask"
            @set-source="setTaskSource"
            @set-view-mode="setViewMode"
          />
        </template>

        <template v-else-if="activePage === 'history'">
          <DownloadsPanel
            :active-menu="activeMenu"
            :active-menu-label="activeMenuLabel"
            :error-message="tasksErrorMessage"
            :loading="tasksLoading"
            page-kind="history"
            :source-profiles="sourceProfiles"
            :tasks="completedTasks"
            :view-mode="viewMode"
            @clear-pending="clearPending"
            @download="downloadTask"
            @remove="removeTask"
            @set-source="setTaskSource"
            @set-view-mode="setViewMode"
          />
        </template>

        <SettingsPage
          v-else-if="activePage === 'settings'"
          v-model:section="settingsSection"
          :cookie-statuses="cookieStatuses"
          :settings="settings"
          :settings-draft="settingsDraft"
          :source-profiles="sourceProfiles"
          @connect-cookies="connectCookies"
          @connect-cookies-source="connectCookiesForSource"
          @remove-cookies="removeCookies"
          @save="saveSettingsDraft"
        />
      </main>
      <BarStatus :count-cards="countCards" @clear-queue="clearPending" />
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
    <div class="sr-only" aria-live="polite" aria-atomic="true">
      {{ srStatus }}
    </div>
  </div>
</template>
