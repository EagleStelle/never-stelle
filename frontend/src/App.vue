<script setup lang="ts">
import { computed, onMounted, useTemplateRef } from "vue";
import { useElementSize } from "@vueuse/core";

import BarStatus from "@/components/layout/BarStatus.vue";
import NavBottom from "@/components/layout/NavBottom.vue";
import NavSide from "@/components/layout/NavSide.vue";
import PageToolbar from "@/components/layout/PageToolbar.vue";
import Login from "@/features/auth/Login.vue";
import DownloadPanel from "@/features/downloads/Panel.vue";
import PlaylistDialog from "@/features/downloads/PlaylistDialog.vue";
import ResolveDialog from "@/features/downloads/ResolveDialog.vue";
import SettingsView from "@/features/settings/View.vue";
import { Toaster } from "@/components/ui/sonner";

import { provideDashboard } from "@/composables/useDashboard";
import { useAuth } from "@/composables/useAuth";

const auth = useAuth();
const authReady = auth.ready;
const authenticated = auth.authenticated;

onMounted(() => {
  void auth.refreshSession();
});

// Built once here; every surface below injects the slice it needs instead of
// receiving it as a prop.
const {
  confirmPlaylistSelection,
  confirmResolve,
  historyResolving,
  isLightMode,
  playlistEntries,
  playlistOpen,
  playlistTitle,
  resolveFlagged,
  resolveOpen,
  resolveTotal,
} = provideDashboard();

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
    v-if="!authReady"
    class="flex h-dvh w-full items-center justify-center bg-primary text-white in-[.light-mode]:text-black"
  >
    <div class="glass-chrome flex items-center gap-3 rounded-lg px-4 py-3">
      <img src="/assets/logo.png" alt="" class="h-8 w-8 rounded-md" />
      <span class="font-display text-base font-semibold">Never Stelle</span>
    </div>
  </div>

  <Login v-else-if="!authenticated" />

  <div
    v-else
    class="w-full max-w-full h-dvh overflow-hidden bg-primary text-white in-[.light-mode]:text-black lg:flex"
  >
    <a
      class="sr-only focus:not-sr-only focus:fixed focus:z-50 focus:top-2 focus:left-2 rounded-lg glass-chrome text-white in-[.light-mode]:text-black px-3 py-2 transition-transform duration-300 ease-glass"
      href="#mainContent"
      >Skip to content</a
    >

    <NavSide class="shrink-0" />

    <div class="flex-1 min-w-0 flex flex-col h-dvh relative">
      <div class="flex-1 relative min-h-0 overflow-hidden">
        <main
          id="mainContent"
          class="absolute inset-0 overflow-y-auto overflow-x-hidden flex flex-col"
          tabindex="-1"
        >
          <PageToolbar placement="top" />

          <div class="flex-1 flex flex-col p-4 pb-36 lg:pb-4">
            <DownloadPanel />
          </div>

          <div class="sticky bottom-0 z-20 flex flex-col shrink-0">
            <PageToolbar placement="bottom" />
            <BarStatus ref="statusBar" />
          </div>
        </main>
      </div>
    </div>

    <NavBottom />

    <SettingsView />

    <PlaylistDialog
      v-model:open="playlistOpen"
      :title="playlistTitle"
      :entries="playlistEntries"
      @confirm="confirmPlaylistSelection"
    />

    <ResolveDialog
      v-model:open="resolveOpen"
      :flagged="resolveFlagged"
      :total="resolveTotal"
      :pending="historyResolving"
      @confirm="confirmResolve"
    />

    <Toaster
      :theme="isLightMode ? 'light' : 'dark'"
      :status-bar-clearance="statusBarHeight"
    />
  </div>
</template>
