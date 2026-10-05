<script setup lang="ts">
import { computed, defineAsyncComponent, onMounted, useTemplateRef } from "vue";
import { useElementSize } from "@vueuse/core";

import BarStatus from "@/components/layout/BarStatus.vue";
import NavBottom from "@/components/layout/NavBottom.vue";
import NavSide from "@/components/layout/NavSide.vue";
import PageToolbar from "@/components/layout/PageToolbar.vue";
import AuthLoading from "@/features/auth/Loading.vue";
import Login from "@/features/auth/Login.vue";
import AccountPanel from "@/features/account/Panel.vue";
import DownloadPanel from "@/features/downloads/Panel.vue";
import PlaylistDialog from "@/features/downloads/PlaylistDialog.vue";
import ResolveDialog from "@/features/downloads/ResolveDialog.vue";
import TrackerPanel from "@/features/trackers/Panel.vue";
import { Button } from "@/components/ui/button";
import { DialogFooter, DialogShell as Dialog } from "@/components/ui/dialog";
import { Toaster } from "@/components/ui/sonner";

import { provideDashboard } from "@/composables/useDashboard";
import { useAuth } from "@/composables/useAuth";
import { provideScrollRoot } from "@/composables/useVirtualRows";
import { ACTION_ICONS, syncIconClass } from "@/ui";
import { plural } from "@/utils/dashboard";

// Loaded on its own chunk, keeping the largest surface out of first paint.
const SettingsView = defineAsyncComponent(() => import("@/features/settings/View.vue"));

const auth = useAuth();
const authReady = auth.ready;
const authenticated = auth.authenticated;

onMounted(() => {
  void auth.refreshSession();
});

// Built once here; every surface below injects the slice it needs instead of
// receiving it as a prop.
const {
  activePage,
  confirmPlaylistSelection,
  confirmResolve,
  confirmTaskAction,
  isLightMode,
  libraryBusy,
  playlistEntries,
  playlistOpen,
  playlistTitle,
  pendingTaskAction,
  confirmRename,
  renameCount,
  renameTarget,
  resolveFlagged,
  resolveOpen,
  resolveTotal,
} = provideDashboard();

provideScrollRoot(useTemplateRef<HTMLElement>("main"));

// What each task action's confirm dialog says and shows.
const TASK_ACTION_CONFIRM = {
  delete: {
    label: "Delete",
    description: (count: number) =>
      count === 1
        ? "Its file and extras like subtitles and thumbnails are deleted from disk. Trackers never download it again."
        : "Their files and extras like subtitles and thumbnails are deleted from disk. Trackers never download them again.",
    variant: "destructive",
    iconClass: "",
  },
  resolve: {
    label: "Resolve",
    description: () => "Looks up missing details from each source so these files can be named.",
    variant: "primary",
    iconClass: syncIconClass(false),
  },
} as const;
const taskConfirm = computed(() => pendingTaskAction.value && TASK_ACTION_CONFIRM[pendingTaskAction.value.kind]);
const confirmSeen = computed(() => pendingTaskAction.value?.seen);
const confirmCount = computed(() => (pendingTaskAction.value?.ids.length ?? 0) + (confirmSeen.value?.count ?? 0));
// Seen items with finished downloads can also be deleted apart.
const confirmMixed = computed(() => Boolean(confirmSeen.value && pendingTaskAction.value?.done?.length));

const isAre = (count: number) => (count === 1 ? "is" : "are");

// One kind of item reads like a plain delete; a mix gives each kind its count and what happens to it.
const confirmDescription = computed(() => {
  const action = pendingTaskAction.value;
  if (!action || !taskConfirm.value) return "";
  if (!action.seen) return taskConfirm.value.description(action.ids.length);
  if (!action.ids.length) {
    return action.seen.count === 1
      ? "It is removed from the list. The tracker never downloads it again."
      : "They are removed from the list. The tracker never downloads them again.";
  }
  const done = action.done?.length ?? 0;
  const queued = action.ids.length - done;
  const seen = action.seen.count;
  return [
    done && `${done} done item${plural(done)} and ${done === 1 ? "its file" : "their files"} are deleted from disk.`,
    queued && `${queued} item${plural(queued)} in the queue ${isAre(queued)} removed.`,
    `${seen} seen item${plural(seen)} ${isAre(seen)} removed from the list.`,
    "The tracker never downloads them again.",
  ]
    .filter(Boolean)
    .join(" ");
});

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
  <AuthLoading v-if="!authReady" />

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
          ref="main"
          id="mainContent"
          class="absolute inset-0 overflow-y-auto overflow-x-hidden flex flex-col"
          tabindex="-1"
        >
          <PageToolbar placement="top" />

          <div class="flex-1 flex flex-col p-4 pb-36 lg:pb-4">
            <TrackerPanel v-if="activePage === 'trackers'" />
            <AccountPanel v-else-if="activePage === 'account'" />
            <DownloadPanel v-else />
          </div>

          <div class="sticky bottom-0 z-20 flex flex-col shrink-0">
            <PageToolbar placement="bottom" />
            <BarStatus v-if="activePage !== 'account'" ref="statusBar" />
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

    <!-- Mounted on open so they stack above the lazily loaded settings dialog. -->
    <ResolveDialog
      v-if="resolveOpen"
      v-model:open="resolveOpen"
      :flagged="resolveFlagged"
      :total="resolveTotal"
      :pending="libraryBusy"
      @confirm="confirmResolve"
    />

    <Dialog
      v-if="renameTarget"
      :open="Boolean(renameTarget)"
      :title="`Resolve ${renameTarget.label}`"
      :description="
        renameTarget.kind === 'templates'
          ? 'Renames and moves the files of this format so they match its new templates.'
          : 'Looks up each file again so it is named by the new field order.'
      "
      content-class="fixed left-1/2 top-1/2 z-70 flex w-[min(460px,96vw)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none"
      @update:open="(open) => !open && (renameTarget = null)"
    >
      <DialogFooter class="mt-5">
        <Button variant="ghost" type="button" @click="renameTarget = null">
          Cancel
        </Button>
        <Button variant="destructive" type="button" @click="confirmRename">
          <template #icon>
            <component :is="ACTION_ICONS.resolve" aria-hidden="true" :class="syncIconClass(false)" />
          </template>
          Resolve All ({{ renameCount(renameTarget.key, renameTarget.kind, renameTarget.format).toLocaleString() }})
        </Button>
      </DialogFooter>
    </Dialog>

    <!-- Rendered after the tracker dialog, so it stacks above it. -->
    <Dialog
      v-if="pendingTaskAction && taskConfirm"
      :open="Boolean(pendingTaskAction)"
      :title="`${taskConfirm.label} ${confirmCount.toLocaleString()} item${confirmCount === 1 ? '' : 's'}?`"
      :description="confirmDescription"
      :content-class="`fixed left-1/2 top-1/2 z-70 flex ${confirmMixed ? 'w-[min(500px,96vw)]' : 'w-[min(460px,96vw)]'} -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none`"
      @update:open="(open) => !open && (pendingTaskAction = null)"
    >
      <DialogFooter class="mt-5">
        <Button variant="ghost" type="button" @click="pendingTaskAction = null">
          Cancel
        </Button>
        <template v-if="confirmMixed">
          <Button variant="destructive-ghost" type="button" @click="confirmTaskAction('seen')">
            <template #icon>
              <component :is="ACTION_ICONS.delete" aria-hidden="true" />
            </template>
            Seen Only
          </Button>
          <Button variant="destructive-ghost" type="button" @click="confirmTaskAction('done')">
            <template #icon>
              <component :is="ACTION_ICONS.delete" aria-hidden="true" />
            </template>
            Done Only
          </Button>
        </template>
        <Button :variant="taskConfirm.variant" type="button" @click="confirmTaskAction()">
          <template #icon>
            <component :is="ACTION_ICONS[pendingTaskAction.kind]" aria-hidden="true" :class="taskConfirm.iconClass" />
          </template>
          {{ taskConfirm.label }}
        </Button>
      </DialogFooter>
    </Dialog>

    <Toaster
      :theme="isLightMode ? 'light' : 'dark'"
      :status-bar-clearance="statusBarHeight"
    />
  </div>
</template>
