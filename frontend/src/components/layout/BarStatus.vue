<script setup lang="ts">
import { computed } from "vue";
import ActionButton from "@/components/task/ActionButton.vue";
import SelectionBar from "@/components/task/SelectionBar.vue";
import { Button } from "@/components/ui/button";
import { useDashboard } from "@/composables/useDashboard";
import type { ItemAction } from "@/types";
import { ACTION_ICONS } from "@/ui";

const {
  activePage,
  clearPending,
  countCards,
  historyRefreshing,
  historyResolving,
  libraryBusy,
  openResolveDialog,
  pageBatch,
  pageSelection,
  refreshHistory,
  refreshStopping,
  resolveQueued,
  resolveStopping,
  stopRefresh,
  stopResolve,
} = useDashboard();

const isHistory = computed(() => activePage.value === "history");

interface Stop {
  label: string;
  title: string;
  // The tooltip while the work winds down.
  waiting: string;
  stopping: boolean;
  variant: ItemAction["variant"];
  run: () => void;
}

// A running pass turns its button into a stop, which spins once asked until the work is gone.
function stopAction(action: ItemAction, stop: Stop): ItemAction {
  const { variant } = stop;
  return stop.stopping
    ? { ...action, variant, label: "Stopping", title: stop.waiting, spinning: true, disabled: true }
    : { ...action, variant, label: stop.label, title: stop.title, icon: ACTION_ICONS.stop, spinning: undefined, disabled: false, run: stop.run };
}

const historyActions = computed<ItemAction[]>(() => {
  const refresh: ItemAction = {
    key: "refresh",
    label: "Refresh History",
    title: "Refresh history",
    icon: ACTION_ICONS.refresh,
    variant: "primary",
    spinning: false,
    run: () => void refreshHistory(),
  };
  // Waits for a refresh, as the missing count reads the flags the refresh sets.
  const resolve: ItemAction = {
    key: "resolve",
    label: "Resolve History",
    title: historyRefreshing.value ? "Available once the refresh ends." : "Resolve history",
    icon: ACTION_ICONS.resolve,
    variant: "ghost",
    disabled: libraryBusy.value,
    spinning: historyResolving.value,
    run: () => void openResolveDialog(),
  };
  return [
    historyRefreshing.value
      ? stopAction(refresh, {
          label: "Stop Refresh",
          title: "Stops the scan. What it saved so far stays.",
          waiting: "Stopping at the next file.",
          stopping: refreshStopping.value,
          variant: "destructive",
          run: () => void stopRefresh(),
        })
      : refresh,
    // Only once the poll shows queued items, so a click always has something to stop.
    resolveQueued.value > 0
      ? stopAction(resolve, {
          label: `Stop Resolve (${resolveQueued.value.toLocaleString()})`,
          title: "Stops the items still waiting. The one in progress finishes first.",
          waiting: "Waiting for the item in progress.",
          stopping: resolveStopping.value,
          variant: "destructive-ghost",
          run: () => void stopResolve(),
        })
      : resolve,
  ];
});
</script>

<template>
  <footer
    class="z-40 glass glass-border-top py-2 px-3 flex items-center justify-between text-white in-[.light-mode]:text-black mb-(--nav-bottom-height) lg:mb-0 gap-3"
    aria-label="Task counts"
  >
    <div class="flex shrink-0 items-center gap-2">
      <SelectionBar v-if="pageSelection.count" :selection="pageSelection" :actions="pageBatch" />

      <template v-else-if="isHistory">
        <ActionButton v-for="action in historyActions" :key="action.key" :action="action" size="sm" compact />
      </template>

      <Button
        v-else
        type="button"
        variant="destructive"
        size="sm"
        compact
        title="Clear Queue"
        @click="clearPending"
      >
        <template #icon>
          <component :is="ACTION_ICONS.delete" aria-hidden="true" />
        </template>
        Clear Queue
      </Button>
    </div>
    <div
      class="flex items-center justify-end gap-3 sm:gap-4 overflow-x-auto scrollbar-hide w-full max-w-full"
      :class="{ 'max-sm:hidden': pageSelection.count }"
    >
      <div
        v-for="item in countCards"
        :key="item.label"
        class="flex items-center gap-1.5 whitespace-nowrap text-sm"
        :title="item.label"
      >
        <component :is="item.icon" class="w-4 h-4" aria-hidden="true" />
        <span class="hidden sm:inline tracking-wider uppercase text-xs"
          >{{ item.label }}:</span
        >
        <strong class="text-white in-[.light-mode]:text-black">{{
          item.value
        }}</strong>
      </div>
    </div>
  </footer>
</template>

<style scoped>
.scrollbar-hide::-webkit-scrollbar {
  display: none;
}
.scrollbar-hide {
  -ms-overflow-style: none;
  scrollbar-width: none;
}
</style>
