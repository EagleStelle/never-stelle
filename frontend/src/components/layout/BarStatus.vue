<script setup lang="ts">
import { computed } from "vue";
import IconRefresh from "~icons/material-symbols/sync";
import IconTrash from "~icons/material-symbols/delete";
import { Button } from "@/components/ui/button";
import { useDashboard } from "@/composables/useDashboard";
import { useIsMobile } from "@/composables/useBreakpoints";

const {
  activePage,
  clearPending,
  countCards,
  historyRefreshing,
  refreshHistory,
} = useDashboard();

const isHistory = computed(() => activePage.value === "history");
const isMobile = useIsMobile();
</script>

<template>
  <footer
    class="z-40 glass glass-border-top py-2 px-3 flex items-center justify-between text-white in-[.light-mode]:text-black mb-(--nav-bottom-height) lg:mb-0 gap-3"
    aria-label="Task counts"
  >
    <Button
      v-if="isHistory"
      type="button"
      size="sm"
      title="Refresh history"
      aria-label="Refresh history"
      :disabled="historyRefreshing"
      @click="refreshHistory"
    >
      <template #icon>
        <IconRefresh
          aria-hidden="true"
          class="group-hover:rotate-45"
          :class="{ 'animate-spin': historyRefreshing }"
        />
      </template>
      <template v-if="!isMobile">Refresh History</template>
    </Button>

    <Button
      v-else
      type="button"
      variant="destructive"
      size="sm"
      title="Clear Queue"
      aria-label="Clear Queue"
      @click="clearPending"
    >
      <template #icon>
        <IconTrash aria-hidden="true" />
      </template>
      <template v-if="!isMobile">Clear Queue</template>
    </Button>
    <div
      class="flex items-center justify-end gap-3 sm:gap-4 overflow-x-auto scrollbar-hide w-full max-w-full"
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
