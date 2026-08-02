<script setup lang="ts">
import { computed } from "vue";

import DownloadsToolbar from "@/features/downloads/Toolbar.vue";
import HistoryToolbar from "@/features/history/Toolbar.vue";
import { useDashboard } from "@/composables/useDashboard";
import { useIsDesktop } from "@/composables/useBreakpoints";

const props = defineProps<{ placement: "top" | "bottom" }>();

const { activePage } = useDashboard();
const isDesktop = useIsDesktop();

// The toolbar docks below the header on desktop and above the status bar on mobile.
// Only the slot for the live breakpoint renders, so the URL field never exists twice.
const visible = computed(() =>
  props.placement === "top" ? isDesktop.value : !isDesktop.value,
);
</script>

<template>
  <div
    v-if="visible"
    class="shrink-0 px-4 glass border-0 border-(--glass-border)"
    :class="
      placement === 'top'
        ? 'sticky top-0 z-20 pt-4 pb-2 border-b'
        : 'pt-2 pb-4 border-t'
    "
  >
    <DownloadsToolbar v-if="activePage === 'downloads'" />
    <HistoryToolbar v-else-if="activePage === 'history'" />
  </div>
</template>
