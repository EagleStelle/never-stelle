<script setup lang="ts">
import { Combobox } from "@/components/ui/combobox";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "@/components/ui/segmented-control";
import IconGrid from "~icons/material-symbols/grid-view";
import IconList from "~icons/material-symbols/list";

import { useDashboard } from "@/composables/useDashboard";
import { useIsDesktop } from "@/composables/useBreakpoints";
import type { MediaFilter, MenuKey, ViewMode } from "@/types";

// Downloads and history narrow the same task list, so both toolbars mount this.
const {
  activeMenu,
  mediaFilter,
  mediaFilterItems,
  navigationItems,
  setActiveMenu,
  setMediaFilter,
  setViewMode,
  viewMode,
} = useDashboard();

const isDesktop = useIsDesktop();

function selectViewMode(value: string | string[]): void {
  if (typeof value === "string" && value) setViewMode(value as ViewMode);
}
</script>

<template>
  <!-- View toggle pins right; below lg the platform fills the row. Inset keeps focus rings unclipped. -->
  <div
    class="@container flex items-center gap-2 lg:gap-3 -m-1 p-1 overflow-x-auto no-scrollbar *:last:ml-auto"
  >
    <Combobox
      :model-value="activeMenu"
      :items="navigationItems"
      @update:model-value="(val) => setActiveMenu(val as MenuKey)"
      :layout="isDesktop ? 'fit' : 'fill'"
      aria-label="Platform"
      placeholder="Select..."
      empty-text="No platforms found."
    />

    <Combobox
      :model-value="mediaFilter"
      :items="mediaFilterItems"
      @update:model-value="(val) => setMediaFilter(val as MediaFilter)"
      aria-label="Media"
      placeholder="Select..."
      empty-text="No types."
    />

    <SegmentedControl
      :model-value="viewMode"
      @update:model-value="selectViewMode"
      aria-label="View mode"
    >
      <SegmentedControlItem value="grid" aria-label="Grid view" title="Grid view">
        <IconGrid class="w-3.5 h-3.5" aria-hidden="true" />
        <span class="hidden @xl:inline">Grid</span>
      </SegmentedControlItem>
      <SegmentedControlItem value="table" aria-label="Table view" title="Table view">
        <IconList class="w-3.5 h-3.5" aria-hidden="true" />
        <span class="hidden @xl:inline">Table</span>
      </SegmentedControlItem>
    </SegmentedControl>
  </div>
</template>
