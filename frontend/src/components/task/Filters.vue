<script setup lang="ts">
import { Combobox } from "@/components/ui/combobox";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "@/components/ui/segmented-control";
import IconGrid from "~icons/material-symbols/grid-view";
import IconList from "~icons/material-symbols/list";

import { useDashboard } from "@/composables/useDashboard";
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

function selectViewMode(value: string | string[]): void {
  if (typeof value === "string" && value) setViewMode(value as ViewMode);
}
</script>

<template>
  <div class="flex items-end gap-3 shrink-0">
    <Combobox
      :model-value="activeMenu"
      :items="navigationItems"
      @update:model-value="(val) => setActiveMenu(val as MenuKey)"
      class="shrink-0"
      label="Platform"
      placeholder="Select..."
      empty-text="No platforms found."
      aria-label="Platform"
    />

    <Combobox
      :model-value="mediaFilter"
      :items="mediaFilterItems"
      @update:model-value="(val) => setMediaFilter(val as MediaFilter)"
      class="shrink-0"
      label="Media"
      placeholder="Select..."
      empty-text="No types."
      aria-label="Media type"
    />

    <SegmentedControl
      :model-value="viewMode"
      @update:model-value="selectViewMode"
      label="View"
      aria-label="View mode"
      class="shrink-0"
    >
      <SegmentedControlItem value="grid" aria-label="Grid view" title="Grid view">
        <IconGrid class="w-3.5 h-3.5" aria-hidden="true" />
        <span>Grid</span>
      </SegmentedControlItem>
      <SegmentedControlItem value="table" aria-label="Table view" title="Table view">
        <IconList class="w-3.5 h-3.5" aria-hidden="true" />
        <span>Table</span>
      </SegmentedControlItem>
    </SegmentedControl>
  </div>
</template>
