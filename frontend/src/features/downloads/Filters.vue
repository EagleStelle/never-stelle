<script setup lang="ts">
import { Combobox } from "../../components/ui/combobox";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "../../components/ui/segmented-control";
import IconGrid from "~icons/material-symbols/grid-view";
import IconList from "~icons/material-symbols/list";
import type { MediaFilter, MenuKey, ViewMode } from "../../types";

defineProps<{
  activeMenu: MenuKey;
  navigationItems: any[];
  mediaFilter: MediaFilter;
  mediaFilterItems: any[];
  viewMode: ViewMode;
}>();

const emit = defineEmits<{
  "update:activeMenu": [val: MenuKey];
  "update:mediaFilter": [val: MediaFilter];
  "update:viewMode": [val: ViewMode];
}>();
</script>

<template>
  <div class="flex flex-wrap items-center gap-2">
    <Combobox
      size="lg"
      :model-value="activeMenu"
      :items="navigationItems"
      @update:model-value="(val) => emit('update:activeMenu', val as MenuKey)"
      class="shrink-0"
      placeholder="Search platform..."
      empty-text="No platforms found."
    />

    <Combobox
      size="lg"
      :model-value="mediaFilter"
      :items="mediaFilterItems"
      @update:model-value="(val) => emit('update:mediaFilter', val as MediaFilter)"
      class="shrink-0"
      placeholder="Media type..."
      empty-text="No types."
    />

    <SegmentedControl
      size="lg"
      :model-value="viewMode"
      @update:model-value="
        (val) => {
          if (val) emit('update:viewMode', val as ViewMode);
        }
      "
      aria-label="View mode"
      class="shrink-0"
    >
      <SegmentedControlItem value="grid" aria-label="Grid view">
        <IconGrid class="w-4 h-4" aria-hidden="true" />
      </SegmentedControlItem>
      <SegmentedControlItem value="table" aria-label="Table view">
        <IconList class="w-4 h-4" aria-hidden="true" />
      </SegmentedControlItem>
    </SegmentedControl>
  </div>
</template>
