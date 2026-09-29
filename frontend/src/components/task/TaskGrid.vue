<script setup lang="ts">
import { computed, ref, useTemplateRef } from "vue";
import { useResizeObserver } from "@vueuse/core";

import TaskTile from "@/components/task/TaskTile.vue";
import type { Selection } from "@/composables/useSelection";
import { useVirtualRows } from "@/composables/useVirtualRows";
import type { SourceProfile, TaskItem } from "@/types";
import { TILE_GRID } from "@/ui";

const props = defineProps<{
  tasks: TaskItem[];
  selection: Selection<TaskItem>;
  sourceProfiles?: SourceProfile[];
  rising: ReadonlySet<string>;
}>();

const grid = useTemplateRef<HTMLElement>("grid");
// Read back from the grid's auto-fill rule, so rows split where the browser wraps them.
const columns = ref(1);
const rowGap = ref(0);
useResizeObserver(grid, () => {
  if (!grid.value) return;
  const style = getComputedStyle(grid.value);
  columns.value = Math.max(1, style.gridTemplateColumns.split(" ").length);
  rowGap.value = parseFloat(style.rowGap) || 0;
});

const { rows, before, after, measure } = useVirtualRows(grid, {
  count: computed(() => Math.ceil(props.tasks.length / columns.value)),
  estimate: 120,
  overscan: 3,
  gap: rowGap,
});

// Tiles of the mounted rows. A row's first tile carries the row index it is measured by.
const tiles = computed(() => {
  const first = (rows.value[0]?.index ?? 0) * columns.value;
  const end = ((rows.value.at(-1)?.index ?? -1) + 1) * columns.value;
  return props.tasks.slice(first, end).map((task, offset) => {
    const index = first + offset;
    return { task, row: index % columns.value === 0 ? index / columns.value : undefined };
  });
});
</script>

<template>
  <section
    ref="grid"
    :class="TILE_GRID"
    :style="{ paddingTop: `${before}px`, paddingBottom: `${after}px` }"
  >
    <TaskTile
      v-for="{ task, row } in tiles"
      :key="task.vid"
      :ref="measure"
      :data-index="row"
      :task="task"
      :selection="props.selection"
      :source-profiles="props.sourceProfiles"
      :rising="props.rising.has(task.vid)"
    />
  </section>
</template>
