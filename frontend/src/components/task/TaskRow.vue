<script setup lang="ts">
import { Checkbox } from "@/components/ui/checkbox";
import { Combobox } from "@/components/ui/combobox";
import { IconImage } from "@/components/ui/icon-image";
import { TableCell, TableRow } from "@/components/ui/table";
import ItemActions from "@/components/task/ItemActions.vue";

import { useDashboard } from "@/composables/useDashboard";
import type { Selection } from "@/composables/useSelection";
import type { SourceProfile, TaskItem } from "@/types";
import { sourceIconUrl, sourceOptions } from "@/utils/dashboard";
import {
  formatSize,
  sourceLink,
  taskProgressState,
  taskProgressStyle,
  taskTitle,
} from "@/utils/task";

// One table row. Its props stay stable, so scrolling the table never re-renders it.
const props = defineProps<{
  task: TaskItem;
  selection: Selection<TaskItem>;
  sourceProfiles?: SourceProfile[];
  rising: boolean;
  expanded: boolean;
}>();

const emit = defineEmits<{
  "toggle-name": [vid: string];
}>();

const { setTaskSource, taskActions } = useDashboard();
</script>

<template>
  <TableRow
    v-bind="props.selection.itemProps(props.task)"
    class="task-progress-surface task-progress-row"
    :class="{ 'glass-rise': props.rising }"
    :data-state="props.selection.isSelected(props.task) ? 'selected' : undefined"
    :data-task-state="taskProgressState(props.task)"
    :style="taskProgressStyle(props.task)"
  >
    <TableCell class="w-px">
      <Checkbox
        :checked="props.selection.isSelected(props.task)"
        :aria-label="`Select ${taskTitle(props.task)}`"
        title="Select"
        @click="(event: MouseEvent) => props.selection.toggle(props.task, event.shiftKey)"
      />
    </TableCell>
    <TableCell class="w-1/2 max-w-0 min-w-72 cursor-pointer" @click="emit('toggle-name', props.task.vid)">
      <div :class="['text-white in-[.light-mode]:text-black', props.expanded ? 'break-all whitespace-normal' : 'truncate']" :title="taskTitle(props.task)">
        {{ taskTitle(props.task) }}
      </div>
    </TableCell>
    <TableCell class="w-px max-w-40 md:max-w-60">
      <div class="truncate text-white in-[.light-mode]:text-black" :title="props.task.creator">
        {{ props.task.creator }}
      </div>
    </TableCell>
    <TableCell class="w-1/2 max-w-0 min-w-72">
      <Combobox
        v-if="props.task.source_pending"
        model-value=""
        :items="sourceOptions(props.task, props.sourceProfiles)"
        creatable
        :disabled="props.selection.count > 0"
        aria-label="Source"
        placeholder="Pick source"
        empty-text="No sources found."
        @update:model-value="(key) => setTaskSource({ taskId: props.task.vid, sourceKey: key })"
      />
      <div v-else class="flex items-center gap-2">
        <IconImage
          :src="sourceIconUrl(props.task.source_key)"
          class="h-4 w-4 shrink-0"
        />
        <a
          v-if="sourceLink(props.task)"
          :href="sourceLink(props.task)"
          target="_blank"
          rel="noopener noreferrer"
          class="truncate block min-w-0 text-white in-[.light-mode]:text-black underline decoration-dotted underline-offset-2 hover:decoration-solid"
          :title="props.task.source_url"
        >
          {{ props.task.source_url }}
        </a>
        <div v-else class="truncate block min-w-0 text-white in-[.light-mode]:text-black" :title="props.task.source_url">
          {{ props.task.source_url }}
        </div>
      </div>
    </TableCell>
    <TableCell class="w-px whitespace-nowrap">
      <div class="text-white in-[.light-mode]:text-black tabular-nums">
        {{ formatSize(props.task.file_size) }}
      </div>
    </TableCell>
    <TableCell class="w-px">
      <ItemActions :actions="taskActions(props.task)" :disabled="props.selection.count > 0" />
    </TableCell>
  </TableRow>
</template>
