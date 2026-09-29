<script setup lang="ts">
import { Checkbox } from "@/components/ui/checkbox";
import { IconImage } from "@/components/ui/icon-image";
import {
  Card,
  CardAction,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import ItemActions from "@/components/task/ItemActions.vue";
import SourcePicker from "@/components/task/SourcePicker.vue";

import { useDashboard } from "@/composables/useDashboard";
import type { Selection } from "@/composables/useSelection";
import type { SourceProfile, TaskItem } from "@/types";
import { sourceIconUrl } from "@/utils/dashboard";
import {
  sourceLink,
  taskProgressState,
  taskProgressStyle,
  taskDetail,
  taskTitle,
} from "@/utils/task";

// One grid tile. Its props stay stable, so scrolling the list never re-renders it.
const props = defineProps<{
  task: TaskItem;
  selection: Selection<TaskItem>;
  sourceProfiles?: SourceProfile[];
  rising: boolean;
}>();

const { setTaskSource, taskActions } = useDashboard();
</script>

<template>
  <Card
    v-bind="props.selection.itemProps(props.task)"
    class="glass-hoverable hover:-translate-y-0.5 task-progress-surface task-progress-card gap-2 py-3 backdrop-filter-none"
    :class="{ 'glass-rise': props.rising }"
    :data-state="props.selection.isSelected(props.task) ? 'selected' : undefined"
    :data-task-state="taskProgressState(props.task)"
    :style="taskProgressStyle(props.task)"
  >
    <CardHeader class="items-center px-4">
      <div class="flex items-center gap-2.5">
        <Checkbox
          :checked="props.selection.isSelected(props.task)"
          :aria-label="`Select ${taskTitle(props.task)}`"
          title="Select"
          @click="(event: MouseEvent) => props.selection.toggle(props.task, event.shiftKey)"
        />
        <IconImage :src="sourceIconUrl(props.task.source_key)" class="h-4 w-4 shrink-0" />
      </div>
      <CardAction class="self-center">
        <ItemActions :actions="taskActions(props.task)" :disabled="props.selection.count > 0" />
      </CardAction>
    </CardHeader>

    <CardContent class="gap-1 px-4">
      <CardTitle class="line-clamp-2" :title="taskTitle(props.task)">
        {{ taskTitle(props.task) }}
      </CardTitle>
      <!-- A named tile shows its state by fill and tint only; this names it for screen readers. -->
      <span v-if="props.task.status !== 'completed' && props.task.resolved_filename" class="sr-only">{{ props.task.status_label }}</span>
      <a
        v-if="sourceLink(props.task)"
        :href="sourceLink(props.task)"
        target="_blank"
        rel="noopener noreferrer"
        class="truncate font-mono text-xs text-accent underline decoration-dotted underline-offset-2 hover:decoration-solid"
        :title="taskDetail(props.task)"
      >
        {{ taskDetail(props.task) }}
      </a>
      <span v-else class="truncate font-mono text-xs text-white/60 in-[.light-mode]:text-black/60">
        {{ taskDetail(props.task) }}
      </span>
      <div
        v-if="props.task.status === 'failed' && props.task.error"
        class="mt-1 wrap-break-word whitespace-pre-line text-sm"
      >
        {{ props.task.error }}
      </div>
      <SourcePicker
        v-if="props.task.source_pending"
        variant="card"
        :task="props.task"
        :source-profiles="props.sourceProfiles"
        @set-source="setTaskSource"
      />
    </CardContent>
  </Card>
</template>
