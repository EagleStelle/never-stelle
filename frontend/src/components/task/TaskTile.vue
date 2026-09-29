<script setup lang="ts">
import { Checkbox } from "@/components/ui/checkbox";
import { Combobox } from "@/components/ui/combobox";
import { IconImage } from "@/components/ui/icon-image";
import {
  Card,
  CardAction,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import ItemActions from "@/components/task/ItemActions.vue";

import { useDashboard } from "@/composables/useDashboard";
import type { Selection } from "@/composables/useSelection";
import type { SourceProfile, TaskItem } from "@/types";
import { sourceIconUrl, sourceOptions } from "@/utils/dashboard";
import {
  sourceLink,
  taskProgressState,
  taskProgressStyle,
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
      <div class="row-span-2 flex min-w-0 items-center gap-2.5">
        <Checkbox
          :checked="props.selection.isSelected(props.task)"
          :aria-label="`Select ${taskTitle(props.task)}`"
          title="Select"
          @click="(event: MouseEvent) => props.selection.toggle(props.task, event.shiftKey)"
        />
        <IconImage :src="sourceIconUrl(props.task.source_key)" class="h-4 w-4 shrink-0" />
        <div v-if="props.task.source_pending" class="min-w-0 flex-1">
          <Combobox
            model-value=""
            :items="sourceOptions(props.task, props.sourceProfiles)"
            creatable
            :disabled="props.selection.count > 0"
            layout="fill"
            aria-label="Source"
            placeholder="Pick source"
            empty-text="No sources found."
            @update:model-value="(key) => setTaskSource({ taskId: props.task.vid, sourceKey: key })"
          />
        </div>
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
        :title="props.task.source_url"
      >
        {{ props.task.source_url }}
      </a>
      <span v-else-if="props.task.source_url" class="truncate font-mono text-xs text-white/60 in-[.light-mode]:text-black/60">
        {{ props.task.source_url }}
      </span>
      <div
        v-if="props.task.status === 'failed' && props.task.error"
        class="mt-1 wrap-break-word whitespace-pre-line text-sm"
      >
        {{ props.task.error }}
      </div>
    </CardContent>
  </Card>
</template>
