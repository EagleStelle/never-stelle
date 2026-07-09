<script setup lang="ts">
import IconSpinner from "~icons/material-symbols/sync";

import TaskGrid from "./TaskGrid.vue";
import TaskTable from "./TaskTable.vue";

import type { SourceProfile, TaskItem, ViewMode } from "../../types";

defineProps<{
  tasks: TaskItem[];
  viewMode: ViewMode;
  pageKind: "downloads" | "history";
  sourceProfiles?: SourceProfile[];
  loading?: boolean;
  errorMessage?: string;
}>();

const emit = defineEmits<{
  download: [taskId: string];
  remove: [taskId: string];
  "set-source": [payload: { taskId: string; sourceKey: string }];
}>();
</script>

<template>
  <section aria-live="polite">
    <div
      v-if="loading"
      class="rounded-lg glass flex min-h-32 items-center justify-center gap-2 text-white in-[.light-mode]:text-black text-center"
    >
      <IconSpinner class="animate-spin text-accent" aria-hidden="true" />
      <span>Loading downloads...</span>
    </div>

    <div
      v-else-if="errorMessage"
      class="rounded-lg glass flex min-h-32 items-center justify-center gap-2 text-center text-white in-[.light-mode]:text-black"
    >
      {{ errorMessage }}
    </div>

    <TaskTable
      v-else-if="viewMode === 'table'"
      :tasks="tasks"
      :source-profiles="sourceProfiles"
      @download="emit('download', $event)"
      @remove="emit('remove', $event)"
      @set-source="emit('set-source', $event)"
    />

    <TaskGrid
      v-else
      :tasks="tasks"
      :page-kind="pageKind"
      :source-profiles="sourceProfiles"
      @download="emit('download', $event)"
      @remove="emit('remove', $event)"
      @set-source="emit('set-source', $event)"
    />
  </section>
</template>
