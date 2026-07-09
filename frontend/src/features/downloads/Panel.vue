<script setup lang="ts">
import TaskCollection from "../../components/task/TaskCollection.vue";
import type { SourceProfile, TaskItem, ViewMode } from "../../types";

defineProps<{
  errorMessage: string;
  loading: boolean;
  pageKind: "downloads" | "history";
  sourceProfiles: SourceProfile[];
  tasks: TaskItem[];
  viewMode: ViewMode;
}>();

const emit = defineEmits<{
  download: [taskId: string];
  remove: [taskId: string];
  "set-source": [payload: { taskId: string; sourceKey: string }];
}>();
</script>

<template>
  <section aria-labelledby="downloadsHeading">
    <TaskCollection
      :tasks="tasks"
      :view-mode="viewMode"
      :loading="loading"
      :page-kind="pageKind"
      :source-profiles="sourceProfiles"
      :error-message="errorMessage"
      @download="emit('download', $event)"
      @remove="emit('remove', $event)"
      @set-source="emit('set-source', $event)"
    />
  </section>
</template>
