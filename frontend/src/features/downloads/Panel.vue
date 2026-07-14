<script setup lang="ts">
import TaskCollection from "../../components/task/TaskCollection.vue";
import type { SourceProfile, TaskItem, ViewMode } from "../../types";

defineProps<{
  errorMessage: string;
  listKey: string;
  loading: boolean;
  pageKind: "downloads" | "history";
  sourceProfiles: SourceProfile[];
  tasks: TaskItem[];
  viewMode: ViewMode;
  hasMore?: boolean;
  fetchingMore?: boolean;
}>();

const emit = defineEmits<{
  cancel: [taskId: string];
  remove: [taskId: string];
  retry: [taskId: string];
  "set-source": [payload: { taskId: string; sourceKey: string }];
  "load-more": [];
}>();
</script>

<template>
  <section aria-labelledby="downloadsHeading">
    <TaskCollection
      :tasks="tasks"
      :view-mode="viewMode"
      :list-key="listKey"
      :loading="loading"
      :page-kind="pageKind"
      :source-profiles="sourceProfiles"
      :error-message="errorMessage"
      :has-more="hasMore"
      :fetching-more="fetchingMore"
      @cancel="emit('cancel', $event)"
      @remove="emit('remove', $event)"
      @retry="emit('retry', $event)"
      @set-source="emit('set-source', $event)"
      @load-more="emit('load-more')"
    />
  </section>
</template>
