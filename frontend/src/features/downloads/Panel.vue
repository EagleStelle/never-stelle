<script setup lang="ts">
import { computed } from "vue";

import TaskCollection from "@/components/task/TaskCollection.vue";
import { useDashboard } from "@/composables/useDashboard";

const {
  activeMenu,
  activePage,
  activeTasks,
  cancelTask,
  completedTasks,
  historyError,
  historyFetchingMore,
  historyHasMore,
  historyLoading,
  loadMoreHistory,
  mediaFilter,
  removeTask,
  resolveTask,
  retryTask,
  setTaskSource,
  sourceProfiles,
  tasksErrorMessage,
  tasksLoading,
  viewMode,
} = useDashboard();

// Both pages render the same list; only the source of its rows differs.
const isHistory = computed(() => activePage.value === "history");
// Paging restarts from the top whenever the query behind the list changes.
const listKey = computed(
  () => `${activePage.value}|${activeMenu.value}|${mediaFilter.value}`,
);
</script>

<template>
  <section :aria-label="isHistory ? 'Download history' : 'Download queue'">
    <TaskCollection
      :tasks="isHistory ? completedTasks : activeTasks"
      :view-mode="viewMode"
      :list-key="listKey"
      :loading="isHistory ? historyLoading : tasksLoading"
      :page-kind="isHistory ? 'history' : 'downloads'"
      :source-profiles="sourceProfiles"
      :error-message="isHistory ? historyError : tasksErrorMessage"
      :has-more="isHistory ? historyHasMore : undefined"
      :fetching-more="isHistory ? historyFetchingMore : undefined"
      @cancel="cancelTask"
      @remove="removeTask"
      @resolve="resolveTask"
      @retry="retryTask"
      @set-source="setTaskSource"
      @load-more="loadMoreHistory"
    />
  </section>
</template>
