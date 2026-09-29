<script setup lang="ts">
import { computed } from "vue";

import TaskCollection from "@/components/task/TaskCollection.vue";
import { useDashboard } from "@/composables/useDashboard";

const {
  activePage,
  historyError,
  historyFetchingMore,
  historyHasMore,
  historyLoading,
  loadMoreHistory,
  pageTasks,
  sourceProfiles,
  taskSelection,
  tasksErrorMessage,
  tasksLoading,
  viewMode,
} = useDashboard();

// Both pages render the same list; only the source of its rows differs.
const isHistory = computed(() => activePage.value === "history");
</script>

<template>
  <section :aria-label="isHistory ? 'Download history' : 'Download queue'">
    <TaskCollection
      :tasks="pageTasks"
      :view-mode="viewMode"
      :selection="taskSelection"
      :loading="isHistory ? historyLoading : tasksLoading"
      :page-kind="isHistory ? 'history' : 'downloads'"
      :source-profiles="sourceProfiles"
      :error-message="isHistory ? historyError : tasksErrorMessage"
      :has-more="isHistory ? historyHasMore : undefined"
      :fetching-more="isHistory ? historyFetchingMore : undefined"
      @load-more="loadMoreHistory"
    />
  </section>
</template>
