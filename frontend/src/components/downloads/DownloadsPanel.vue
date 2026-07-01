<script setup lang="ts">
import TaskList from "../TaskList.vue";
import type { MenuKey, SourceProfile, TaskItem, ViewMode } from "../../types";

defineProps<{
  activeMenu: MenuKey;
  activeMenuLabel: string;
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
}>();
</script>

<template>
  <section aria-labelledby="downloadsHeading">
    <TaskList
      :tasks="tasks"
      :view-mode="viewMode"
      :active-menu="activeMenu"
      :active-menu-label="activeMenuLabel"
      :loading="loading"
      :page-kind="pageKind"
      :source-profiles="sourceProfiles"
      :error-message="errorMessage"
      @download="emit('download', $event)"
      @remove="emit('remove', $event)"
    />
  </section>
</template>
