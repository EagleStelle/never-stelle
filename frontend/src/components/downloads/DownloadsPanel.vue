<script setup lang="ts">
import TaskList from "../TaskList.vue";
import type { MenuKey, TaskItem, ViewMode } from "../../types";

defineProps<{
  activeMenu: MenuKey;
  activeMenuLabel: string;
  errorMessage: string;
  loading: boolean;
  tasks: TaskItem[];
  viewMode: ViewMode;
}>();

const emit = defineEmits<{
  download: [taskId: string];
  hide: [taskId: string];
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
      :error-message="errorMessage"
      @download="emit('download', $event)"
      @hide="emit('hide', $event)"
      @remove="emit('remove', $event)"
    />
  </section>
</template>
