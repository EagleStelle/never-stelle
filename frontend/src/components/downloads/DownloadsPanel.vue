<script setup lang="ts">
import IconGrid from "~icons/material-symbols/grid-view";
import IconList from "~icons/material-symbols/list";
import IconTrash from "~icons/material-symbols/delete";

import TaskList from "../TaskList.vue";
import { FILTER_LABELS } from "../../ui";
import type { MenuKey, TaskFilter, TaskItem, ViewMode } from "../../types";

const filterEntries = Object.entries(FILTER_LABELS) as Array<[TaskFilter, string]>;

defineProps<{
  activeFilter: TaskFilter;
  activeMenu: MenuKey;
  activeMenuLabel: string;
  errorMessage: string;
  loading: boolean;
  tasks: TaskItem[];
  viewMode: ViewMode;
}>();

const emit = defineEmits<{
  clearCompleted: [];
  clearPending: [];
  download: [taskId: string];
  hide: [taskId: string];
  remove: [taskId: string];
  setFilter: [filter: TaskFilter];
  setViewMode: [mode: ViewMode];
}>();
</script>

<template>
  <section class="border border-accent-subtle rounded-lg bg-panel p-[0.65rem]" aria-labelledby="downloadsHeading">
    <div class="flex flex-col gap-[0.7rem] mb-[0.65rem] md:flex-row md:items-end md:justify-between">
      <div>
        <span class="block mb-[0.25rem] text-text-muted text-[0.74rem] font-[800] tracking-[0.08em] uppercase">Downloads</span>
        <h2 id="downloadsHeading">{{ activeMenuLabel }}</h2>
      </div>

      <div class="flex flex-wrap items-center gap-[0.45rem]">
        <div class="inline-flex items-center min-w-0 gap-[0.25rem] border border-accent-subtle rounded-lg bg-panel-subtle p-[0.25rem] col-span-full order-3 md:col-auto md:row-auto md:order-0" aria-label="View mode">
          <button type="button" class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.35rem] w-[2.35rem] p-0 border border-transparent rounded-lg bg-transparent text-text-muted font-[800] leading-none whitespace-nowrap transition-all duration-[180ms] ease-out active:scale-[0.98] aria-pressed:bg-accent aria-pressed:text-bg" :aria-pressed="viewMode === 'grid'" aria-label="Grid view" @click="emit('setViewMode', 'grid')">
            <IconGrid class="w-[1.35rem] h-[1.35rem]" aria-hidden="true" />
          </button>
          <button type="button" class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.35rem] w-[2.35rem] p-0 border border-transparent rounded-lg bg-transparent text-text-muted font-[800] leading-none whitespace-nowrap transition-all duration-[180ms] ease-out active:scale-[0.98] aria-pressed:bg-accent aria-pressed:text-bg" :aria-pressed="viewMode === 'table'" aria-label="Table view" @click="emit('setViewMode', 'table')">
            <IconList class="w-[1.35rem] h-[1.35rem]" aria-hidden="true" />
          </button>
        </div>

        <div class="inline-flex items-center min-w-0 gap-[0.25rem] border border-accent-subtle rounded-lg bg-panel-subtle p-[0.25rem] col-span-full order-3 md:col-auto md:row-auto md:order-0" aria-label="Task filter">
          <button
              v-for="[filter, label] in filterEntries"
            :key="filter"
            type="button"
            class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.35rem] px-[0.7rem] text-[0.88rem] border border-transparent rounded-lg bg-transparent text-text-muted font-[800] leading-none whitespace-nowrap transition-all duration-[180ms] ease-out active:scale-[0.98] aria-pressed:bg-accent aria-pressed:text-bg"
            :aria-pressed="activeFilter === filter"
            @click="emit('setFilter', filter)"
          >
            {{ label }}
          </button>
        </div>

        <div class="flex items-center gap-[0.45rem] w-full md:w-auto">
          <button type="button" class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.55rem] px-[0.85rem] text-[0.9rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text-muted font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:border-accent hover:text-text md:flex-none border-red-600 bg-red-600 text-white hover:bg-red-700 hover:border-red-700 hover:text-white" @click="emit('clearCompleted')">
            <IconTrash aria-hidden="true" />
            <span>Done</span>
          </button>
          <button type="button" class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.55rem] px-[0.85rem] text-[0.9rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text-muted font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:border-accent hover:text-text md:flex-none border-red-600 bg-red-600 text-white hover:bg-red-700 hover:border-red-700 hover:text-white" @click="emit('clearPending')">
            <IconTrash aria-hidden="true" />
            <span>Queue</span>
          </button>
        </div>
      </div>
    </div>

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
