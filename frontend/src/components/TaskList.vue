<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconEyeClosed from "~icons/material-symbols/visibility-off";
import IconSpinner from "~icons/material-symbols/sync";
import IconX from "~icons/material-symbols/close";

import type { MenuKey, TaskItem, ViewMode } from "../types";

const props = defineProps<{
  tasks: TaskItem[];
  viewMode: ViewMode;
  activeMenu: MenuKey;
  activeMenuLabel: string;
  loading?: boolean;
  errorMessage?: string;
}>();

const emit = defineEmits<{
  download: [taskId: string];
  hide: [taskId: string];
  remove: [taskId: string];
}>();

function progressPct(task: TaskItem): number {
  return Math.max(0, Math.min(100, Number(task.progress_pct) || 0));
}

function getStatusBadgeClass(status: string): string {
  if (status === "pending") return "border-amber-500 bg-amber-500/20 text-amber-500 dark:border-amber-400 dark:bg-amber-400/20 dark:text-amber-400";
  if (status === "running") return "border-accent bg-accent/24 text-text";
  if (status === "completed") return "border-accent-subtle bg-accent-subtle/34 text-text";
  if (status === "failed") return "border-red-600 bg-red-600 text-white dark:border-red-500 dark:bg-red-500";
  return "border-accent-subtle bg-panel-subtle text-text-muted";
}

function getSiteBadgeClass(siteCategory: string): string {
  if (siteCategory === "youtube") return "border-accent bg-accent/24 text-text";
  if (siteCategory === "facebook") return "border-accent-subtle bg-accent-subtle/34 text-text";
  if (siteCategory === "instagram") return "border-accent bg-accent/24 text-text";
  if (siteCategory === "tiktok") return "border-accent-subtle bg-accent-subtle/34 text-text";
  return "border-accent-subtle bg-panel-subtle text-text-muted";
}
</script>

<template>
  <section class="grid grid-cols-[minmax(0,1fr)] gap-[0.5rem] sm:grid-cols-[repeat(2,minmax(0,1fr))]" :class="{ 'block': viewMode === 'table' }" aria-live="polite">
    <div v-if="props.loading" class="border border-dashed border-accent-subtle rounded-lg bg-panel flex min-h-[12rem] items-center justify-center gap-[0.55rem] text-text-muted font-[800] text-center">
      <IconSpinner class="animate-[ns-spin_900ms_linear_infinite]" aria-hidden="true" />
      <span>Loading downloads...</span>
    </div>

    <div v-else-if="props.errorMessage" class="border border-dashed border-accent-subtle rounded-lg bg-panel flex min-h-[12rem] items-center justify-center gap-[0.55rem] text-text-muted font-[800] text-center border-accent text-text">
      {{ props.errorMessage }}
    </div>

    <div v-else-if="viewMode === 'table'" class="w-full overflow-auto rounded-lg">
      <table class="w-full min-w-[58rem] border-separate border-spacing-0 overflow-hidden text-left border border-accent-subtle rounded-lg bg-panel">
        <thead>
          <tr>
            <th>Site</th>
            <th>Source</th>
            <th>Folder</th>
            <th>Filename</th>
            <th>Status</th>
            <th>Progress</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="task in props.tasks" :key="task.vid">
            <td>
              <span class="inline-flex max-w-full items-center border rounded-full px-[0.55rem] py-[0.32rem] text-[0.7rem] font-[900] tracking-[0.04em] leading-none uppercase" :class="getSiteBadgeClass(task.site_category)">
                {{ task.site_label }}
              </span>
            </td>
            <td>
              <div class="break-words">{{ task.source_url || task.vid }}</div>
            </td>
            <td class="text-text font-[800]">{{ task.resolved_folder }}</td>
            <td>{{ task.resolved_filename }}</td>
            <td>
              <span class="inline-flex max-w-full items-center border rounded-full px-[0.55rem] py-[0.32rem] text-[0.7rem] font-[900] tracking-[0.04em] leading-none uppercase" :class="getStatusBadgeClass(task.status)">
                {{ task.status_label }}
              </span>
            </td>
            <td>{{ progressPct(task) }}%</td>
            <td>
              <div class="flex items-center gap-[0.45rem]">
                <button v-if="task.can_hide" class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.45rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text-muted font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] w-[2.7rem] h-[2.7rem] hover:border-accent hover:text-text" type="button" aria-label="Clear task" title="Clear task" @click="emit('hide', task.vid)">
                  <IconEyeClosed aria-hidden="true" />
                </button>
                <button
                  v-if="task.can_download && task.status !== 'failed'"
                  class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.45rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text-muted font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] w-[2.7rem] h-[2.7rem] hover:border-accent hover:text-text border-text bg-accent text-bg hover:border-accent hover:bg-text hover:text-bg"
                  type="button"
                  aria-label="Download file"
                  title="Download file"
                  @click="emit('download', task.vid)"
                >
                  <IconDownload aria-hidden="true" />
                </button>
                <button
                  v-if="task.can_remove && task.status !== 'running'"
                  class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.45rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text-muted font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] w-[2.7rem] h-[2.7rem] hover:border-accent hover:text-text border-red-600 bg-red-600 text-white hover:bg-red-700 hover:border-red-700 hover:text-white"
                  type="button"
                  aria-label="Remove task from the list"
                  title="Remove task from the list"
                  @click="emit('remove', task.vid)"
                >
                  <IconX aria-hidden="true" />
                </button>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <template v-else>
      <article v-for="task in props.tasks" :key="task.vid" class="border border-accent-subtle rounded-lg bg-panel grid gap-[0.75rem] p-[0.85rem]">
        <div class="flex flex-wrap justify-between gap-[0.45rem]">
          <span class="inline-flex max-w-full items-center border rounded-full px-[0.55rem] py-[0.32rem] text-[0.7rem] font-[900] tracking-[0.04em] leading-none uppercase" :class="getSiteBadgeClass(task.site_category)">
            {{ task.site_label }}
          </span>
          <span class="inline-flex max-w-full items-center border rounded-full px-[0.55rem] py-[0.32rem] text-[0.7rem] font-[900] tracking-[0.04em] leading-none uppercase" :class="getStatusBadgeClass(task.status)">
            {{ task.status_label }}
          </span>
        </div>

        <div class="break-words text-text-muted">{{ task.source_url || task.vid }}</div>

        <div class="grid gap-[0.3rem]">
          <div v-if="task.resolved_folder" class="break-words font-[800]">{{ task.resolved_folder }}</div>
          <div v-if="task.resolved_filename">{{ task.resolved_filename }}</div>
          <div v-if="task.status === 'failed' && task.error" class="break-words whitespace-pre-line">
            {{ task.error }}
          </div>
        </div>

        <div class="grid gap-[0.35rem] text-text-muted font-[800] text-right">
          <div class="h-[0.45rem] overflow-hidden rounded-full bg-panel-subtle">
            <div class="h-full rounded-full bg-accent transition-[width] duration-[260ms] ease-out" :style="{ width: `${progressPct(task)}%` }"></div>
          </div>
          <span>{{ progressPct(task) }}%</span>
        </div>

        <div class="flex items-center gap-[0.45rem] flex-wrap justify-end">
          <button v-if="task.can_hide" class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.55rem] px-[0.85rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text-muted font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:border-accent hover:text-text md:flex-none" type="button" @click="emit('hide', task.vid)">
            <IconEyeClosed aria-hidden="true" />
            <span>Clear</span>
          </button>
          <button v-if="task.can_download && task.status !== 'failed'" class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.55rem] px-[0.85rem] border border-text rounded-lg bg-accent text-bg font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:border-accent hover:bg-text hover:text-bg" type="button" @click="emit('download', task.vid)">
            <IconDownload aria-hidden="true" />
            <span>Download</span>
          </button>
          <button v-if="task.can_remove && task.status !== 'running'" class="inline-flex items-center justify-center gap-[0.42rem] min-h-[2.55rem] px-[0.85rem] border border-accent-subtle rounded-lg bg-panel-subtle text-text-muted font-[800] leading-none transition-all duration-[180ms] ease-out active:scale-[0.98] hover:border-accent hover:text-text md:flex-none border-red-600 bg-red-600 text-white hover:bg-red-700 hover:border-red-700 hover:text-white" type="button" @click="emit('remove', task.vid)">
            <IconX aria-hidden="true" />
            <span>Remove</span>
          </button>
        </div>
      </article>
    </template>
  </section>
</template>
