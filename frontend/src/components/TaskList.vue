<script setup lang="ts">
import IconDownload from"~icons/material-symbols/download";
import IconSpinner from"~icons/material-symbols/sync";
import IconTrash from"~icons/material-symbols/delete";
import IconX from"~icons/material-symbols/close";

import type { MenuKey, TaskItem, ViewMode } from"../types";

const props = defineProps<{
 tasks: TaskItem[];
 viewMode: ViewMode;
 activeMenu: MenuKey;
 activeMenuLabel: string;
 pageKind:"downloads" |"history";
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

function cardTitle(task: TaskItem): string {
 const filename = String(task.resolved_filename ||"").trim();
 if (filename) return filename;
 const siteLabel = String(task.site_label ||"").trim();
 const statusLabel = String(task.status_label ||"Download").trim().toLowerCase();
 return siteLabel ? `${siteLabel} ${statusLabel}` :"Download";
}

function cardDetail(task: TaskItem): string {
 return String(task.source_url || task.vid ||"").trim();
}

function canShowCardCancel(task: TaskItem): boolean {
 return props.pageKind ==="downloads" && task.can_remove && task.status !=="running";
}

function canShowHistoryDelete(task: TaskItem): boolean {
 return props.pageKind ==="history" && task.can_hide;
}

function canShowHistoryDownload(task: TaskItem): boolean {
 return props.pageKind ==="history" && task.can_download && task.status !=="failed";
}

function getStatusBadgeClass(status: string): string {
 if (status ==="pending") return" bg-amber-500/20 text-amber-500 dark:bg-amber-400/20 dark:text-amber-400";
 if (status ==="running") return" bg-primary/20 text-text";
 if (status ==="completed") return" bg-primary-subtle/30 text-text";
 if (status ==="failed") return" bg-red-600 text-white dark:bg-red-500";
 return" bg-secondary-muted text-text-muted";
}

function getSiteBadgeClass(siteCategory: string): string {
 if (siteCategory ==="youtube") return" bg-primary/20 text-text";
 if (siteCategory ==="facebook") return" bg-primary-subtle/30 text-text";
 if (siteCategory ==="instagram") return" bg-primary/20 text-text";
 if (siteCategory ==="tiktok") return" bg-primary-subtle/30 text-text";
 return" bg-secondary-muted text-text-muted";
}
</script>

<template>
 <section class="grid grid-cols-1 gap-2" :class="{'block': viewMode ==='table'}" aria-live="polite">
 <div v-if="props.loading" class="rounded-2xl glass flex min-h-32 items-center justify-center gap-2 text-text-muted text-center">
 <IconSpinner class="animate-spin text-primary" aria-hidden="true" />
 <span>Loading downloads...</span>
 </div>

 <div v-else-if="props.errorMessage" class="rounded-2xl glass flex min-h-32 items-center justify-center gap-2 text-center text-text">
 {{ props.errorMessage }}
 </div>

 <div v-else-if="viewMode ==='table'" class="w-full overflow-auto rounded-2xl glass">
 <table class="w-full min-w-full border-separate border-spacing-0 overflow-hidden text-left rounded-2xl">
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
 <span class="inline-flex max-w-full items-center rounded-full px-2 py-1 tracking-wide leading-none uppercase" :class="getSiteBadgeClass(task.site_category)">
 {{ task.site_label }}
 </span>
 </td>
 <td>
 <div class="break-words">{{ task.source_url || task.vid }}</div>
 </td>
 <td class="text-text">{{ task.resolved_folder }}</td>
 <td>{{ task.resolved_filename }}</td>
 <td>
 <span class="inline-flex max-w-full items-center rounded-full px-2 py-1 tracking-wide leading-none uppercase" :class="getStatusBadgeClass(task.status)">
 {{ task.status_label }}
 </span>
 </td>
 <td>{{ progressPct(task) }}%</td>
 <td>
 <div class="flex items-center gap-1.5">
 <button
 v-if="props.pageKind ==='history'&& task.can_hide"
 class="inline-flex items-center justify-center gap-1.5 min-h-8 rounded-lg glass-soft glass-hoverable text-text-muted leading-none transition-all duration-300 ease-glass active:scale-[0.97] h-8 px-2.5 hover:text-text"
 type="button"
 aria-label="Delete from history"
 title="Delete from history"
 @click="emit('hide', task.vid)"
 >
 <IconTrash aria-hidden="true" />
 <span>Delete</span>
 </button>
 <button
 v-if="task.can_download && task.status !=='failed'"
 class="inline-flex items-center justify-center gap-1.5 min-h-8 rounded-lg leading-none transition-all duration-300 ease-glass active:scale-[0.96] w-8 h-8 bg-primary text-text-on-primary shadow-[0_8px_22px_-10px_rgba(237,158,89,0.7)] hover:shadow-[0_12px_28px_-8px_rgba(237,158,89,0.9)]"
 type="button"
 aria-label="Download file"
 title="Download file"
 @click="emit('download', task.vid)"
 >
 <IconDownload aria-hidden="true" />
 </button>
 <button
 v-if="task.can_remove && task.status !=='running'"
 class="inline-flex items-center justify-center gap-1.5 min-h-8 rounded-lg leading-none transition-all duration-300 ease-glass active:scale-[0.96] w-8 h-8 bg-red-500/90 text-white hover:bg-red-500"
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
 <article v-for="task in props.tasks" :key="task.vid" class="glass-rise glass glass-hoverable rounded-2xl p-4 hover:-translate-y-0.5">
 <div class="grid grid-cols-[minmax(0,1fr)_auto] gap-x-4 gap-y-4">
 <div class="min-w-0">
 <h3 class="wrap-break-word leading-snug text-text font-display font-semibold text-base tracking-tight">{{ cardTitle(task) }}</h3>
 <div class="mt-2 break-all text-text-muted text-[0.8rem] font-mono">{{ cardDetail(task) }}</div>
 <div v-if="task.status ==='failed'&& task.error" class="mt-2 break-words whitespace-pre-line text-text-muted">
 {{ task.error }}
 </div>
 </div>

 <div class="flex items-start justify-end gap-1.5">
 <button
 v-if="canShowHistoryDownload(task)"
 class="inline-flex items-center justify-center gap-1.5 h-8 w-8 rounded-lg glass-soft glass-hoverable text-text-muted leading-none transition-all duration-300 ease-glass active:scale-[0.96] hover:text-text"
 type="button"
 aria-label="Download file"
 title="Download file"
 @click="emit('download', task.vid)"
 >
 <IconDownload aria-hidden="true" />
 </button>
 <button
 v-if="canShowHistoryDelete(task)"
 class="inline-flex items-center justify-center gap-1.5 h-8 px-3 rounded-lg glass-soft glass-hoverable text-text-muted leading-none transition-all duration-300 ease-glass active:scale-[0.97] hover:text-text"
 type="button"
 aria-label="Delete from history"
 title="Delete from history"
 @click="emit('hide', task.vid)"
 >
 <IconTrash aria-hidden="true" />
 <span>Delete</span>
 </button>
 <button
 v-if="canShowCardCancel(task)"
 class="inline-flex items-center justify-center h-8 w-8 rounded-lg text-text-muted transition-all duration-300 ease-glass active:scale-[0.96] hover:bg-white/10 hover:text-text"
 type="button"
 aria-label="Cancel download"
 title="Cancel download"
 @click="emit('remove', task.vid)"
 >
 <IconX class="w-5 h-5" aria-hidden="true" />
 </button>
 </div>

 <div class="col-span-full grid grid-cols-[minmax(0,1fr)_3.5rem] items-center gap-4">
 <div class="h-1.5 overflow-hidden rounded-full bg-white/10">
 <div class="h-full rounded-full bg-primary shadow-[0_0_12px_0_rgba(237,158,89,0.6)] transition-all duration-500 ease-glass" :style="{ width: `${progressPct(task)}%` }"></div>
 </div>
 <span class="text-right tabular-nums text-text-muted">{{ progressPct(task) }}%</span>
 </div>
 </div>
 </article>
 </template>
 </section>
</template>
