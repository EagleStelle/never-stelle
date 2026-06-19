<script setup lang="ts">
import { Download, EyeOff, X } from "@lucide/vue";

import type { MenuKey, TaskItem, ViewMode } from "../types";

const props = defineProps<{
  tasks: TaskItem[];
  viewMode: ViewMode;
  activeMenu: MenuKey;
  activeMenuLabel: string;
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
  if (status === "pending") {
    return "bg-[color-mix(in_srgb,var(--ns-warning)_10%,transparent)] text-[var(--ns-text)] border border-[color-mix(in_srgb,var(--ns-warning)_20%,transparent)]";
  }
  if (status === "running") {
    return "bg-[var(--ns-accent)]/10 text-[var(--ns-text)] border border-[color-mix(in_srgb,var(--ns-accent)_20%,transparent)]";
  }
  if (status === "completed") {
    return "bg-[color-mix(in_srgb,var(--ns-accent2)_10%,var(--ns-panel))] text-[color-mix(in_srgb,var(--ns-text)_88%,var(--ns-accent2))] border border-[color-mix(in_srgb,var(--ns-accent2)_18%,transparent)]";
  }
  return "bg-[var(--ns-soft-fill)] text-[var(--ns-text)] border border-[var(--ns-border)]";
}

function getSiteBadgeClass(siteCategory: string): string {
  if (siteCategory === "youtube") return "border border-[#ff3d3d]/30 bg-[#ff3d3d]/10 text-[var(--ns-text)]";
  if (siteCategory === "facebook") return "border border-[#6ea8ff]/30 bg-[#6ea8ff]/10 text-[var(--ns-text)]";
  if (siteCategory === "instagram") return "border border-[#ff8ad4]/30 bg-[#ff8ad4]/10 text-[var(--ns-text)]";
  if (siteCategory === "tiktok") return "border border-[#7ce7ff]/30 bg-[#7ce7ff]/10 text-[var(--ns-text)]";
  return "border border-[var(--ns-border)] bg-[var(--ns-soft-fill)] text-[var(--ns-muted)]";
}
</script>

<template>
  <section
    :class="viewMode === 'table' ? 'w-full overflow-x-auto' : 'grid w-full grid-cols-1 gap-4 md:grid-cols-2'"
    aria-live="polite"
  >
    <div
      v-if="!props.tasks.length"
      class="rounded-2xl border border-dashed border-[var(--ns-border)] bg-[var(--ns-panel)] px-5 py-10 text-center text-[var(--ns-muted)]"
    >
      No {{ activeMenuLabel }} tasks right now.
    </div>

    <table
      v-else-if="viewMode === 'table'"
      class="min-w-full overflow-hidden rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel)] text-left shadow-soft"
    >
      <thead>
        <tr class="border-b border-[var(--ns-border)] text-xs uppercase tracking-[0.08em] text-[var(--ns-muted)]">
          <th class="px-4 py-3">Site</th>
          <th class="px-4 py-3">Source</th>
          <th class="px-4 py-3">Folder</th>
          <th class="px-4 py-3">Filename</th>
          <th class="px-4 py-3">Status</th>
          <th class="px-4 py-3">Progress</th>
          <th class="px-4 py-3"></th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="task in props.tasks" :key="task.vid" class="border-b border-[var(--ns-border)] last:border-b-0">
          <td class="px-4 py-3 align-top">
            <span class="inline-flex rounded-xl px-3 py-1 text-xs font-bold" :class="getSiteBadgeClass(task.site_category)">
              {{ task.site_label }}
            </span>
          </td>
          <td class="px-4 py-3 align-top">
            <div class="break-all text-sm text-[var(--ns-muted)]">{{ task.source_url || task.vid }}</div>
          </td>
          <td class="px-4 py-3 align-top text-sm font-semibold">{{ task.resolved_folder }}</td>
          <td class="px-4 py-3 align-top text-sm">{{ task.resolved_filename }}</td>
          <td class="px-4 py-3 align-top">
            <span class="inline-flex rounded-xl px-3 py-1 text-xs font-bold" :class="getStatusBadgeClass(task.status)">
              {{ task.status_label }}
            </span>
          </td>
          <td class="px-4 py-3 align-top text-sm text-[var(--ns-muted)]">{{ progressPct(task) }}%</td>
          <td class="px-4 py-3 align-top text-right">
            <div class="inline-flex gap-2">
              <button
                v-if="task.can_hide"
                class="inline-flex h-9 w-9 items-center justify-center rounded-xl border-[1.5px] border-[var(--ns-border)] bg-[var(--ns-soft-fill)] text-[var(--ns-muted)] transition hover:-translate-y-px hover:border-[var(--ns-border-strong)] hover:text-[var(--ns-text)]"
                type="button"
                aria-label="Clear task"
                title="Clear task"
                @click="emit('hide', task.vid)"
              >
                <EyeOff class="h-4 w-4" />
              </button>
              <button
                v-if="task.can_download && task.status !== 'failed'"
                class="inline-flex h-9 w-9 items-center justify-center rounded-xl border-[1.5px] border-[var(--ns-action-border)] bg-[var(--ns-action-bg)] text-[var(--ns-action-text)] transition hover:-translate-y-px hover:border-[var(--ns-action-hover-border)] hover:bg-[var(--ns-action-hover-bg)] hover:text-[var(--ns-action-hover-text)]"
                type="button"
                aria-label="Download file"
                title="Download file"
                @click="emit('download', task.vid)"
              >
                <Download class="h-4 w-4" />
              </button>
              <button
                v-if="task.can_remove && task.status !== 'running'"
                class="inline-flex h-9 w-9 items-center justify-center rounded-xl border-[1.5px] border-[var(--ns-danger-border)] bg-[var(--ns-danger-bg)] text-[var(--ns-danger-text)] transition hover:-translate-y-px hover:border-[var(--ns-danger-hover-border)] hover:bg-[var(--ns-danger-hover-bg)] hover:text-[var(--ns-danger-hover-text)]"
                type="button"
                aria-label="Remove task from the list"
                title="Remove task from the list"
                @click="emit('remove', task.vid)"
              >
                <X class="h-4 w-4" />
              </button>
            </div>
          </td>
        </tr>
      </tbody>
    </table>

    <template v-else>
      <article
        v-for="task in props.tasks"
        :key="task.vid"
        class="w-full rounded-2xl border border-[var(--ns-border)] bg-[var(--ns-panel)] p-4 shadow-soft"
      >
        <div class="grid grid-cols-1 gap-3">
          <div class="grid grid-cols-[minmax(0,1fr)_auto] items-start gap-3">
            <div class="min-w-0">
              <span class="inline-flex max-w-full rounded-xl px-3 py-1 text-xs font-bold" :class="getSiteBadgeClass(task.site_category)">
                {{ task.site_label }}
              </span>
            </div>
            <div class="flex shrink-0 flex-wrap items-start justify-end gap-2">
              <span class="inline-flex rounded-xl px-3 py-1 text-xs font-bold" :class="getStatusBadgeClass(task.status)">
                {{ task.status_label }}
              </span>
              <button
                v-if="task.can_hide"
                class="inline-flex h-9 w-9 items-center justify-center rounded-xl border-[1.5px] border-[var(--ns-border)] bg-[var(--ns-soft-fill)] text-[var(--ns-muted)] transition hover:-translate-y-px hover:border-[var(--ns-border-strong)] hover:text-[var(--ns-text)]"
                type="button"
                aria-label="Clear task"
                title="Clear task"
                @click="emit('hide', task.vid)"
              >
                <EyeOff class="h-4 w-4" />
              </button>
              <button
                v-if="task.can_download && task.status !== 'failed'"
                class="inline-flex h-9 w-9 items-center justify-center rounded-xl border-[1.5px] border-[var(--ns-action-border)] bg-[var(--ns-action-bg)] text-[var(--ns-action-text)] transition hover:-translate-y-px hover:border-[var(--ns-action-hover-border)] hover:bg-[var(--ns-action-hover-bg)] hover:text-[var(--ns-action-hover-text)]"
                type="button"
                aria-label="Download file"
                title="Download file"
                @click="emit('download', task.vid)"
              >
                <Download class="h-4 w-4" />
              </button>
              <button
                v-if="task.can_remove && task.status !== 'running'"
                class="inline-flex h-9 w-9 items-center justify-center rounded-xl border-[1.5px] border-[var(--ns-danger-border)] bg-[var(--ns-danger-bg)] text-[var(--ns-danger-text)] transition hover:-translate-y-px hover:border-[var(--ns-danger-hover-border)] hover:bg-[var(--ns-danger-hover-bg)] hover:text-[var(--ns-danger-hover-text)]"
                type="button"
                aria-label="Remove task from the list"
                title="Remove task from the list"
                @click="emit('remove', task.vid)"
              >
                <X class="h-4 w-4" />
              </button>
            </div>
          </div>
          <div class="min-w-0 break-all text-sm text-[var(--ns-muted)]">{{ task.source_url || task.vid }}</div>
          <div class="grid gap-2">
            <div v-if="task.resolved_folder" class="break-all text-sm font-semibold">{{ task.resolved_folder }}</div>
            <div v-if="task.resolved_filename" class="break-all text-sm">{{ task.resolved_filename }}</div>
            <div v-if="task.status === 'failed' && task.error" class="whitespace-pre-line text-xs text-[var(--ns-text)]">
              {{ task.error }}
            </div>
          </div>
        </div>
        <div class="mt-4">
          <div class="h-2.5 overflow-hidden rounded-full bg-[var(--ns-soft-fill)]">
            <div class="h-full rounded-full bg-[var(--ns-accent)] transition-all" :style="{ width: `${progressPct(task)}%` }"></div>
          </div>
          <div class="mt-2 flex justify-end text-xs text-[var(--ns-muted)]">{{ progressPct(task) }}%</div>
        </div>
      </article>
    </template>
  </section>
</template>
