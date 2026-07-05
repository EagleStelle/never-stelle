<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconSpinner from "~icons/material-symbols/sync";
import IconX from "~icons/material-symbols/close";

import type { MenuKey, SourceProfile, TaskItem, ViewMode } from "../types";
import {
  faviconUrlForHost,
  hostFromUrl,
  sourceLabelFromKey,
} from "../utils/dashboard";

const props = defineProps<{
  tasks: TaskItem[];
  viewMode: ViewMode;
  activeMenu: MenuKey;
  activeMenuLabel: string;
  pageKind: "downloads" | "history";
  sourceProfiles?: SourceProfile[];
  loading?: boolean;
  errorMessage?: string;
}>();

const emit = defineEmits<{
  download: [taskId: string];
  remove: [taskId: string];
}>();

function progressPct(task: TaskItem): number {
  return Math.max(0, Math.min(100, Number(task.progress_pct) || 0));
}

function cardTitle(task: TaskItem): string {
  const filename = String(task.resolved_filename || "").trim();
  if (filename) return filename;
  const siteLabel = sourceLabel(task);
  const statusLabel = String(task.status_label || "Download")
    .trim()
    .toLowerCase();
  return siteLabel ? `${siteLabel} ${statusLabel}` : "Download";
}

function cardDetail(task: TaskItem): string {
  return String(task.source_url || task.vid || "").trim();
}

function sourceProfileFor(task: TaskItem): SourceProfile | undefined {
  const key = task.source_key || "others";
  return (props.sourceProfiles || []).find((profile) => profile.key === key);
}

function sourceLabel(task: TaskItem): string {
  return String(
    sourceProfileFor(task)?.label ||
      sourceLabelFromKey(task.source_key || "others"),
  ).trim();
}

function sourceIconUrl(task: TaskItem): string {
  const profile = sourceProfileFor(task);
  if (profile?.icon_url) return profile.icon_url;
  return faviconUrlForHost(hostFromUrl(String(task.source_url || ""))).trim();
}

function canShowCardCancel(task: TaskItem): boolean {
  return (
    props.pageKind === "downloads" &&
    task.can_remove &&
    task.status !== "running"
  );
}

function canShowHistoryDownload(task: TaskItem): boolean {
  return (
    props.pageKind === "history" &&
    task.can_download &&
    task.status !== "failed"
  );
}

function getStatusBadgeClass(status: string): string {
  if (status === "pending")
    return " bg-amber-600 text-white [.light-mode_&]:bg-amber-400 [.light-mode_&]:text-black";
  if (status === "running") return " bg-accent text-black";
  if (status === "completed") return " bg-accent text-black";
  if (status === "failed") return " bg-red-600 text-white [.light-mode_&]:bg-red-500 [.light-mode_&]:text-black";
  return " bg-secondary text-white [.light-mode_&]:text-black";
}

function getSiteBadgeClass(): string {
  return " bg-secondary text-white [.light-mode_&]:text-black";
}
</script>

<template>
  <section
    class="grid grid-cols-1 gap-2"
    :class="{ block: viewMode === 'table' }"
    aria-live="polite"
  >
    <div
      v-if="props.loading"
      class="rounded-2xl glass flex min-h-32 items-center justify-center gap-2 text-white [.light-mode_&]:text-black text-center"
    >
      <IconSpinner class="animate-spin text-accent" aria-hidden="true" />
      <span>Loading downloads...</span>
    </div>

    <div
      v-else-if="props.errorMessage"
      class="rounded-2xl glass flex min-h-32 items-center justify-center gap-2 text-center text-white [.light-mode_&]:text-black"
    >
      {{ props.errorMessage }}
    </div>

    <div
      v-else-if="viewMode === 'table'"
      class="w-full overflow-auto rounded-2xl glass"
    >
      <table
        class="w-full min-w-full border-separate border-spacing-0 overflow-hidden text-left rounded-2xl"
      >
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
              <span
                class="inline-flex max-w-full items-center gap-1.5 rounded-full px-2 py-1 tracking-wide leading-none uppercase"
                :class="getSiteBadgeClass()"
              >
                <img
                  v-if="sourceIconUrl(task)"
                  :src="sourceIconUrl(task)"
                  class="h-3.5 w-3.5 rounded-sm"
                  alt=""
                  aria-hidden="true"
                />
                {{ sourceLabel(task) }}
              </span>
            </td>
            <td>
              <div class="wrap-break-word">
                {{ task.source_url || task.vid }}
              </div>
            </td>
            <td class="text-white [.light-mode_&]:text-black">{{ task.resolved_folder }}</td>
            <td>{{ task.resolved_filename }}</td>
            <td>
              <span
                class="inline-flex max-w-full items-center rounded-full px-2 py-1 tracking-wide leading-none uppercase"
                :class="getStatusBadgeClass(task.status)"
              >
                {{ task.status_label }}
              </span>
            </td>
            <td>{{ progressPct(task) }}%</td>
            <td>
              <div class="flex items-center gap-1.5">
                <button
                  v-if="task.can_download && task.status !== 'failed'"
                  class="inline-flex items-center justify-center gap-1.5 min-h-8 rounded-lg leading-none transition-all duration-300 ease-glass active:scale-[0.96] w-8 h-8 glass-primary"
                  type="button"
                  aria-label="Download file"
                  title="Download file"
                  @click="emit('download', task.vid)"
                >
                  <IconDownload aria-hidden="true" />
                </button>
                <button
                  v-if="task.can_remove && task.status !== 'running'"
                  class="inline-flex items-center justify-center gap-1.5 min-h-8 rounded-lg leading-none transition-all duration-300 ease-glass active:scale-[0.96] w-8 h-8 glass-danger"
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
      <article
        v-for="task in props.tasks"
        :key="task.vid"
        class="glass-rise glass glass-hoverable rounded-2xl p-4 hover:-translate-y-0.5"
      >
        <div class="grid grid-cols-[minmax(0,1fr)_auto] gap-x-4 gap-y-4">
          <div class="min-w-0">
            <h3
              class="wrap-break-word leading-snug text-white [.light-mode_&]:text-black font-display font-semibold text-base tracking-tight"
            >
              <img
                v-if="sourceIconUrl(task)"
                :src="sourceIconUrl(task)"
                class="mr-1.5 inline h-4 w-4 rounded-sm align-[-2px]"
                alt=""
                aria-hidden="true"
              />
              {{ cardTitle(task) }}
            </h3>
            <div class="mt-2 break-all text-white [.light-mode_&]:text-black text-[0.8rem] font-mono">
              {{ cardDetail(task) }}
            </div>
            <div
              v-if="task.status === 'failed' && task.error"
              class="mt-2 wrap-break-word whitespace-pre-line text-white [.light-mode_&]:text-black"
            >
              {{ task.error }}
            </div>
          </div>

          <div class="flex items-start justify-end gap-1.5">
            <button
              v-if="canShowHistoryDownload(task)"
              class="inline-flex items-center justify-center gap-1.5 h-8 w-8 rounded-lg glass-soft glass-hoverable text-white [.light-mode_&]:text-black leading-none transition-all duration-300 ease-glass active:scale-[0.96] hover:text-white [.light-mode_&]:hover:text-black"
              type="button"
              aria-label="Download file"
              title="Download file"
              @click="emit('download', task.vid)"
            >
              <IconDownload aria-hidden="true" />
            </button>
            <button
              v-if="canShowCardCancel(task)"
              class="inline-flex items-center justify-center h-8 w-8 rounded-lg glass-soft glass-hoverable text-white [.light-mode_&]:text-black transition-all duration-300 ease-glass active:scale-[0.96] hover:text-white [.light-mode_&]:hover:text-black"
              type="button"
              aria-label="Cancel download"
              title="Cancel download"
              @click="emit('remove', task.vid)"
            >
              <IconX class="w-5 h-5" aria-hidden="true" />
            </button>
          </div>

          <div
            class="col-span-full grid grid-cols-[minmax(0,1fr)_3.5rem] items-center gap-4"
          >
            <div class="h-1.5 overflow-hidden rounded-full bg-secondary">
              <div
                class="h-full rounded-full bg-accent transition-all duration-500 ease-glass"
                :style="{ width: `${progressPct(task)}%` }"
              ></div>
            </div>
            <span class="text-right tabular-nums text-white [.light-mode_&]:text-black"
              >{{ progressPct(task) }}%</span
            >
          </div>
        </div>
      </article>
    </template>
  </section>
</template>
