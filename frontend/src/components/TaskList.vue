<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconSpinner from "~icons/material-symbols/sync";
import IconX from "~icons/material-symbols/close";
import { reactive } from "vue";

import { Button } from "./ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "./ui/table";

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
  "set-source": [payload: { taskId: string; sourceKey: string }];
}>();

function progressPct(task: TaskItem): number {
  return Math.max(0, Math.min(100, Number(task.progress_pct) || 0));
}

function pickSource(task: TaskItem, sourceKey: string): void {
  const value = String(sourceKey || "").trim();
  if (value) emit("set-source", { taskId: task.vid, sourceKey: value });
}

function submitSource(task: TaskItem, event: Event): void {
  const form = event.target as HTMLFormElement;
  const input = form.elements.namedItem("source") as HTMLInputElement | null;
  const value = String(input?.value || "").trim();
  if (!value) return;
  pickSource(task, value);
  if (input) input.value = "";
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

function sourceLink(task: TaskItem): string {
  const url = String(task.source_url || "").trim();
  return /^https?:\/\//i.test(url) ? url : "";
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
  if (status === "failed")
    return " bg-red-600 text-white [.light-mode_&]:bg-red-500 [.light-mode_&]:text-black";
  return " bg-secondary text-white [.light-mode_&]:text-black";
}

function getSiteBadgeClass(): string {
  return " bg-secondary text-white [.light-mode_&]:text-black";
}

const expandedFolders = reactive(new Set<string>());
const expandedFilenames = reactive(new Set<string>());

function rowBackgroundStyle(task: TaskItem) {
  if (task.status === "failed") {
    return { background: "color-mix(in srgb, var(--color-red-500) 20%, transparent)" };
  }
  const pct = progressPct(task);
  if (task.status === "running" || (pct > 0 && pct < 100)) {
    return {
      background: `linear-gradient(to right, color-mix(in srgb, var(--accent) 20%, transparent) ${pct}%, transparent ${pct}%)`
    };
  }
  return {};
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
      class="rounded-lg glass flex min-h-32 items-center justify-center gap-2 text-white [.light-mode_&]:text-black text-center"
    >
      <IconSpinner class="animate-spin text-accent" aria-hidden="true" />
      <span>Loading downloads...</span>
    </div>

    <div
      v-else-if="props.errorMessage"
      class="rounded-lg glass flex min-h-32 items-center justify-center gap-2 text-center text-white [.light-mode_&]:text-black"
    >
      {{ props.errorMessage }}
    </div>

    <Table v-else-if="viewMode === 'table'">
      <TableHeader>
        <TableRow>
          <TableHead class="w-px whitespace-nowrap">Source</TableHead>
          <TableHead class="w-px whitespace-nowrap">Folder</TableHead>
          <TableHead class="w-px whitespace-nowrap">Filename</TableHead>
          <TableHead class="w-px"></TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        <TableRow v-for="task in props.tasks" :key="task.vid" :style="rowBackgroundStyle(task)">
          <TableCell class="w-px max-w-[12rem] sm:max-w-[18rem] lg:max-w-[24rem]">
            <div class="flex flex-col gap-1.5">
              <div class="flex items-center gap-2">
                <img
                  v-if="sourceIconUrl(task)"
                  :src="sourceIconUrl(task)"
                  class="h-4 w-4 rounded-lg shrink-0"
                  alt=""
                  aria-hidden="true"
                />
                <a
                  v-if="sourceLink(task)"
                  :href="sourceLink(task)"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="truncate block max-w-[12rem] md:max-w-[20rem] text-white [.light-mode_&]:text-black underline decoration-dotted underline-offset-2 hover:decoration-solid min-w-0"
                  :title="task.source_url"
                >
                  {{ task.source_url }}
                </a>
                <div v-else class="truncate block max-w-[12rem] md:max-w-[20rem] text-white [.light-mode_&]:text-black min-w-0" :title="task.source_url || task.vid">
                  {{ task.source_url || task.vid }}
                </div>
              </div>
              <form
                v-if="task.source_pending"
                class="flex items-center gap-1.5"
                @submit.prevent="submitSource(task, $event)"
              >
                <input
                  :list="`sources-row-${task.vid}`"
                  name="source"
                  type="text"
                  autocomplete="off"
                  placeholder="set source"
                  class="w-24 min-w-0 rounded-lg glass-soft px-2 py-1 text-[0.7rem] leading-none text-white [.light-mode_&]:text-black outline-none focus:ring-2 focus:ring-accent"
                />
                <datalist :id="`sources-row-${task.vid}`">
                  <option
                    v-for="profile in props.sourceProfiles || []"
                    :key="profile.key"
                    :value="profile.key"
                  >
                    {{ profile.label }}
                  </option>
                </datalist>
                <Button variant="primary" size="sm" type="submit">
                  Set
                </Button>
              </form>
            </div>
          </TableCell>
          <TableCell class="w-px max-w-[10rem] md:max-w-[15rem] lg:max-w-[20rem] cursor-pointer" @click="expandedFolders.has(task.vid) ? expandedFolders.delete(task.vid) : expandedFolders.add(task.vid)">
            <div :class="['text-white [.light-mode_&]:text-black', expandedFolders.has(task.vid) ? 'break-all whitespace-normal' : 'truncate']" :title="task.resolved_folder">
              {{ task.resolved_folder }}
            </div>
          </TableCell>
          <TableCell class="w-full max-w-[0] cursor-pointer" @click="expandedFilenames.has(task.vid) ? expandedFilenames.delete(task.vid) : expandedFilenames.add(task.vid)">
            <div :class="expandedFilenames.has(task.vid) ? 'break-all whitespace-normal' : 'truncate'" :title="task.resolved_filename">
              {{ task.resolved_filename }}
            </div>
          </TableCell>
          <TableCell class="w-px">
            <div class="flex items-center justify-end gap-1.5">
              <Button
                v-if="task.can_download && task.status !== 'failed'"
                variant="primary"
                size="sm"
                type="button"
                aria-label="Download file"
                title="Download file"
                @click="emit('download', task.vid)"
              >
                <template #icon>
                  <IconDownload aria-hidden="true" />
                </template>
              </Button>
              <Button
                v-if="task.can_remove && task.status !== 'running'"
                variant="danger"
                size="sm"
                type="button"
                aria-label="Remove task from the list"
                title="Remove task from the list"
                @click="emit('remove', task.vid)"
              >
                <template #icon>
                  <IconX aria-hidden="true" />
                </template>
              </Button>
            </div>
          </TableCell>
        </TableRow>
      </TableBody>
    </Table>

    <template v-else>
      <article
        v-for="task in props.tasks"
        :key="task.vid"
        class="glass-rise glass glass-hoverable rounded-lg p-4 hover:-translate-y-0.5 relative overflow-hidden"
      >
        <div class="absolute inset-0 pointer-events-none z-0" :style="rowBackgroundStyle(task)"></div>
        <div class="grid grid-cols-[minmax(0,1fr)_auto] gap-x-4 gap-y-4 relative z-10">
          <div class="min-w-0">
            <h3
              class="wrap-break-word leading-snug text-white [.light-mode_&]:text-black font-display font-semibold text-base tracking-tight"
            >
              <img
                v-if="sourceIconUrl(task)"
                :src="sourceIconUrl(task)"
                class="mr-1.5 inline h-4 w-4 rounded-lg align-[-2px]"
                alt=""
                aria-hidden="true"
              />
              {{ cardTitle(task) }}
            </h3>
            <a
              v-if="sourceLink(task)"
              :href="sourceLink(task)"
              target="_blank"
              rel="noopener noreferrer"
              class="mt-2 block break-all text-accent text-[0.8rem] font-mono underline decoration-dotted underline-offset-2 hover:decoration-solid"
            >
              {{ cardDetail(task) }}
            </a>
            <div
              v-else
              class="mt-2 break-all text-white [.light-mode_&]:text-black text-[0.8rem] font-mono"
            >
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
            <Button
              v-if="canShowHistoryDownload(task)"
              variant="soft"
              size="sm"
              type="button"
              aria-label="Download file"
              title="Download file"
              @click="emit('download', task.vid)"
            >
              <template #icon>
                <IconDownload aria-hidden="true" />
              </template>
            </Button>
            <Button
              v-if="canShowCardCancel(task)"
              variant="soft"
              size="sm"
              type="button"
              aria-label="Cancel download"
              title="Cancel download"
              @click="emit('remove', task.vid)"
            >
              <template #icon>
                <IconX class="w-5 h-5" aria-hidden="true" />
              </template>
            </Button>
          </div>

          <div
            v-if="task.source_pending"
            class="col-span-full flex flex-col gap-2 rounded-lg glass-soft p-3"
          >
            <span class="text-[0.8rem] text-white [.light-mode_&]:text-black">
              Unknown source — pick or type:
            </span>
            <div class="flex flex-wrap items-center gap-1.5">
              <Button
                v-for="candidate in task.source_candidates || []"
                :key="candidate"
                variant="soft"
                size="sm"
                type="button"
                class="rounded-lg"
                @click="pickSource(task, candidate)"
              >
                {{ sourceLabelFromKey(candidate) }}
              </Button>
              <form
                class="flex items-center gap-1.5"
                @submit.prevent="submitSource(task, $event)"
              >
                <input
                  :list="`sources-${task.vid}`"
                  name="source"
                  type="text"
                  autocomplete="off"
                  placeholder="e.g. youtube"
                  class="w-28 min-w-0 rounded-lg glass-soft px-2 py-1 text-[0.75rem] leading-none text-white [.light-mode_&]:text-black outline-none focus:ring-2 focus:ring-accent"
                />
                <datalist :id="`sources-${task.vid}`">
                  <option
                    v-for="profile in props.sourceProfiles || []"
                    :key="profile.key"
                    :value="profile.key"
                  >
                    {{ profile.label }}
                  </option>
                </datalist>
                <Button variant="primary" size="sm" type="submit">
                  Set
                </Button>
              </form>
            </div>
          </div>


        </div>
      </article>
    </template>
  </section>
</template>
