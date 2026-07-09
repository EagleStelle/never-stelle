<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconX from "~icons/material-symbols/close";

import { Button } from "../ui/button";
import SourcePicker from "./SourcePicker.vue";

import type { SourceProfile, TaskItem } from "../../types";
import {
  sourceIconUrl,
  sourceLink,
  taskBackgroundStyle,
  taskDetail,
  taskTitle,
} from "../../utils/task";

const props = defineProps<{
  tasks: TaskItem[];
  pageKind: "downloads" | "history";
  sourceProfiles?: SourceProfile[];
}>();

const emit = defineEmits<{
  download: [taskId: string];
  remove: [taskId: string];
  "set-source": [payload: { taskId: string; sourceKey: string }];
}>();

function canShowCancel(task: TaskItem): boolean {
  return props.pageKind === "downloads" && task.can_remove && task.status !== "running";
}

function canShowDownload(task: TaskItem): boolean {
  return props.pageKind === "history" && task.can_download && task.status !== "failed";
}
</script>

<template>
  <section class="grid grid-cols-1 gap-2">
    <article
      v-for="task in props.tasks"
      :key="task.vid"
      class="glass-rise glass glass-hoverable rounded-lg p-4 hover:-translate-y-0.5 relative overflow-hidden"
    >
      <div class="absolute inset-0 pointer-events-none z-0" :style="taskBackgroundStyle(task)"></div>
      <div class="grid grid-cols-[minmax(0,1fr)_auto] gap-x-4 gap-y-4 relative z-10">
        <div class="min-w-0">
          <h3
            class="wrap-break-word leading-snug text-white [.light-mode_&]:text-black font-display font-semibold text-base tracking-tight"
          >
            <img
              v-if="sourceIconUrl(task, props.sourceProfiles)"
              :src="sourceIconUrl(task, props.sourceProfiles)"
              class="mr-1.5 inline h-4 w-4 rounded-lg align-[-2px]"
              alt=""
              aria-hidden="true"
            />
            {{ taskTitle(task, props.sourceProfiles) }}
          </h3>
          <a
            v-if="sourceLink(task)"
            :href="sourceLink(task)"
            target="_blank"
            rel="noopener noreferrer"
            class="mt-2 block break-all text-accent text-[0.8rem] font-mono underline decoration-dotted underline-offset-2 hover:decoration-solid"
          >
            {{ taskDetail(task) }}
          </a>
          <div
            v-else
            class="mt-2 break-all text-white [.light-mode_&]:text-black text-[0.8rem] font-mono"
          >
            {{ taskDetail(task) }}
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
            v-if="canShowDownload(task)"
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
            v-if="canShowCancel(task)"
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

        <SourcePicker
          v-if="task.source_pending"
          variant="card"
          :task="task"
          :source-profiles="props.sourceProfiles"
          @set-source="emit('set-source', $event)"
        />
      </div>
    </article>
  </section>
</template>
