<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconResolve from "~icons/material-symbols/cloud-sync";
import IconX from "~icons/material-symbols/close";
import IconStop from "~icons/material-symbols/stop";
import IconRetry from "~icons/material-symbols/replay";

import { Button } from "@/components/ui/button";
import {
  Card,
  CardAction,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import SourcePicker from "@/components/task/SourcePicker.vue";

import { taskFileUrl } from "@/api";
import type { SourceProfile, TaskItem } from "@/types";
import {
  sourceIconUrl,
  sourceLink,
  taskProgressState,
  taskProgressStyle,
  taskDetail,
  taskTitle,
} from "@/utils/task";

const props = defineProps<{
  tasks: TaskItem[];
  pageKind: "downloads" | "history";
  sourceProfiles?: SourceProfile[];
}>();

const emit = defineEmits<{
  cancel: [taskId: string];
  remove: [taskId: string];
  resolve: [taskId: string];
  retry: [taskId: string];
  "set-source": [payload: { taskId: string; sourceKey: string }];
}>();

function canDownload(task: TaskItem): boolean {
  return props.pageKind === "history" && task.can_download;
}

function canResolve(task: TaskItem): boolean {
  return props.pageKind === "history" && Boolean(task.can_resolve);
}

function canRetry(task: TaskItem): boolean {
  return props.pageKind === "downloads" && task.can_retry;
}

function canCancel(task: TaskItem): boolean {
  return props.pageKind === "downloads" && task.can_cancel;
}

function canRemove(task: TaskItem): boolean {
  return props.pageKind === "downloads" && task.can_remove;
}

function hasActions(task: TaskItem): boolean {
  return canResolve(task) || canDownload(task) || canRetry(task) || canCancel(task) || canRemove(task);
}
</script>

<template>
  <section class="grid grid-cols-1 gap-2">
    <Card
      v-for="task in props.tasks"
      :key="task.vid"
      class="glass-rise glass-hoverable hover:-translate-y-0.5 task-progress-surface task-progress-card"
      :data-task-state="taskProgressState(task)"
      :style="taskProgressStyle(task)"
    >
      <CardHeader>
        <CardTitle>
          <img
            v-if="sourceIconUrl(task, props.sourceProfiles)"
            :src="sourceIconUrl(task, props.sourceProfiles)"
            class="mr-1.5 inline h-4 w-4 rounded-lg align-[-2px]"
            alt=""
            aria-hidden="true"
          />
          {{ taskTitle(task, props.sourceProfiles) }}
        </CardTitle>
        <CardDescription>
          <a
            v-if="sourceLink(task)"
            :href="sourceLink(task)"
            target="_blank"
            rel="noopener noreferrer"
            class="block font-mono text-accent underline decoration-dotted underline-offset-2 hover:decoration-solid"
          >
            {{ taskDetail(task) }}
          </a>
          <span v-else class="block font-mono">
            {{ taskDetail(task) }}
          </span>
        </CardDescription>
        <CardAction v-if="hasActions(task)">
          <Button
            v-if="canResolve(task)"
            variant="primary"
            type="button"
            aria-label="Fetch info from source"
            title="Fetch info from source"
            @click="emit('resolve', task.vid)"
          >
            <template #icon>
              <IconResolve aria-hidden="true" />
            </template>
          </Button>
          <Button
            v-if="canDownload(task)"
            as="a"
            variant="primary"
            :href="taskFileUrl(task.vid)"
            download
            aria-label="Download file"
            title="Download file"
          >
            <template #icon>
              <IconDownload aria-hidden="true" />
            </template>
          </Button>
          <Button
            v-if="canRetry(task)"
            variant="primary"
            type="button"
            aria-label="Retry download"
            title="Retry download"
            @click="emit('retry', task.vid)"
          >
            <template #icon>
              <IconRetry aria-hidden="true" />
            </template>
          </Button>
          <Button
            v-if="canCancel(task)"
            variant="destructive"
            type="button"
            aria-label="Cancel download"
            title="Cancel download"
            @click="emit('cancel', task.vid)"
          >
            <template #icon>
              <IconStop aria-hidden="true" />
            </template>
          </Button>
          <Button
            v-if="canRemove(task)"
            variant="primary"
            type="button"
            aria-label="Remove task from the list"
            title="Remove task from the list"
            @click="emit('remove', task.vid)"
          >
            <template #icon>
              <IconX aria-hidden="true" />
            </template>
          </Button>
        </CardAction>
      </CardHeader>

      <CardContent
        v-if="(task.status === 'failed' && task.error) || task.source_pending"
      >
        <div
          v-if="task.status === 'failed' && task.error"
          class="wrap-break-word whitespace-pre-line"
        >
          {{ task.error }}
        </div>
        <SourcePicker
          v-if="task.source_pending"
          variant="card"
          :task="task"
          :source-profiles="props.sourceProfiles"
          @set-source="emit('set-source', $event)"
        />
      </CardContent>
    </Card>
  </section>
</template>
