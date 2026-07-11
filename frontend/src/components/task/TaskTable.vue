<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconX from "~icons/material-symbols/close";
import IconStop from "~icons/material-symbols/stop";
import IconRetry from "~icons/material-symbols/replay";
import { reactive } from "vue";

import { Button } from "../ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../ui/table";
import SourcePicker from "./SourcePicker.vue";

import type { SourceProfile, TaskItem } from "../../types";
import { formatSize, sourceIconUrl, sourceLink, taskBackgroundStyle } from "../../utils/task";

const props = defineProps<{
  tasks: TaskItem[];
  sourceProfiles?: SourceProfile[];
}>();

const emit = defineEmits<{
  cancel: [taskId: string];
  download: [taskId: string];
  remove: [taskId: string];
  retry: [taskId: string];
  "set-source": [payload: { taskId: string; sourceKey: string }];
}>();

const expandedFilenames = reactive(new Set<string>());

function toggle(set: Set<string>, id: string): void {
  if (set.has(id)) set.delete(id);
  else set.add(id);
}
</script>

<template>
  <Table>
    <TableHeader>
      <TableRow>
        <TableHead class="w-1/2 min-w-72 whitespace-nowrap">Name</TableHead>
        <TableHead class="w-px whitespace-nowrap">Creator</TableHead>
        <TableHead class="w-1/2 min-w-72 whitespace-nowrap">Source</TableHead>
        <TableHead class="w-px whitespace-nowrap">Size</TableHead>
        <TableHead class="w-px"></TableHead>
      </TableRow>
    </TableHeader>
    <TableBody>
      <TableRow v-for="task in props.tasks" :key="task.vid" class="glass-rise" :style="taskBackgroundStyle(task)">
        <TableCell class="w-1/2 max-w-0 min-w-72 cursor-pointer" @click="toggle(expandedFilenames, task.vid)">
          <div :class="['text-white in-[.light-mode]:text-black', expandedFilenames.has(task.vid) ? 'break-all whitespace-normal' : 'truncate']" :title="task.resolved_filename">
            {{ task.resolved_filename }}
          </div>
        </TableCell>
        <TableCell class="w-px max-w-40 md:max-w-60">
          <div class="truncate text-white in-[.light-mode]:text-black" :title="task.creator">
            {{ task.creator }}
          </div>
        </TableCell>
        <TableCell class="w-1/2 max-w-0 min-w-72">
          <div class="flex flex-col gap-1.5">
            <div class="flex items-center gap-2">
              <img
                v-if="sourceIconUrl(task, props.sourceProfiles)"
                :src="sourceIconUrl(task, props.sourceProfiles)"
                class="h-4 w-4 rounded-lg shrink-0"
                alt=""
                aria-hidden="true"
              />
              <a
                v-if="sourceLink(task)"
                :href="sourceLink(task)"
                target="_blank"
                rel="noopener noreferrer"
                class="truncate block min-w-0 text-white in-[.light-mode]:text-black underline decoration-dotted underline-offset-2 hover:decoration-solid"
                :title="task.source_url"
              >
                {{ task.source_url }}
              </a>
              <div v-else class="truncate block min-w-0 text-white in-[.light-mode]:text-black" :title="task.source_url || task.vid">
                {{ task.source_url || task.vid }}
              </div>
            </div>
            <SourcePicker
              v-if="task.source_pending"
              variant="row"
              :task="task"
              :source-profiles="props.sourceProfiles"
              @set-source="emit('set-source', $event)"
            />
          </div>
        </TableCell>
        <TableCell class="w-px whitespace-nowrap">
          <div class="text-white in-[.light-mode]:text-black tabular-nums">
            {{ formatSize(task.file_size) }}
          </div>
        </TableCell>
        <TableCell class="w-px">
          <div class="flex items-center justify-end gap-1.5">
            <Button
              v-if="task.can_download"
              variant="primary"
              size="xs"
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
              v-if="task.can_retry"
              variant="primary"
              size="xs"
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
              v-if="task.can_cancel"
              variant="danger"
              size="xs"
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
              v-if="task.can_remove"
              variant="danger"
              size="xs"
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
</template>
