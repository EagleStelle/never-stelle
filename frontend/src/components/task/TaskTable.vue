<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconX from "~icons/material-symbols/close";
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
import { sourceIconUrl, sourceLink, taskBackgroundStyle } from "../../utils/task";

const props = defineProps<{
  tasks: TaskItem[];
  sourceProfiles?: SourceProfile[];
}>();

const emit = defineEmits<{
  download: [taskId: string];
  remove: [taskId: string];
  "set-source": [payload: { taskId: string; sourceKey: string }];
}>();

const expandedFolders = reactive(new Set<string>());
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
        <TableHead class="w-px whitespace-nowrap">Source</TableHead>
        <TableHead class="w-px whitespace-nowrap">Folder</TableHead>
        <TableHead class="w-px whitespace-nowrap">Filename</TableHead>
        <TableHead class="w-px"></TableHead>
      </TableRow>
    </TableHeader>
    <TableBody>
      <TableRow v-for="task in props.tasks" :key="task.vid" class="glass-rise" :style="taskBackgroundStyle(task)">
        <TableCell class="w-px max-w-[12rem] sm:max-w-[18rem] lg:max-w-[24rem]">
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
                class="truncate block max-w-[12rem] md:max-w-[20rem] text-white [.light-mode_&]:text-black underline decoration-dotted underline-offset-2 hover:decoration-solid min-w-0"
                :title="task.source_url"
              >
                {{ task.source_url }}
              </a>
              <div v-else class="truncate block max-w-[12rem] md:max-w-[20rem] text-white [.light-mode_&]:text-black min-w-0" :title="task.source_url || task.vid">
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
        <TableCell class="w-px max-w-[10rem] md:max-w-[15rem] lg:max-w-[20rem] cursor-pointer" @click="toggle(expandedFolders, task.vid)">
          <div :class="['text-white [.light-mode_&]:text-black', expandedFolders.has(task.vid) ? 'break-all whitespace-normal' : 'truncate']" :title="task.resolved_folder">
            {{ task.resolved_folder }}
          </div>
        </TableCell>
        <TableCell class="w-full max-w-[0] cursor-pointer" @click="toggle(expandedFilenames, task.vid)">
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
</template>
