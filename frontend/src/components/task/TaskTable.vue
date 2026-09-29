<script setup lang="ts">
import { computed, reactive, useTemplateRef } from "vue";

import { Checkbox } from "@/components/ui/checkbox";
import {
  Table,
  TableBody,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import TaskRow from "@/components/task/TaskRow.vue";

import type { Selection } from "@/composables/useSelection";
import { useVirtualRows } from "@/composables/useVirtualRows";
import type { SourceProfile, TaskItem } from "@/types";

const props = defineProps<{
  tasks: TaskItem[];
  selection: Selection<TaskItem>;
  sourceProfiles?: SourceProfile[];
  rising: ReadonlySet<string>;
}>();

const body = useTemplateRef<InstanceType<typeof TableBody>>("body");
const { rows, before, after, measure } = useVirtualRows(body, {
  count: computed(() => props.tasks.length),
  estimate: 48,
  overscan: 10,
});
const shown = computed(() => rows.value.map((row) => ({ task: props.tasks[row.index], index: row.index })));
const creators = computed(() => [...new Set(props.tasks.map((task) => task.creator).filter(Boolean))]);

const expandedFilenames = reactive(new Set<string>());

function toggleFilename(id: string): void {
  if (expandedFilenames.has(id)) expandedFilenames.delete(id);
  else expandedFilenames.add(id);
}
</script>

<template>
  <Table v-if="props.tasks.length > 0">
    <TableHeader>
      <TableRow>
        <TableHead class="w-px">
          <Checkbox
            :checked="props.selection.state"
            aria-label="Select all"
            title="Select all"
            @update:checked="props.selection.setAll"
          />
        </TableHead>
        <TableHead class="w-1/2 min-w-72 whitespace-nowrap">Name</TableHead>
        <TableHead class="w-px max-w-40 md:max-w-60 whitespace-nowrap">
          Creator
          <!-- Holds the column at the widest creator of every loaded row, not just the mounted ones. -->
          <div aria-hidden="true" class="invisible h-0 overflow-hidden font-normal">
            <div v-for="creator in creators" :key="creator">{{ creator }}</div>
          </div>
        </TableHead>
        <TableHead class="w-1/2 min-w-72 whitespace-nowrap">Source</TableHead>
        <TableHead class="w-px min-w-18 whitespace-nowrap">Size</TableHead>
        <TableHead class="w-px"></TableHead>
      </TableRow>
    </TableHeader>
    <TableBody ref="body">
      <tr v-if="before" aria-hidden="true">
        <td colspan="6" :style="{ height: `${before}px` }" />
      </tr>
      <TaskRow
        v-for="{ task, index } in shown"
        :key="task.vid"
        :ref="measure"
        :data-index="index"
        :task="task"
        :selection="props.selection"
        :source-profiles="props.sourceProfiles"
        :rising="props.rising.has(task.vid)"
        :expanded="expandedFilenames.has(task.vid)"
        @toggle-name="toggleFilename"
      />
      <tr v-if="after" aria-hidden="true">
        <td colspan="6" :style="{ height: `${after}px` }" />
      </tr>
    </TableBody>
  </Table>
</template>
