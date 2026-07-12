<script setup lang="ts">
import { computed, ref, useTemplateRef, watch } from "vue";
import { useIntersectionObserver } from "@vueuse/core";
import IconSpinner from "~icons/material-symbols/sync";

import TaskGrid from "./TaskGrid.vue";
import TaskTable from "./TaskTable.vue";

import type { SourceProfile, TaskItem, ViewMode } from "../../types";

const PAGE_SIZE = 30;

const props = defineProps<{
  tasks: TaskItem[];
  viewMode: ViewMode;
  pageKind: "downloads" | "history";
  listKey: string;
  sourceProfiles?: SourceProfile[];
  loading?: boolean;
  errorMessage?: string;
  // Set by server-paginated callers (history); undefined uses the client-side window.
  hasMore?: boolean;
}>();

const emit = defineEmits<{
  cancel: [taskId: string];
  download: [taskId: string];
  remove: [taskId: string];
  retry: [taskId: string];
  "set-source": [payload: { taskId: string; sourceKey: string }];
  "load-more": [];
}>();

const clientMode = computed(() => props.hasMore === undefined);
const visibleCount = ref(PAGE_SIZE);
const visibleTasks = computed(() => (clientMode.value ? props.tasks.slice(0, visibleCount.value) : props.tasks));
const showSentinel = computed(() =>
  clientMode.value ? visibleCount.value < props.tasks.length : Boolean(props.hasMore),
);

watch(() => props.listKey, () => (visibleCount.value = PAGE_SIZE));

const sentinel = useTemplateRef<HTMLElement>("sentinel");
useIntersectionObserver(
  sentinel,
  ([entry]) => {
    if (!entry.isIntersecting) return;
    if (clientMode.value) visibleCount.value += PAGE_SIZE;
    else emit("load-more");
  },
  { rootMargin: "0px 0px 400px 0px" },
);
</script>

<template>
  <section aria-live="polite">
    <div
      v-if="loading"
      class="rounded-lg glass flex min-h-32 items-center justify-center gap-2 text-white in-[.light-mode]:text-black text-center"
    >
      <IconSpinner class="animate-spin text-accent" aria-hidden="true" />
      <span>Loading downloads...</span>
    </div>

    <div
      v-else-if="errorMessage"
      class="rounded-lg glass flex min-h-32 items-center justify-center gap-2 text-center text-white in-[.light-mode]:text-black"
    >
      {{ errorMessage }}
    </div>

    <template v-else>
      <TaskTable
        v-if="viewMode === 'table'"
        :tasks="visibleTasks"
        :source-profiles="sourceProfiles"
        @cancel="emit('cancel', $event)"
        @download="emit('download', $event)"
        @remove="emit('remove', $event)"
        @retry="emit('retry', $event)"
        @set-source="emit('set-source', $event)"
      />

      <TaskGrid
        v-else
        :tasks="visibleTasks"
        :page-kind="pageKind"
        :source-profiles="sourceProfiles"
        @cancel="emit('cancel', $event)"
        @download="emit('download', $event)"
        @remove="emit('remove', $event)"
        @retry="emit('retry', $event)"
        @set-source="emit('set-source', $event)"
      />

      <div
        v-if="showSentinel"
        ref="sentinel"
        class="flex min-h-16 items-center justify-center gap-2 text-white in-[.light-mode]:text-black"
      >
        <IconSpinner class="animate-spin text-accent" aria-hidden="true" />
        <span class="text-sm">Loading more...</span>
      </div>
    </template>
  </section>
</template>
