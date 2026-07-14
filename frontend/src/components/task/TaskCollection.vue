<script setup lang="ts">
import { computed, onUnmounted, ref, useTemplateRef, watch, watchEffect } from "vue";
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
  fetchingMore?: boolean;
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
const canLoadMore = computed(() =>
  clientMode.value ? visibleCount.value < props.tasks.length : Boolean(props.hasMore),
);
const showSentinel = computed(() =>
  clientMode.value ? canLoadMore.value : props.pageKind === "history" || canLoadMore.value,
);

watch(() => props.listKey, () => (visibleCount.value = PAGE_SIZE));

const sentinel = useTemplateRef<HTMLElement>("sentinel");
let observer: IntersectionObserver | null = null;

watchEffect((onCleanup) => {
  const element = sentinel.value;
  if (!element || !canLoadMore.value || (!clientMode.value && props.fetchingMore)) return;

  let didRequestNextPage = false;
  observer = new IntersectionObserver(
    ([entry]) => {
      if (!entry?.isIntersecting || didRequestNextPage) return;
      didRequestNextPage = true;
      if (clientMode.value) visibleCount.value += PAGE_SIZE;
      else emit("load-more");
    },
    { rootMargin: "320px 0px" },
  );

  observer.observe(element);
  onCleanup(() => {
    observer?.disconnect();
    observer = null;
  });
});

onUnmounted(() => observer?.disconnect());
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
      v-else-if="errorMessage && tasks.length === 0"
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
        :aria-hidden="!canLoadMore"
        class="min-h-10"
        :data-testid="pageKind === 'history' ? 'history-scroll-sentinel' : undefined"
      >
        <div
          v-if="clientMode"
          class="flex min-h-10 items-center justify-center gap-2 text-white in-[.light-mode]:text-black"
        >
          <IconSpinner class="animate-spin text-accent" aria-hidden="true" />
          <span class="text-sm">Loading more...</span>
        </div>
        <div
          v-else-if="fetchingMore"
          role="status"
          class="flex justify-center py-2 text-sm text-white/70 in-[.light-mode]:text-black/70"
        >
          Loading more...
        </div>
      </div>
    </template>
  </section>
</template>
