<script setup lang="ts">
import { computed, onUnmounted, shallowRef, useTemplateRef, watch, watchEffect } from "vue";
import IconSpinner from "~icons/material-symbols/sync";

import TaskGrid from "@/components/task/TaskGrid.vue";
import TaskTable from "@/components/task/TaskTable.vue";
import type { Selection } from "@/composables/useSelection";

import type { SourceProfile, TaskItem, ViewMode } from "@/types";

// Covers the longest staggered rise: 300ms delay and a 640ms animation.
const RISE_MS = 1000;

const props = defineProps<{
  tasks: TaskItem[];
  viewMode: ViewMode;
  pageKind: "downloads" | "history" | "trackers";
  selection: Selection<TaskItem>;
  sourceProfiles?: SourceProfile[];
  loading?: boolean;
  errorMessage?: string;
  // Set by server-paginated callers (history and tracker items).
  hasMore?: boolean;
  fetchingMore?: boolean;
}>();

const emit = defineEmits<{
  "load-more": [];
}>();

const paged = computed(() => props.hasMore !== undefined);

// Items rise in when they join the list. Ones scrolled back into view appear at once.
const rising = shallowRef<ReadonlySet<string>>(new Set());
let listed = new Set<string>();
let riseTimer = 0;

watch(
  () => props.tasks,
  (tasks) => {
    const ids = new Set(tasks.map((task) => task.vid));
    const joined = [...ids].filter((id) => !listed.has(id));
    listed = ids;
    if (!joined.length) return;
    rising.value = new Set([...rising.value, ...joined]);
    window.clearTimeout(riseTimer);
    riseTimer = window.setTimeout(() => (rising.value = new Set()), RISE_MS);
  },
  { immediate: true },
);

const sentinel = useTemplateRef<HTMLElement>("sentinel");
let observer: IntersectionObserver | null = null;

watchEffect((onCleanup) => {
  const element = sentinel.value;
  if (!element || !props.hasMore || props.fetchingMore) return;

  let didRequestNextPage = false;
  observer = new IntersectionObserver(
    ([entry]) => {
      if (!entry?.isIntersecting || didRequestNextPage) return;
      didRequestNextPage = true;
      emit("load-more");
    },
    { rootMargin: "320px 0px" },
  );

  observer.observe(element);
  onCleanup(() => {
    observer?.disconnect();
    observer = null;
  });
});

onUnmounted(() => {
  observer?.disconnect();
  window.clearTimeout(riseTimer);
});
</script>

<template>
  <section>
    <div
      v-if="loading"
      role="status"
      class="rounded-lg glass flex min-h-32 items-center justify-center gap-2 text-white in-[.light-mode]:text-black text-center"
    >
      <IconSpinner class="animate-sync text-accent" aria-hidden="true" />
      <span>Loading downloads...</span>
    </div>

    <div
      v-else-if="errorMessage && tasks.length === 0"
      role="status"
      class="rounded-lg glass flex min-h-32 items-center justify-center gap-2 text-center text-white in-[.light-mode]:text-black"
    >
      {{ errorMessage }}
    </div>

    <template v-else>
      <TaskTable
        v-if="viewMode === 'table'"
        :tasks="tasks"
        :selection="selection"
        :source-profiles="sourceProfiles"
        :rising="rising"
      />

      <TaskGrid
        v-else
        :tasks="tasks"
        :selection="selection"
        :source-profiles="sourceProfiles"
        :rising="rising"
      />

      <div
        v-if="paged"
        ref="sentinel"
        :aria-hidden="!hasMore"
        class="min-h-10"
        :data-testid="pageKind === 'history' ? 'history-scroll-sentinel' : undefined"
      >
        <div
          v-if="fetchingMore"
          role="status"
          class="flex justify-center py-2 text-sm text-white/70 in-[.light-mode]:text-black/70"
        >
          Loading more...
        </div>
      </div>
    </template>
  </section>
</template>
