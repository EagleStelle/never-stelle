<script setup lang="ts">
import { computed } from "vue";

import { Button } from "@/components/ui/button";
import type { SourceProfile, TaskItem } from "@/types";
import { sourceLabelFromKey } from "@/utils/dashboard";

const props = defineProps<{
  task: TaskItem;
  sourceProfiles?: SourceProfile[];
  variant: "row" | "card";
}>();

const emit = defineEmits<{
  "set-source": [payload: { taskId: string; sourceKey: string }];
}>();

const selectableSourceProfiles = computed(() =>
  (props.sourceProfiles || []).filter((profile) => profile.key),
);

function pickSource(sourceKey: string): void {
  const value = String(sourceKey || "").trim();
  if (value) emit("set-source", { taskId: props.task.vid, sourceKey: value });
}

function submitSource(event: Event): void {
  const form = event.target as HTMLFormElement;
  const input = form.elements.namedItem("source") as HTMLInputElement | null;
  const value = String(input?.value || "").trim();
  if (!value) return;
  pickSource(value);
  if (input) input.value = "";
}
</script>

<template>
  <!-- Compact inline editor for table rows -->
  <form
    v-if="variant === 'row'"
    class="flex items-center gap-1.5"
    @submit.prevent="submitSource"
  >
    <input
      :list="`sources-row-${task.vid}`"
      name="source"
      type="text"
      autocomplete="off"
      placeholder="set source"
      class="w-24 min-w-0 rounded-lg glass-soft px-2 py-1 text-[0.7rem] leading-none text-white in-[.light-mode]:text-black outline-none focus:ring-2 focus:ring-accent"
    />
    <datalist :id="`sources-row-${task.vid}`">
      <option
        v-for="profile in selectableSourceProfiles"
        :key="profile.key"
        :value="profile.key"
      >
        {{ profile.label }}
      </option>
    </datalist>
    <Button variant="primary" type="submit">Set</Button>
  </form>

  <!-- Labelled editor with candidate chips for cards -->
  <div v-else class="col-span-full flex flex-col gap-2 rounded-lg glass-soft p-3">
    <span class="text-[0.8rem] text-white in-[.light-mode]:text-black">
      Unresolved source - pick or type:
    </span>
    <div class="flex flex-wrap items-center gap-1.5">
      <Button
        v-for="candidate in task.source_candidates || []"
        :key="candidate"
        variant="soft"
        type="button"
        class="rounded-lg"
        @click="pickSource(candidate)"
      >
        {{ sourceLabelFromKey(candidate) }}
      </Button>
      <form class="flex items-center gap-1.5" @submit.prevent="submitSource">
        <input
          :list="`sources-card-${task.vid}`"
          name="source"
          type="text"
          autocomplete="off"
          placeholder="e.g. youtube"
          class="w-28 min-w-0 rounded-lg glass-soft px-2 py-1 text-[0.75rem] leading-none text-white in-[.light-mode]:text-black outline-none focus:ring-2 focus:ring-accent"
        />
        <datalist :id="`sources-card-${task.vid}`">
          <option
            v-for="profile in selectableSourceProfiles"
            :key="profile.key"
            :value="profile.key"
          >
            {{ profile.label }}
          </option>
        </datalist>
        <Button variant="primary" type="submit">Set</Button>
      </form>
    </div>
  </div>
</template>
