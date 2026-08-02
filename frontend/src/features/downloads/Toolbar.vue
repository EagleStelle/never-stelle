<script setup lang="ts">
import { computed } from "vue";

import IconMovie from "~icons/material-symbols/movie";
import IconMusic from "~icons/material-symbols/music-note";
import TaskFilters from "@/components/task/Filters.vue";
import { Combobox } from "@/components/ui/combobox";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "@/components/ui/segmented-control";
import UrlForm from "@/features/downloads/UrlForm.vue";
import { qualityFieldsFor, type QualityField } from "@/features/downloads/qualityFields";
import { useDashboard } from "@/composables/useDashboard";
import type { QualitySelection } from "@/types";

const {
  downloadSelection: selection,
  qualityOptions,
  setDownloadQuality,
} = useDashboard();

const qualityFields = computed(() =>
  qualityFieldsFor(selection, qualityOptions.value),
);

function update(patch: Partial<QualitySelection>): void {
  setDownloadQuality({ ...selection, ...patch });
}

function setField(key: QualityField["key"], value: string): void {
  update({ [key]: value } as Partial<QualitySelection>);
}

function setMode(value: string | string[]): void {
  if (value === "video" || value === "audio") update({ mode: value });
}
</script>

<template>
  <!-- Mobile reverses the rows so the URL field sits closest to the keyboard. -->
  <div class="flex flex-col-reverse lg:flex-col gap-2 w-full">
    <UrlForm />

    <div
      class="flex overflow-x-auto no-scrollbar items-center justify-between lg:justify-start gap-2 w-full py-1"
    >
      <div class="flex items-center gap-2 shrink-0">
        <SegmentedControl
          v-if="qualityOptions.video.length"
          :model-value="selection.mode"
          @update:model-value="setMode"
          aria-label="Media type"
          class="shrink-0"
        >
          <SegmentedControlItem value="video" aria-label="Video">
            <IconMovie class="w-4 h-4" aria-hidden="true" />
          </SegmentedControlItem>
          <SegmentedControlItem value="audio" aria-label="Audio">
            <IconMusic class="w-4 h-4" aria-hidden="true" />
          </SegmentedControlItem>
        </SegmentedControl>

        <Combobox
          v-for="field in qualityFields"
          :key="field.key"
          :model-value="selection[field.key]"
          :items="field.items"
          @update:model-value="(val) => setField(field.key, val)"
          class="shrink-0"
          :placeholder="field.placeholder"
          :empty-text="field.emptyText"
          :aria-label="field.label"
        />
      </div>

      <TaskFilters class="shrink-0 lg:ml-auto" />
    </div>
  </div>
</template>
