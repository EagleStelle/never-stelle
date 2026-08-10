<script setup lang="ts">
import { computed, ref } from "vue";

import IconMovie from "~icons/material-symbols/movie";
import IconMusic from "~icons/material-symbols/music-note";
import IconTune from "~icons/material-symbols/tune";
import TaskFilters from "@/components/task/Filters.vue";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Combobox } from "@/components/ui/combobox";
import { DialogShell as Dialog } from "@/components/ui/dialog";
import { FieldGroup } from "@/components/ui/field";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "@/components/ui/segmented-control";
import UrlForm from "@/features/downloads/UrlForm.vue";
import { qualityFieldsFor, type QualityField } from "@/features/downloads/qualityFields";
import { useDashboard } from "@/composables/useDashboard";
import { useIsMobile } from "@/composables/useBreakpoints";
import type { QualitySelection } from "@/types";

const {
  downloadSelection: selection,
  downloadPostProcessing: postProcessing,
  qualityOptions,
  setDownloadQuality,
  setDownloadPostProcessing,
} = useDashboard();

const isMobile = useIsMobile();
const isAdvancedDialogOpen = ref(false);

const qualityFields = computed(() =>
  qualityFieldsFor(selection, qualityOptions.value),
);

const visibleQualityFields = computed(() =>
  qualityFields.value.filter((field) => !field.isAdvanced),
);

const advancedQualityFields = computed(() =>
  qualityFields.value.filter((field) => field.isAdvanced),
);

const hasAdvancedFields = computed(() => advancedQualityFields.value.length > 0);

function update(patch: Partial<QualitySelection>): void {
  setDownloadQuality({ ...selection, ...patch });
}

function setField(key: QualityField["key"], value: string): void {
  update({ [key]: value } as Partial<QualitySelection>);
}

function setMode(value: string | string[]): void {
  if (value === "video" || value === "audio") update({ mode: value });
}

function setPostProcessingSaveAs(value: string | string[]): void {
  if (value !== "sidecar" && value !== "embed") return;
  setDownloadPostProcessing({ ...postProcessing, save_as: value });
}
</script>

<template>
  <!-- Mobile reverses the rows so the URL field sits closest to the keyboard. -->
  <div class="flex flex-col-reverse lg:flex-col gap-3 w-full">
    <UrlForm />

    <div
      class="flex flex-col lg:flex-row items-stretch lg:items-center justify-between gap-3 w-full py-1"
    >
      <div
        class="flex items-center gap-3 overflow-x-auto no-scrollbar shrink-0 max-w-full"
      >
        <SegmentedControl
          v-if="qualityOptions.video.length"
          :model-value="selection.mode"
          @update:model-value="setMode"
          aria-label="Mode"
          class="shrink-0"
        >
          <SegmentedControlItem value="video" aria-label="Video" title="Video">
            <IconMovie class="w-3.5 h-3.5" aria-hidden="true" />
            <span class="hidden lg:inline">Video</span>
          </SegmentedControlItem>
          <SegmentedControlItem value="audio" aria-label="Audio" title="Audio">
            <IconMusic class="w-3.5 h-3.5" aria-hidden="true" />
            <span class="hidden lg:inline">Audio</span>
          </SegmentedControlItem>
        </SegmentedControl>

        <Combobox
          v-for="field in visibleQualityFields"
          :key="field.key"
          :model-value="selection[field.key]"
          :items="field.items"
          @update:model-value="(val) => setField(field.key, val)"
          class="shrink-0"
          :aria-label="field.label"
          :placeholder="field.placeholder"
          :empty-text="field.emptyText"
        />

        <Button
          v-if="hasAdvancedFields"
          type="button"
          aria-label="Advanced settings"
          title="Advanced settings"
          @click="isAdvancedDialogOpen = true"
        >
          <template #icon>
            <IconTune aria-hidden="true" />
          </template>
          <template v-if="!isMobile">Advanced Settings</template>
        </Button>
      </div>

      <TaskFilters class="shrink-0 lg:ml-auto overflow-x-auto no-scrollbar max-w-full" />
    </div>

    <Dialog
      v-model:open="isAdvancedDialogOpen"
      title="Advanced Settings"
      content-class="fixed left-1/2 top-1/2 z-70 flex w-[min(480px,96vw)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none"
    >
      <div class="p-5 sm:p-6">
        <FieldGroup class="gap-4">
          <Combobox
            v-for="field in advancedQualityFields"
            :key="field.key"
            :model-value="selection[field.key]"
            :items="field.items"
            @update:model-value="(val) => setField(field.key, val)"
            :label="field.label"
            label-placement="start"
            :placeholder="field.placeholder"
            :empty-text="field.emptyText"
            layout="fill"
          />
          <label class="flex items-center gap-2 cursor-pointer select-none text-sm">
            <Checkbox
              :checked="postProcessing.metadata"
              @update:checked="
                (value: boolean) =>
                  setDownloadPostProcessing({
                    ...postProcessing,
                    metadata: Boolean(value),
                  })
              "
            />
            <span>Metadata</span>
          </label>
          <label class="flex items-center gap-2 cursor-pointer select-none text-sm">
            <Checkbox
              :checked="postProcessing.thumbnail"
              @update:checked="
                (value: boolean) =>
                  setDownloadPostProcessing({
                    ...postProcessing,
                    thumbnail: Boolean(value),
                  })
              "
            />
            <span>Thumbnail</span>
          </label>
          <SegmentedControl
            :model-value="postProcessing.save_as"
            label="Save as"
            label-placement="start"
            @update:model-value="setPostProcessingSaveAs"
          >
            <SegmentedControlItem value="sidecar">Sidecar</SegmentedControlItem>
            <SegmentedControlItem value="embed">Embed</SegmentedControlItem>
          </SegmentedControl>
        </FieldGroup>
      </div>
    </Dialog>
  </div>
</template>
