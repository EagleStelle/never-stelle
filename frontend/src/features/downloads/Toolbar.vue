<script setup lang="ts">
import { computed, ref } from "vue";

import IconTune from "~icons/material-symbols/tune";
import TaskFilters from "@/components/task/Filters.vue";
import { Button } from "@/components/ui/button";
import { Combobox } from "@/components/ui/combobox";
import { DialogShell as Dialog } from "@/components/ui/dialog";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "@/components/ui/segmented-control";
import DownloadFields from "@/features/downloads/DownloadFields.vue";
import UrlForm from "@/features/downloads/UrlForm.vue";
import {
  MEDIA_MODE_ITEMS,
  quickQualityField,
  qualityFieldGroups,
  type QualityField,
} from "@/features/downloads/qualityFields";
import { useDashboard } from "@/composables/useDashboard";
import { useIsMobile } from "@/composables/useBreakpoints";
import type { PostProcessingSelection } from "@/types";
import { isMediaMode, postProcessingCapabilitiesForQuality } from "@/utils/dashboard";

const {
  downloadSelection: selection,
  downloadPostProcessing: postProcessing,
  qualityOptions,
  settings,
  setDownloadQuality,
  setDownloadPostProcessing,
} = useDashboard();

const isMobile = useIsMobile();
const isAdvancedDialogOpen = ref(false);

const qualityGroups = computed(() =>
  qualityFieldGroups(selection, qualityOptions.value),
);

const quickField = computed(() =>
  quickQualityField(qualityGroups.value, selection.mode),
);

const embedCapabilities = computed(() =>
  postProcessingCapabilitiesForQuality(selection, qualityOptions.value),
);

function setField(key: QualityField["key"], value: string): void {
  setDownloadQuality({ ...selection, [key]: value });
}

// A mode starts from its own defaults; toolbar edits are not kept per mode.
function setMode(value: string | string[]): void {
  if (isMediaMode(value)) setDownloadQuality(settings.default_quality[value]);
}

function setPostProcessing(next: PostProcessingSelection): void {
  // Keep the user's choices when switching mode, format, or save target. The
  // unsupported choices are filtered only when a task is queued, so returning
  // to a compatible video target restores the exact manual/auto subtitle state.
  setDownloadPostProcessing(next);
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
          <SegmentedControlItem
            v-for="item in MEDIA_MODE_ITEMS"
            :key="item.value"
            :value="item.value"
            :aria-label="item.label"
            :title="item.title"
          >
            <component :is="item.icon" class="w-3.5 h-3.5" aria-hidden="true" />
            <span class="hidden lg:inline">{{ item.label }}</span>
          </SegmentedControlItem>
        </SegmentedControl>

        <Combobox
          v-if="quickField"
          :key="quickField.key"
          :model-value="selection[quickField.key]"
          :items="quickField.items"
          @update:model-value="(val) => quickField && setField(quickField.key, val)"
          class="shrink-0"
          :aria-label="quickField.label"
          :placeholder="quickField.placeholder"
          :empty-text="quickField.emptyText"
        />

        <Button
          v-if="qualityGroups.length"
          type="button"
          variant="outline"
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
      content-class="fixed left-1/2 top-1/2 z-70 flex max-h-[85dvh] w-[min(740px,96vw)] -translate-x-1/2 -translate-y-1/2 flex-col overflow-hidden rounded-2xl border border-(--glass-border) bg-primary focus:outline-none"
    >
      <div class="min-h-0 flex-1 overflow-y-auto p-5 sm:p-6">
        <DownloadFields
          :selection="selection"
          :options="qualityOptions"
          :post-processing="postProcessing"
          :capabilities="embedCapabilities"
          @update:mode="setMode"
          @update:field="setField"
          @update:post-processing="setPostProcessing"
        />
      </div>
    </Dialog>
  </div>
</template>
