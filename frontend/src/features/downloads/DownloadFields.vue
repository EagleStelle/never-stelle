<script setup lang="ts">
import { computed, ref } from "vue";
import IconInfo from "~icons/material-symbols/info-outline";

import { Checkbox } from "@/components/ui/checkbox";
import { Combobox } from "@/components/ui/combobox";
import {
  Field,
  FieldContent,
  FieldGroup,
  FieldLabel,
  FieldLegend,
  FieldSet,
  FieldTitle,
} from "@/components/ui/field";
import { Label } from "@/components/ui/label";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "@/components/ui/segmented-control";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  MEDIA_MODE_ITEMS,
  qualityFieldGroups,
  type QualityField,
} from "@/features/downloads/qualityFields";
import type {
  MediaMode,
  PostProcessingMode,
  PostProcessingSelection,
  QualityOptions,
  QualitySelection,
} from "@/types";
import {
  isMediaMode,
  POST_PROCESSING_FIELDS,
  SUBTITLE_LANGUAGE_OPTIONS,
  subtitleLanguageMode,
  subtitleLanguagesForMode,
  type PostProcessingCapabilities,
  type PostProcessingCapability,
  type SubtitleLanguageMode,
} from "@/utils/dashboard";

type Destination = "sidecar" | "embed";

const OPTIONS: Array<{ key: "split_chapters" | "mtime"; label: string; help: string }> = [
  {
    key: "split_chapters",
    label: "Split chapters",
    help: "Also saves each chapter as its own file, in a folder named after the download.",
  },
  {
    key: "mtime",
    label: "Set mtime",
    help: "Sets each file's modified date to when it was uploaded.",
  },
];

const LANGUAGE_MODES: Array<{ value: SubtitleLanguageMode; label: string }> = [
  { value: "original", label: "Original" },
  { value: "all", label: "All" },
  { value: "custom", label: "Custom" },
];

const props = defineProps<{
  selection: QualitySelection;
  options: QualityOptions;
  postProcessing: PostProcessingSelection;
  capabilities: PostProcessingCapabilities;
}>();

const emit = defineEmits<{
  "update:mode": [mode: MediaMode];
  "update:field": [key: QualityField["key"], value: string];
  "update:postProcessing": [value: PostProcessingSelection];
}>();

const groups = computed(() => qualityFieldGroups(props.selection, props.options));

// A segmented control clears its value when the active item is clicked again.
function setMode(value: string | string[]): void {
  if (isMediaMode(value)) emit("update:mode", value);
}

const subtitlesRequested = computed(
  () =>
    props.postProcessing.subtitles !== "off" ||
    props.postProcessing.automatic_subtitles !== "off",
);

// Custom stays selected until its first language is picked.
const customSelected = ref(false);

const languageMode = computed<SubtitleLanguageMode>(() => {
  const languages = props.postProcessing.subtitle_languages;
  return customSelected.value && !languages.length ? "custom" : subtitleLanguageMode(languages);
});

function update(patch: Partial<PostProcessingSelection>): void {
  emit("update:postProcessing", { ...props.postProcessing, ...patch });
}

function isChecked(key: PostProcessingCapability, destination: Destination): boolean {
  const mode = props.postProcessing[key];
  return mode === destination || mode === "both";
}

function toggle(
  key: PostProcessingCapability,
  destination: Destination,
  checked: boolean,
): void {
  const sidecar = destination === "sidecar" ? checked : isChecked(key, "sidecar");
  const embed = destination === "embed" ? checked : isChecked(key, "embed");
  const mode: PostProcessingMode =
    sidecar && embed ? "both" : sidecar ? "sidecar" : embed ? "embed" : "off";
  update({ [key]: mode });
}

function setLanguageMode(value: string | string[]): void {
  const mode = LANGUAGE_MODES.find((item) => item.value === value)?.value;
  if (!mode) return;
  customSelected.value = mode === "custom";
  update({ subtitle_languages: subtitleLanguagesForMode(mode) });
}
</script>

<template>
  <TooltipProvider>
    <div class="flex flex-col gap-6">
      <SegmentedControl
        v-if="options.video.length"
        :model-value="selection.mode"
        label="Mode"
        label-placement="start"
        @update:model-value="setMode"
      >
        <SegmentedControlItem
          v-for="item in MEDIA_MODE_ITEMS"
          :key="item.value"
          :value="item.value"
          :aria-label="item.label"
          :title="item.title"
        >
          <!-- Phones drop the icons so all three labels fit on one row. -->
          <component :is="item.icon" class="hidden size-3.5 sm:block" aria-hidden="true" />
          <span>{{ item.label }}</span>
        </SegmentedControlItem>
      </SegmentedControl>

      <FieldSet v-for="group in groups" :key="group.legend">
        <FieldLegend variant="divider">{{ group.legend }}</FieldLegend>
        <FieldGroup>
          <Combobox
            v-for="field in group.fields"
            :key="field.key"
            :model-value="selection[field.key]"
            :items="field.items"
            :label="field.label"
            label-placement="start"
            :placeholder="field.placeholder"
            :empty-text="field.emptyText"
            @update:model-value="(value: string) => emit('update:field', field.key, value)"
          />
        </FieldGroup>
      </FieldSet>

      <FieldSet>
        <FieldLegend variant="divider">Post-Processing</FieldLegend>
        <div class="grid grid-cols-[1fr_auto_auto] items-center gap-x-6 gap-y-3">
          <span aria-hidden="true" />
          <span class="justify-self-center text-sm font-medium text-muted-foreground">Sidecar</span>
          <span class="justify-self-center text-sm font-medium text-muted-foreground">Embed</span>
          <template v-for="field in POST_PROCESSING_FIELDS" :key="field.key">
            <Label>{{ field.label }}</Label>
            <FieldLabel class="cursor-pointer justify-self-center">
              <Checkbox
                :checked="isChecked(field.key, 'sidecar')"
                @update:checked="(value: boolean) => toggle(field.key, 'sidecar', value)"
              />
              <span class="sr-only">{{ field.label }} sidecar</span>
            </FieldLabel>
            <FieldLabel
              :class="
                capabilities[field.key]
                  ? 'cursor-pointer justify-self-center'
                  : 'cursor-not-allowed justify-self-center opacity-60'
              "
            >
              <Checkbox
                :checked="isChecked(field.key, 'embed')"
                :disabled="!capabilities[field.key]"
                @update:checked="(value: boolean) => toggle(field.key, 'embed', value)"
              />
              <span class="sr-only">{{ field.label }} embed</span>
            </FieldLabel>
          </template>
        </div>

        <div class="flex flex-wrap items-center gap-x-6 gap-y-3">
          <FieldLabel
            v-for="option in OPTIONS"
            :key="option.key"
            class="cursor-pointer items-center gap-2"
          >
            <Checkbox
              :checked="postProcessing[option.key]"
              @update:checked="(value: boolean) => update({ [option.key]: value })"
            />
            <span>{{ option.label }}</span>
            <Tooltip>
              <TooltipTrigger as-child>
                <button
                  type="button"
                  class="-m-1 inline-flex size-6 items-center justify-center rounded-md text-muted-foreground transition-colors duration-200 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
                  :aria-label="`${option.label} help`"
                >
                  <IconInfo class="size-4" aria-hidden="true" />
                </button>
              </TooltipTrigger>
              <TooltipContent side="top">
                {{ option.help }}
              </TooltipContent>
            </Tooltip>
          </FieldLabel>
        </div>

        <Field :data-disabled="subtitlesRequested ? undefined : 'true'">
          <FieldTitle>Subtitle languages</FieldTitle>
          <FieldContent class="flex-row items-center gap-2">
            <SegmentedControl
              :model-value="languageMode"
              :disabled="!subtitlesRequested"
              aria-label="Subtitle languages"
              @update:model-value="setLanguageMode"
            >
              <SegmentedControlItem
                v-for="mode in LANGUAGE_MODES"
                :key="mode.value"
                :value="mode.value"
              >
                {{ mode.label }}
              </SegmentedControlItem>
            </SegmentedControl>
            <Combobox
              v-if="languageMode === 'custom'"
              multiple
              layout="fill"
              :model-value="postProcessing.subtitle_languages"
              :items="SUBTITLE_LANGUAGE_OPTIONS"
              :disabled="!subtitlesRequested"
              aria-label="Custom subtitle languages"
              placeholder="Search languages"
              empty-text="No languages."
              @update:model-value="(languages) => update({ subtitle_languages: languages })"
            />
          </FieldContent>
        </Field>
      </FieldSet>
    </div>
  </TooltipProvider>
</template>
