<script setup lang="ts">
import { computed, ref, useId } from "vue";

import { Checkbox } from "@/components/ui/checkbox";
import { Combobox } from "@/components/ui/combobox";
import {
  Field,
  FieldContent,
  FieldLabel,
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
import type { PostProcessingMode, PostProcessingSelection } from "@/types";
import {
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
  modelValue: PostProcessingSelection;
  capabilities: PostProcessingCapabilities;
}>();

const emit = defineEmits<{
  "update:modelValue": [value: PostProcessingSelection];
}>();

const titleId = useId();

const subtitlesRequested = computed(
  () =>
    props.modelValue.subtitles !== "off" ||
    props.modelValue.automatic_subtitles !== "off",
);

// Custom stays selected until its first language is picked.
const customSelected = ref(false);

const languageMode = computed<SubtitleLanguageMode>(() => {
  const languages = props.modelValue.subtitle_languages;
  return customSelected.value && !languages.length ? "custom" : subtitleLanguageMode(languages);
});

function update(patch: Partial<PostProcessingSelection>): void {
  emit("update:modelValue", { ...props.modelValue, ...patch });
}

function isChecked(key: PostProcessingCapability, destination: Destination): boolean {
  const mode = props.modelValue[key];
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

// A segmented control clears its value when the active item is clicked again.
function setLanguageMode(value: string | string[]): void {
  const mode = LANGUAGE_MODES.find((item) => item.value === value)?.value;
  if (!mode) return;
  customSelected.value = mode === "custom";
  update({ subtitle_languages: subtitleLanguagesForMode(mode) });
}
</script>

<template>
  <TooltipProvider>
    <FieldSet :aria-labelledby="titleId">
      <div class="grid grid-cols-[1fr_auto_auto] items-center gap-x-6 gap-y-3">
        <span :id="titleId" class="text-base font-medium">Post-Processing</span>
        <span class="justify-self-center text-base font-medium">Sidecar</span>
        <span class="justify-self-center text-base font-medium">Embed</span>
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
            :checked="modelValue[option.key]"
            @update:checked="(value: boolean) => update({ [option.key]: value })"
          />
          <span>{{ option.label }}</span>
          <Tooltip>
            <TooltipTrigger as-child>
              <button
                type="button"
                class="inline-flex h-4 w-4 items-center justify-center rounded-full border border-(--glass-border) bg-black/20 text-[0.625rem] font-semibold leading-none text-muted-foreground transition-all duration-300 ease-glass hover:border-accent hover:text-white focus-visible:ring-2 focus-visible:ring-accent in-[.light-mode]:bg-white/40 in-[.light-mode]:hover:text-black"
                :aria-label="`${option.label} help`"
              >
                i
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
            :model-value="modelValue.subtitle_languages"
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
  </TooltipProvider>
</template>
