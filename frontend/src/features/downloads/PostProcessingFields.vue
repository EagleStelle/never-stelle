<script setup lang="ts">
import { computed } from "vue";

import { Checkbox } from "@/components/ui/checkbox";
import { FieldLabel } from "@/components/ui/field";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type { PostProcessingMode, PostProcessingSelection } from "@/types";
import {
  POST_PROCESSING_FIELDS,
  normalizeSubtitleLanguages,
  type PostProcessingCapabilities,
  type PostProcessingCapability,
} from "@/utils/dashboard";

type Destination = "sidecar" | "embed";

const props = defineProps<{
  modelValue: PostProcessingSelection;
  capabilities: PostProcessingCapabilities;
}>();

const emit = defineEmits<{
  "update:modelValue": [value: PostProcessingSelection];
}>();

const subtitlesRequested = computed(
  () =>
    props.modelValue.subtitles !== "off" ||
    props.modelValue.automatic_subtitles !== "off",
);

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
  emit("update:modelValue", { ...props.modelValue, [key]: mode });
}

function setLanguages(value: string): void {
  emit("update:modelValue", {
    ...props.modelValue,
    subtitle_languages: normalizeSubtitleLanguages(value),
  });
}
</script>

<template>
  <div class="grid grid-cols-[1fr_auto_auto] items-center gap-x-6 gap-y-3">
    <span />
    <Label class="justify-self-center">Sidecar</Label>
    <Label class="justify-self-center">Embed</Label>
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
  <FieldLabel v-if="subtitlesRequested" class="w-full flex-col items-start gap-1.5">
    <span class="flex items-center gap-1.5">
      <span>Subtitle languages</span>
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger as-child>
            <button
              type="button"
              class="inline-flex h-4 w-4 items-center justify-center rounded-full border border-(--glass-border) bg-black/20 text-[0.625rem] font-semibold leading-none text-muted-foreground transition-all duration-300 ease-glass hover:border-accent hover:text-white focus-visible:ring-2 focus-visible:ring-accent in-[.light-mode]:bg-white/40 in-[.light-mode]:hover:text-black"
              aria-label="Subtitle languages help"
            >
              i
            </button>
          </TooltipTrigger>
          <TooltipContent side="top">
            Language codes like en (English) or ja (Japanese). Type "all" for every
            language. Leave empty for the original language.
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
    </span>
    <Textarea
      class="min-h-20 w-full resize-y"
      :model-value="modelValue.subtitle_languages.join(', ')"
      @change="(event: Event) => setLanguages((event.target as HTMLTextAreaElement).value)"
    />
  </FieldLabel>
</template>
