<script setup lang="ts">
import IconDownload from "~icons/material-symbols/download";
import IconLink from "~icons/material-symbols/link";
import IconPaste from "~icons/material-symbols/content-paste-rounded";
import IconMovie from "~icons/material-symbols/movie";
import IconMusic from "~icons/material-symbols/music-note";
import { Input } from "../../components/ui/input";
import { Button } from "../../components/ui/button";
import { ComboboxSelect as Combobox } from "../../components/ui/combobox";
import {
  SegmentedControl,
  SegmentedControlItem,
} from "../../components/ui/segmented-control";

import { computed } from "vue";
import type { QualityOptions, QualitySelection, SavedSettings } from "../../types";
import { createQualitySelection, isCodecAllowed, isLosslessAudioFormat } from "../../utils/dashboard";

const props = defineProps<{
  savedSettings: SavedSettings;
  url: string;
  quality: QualitySelection;
  qualityOptions: QualityOptions;
}>();

const emit = defineEmits<{
  addDownload: [];
  "update:url": [url: string];
  "update:quality": [quality: QualitySelection];
}>();

// Grey out codecs the chosen container can't hold.
const codecItems = computed(() =>
  props.qualityOptions.video_codecs.map((codec) => ({
    ...codec,
    disabled: !isCodecAllowed(codec.key, props.quality.video_container, props.qualityOptions.video_containers),
  })),
);

function update(patch: Partial<QualitySelection>): void {
  emit("update:quality", createQualitySelection({ ...props.quality, ...patch }, props.qualityOptions));
}

const pasteFromClipboard = async () => {
  try {
    const text = await navigator.clipboard.readText();
    emit("update:url", text);
  } catch (err) {
    console.error("Failed to read clipboard contents:", err);
  }
};
</script>

<template>
  <section aria-label="Add download">
    <form class="flex flex-col-reverse lg:flex-col gap-2 w-full" @submit.prevent="emit('addDownload')">
      <div class="flex items-center gap-2 w-full">
        <Input
          class="flex-1 min-w-0"
          :model-value="url"
          @update:model-value="(val) => emit('update:url', String(val))"
          name="url"
          type="url"
          autocomplete="off"
          placeholder="Paste a supported URL"
          required
        >
          <template #icon>
            <IconLink class="w-5 h-5 text-white in-[.light-mode]:text-black" aria-hidden="true" />
          </template>
          <template #action>
            <button
              type="button"
              class="flex items-center justify-center shrink-0 w-8 h-8 rounded-lg mr-0.5 text-white in-[.light-mode]:text-black hover:bg-white/10 in-[.light-mode]:hover:bg-black/5 transition-all duration-300 ease-glass active:scale-[0.96]"
              aria-label="Paste from clipboard"
              @click="pasteFromClipboard"
            >
              <IconPaste class="w-5 h-5" aria-hidden="true" />
            </button>
          </template>
        </Input>

        <Button
          variant="primary"
          type="submit"
          class="shrink-0"
          aria-label="Download"
        >
          <template #icon>
            <IconDownload
              aria-hidden="true"
              class="w-6 h-6 transition-transform duration-300 ease-glass group-hover:-translate-y-px group-hover:scale-110"
            />
          </template>
        </Button>
      </div>

      <div class="flex overflow-x-auto no-scrollbar items-center justify-between lg:justify-start gap-2 w-full py-1">
        <div class="flex items-center gap-2 shrink-0">
          <SegmentedControl
            v-if="qualityOptions.video.length"
            :model-value="quality.mode"
            @update:model-value="(val) => { if (val) update({ mode: val as 'video' | 'audio' }); }"
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

          <template v-if="quality.mode === 'video'">
            <Combobox
              v-if="qualityOptions.video.length"
              :model-value="quality.video_quality"
              :items="qualityOptions.video"
              @update:model-value="(val) => update({ video_quality: val })"
              class="shrink-0"
              placeholder="Quality..."
              empty-text="No presets."
              aria-label="Video quality"
            />
            <Combobox
              v-if="qualityOptions.video_containers.length"
              :model-value="quality.video_container"
              :items="qualityOptions.video_containers"
              @update:model-value="(val) => update({ video_container: val })"
              class="shrink-0"
              placeholder="Container..."
              empty-text="No containers."
              aria-label="Video container"
            />
            <Combobox
              v-if="codecItems.length"
              :model-value="quality.video_codec"
              :items="codecItems"
              @update:model-value="(val) => update({ video_codec: val })"
              class="shrink-0"
              placeholder="Codec..."
              empty-text="No codecs."
              aria-label="Video codec"
            />
          </template>

          <template v-else-if="quality.mode === 'audio'">
            <Combobox
              v-if="qualityOptions.audio_formats.length"
              :model-value="quality.audio_format"
              :items="qualityOptions.audio_formats"
              @update:model-value="(val) => update({ audio_format: val })"
              class="shrink-0"
              placeholder="Format..."
              empty-text="No formats."
              aria-label="Audio format"
            />
            <Combobox
              v-if="qualityOptions.audio_bitrates.length && !isLosslessAudioFormat(quality.audio_format)"
              :model-value="quality.audio_bitrate"
              :items="qualityOptions.audio_bitrates"
              @update:model-value="(val) => update({ audio_bitrate: val })"
              class="shrink-0"
              placeholder="Bitrate..."
              empty-text="No bitrates."
              aria-label="Audio bitrate"
            />
          </template>
        </div>

        <div class="shrink-0 lg:ml-auto">
          <slot name="filters" />
        </div>
      </div>
    </form>
  </section>
</template>
