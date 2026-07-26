<script setup lang="ts">
import { computed } from "vue";

import { ComboboxSelect as Combobox } from "../../../../components/ui/combobox";
import {
  FieldGroup,
  FieldLegend,
  FieldSet,
} from "../../../../components/ui/field";
import { Label } from "../../../../components/ui/label";
import {
  isCodecCompatibleWithContainer,
  isLosslessAudioFormat,
  videoCodecOptionsForContainer,
} from "../../../../utils/dashboard";
import { useSettingsContext } from "../../context";

const { settings, settingsDraft } = useSettingsContext();

// Only codecs the chosen container can play back (Auto always fits); prevents VP9-in-MP4.
const videoCodecItems = computed(() =>
  videoCodecOptionsForContainer(
    settings.quality_options,
    settingsDraft.default_quality.video_container,
  ),
);

function updateContainer(container: string): void {
  settingsDraft.default_quality.video_container = container;
  // A codec the new container can't play back would force an unplayable stream; reset to Auto.
  if (
    !isCodecCompatibleWithContainer(
      settingsDraft.default_quality.video_codec,
      container,
      settings.quality_options.video_containers,
    )
  ) {
    settingsDraft.default_quality.video_codec = "auto";
  }
}
</script>

<template>
  <div class="flex flex-col gap-6">
    <FieldSet>
      <FieldLegend class="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-2">
        Video
      </FieldLegend>
      <FieldGroup class="gap-4">
        <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
          <Label class="sm:w-40 sm:shrink-0">Quality</Label>
          <div class="w-full sm:flex-auto">
            <Combobox
              :model-value="settingsDraft.default_quality.video_quality"
              :items="settings.quality_options.video"
              @update:model-value="
                (val) => (settingsDraft.default_quality.video_quality = val)
              "
              layout="fill"
              placeholder="Choose a quality"
              empty-text="No presets."
            />
          </div>
        </div>
        <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
          <Label class="sm:w-40 sm:shrink-0">Container</Label>
          <div class="w-full sm:flex-auto">
            <Combobox
              :model-value="settingsDraft.default_quality.video_container"
              :items="settings.quality_options.video_containers"
              @update:model-value="(val) => updateContainer(val)"
              layout="fill"
              placeholder="Choose a container"
              empty-text="No containers."
            />
          </div>
        </div>
        <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
          <Label class="sm:w-40 sm:shrink-0">Codec</Label>
          <div class="w-full sm:flex-auto">
            <Combobox
              :model-value="settingsDraft.default_quality.video_codec"
              :items="videoCodecItems"
              @update:model-value="
                (val) => (settingsDraft.default_quality.video_codec = val)
              "
              layout="fill"
              placeholder="Choose a codec"
              empty-text="No codecs."
            />
          </div>
        </div>
      </FieldGroup>
    </FieldSet>

    <FieldSet>
      <FieldLegend class="text-xs font-bold uppercase tracking-wider text-muted-foreground mb-2">
        Audio
      </FieldLegend>
      <FieldGroup class="gap-4">
        <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
          <Label class="sm:w-40 sm:shrink-0">Format</Label>
          <div class="w-full sm:flex-auto">
            <Combobox
              :model-value="settingsDraft.default_quality.audio_format"
              :items="settings.quality_options.audio_formats"
              @update:model-value="
                (val) => (settingsDraft.default_quality.audio_format = val)
              "
              layout="fill"
              placeholder="Choose a format"
              empty-text="No formats."
            />
          </div>
        </div>
        <div
          v-if="
            !isLosslessAudioFormat(settingsDraft.default_quality.audio_format)
          "
          class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3"
        >
          <Label class="sm:w-40 sm:shrink-0">Bitrate</Label>
          <div class="w-full sm:flex-auto">
            <Combobox
              :model-value="settingsDraft.default_quality.audio_bitrate"
              :items="settings.quality_options.audio_bitrates"
              @update:model-value="
                (val) => (settingsDraft.default_quality.audio_bitrate = val)
              "
              layout="fill"
              placeholder="Choose a bitrate"
              empty-text="No bitrates."
            />
          </div>
        </div>
      </FieldGroup>
    </FieldSet>
  </div>
</template>
