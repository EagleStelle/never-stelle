<script setup lang="ts">
import { computed } from "vue";

import { ComboboxSelect as Combobox } from "../../../../components/ui/combobox";
import {
  isCodecCompatibleWithContainer,
  isLosslessAudioFormat,
  videoCodecOptionsForContainer,
} from "../../../../utils/dashboard";
import { useSettingsContext } from "../../context";
import SettingsLabel from "../../SettingsLabel.vue";
import SettingsRow from "../../SettingsRow.vue";

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
  <SettingsLabel class="mb-2">Video</SettingsLabel>

  <SettingsRow label="Quality">
    <Combobox
      :model-value="settingsDraft.default_quality.video_quality"
      :items="settings.quality_options.video"
      @update:model-value="(val) => (settingsDraft.default_quality.video_quality = val)"
      layout="fill"
      placeholder="Choose a quality"
      empty-text="No presets."
    />
  </SettingsRow>
  <SettingsRow label="Container">
    <Combobox
      :model-value="settingsDraft.default_quality.video_container"
      :items="settings.quality_options.video_containers"
      @update:model-value="(val) => updateContainer(val)"
      layout="fill"
      placeholder="Choose a container"
      empty-text="No containers."
    />
  </SettingsRow>
  <SettingsRow label="Codec">
    <Combobox
      :model-value="settingsDraft.default_quality.video_codec"
      :items="videoCodecItems"
      @update:model-value="(val) => (settingsDraft.default_quality.video_codec = val)"
      layout="fill"
      placeholder="Choose a codec"
      empty-text="No codecs."
    />
  </SettingsRow>

  <SettingsLabel class="mb-2 mt-6">Audio</SettingsLabel>

  <SettingsRow label="Format">
    <Combobox
      :model-value="settingsDraft.default_quality.audio_format"
      :items="settings.quality_options.audio_formats"
      @update:model-value="(val) => (settingsDraft.default_quality.audio_format = val)"
      layout="fill"
      placeholder="Choose a format"
      empty-text="No formats."
    />
  </SettingsRow>
  <SettingsRow
    v-if="!isLosslessAudioFormat(settingsDraft.default_quality.audio_format)"
    label="Bitrate"
  >
    <Combobox
      :model-value="settingsDraft.default_quality.audio_bitrate"
      :items="settings.quality_options.audio_bitrates"
      @update:model-value="(val) => (settingsDraft.default_quality.audio_bitrate = val)"
      layout="fill"
      placeholder="Choose a bitrate"
      empty-text="No bitrates."
    />
  </SettingsRow>
</template>
