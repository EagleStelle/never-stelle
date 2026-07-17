<script setup lang="ts">
import { computed } from "vue";

import { Combobox } from "../../../../components/ui/combobox";
import type { QualitySelection } from "../../../../types";
import { isCodecAllowed, isLosslessAudioFormat, resolveCodec } from "../../../../utils/dashboard";
import { useSettingsContext } from "../../context";
import SettingsRow from "../../SettingsRow.vue";

const { settings, settingsDraft } = useSettingsContext();

// Grey out codecs the chosen container can't hold.
const codecItems = computed(() =>
  settings.quality_options.video_codecs.map((codec) => ({
    ...codec,
    disabled: !isCodecAllowed(
      codec.key,
      settingsDraft.default_quality.video_container,
      settings.quality_options.video_containers,
    ),
  })),
);

// Mutate the draft in place; a container switch may invalidate the codec -> Auto.
function setDefaultQuality(patch: Partial<QualitySelection>): void {
  const quality = settingsDraft.default_quality;
  Object.assign(quality, patch);
  quality.video_codec = resolveCodec(
    quality.video_codec,
    quality.video_container,
    settings.quality_options.video_containers,
  );
}
</script>

<template>
  <h3
    class="mb-2 text-xs font-bold text-white/50 in-[.light-mode]:text-black/50 uppercase tracking-wider"
  >
    Video
  </h3>

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
      @update:model-value="(val) => setDefaultQuality({ video_container: val })"
      layout="fill"
      placeholder="Choose a container"
      empty-text="No containers."
    />
  </SettingsRow>
  <SettingsRow label="Codec">
    <Combobox
      :model-value="settingsDraft.default_quality.video_codec"
      :items="codecItems"
      @update:model-value="(val) => setDefaultQuality({ video_codec: val })"
      layout="fill"
      placeholder="Choose a codec"
      empty-text="No codecs."
    />
  </SettingsRow>

  <h3
    class="mb-2 mt-6 text-xs font-bold text-white/50 in-[.light-mode]:text-black/50 uppercase tracking-wider"
  >
    Audio
  </h3>

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
