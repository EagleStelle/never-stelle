<script setup lang="ts">
import type { SourceProfile } from "../../../types";
import { useSettingsContext } from "../context";
import SettingsRow from "../SettingsRow.vue";

// One labelled row per editable source; the control is supplied by the caller.
const { editableSourceProfiles } = useSettingsContext();

defineSlots<{ default(props: { site: SourceProfile }): unknown }>();
</script>

<template>
  <div class="flex flex-col">
    <SettingsRow
      v-for="site in editableSourceProfiles"
      :key="site.key"
      :label="site.label"
    >
      <slot :site="site" />
    </SettingsRow>
  </div>
</template>
