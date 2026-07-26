<script setup lang="ts">
import { Input } from "@/components/ui/input";
import { useSettingsContext } from "@/features/settings/context";
import SourceRows from "@/features/settings/sections/SourceRows.vue";

const { settingsDraft } = useSettingsContext();
</script>

<template>
  <SourceRows #default="{ site }">
    <Input
      :id="`${site.key}LocationInput`"
      v-model="settingsDraft.site_locations[site.key]"
      list="downloadLocationSuggestions"
      placeholder="Enter a save path"
    />
  </SourceRows>
</template>
