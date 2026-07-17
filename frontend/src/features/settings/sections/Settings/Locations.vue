<script setup lang="ts">
import { Input } from "../../../../components/ui/input";
import { useSettingsContext } from "../../context";
import SourceRows from "../SourceRows.vue";

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
