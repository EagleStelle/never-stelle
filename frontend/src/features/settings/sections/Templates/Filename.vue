<script setup lang="ts">
import { Input } from "../../../../components/ui/input";
import { useSettingsContext } from "../../context";
import SourceRows from "../SourceRows.vue";

const { settingsDraft } = useSettingsContext();
</script>

<template>
  <SourceRows #default="{ site }">
    <Input
      :id="`${site.key}FilenameTemplateInput`"
      v-model="settingsDraft.source_templates[site.key].filename_template"
      input-class="font-mono"
    />
  </SourceRows>
</template>
