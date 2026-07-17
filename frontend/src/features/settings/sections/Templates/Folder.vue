<script setup lang="ts">
import SourceRows from "../SourceRows.vue";
import TemplateField from "../TemplateField.vue";
</script>

<template>
  <SourceRows #default="{ site }">
    <TemplateField :site="site" field="folder_template" id-suffix="FolderTemplateInput" />
  </SourceRows>
</template>
