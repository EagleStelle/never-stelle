<script setup lang="ts">
import { FieldGroup, FieldSet } from "../../../components/ui/field";
import { Label } from "../../../components/ui/label";
import type { SourceProfile } from "../../../types";
import { useSettingsContext } from "../context";

// One labelled row per editable source; the control is supplied by the caller.
const { editableSourceProfiles } = useSettingsContext();

defineSlots<{ default(props: { site: SourceProfile }): unknown }>();
</script>

<template>
  <FieldSet>
    <FieldGroup class="gap-4">
      <div
        v-for="site in editableSourceProfiles"
        :key="site.key"
        class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3"
      >
        <Label class="sm:w-40 sm:shrink-0">{{ site.label }}</Label>
        <div class="w-full sm:flex-auto">
          <slot :site="site" />
        </div>
      </div>
    </FieldGroup>
  </FieldSet>
</template>
