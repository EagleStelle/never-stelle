<script setup lang="ts">
import SettingsLabel from "./SettingsLabel.vue";

// A cluster of controls inside a pane, optionally titled. This component owns the space
// between its children, so rows and controls stay spacing-free and reusable.
// `dense` is the tighter rhythm for lists of like items (checkboxes), not for rows.
withDefaults(defineProps<{ label?: string; dense?: boolean }>(), {
  label: "",
  dense: false,
});

defineSlots<{ default(): unknown; header(): unknown }>();
</script>

<template>
  <div class="flex flex-col gap-2">
    <div
      v-if="label || $slots.header"
      class="flex items-center justify-between gap-2"
    >
      <SettingsLabel v-if="label">{{ label }}</SettingsLabel>
      <span v-else />
      <slot name="header" />
    </div>
    <div class="flex flex-col" :class="dense ? 'gap-2' : 'gap-4'">
      <slot />
    </div>
  </div>
</template>
