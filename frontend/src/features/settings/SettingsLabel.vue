<script setup lang="ts">
// Uppercase micro-label for a control or a cluster of controls in a settings pane.
import { cn } from "@/lib/utils";

defineProps<{ class?: string }>();
</script>

<template>
  <span
    :class="
      cn(
        'block text-xs font-bold text-white/50 in-[.light-mode]:text-black/50 uppercase tracking-wider',
        $props.class,
      )
    "
  >
    <slot />
  </span>
</template>
