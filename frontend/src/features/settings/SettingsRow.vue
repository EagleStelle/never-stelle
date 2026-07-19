<script setup lang="ts">
// Label + control row shared by every stacked settings pane.
defineProps<{ label: string }>();
</script>

<template>
  <div class="flex flex-col gap-2 py-2 sm:flex-row sm:items-center sm:gap-3">
    <span class="text-sm font-medium sm:shrink-0 sm:grow-0 sm:basis-40">{{
      label
    }}</span>
    <div class="w-full sm:flex-auto">
      <slot />
    </div>
  </div>
</template>
