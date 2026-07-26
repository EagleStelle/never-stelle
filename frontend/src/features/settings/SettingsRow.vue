<script setup lang="ts">
// Label + control row shared by every stacked settings pane. Spacing between rows is the
// stacking parent's `gap`, never padding here, so a row never carries space it doesn't own.
defineProps<{ label: string }>();
</script>

<template>
  <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:gap-3">
    <span class="text-sm font-medium sm:shrink-0 sm:grow-0 sm:basis-40">{{
      label
    }}</span>
    <div class="w-full sm:flex-auto">
      <slot />
    </div>
  </div>
</template>
