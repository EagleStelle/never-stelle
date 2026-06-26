<script setup lang="ts">
import { ToggleGroupItem, type ToggleGroupItemProps } from 'reka-ui'

const props = defineProps<ToggleGroupItemProps & { class?: string }>()
</script>

<template>
  <ToggleGroupItem
    v-bind="props"
    :class="[
      'inline-flex items-center justify-center gap-[0.42rem] h-10 rounded-lg text-text-muted font-[800] leading-none whitespace-nowrap transition-all duration-[180ms] ease-out active:scale-[0.98] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent data-[state=on]:bg-accent data-[state=on]:text-bg data-[state=on]:shadow-sm data-[state=off]:hover:bg-panel data-[state=off]:hover:text-text border border-transparent',
      props.class
    ]"
  >
    <slot />
  </ToggleGroupItem>
</template>
