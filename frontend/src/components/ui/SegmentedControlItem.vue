<script setup lang="ts">
import { ToggleGroupItem, type ToggleGroupItemProps } from 'reka-ui'

const props = defineProps<ToggleGroupItemProps & { class?: string }>()
</script>

<template>
  <ToggleGroupItem
    v-bind="props"
    :class="[ 'segmented-control-item inline-flex items-center justify-center gap-2 px-3 h-full rounded-md text-text-muted leading-none whitespace-nowrap transition-all duration-200 ease-out active:scale-95 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent border border-transparent', props.class ]"
  >
    <slot />
  </ToggleGroupItem>
</template>

<style scoped>
.segmented-control-item[data-state="on"] {
  background: var(--primary);
  color: var(--background);
  box-shadow: 0 1px 2px rgb(0 0 0 / 0.05);
}

.segmented-control-item[data-state="off"]:hover {
  background: var(--panel);
  color: var(--text);
}
</style>
