<script setup lang="ts">
import { ToggleGroupItem, type ToggleGroupItemProps } from'reka-ui'

const props = defineProps<ToggleGroupItemProps & { class?: string }>()
</script>

<template>
 <ToggleGroupItem
 v-bind="props"
 :class="['segmented-control-item inline-flex items-center justify-center gap-2 px-3 h-full rounded-lg bg-transparent text-white [.light-mode_&]:text-black leading-none whitespace-nowrap transition-all duration-300 ease-glass active:scale-[0.97] hover:text-white [.light-mode_&]:hover:text-black focus-visible:outline-none', props.class ]"
 >
 <slot />
 </ToggleGroupItem>
</template>

<style scoped>
.segmented-control-item[data-state="on"] {
  background: var(--accent);
  color: #1B1931;
}
</style>

