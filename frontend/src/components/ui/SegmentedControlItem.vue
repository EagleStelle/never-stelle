<script setup lang="ts">
import { ToggleGroupItem, type ToggleGroupItemProps } from'reka-ui'

const props = defineProps<ToggleGroupItemProps & { class?: string }>()
</script>

<template>
 <ToggleGroupItem
 v-bind="props"
 :class="['segmented-control-item inline-flex items-center justify-center gap-2 px-3 h-full rounded-lg text-text-muted leading-none whitespace-nowrap transition-all duration-300 ease-glass active:scale-[0.97] focus-visible:outline-none', props.class ]"
 >
 <slot />
 </ToggleGroupItem>
</template>

<style scoped>
.segmented-control-item[data-state="on"] {
 background: var(--primary);
 color: var(--text-on-primary);
 box-shadow: 0 8px 20px -8px rgba(237, 158, 89, 0.6);
}

.segmented-control-item[data-state="off"]:hover {
 background: var(--glass-hover);
 color: var(--text);
}
</style>
