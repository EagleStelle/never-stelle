<script setup lang="ts">
import { ToggleGroupRoot, type ToggleGroupRootProps } from 'reka-ui'

const props = withDefaults(defineProps<ToggleGroupRootProps & { class?: string }>(), {
  type: 'single'
})

const emit = defineEmits<{
  'update:modelValue': [value: any]
}>()
</script>

<template>
  <ToggleGroupRoot
    v-bind="props"
    @update:modelValue="emit('update:modelValue', $event)"
    :class="['inline-flex items-center min-w-0 p-[0.25rem] gap-[0.25rem] bg-panel-subtle rounded-xl border border-accent-subtle', props.class]"
  >
    <slot />
  </ToggleGroupRoot>
</template>
